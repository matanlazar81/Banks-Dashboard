#!/usr/bin/env node
/**
 * Server-side cashflow-forecast recompute (no browser)
 * ───────────────────────────────────────────────────────────────────────────
 * Reproduces, in Node, exactly what the dashboard's `fetchData` does: it gathers
 * every input the forecast engine needs (NetSuite + Snowflake, via the same
 * client methods the dev/prod API routes call), loads the "Exit plan June26"
 * scenario knobs, forces Revenue: Pipeline + Salary: Last-Actual (the persist
 * basis), then runs the SHARED engine (src/forecast/forecast-core.mjs — the same
 * module the browser runs) and writes the December after-savings closing to
 *   data/net-cash-forecast.json
 * so scripts/net-cash-snapshot.cjs can push it to Snowflake at 23:00.
 *
 * This replaces the headless-browser refresh: the number is recomputed from
 * fresh data with zero human access, and — because it runs the identical engine
 * with the identical inputs the browser builds — it cannot diverge from the
 * on-screen forecast (assuming the same data + scenario).
 *
 * Scope: LSports (subsidiary 3), the current calendar year. Several NetSuite
 * methods (fetchBankBalance / fetchMonthlyRevaluation / fetchCurrencyDefenseBudget /
 * fetchSalaryData …) derive their own current-year ranges, and the Snowflake SQL
 * hardcodes SUBSIDIARY_ID=3 — matching the dashboard for the live current-year run.
 *
 * Scenario source (knobs like dept salary cuts, vendor cuts, currency-defense %):
 *   1. NET_CASH_SCENARIO_FILE   — a JSON file holding one ScenarioData (or {data}/record).
 *   2. NET_CASH_SCENARIOS_PATH  — a scenarios.json array; matched by name.
 *   3. data/scenarios.json      — dev default; matched by name.
 *   Name from NET_CASH_SCENARIO_NAME (default "Exit plan June26"). If none is
 *   found the job runs the BASE plan (no adjustments) and says so loudly — the
 *   number will be too high because the savings are missing.
 *
 * Flags
 * ─────
 *   --dry-run              compute + print the 12 rows and the Dec closing; do NOT write the file.
 *   --dump-inputs[=path]   write the gathered inputs to JSON (default data/net-cash-inputs.json)
 *                          so it can be diffed against the browser's ?fccapture=1 window.__fcInputs.
 *   --year=YYYY            override the year (default = current). NOTE: current-year-locked NS
 *                          methods mean a non-current year will NOT be accurate — a guard warns.
 *   --scenario=NAME        override the scenario name to match.
 *
 * Exit codes: 0 = computed (and written unless --dry-run); 1 = fatal error.
 */

const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');

try { require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') }); } catch { /* env may already be exported */ }

const { createNetSuiteClient } = require('../netsuite-api.cjs');
const { createSnowflakeClient } = require('../snowflake-api.cjs');

const REPO_ROOT = path.resolve(__dirname, '..');
const SUBSIDIARY = parseInt(process.env.NET_CASH_SUBSIDIARY || '3', 10) || 3; // 3 = LSports
const ILS_REVAL_RATE = 3.59; // matches App.tsx cashflowForecast (ilsRevalRate)

// ── args ─────────────────────────────────────────────────────────────────
function parseArgs(argv) {
  const a = { dryRun: false, dumpInputs: null, year: null, scenario: null };
  for (const arg of argv.slice(2)) {
    if (arg === '--dry-run') a.dryRun = true;
    else if (arg === '--dump-inputs') a.dumpInputs = path.resolve(REPO_ROOT, 'data', 'net-cash-inputs.json');
    else if (arg.startsWith('--dump-inputs=')) a.dumpInputs = path.resolve(arg.slice('--dump-inputs='.length));
    else if (arg.startsWith('--year=')) a.year = parseInt(arg.slice('--year='.length), 10) || null;
    else if (arg.startsWith('--scenario=')) a.scenario = arg.slice('--scenario='.length);
  }
  return a;
}

// ── helpers ────────────────────────────────────────────────────────────────
const pad2 = (n) => String(n).padStart(2, '0');
const mkeys = (year) => Array.from({ length: 12 }, (_, i) => `${year}-${pad2(i + 1)}`);
const sumField = (rows, field) => (rows || []).reduce((s, r) => s + (Number(r[field]) || 0), 0);
const fmtEur = (v) => `€${Math.round(Number(v) || 0).toLocaleString('en-US')}`;

// Run a fetch, log a one-line summary, and never throw — a failed feed defaults
// (the engine treats missing inputs as empty, exactly like the dashboard's safe()).
async function tryFetch(label, fn, fallback) {
  try {
    const v = await fn();
    const n = Array.isArray(v) ? v.length
      : (v && typeof v === 'object' ? Object.keys(v.byMonth || v).length : v);
    console.log(`[compute]   ✓ ${label} (${Array.isArray(v) ? n + ' rows' : (typeof v === 'object' ? n + ' keys' : n)})`);
    return v;
  } catch (e) {
    console.warn(`[compute]   ✗ ${label} FAILED: ${e && e.message ? e.message : e} — using default`);
    return typeof fallback === 'function' ? fallback() : fallback;
  }
}

// ── scenario loading ─────────────────────────────────────────────────────
function loadScenarioData(scenarioName) {
  // 1. explicit single-scenario file
  const single = process.env.NET_CASH_SCENARIO_FILE;
  if (single) {
    try {
      const raw = JSON.parse(fs.readFileSync(path.resolve(single), 'utf-8'));
      const data = raw.data || raw; // {data} record, or a bare ScenarioData
      return { data, source: `NET_CASH_SCENARIO_FILE (${single})` };
    } catch (e) {
      console.warn(`[compute] NET_CASH_SCENARIO_FILE unreadable: ${e.message}`);
    }
  }
  // 2/3. scenarios array (env path or dev default), matched by name
  const arrPath = process.env.NET_CASH_SCENARIOS_PATH
    ? path.resolve(process.env.NET_CASH_SCENARIOS_PATH)
    : path.resolve(REPO_ROOT, 'data', 'scenarios.json');
  try {
    const arr = JSON.parse(fs.readFileSync(arrPath, 'utf-8'));
    const list = Array.isArray(arr) ? arr : (Array.isArray(arr.data) ? arr.data : []);
    const rec = list.find((s) => s && s.name === scenarioName)
      || list.find((s) => s && s.name === scenarioName && (s.company || 'lsports') === 'lsports');
    if (rec && rec.data) return { data: rec.data, source: `${arrPath} (name="${scenarioName}")` };
    console.warn(`[compute] scenario "${scenarioName}" not found in ${arrPath} (${list.length} scenarios).`);
  } catch (e) {
    console.warn(`[compute] scenarios file unreadable (${arrPath}): ${e.message}`);
  }
  return { data: {}, source: 'NONE — base plan, no adjustments (forecast will be too high)' };
}

// Mirror applyScenarioData (App.tsx:2047-2114): maps reset to {} when omitted; the
// toggles + currencyDefensePct + fxRateByYear fall back to the UI defaults. We then
// FORCE pipeline + lastActual (the persist basis the dashboard writes from).
function scenarioKnobs(sd) {
  return {
    salaryAdjPctByMonth: sd.salaryAdjPctByMonth || {},
    collPctByMonth: sd.collPctByMonth || {},
    salaryDeptAdj: sd.salaryDeptAdj || {},
    vendorCatAdj: sd.vendorCatAdj || {},
    vendorDetailAdj: sd.vendorDetailAdj || {},
    pipelineMinProb: sd.pipelineMinProb ?? 100,
    currencyDefensePct: sd.currencyDefensePct !== undefined ? sd.currencyDefensePct : 30,
    currencyDefensePctByMonth: sd.currencyDefensePctByMonth || {},
    salaryManualILS: sd.salaryManualILS || {},
    pipelineAdjPctByMonth: sd.pipelineAdjPctByMonth || {},
    churnOverride: sd.churnOverride || {},
    fxRateByYear: sd.fxRateByYear || {},
    // Forced basis (persist gate): the nightly figure is always Pipeline + Last-Actual.
    revenueMethodology: 'pipeline',
    salaryProjectionMode: 'lastActual',
  };
}

// ── gather every data input server-side (mirrors App.tsx fetchData) ──────────
async function gatherInputs(ns, sf, year) {
  console.log(`[compute] Gathering inputs for LSports (sub ${SUBSIDIARY}), year ${year}…`);

  // Salary actuals by dept — also the SOLE source of lastActualSalaryMonth.
  const salActDept = await tryFetch('sf.fetchSalaryActualsByDept', () => sf.fetchSalaryActualsByDept(year), { byMonth: {}, lastActualMonth: '' });
  const salaryActualsByDept = salActDept.byMonth || {};
  const lastActualSalaryMonth = salActDept.lastActualMonth || '';

  // Budget overrides (used by BOTH the salary-budget and category-budget merges).
  const overrides = await tryFetch('sf.fetchBudgetOverrides', () => sf.fetchBudgetOverrides(), []);
  let payrollAccounts = new Set();
  try {
    const rows = await sf.query(`SELECT DISTINCT GL_ACCOUNT_NUMBER FROM DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT WHERE IS_PAYROLL = TRUE`);
    payrollAccounts = new Set((rows || []).map((r) => r.GL_ACCOUNT_NUMBER));
  } catch (e) { console.warn(`[compute]   ! payroll-accounts query failed: ${e.message}`); }

  // Fire the remaining independent feeds concurrently (retry on 429 is built into netsuite-api).
  const [
    salaryData, vendorBills, vendorActuals, vendorHistory, expenseCategories, paidVendorsRaw,
    monthlyReval, sfFinanceBudget, bankBal, ysAccts, pmAccts, bankClassifiedRaw, collections,
    revenueActuals, customerReceipts,
    sfActualsSplit, salBudgetBase, sfBudgetBase, sfRevenue, sfRevenuePaid, sfPipeline,
    sfConversion, pipelineMethodology, churnAnalysis, sfChurnQuarterly,
  ] = await Promise.all([
    // NetSuite
    tryFetch('ns.fetchSalaryData', () => ns.fetchSalaryData(), []),
    tryFetch('ns.fetchVendorBills', () => ns.fetchVendorBills(), []),
    tryFetch('ns.fetchVendorActuals', () => ns.fetchVendorActuals(), []),
    tryFetch('ns.fetchVendorPaymentHistory', () => ns.fetchVendorPaymentHistory(), []),
    tryFetch('ns.fetchPaymentsByCategory', () => ns.fetchPaymentsByCategory(), { byMonth: {}, categories: [] }),
    tryFetch('ns.fetchPaidVendorsYearly', () => ns.fetchPaidVendorsYearly(year), { accounts: [], months: [], grid: {} }),
    tryFetch('ns.fetchMonthlyRevaluation', () => ns.fetchMonthlyRevaluation(), { byMonth: {}, preYear: { eur: 0, ils: 0 } }),
    tryFetch('ns.fetchCurrencyDefenseBudget', () => ns.fetchCurrencyDefenseBudget(), {}),
    tryFetch('ns.fetchBankBalance', () => ns.fetchBankBalance(), { primary: null, local: null }),
    tryFetch('ns.fetchBankAccountListAsOf(yearStart)', () => ns.fetchBankAccountListAsOf(`${year - 1}-12-31`), []),
    tryFetch('ns.fetchBankAccountListAsOf(prevMonthEnd)', () => ns.fetchBankAccountListAsOf(prevMonthEndDate()), []),
    tryFetch('ns.fetchBankClassifiedYearly', () => ns.fetchBankClassifiedYearly(year), { byMonth: {} }),
    tryFetch('collections (income-credits SQL)', () => fetchCollections(ns, year), {}),
    tryFetch('ns.fetchRevenueActuals', () => ns.fetchRevenueActuals(), []),
    tryFetch('ns.fetchCustomerCashReceipts', () => ns.fetchCustomerCashReceipts(), {}),
    // Snowflake
    tryFetch('sf.fetchMonthlyActualsSplit', () => sf.fetchMonthlyActualsSplit(), {}),
    tryFetch('sf.fetchSalaryBudget', () => sf.fetchSalaryBudget(year), {}),
    tryFetch('sf.fetchBudgetByCategory', () => sf.fetchBudgetByCategory(year), { byMonth: {}, totalByMonth: {}, financeBudget: {} }),
    tryFetch('sf.fetchRevenueProjection', () => sf.fetchRevenueProjection(year), { budget: {}, actuals: {}, targets: {} }),
    tryFetch('sf.fetchMonthlyRevenuePaid', () => sf.fetchMonthlyRevenuePaid(year), {}),
    tryFetch('sf.fetchOpenPipeline', () => sf.fetchOpenPipeline(year), []),
    tryFetch('sf.fetchConversionAnalysis', () => sf.fetchConversionAnalysis(), { yearly: [] }),
    tryFetch('sf.fetchPipelineMethodology', () => sf.fetchPipelineMethodology(year), { byMonth: {} }),
    tryFetch('sf.fetchChurnAnalysis', () => sf.fetchChurnAnalysis(), { yearly: [], recentMonthlyAvg: 0 }),
    tryFetch('sf.fetchQuarterlyChurnMRR', () => sf.fetchQuarterlyChurnMRR(), []),
  ]);

  // Dependent: cumulative headcount impact needs lastActualSalaryMonth.
  const monthlyHCImpact = await tryFetch('sf.fetchMonthlyHCImpact', () => sf.fetchMonthlyHCImpact(year, lastActualSalaryMonth), {});

  // ── Apply the two budget-override merges exactly as the vite/prod handlers do ──
  // Salary budget: payroll-account overrides only (vite.config.ts:1016-1029).
  const sfSalaryBudget = salBudgetBase || {};
  const sfSalaryOverrides = [];
  for (const ov of overrides || []) {
    if (!payrollAccounts.has(ov.account)) continue;
    const mKey = ov.month;
    if (!mKey || mKey < `${year}-01`) continue;
    if (!sfSalaryBudget[mKey]) sfSalaryBudget[mKey] = { eur: 0, ils: 0 };
    const oldVal = sfSalaryBudget[mKey].eur;
    if (ov.mode === 'Override') sfSalaryBudget[mKey].eur = ov.amountEUR;
    else sfSalaryBudget[mKey].eur += ov.amountEUR;
    sfSalaryOverrides.push({ ...ov, mKey, oldVal, newVal: sfSalaryBudget[mKey].eur });
  }
  // Category budget: non-payroll overrides only (vite.config.ts:414-433).
  const sfBudget = sfBudgetBase || { byMonth: {}, totalByMonth: {} };
  if (!sfBudget.byMonth) sfBudget.byMonth = {};
  if (!sfBudget.totalByMonth) sfBudget.totalByMonth = {};
  for (const ov of overrides || []) {
    const mKey = ov.month;
    const category = ov.category || `Acct ${(ov.account || '').substring(0, 3)}`;
    if (!mKey || mKey < `${year}-01`) continue;
    if (category === 'Payroll') continue;
    if (!sfBudget.byMonth[mKey]) sfBudget.byMonth[mKey] = {};
    if (!sfBudget.totalByMonth[mKey]) sfBudget.totalByMonth[mKey] = { eur: 0, ils: 0 };
    const oldVal = sfBudget.byMonth[mKey][category] || 0;
    if (ov.mode === 'Override') { sfBudget.byMonth[mKey][category] = ov.amountEUR; sfBudget.totalByMonth[mKey].eur += (ov.amountEUR - oldVal); }
    else { sfBudget.byMonth[mKey][category] = oldVal + ov.amountEUR; sfBudget.totalByMonth[mKey].eur += ov.amountEUR; }
  }

  // nsPaidVendors: sum the grid into byMonth (App.tsx:2790-2795); seed fallback for LSports/2026.
  let nsPaidVendors = { byMonth: {}, grid: paidVendorsRaw.grid || {}, accounts: paidVendorsRaw.accounts || [] };
  for (const m of paidVendorsRaw.months || []) {
    nsPaidVendors.byMonth[m] = (paidVendorsRaw.accounts || []).reduce((s, a) => s + ((paidVendorsRaw.grid?.[a.number]?.[m]) || 0), 0);
  }
  if (Object.keys(nsPaidVendors.byMonth).length === 0) {
    try {
      const seed = require('../src/seeds/paid-vendors-lsports-2026.json');
      if (seed.byMonth) nsPaidVendors = { byMonth: seed.byMonth, grid: seed.grid || {}, accounts: seed.accounts || [] };
      else if (seed.grid && seed.accounts && seed.months) {
        const bm = {}; for (const m of seed.months) bm[m] = seed.accounts.reduce((s, a) => s + ((seed.grid?.[a.number]?.[m]) || 0), 0);
        nsPaidVendors = { byMonth: bm, grid: seed.grid, accounts: seed.accounts };
      }
      console.log('[compute]   → nsPaidVendors: live empty, used bundled 2026 seed');
    } catch { /* no seed */ }
  }

  // nsBankClassified: live yearly, else the committed LSports 2026 seed (App.tsx:2777-2783).
  let nsBankClassified = { byMonth: {} };
  if (bankClassifiedRaw && bankClassifiedRaw.byMonth && Object.keys(bankClassifiedRaw.byMonth).length) {
    nsBankClassified = { byMonth: bankClassifiedRaw.byMonth };
  } else {
    try { const seed = require('../src/seeds/bank-classified-lsports-2026.json'); nsBankClassified = { byMonth: seed.byMonth || {} }; console.log('[compute]   → nsBankClassified: live empty, used bundled 2026 seed'); }
    catch { /* no seed */ }
  }

  // salaryDeptBudgets: only needed as the non-lastActual fallback; fetch the breakdown
  // for months carrying a dept adjustment (mirrors App.tsx's lazy per-month fetch).
  const salaryDeptBudgets = {};

  // book / bookLocal (App.tsx:3095-3108 — no asOfDate ⇒ raw primary/local).
  const book = bankBal.primary || null;
  const bookLocal = bankBal.local || null;
  const yearStartBalance = ysAccts.length ? { eur: sumField(ysAccts, 'primaryBalance'), ils: sumField(ysAccts, 'localBalance') } : null;
  const prevMonthEndBalance = pmAccts.length ? { eur: sumField(pmAccts, 'primaryBalance'), ils: sumField(pmAccts, 'localBalance') } : null;

  // liveFxRate (App.tsx:3115-3120): ILS/EUR from adjusted balances, clamped; else 3.68.
  const adjEur = (book && (book.adjustedCurrentBalance || book.currentBalance)) || 0;
  const adjIls = (bookLocal && (bookLocal.adjustedCurrentBalance || bookLocal.currentBalance)) || 0;
  let liveFxRate = 3.68;
  if (adjEur > 0 && adjIls > 0) { const r = adjIls / adjEur; if (r > 0.5 && r < 10) liveFxRate = r; }

  return {
    inputs: {
      // data feeds
      salaryData, salaryActualsByDept, salaryDeptBudgets, sfSalaryBudget, sfSalaryOverrides,
      monthlyHCImpact, sfActualsSplit,
      vendorBills, vendorActuals, nsPaidVendors, vendorHistory, sfBudget,
      nsBudget: { byMonth: {} }, // LSports uses SF budget; NS budget stays empty (hasSF branch)
      expenseCategories,
      sfRevenuePaid, actualCollections: collections, sfRevenue, revenueActuals, customerReceipts,
      sfPipeline, sfConversion, pipelineMethodology,
      sfChurnQuarterly, churnData: churnAnalysis.yearly || [], churnMonthlyAvg: churnAnalysis.recentMonthlyAvg || 0,
      monthlyReval, nsBankClassified, sfFinanceBudget,
      book, bookLocal, yearStartBalance, prevMonthEndBalance, liveFxRate,
    },
    meta: { lastActualSalaryMonth, overridesApplied: sfSalaryOverrides.length, adjEur, adjIls },
  };
}

// Last calendar day of the month BEFORE today, 'YYYY-MM-DD'.
function prevMonthEndDate() {
  const now = new Date();
  const d = new Date(now.getFullYear(), now.getMonth(), 0); // day 0 of current month = last day of prev
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

// Collections actuals (income credits, cash-basis) — replicates vite.config.ts:226-267.
async function fetchCollections(ns, year) {
  const byMonth = {};
  try {
    const rows = await ns.suiteqlAll(`
      SELECT TO_CHAR(
               CASE WHEN t.type = 'CustInvc' AND t.status IN ('B','X') THEN t.closedate
                    ELSE t.trandate END,
               'YYYY-MM'
             ) AS mkey,
             SUM(COALESCE(tal.credit, 0)) - SUM(COALESCE(tal.debit, 0)) AS net_revenue
      FROM transactionaccountingline tal
      JOIN transaction t ON tal.transaction = t.id
      JOIN account a ON tal.account = a.id
      WHERE t.subsidiary = ${SUBSIDIARY}
        AND a.accttype = 'Income'
        AND tal.posting = 'T'
        AND tal.accountingbook = 1
        AND (t.type <> 'CustInvc' OR t.status IN ('B','X'))
        AND CASE WHEN t.type = 'CustInvc' AND t.status IN ('B','X') THEN t.closedate
                 ELSE t.trandate END >= TO_DATE('${year}-01-01', 'YYYY-MM-DD')
      GROUP BY TO_CHAR(
               CASE WHEN t.type = 'CustInvc' AND t.status IN ('B','X') THEN t.closedate
                    ELSE t.trandate END,
               'YYYY-MM')
      ORDER BY mkey
    `);
    for (const r of rows) {
      if (r.mkey && parseFloat(r.net_revenue) > 0) byMonth[r.mkey] = Math.round(parseFloat(r.net_revenue));
    }
    return byMonth;
  } catch (e) {
    // Fallback: CustInvc-by-closedate (vite.config.ts:257-266).
    const data = await ns.fetchCollectionData();
    for (const r of data || []) {
      if (r.dateClosed) {
        const p = r.dateClosed.split('/');
        if (p.length === 3) { const m = `${p[2]}-${p[1].padStart(2, '0')}`; byMonth[m] = (byMonth[m] || 0) + (r.amountEUR || 0); }
      }
    }
    return byMonth;
  }
}

// ── main ─────────────────────────────────────────────────────────────────
async function main() {
  const args = parseArgs(process.argv);
  const currentYear = new Date().getFullYear();
  const year = args.year || currentYear;
  const scenarioName = args.scenario || process.env.NET_CASH_SCENARIO_NAME || 'Exit plan June26';

  if (year !== currentYear) {
    console.warn(`[compute] ⚠ year ${year} != current ${currentYear}. Several NetSuite methods are current-year-locked; the number will NOT be accurate for a non-current year.`);
  }

  const { data: scenarioData, source: scenarioSource } = loadScenarioData(scenarioName);
  console.log(`[compute] Scenario: ${scenarioSource}`);

  const ns = createNetSuiteClient(process.env, SUBSIDIARY);
  const sf = createSnowflakeClient(process.env);

  const t0 = Date.now();
  const { inputs, meta } = await gatherInputs(ns, sf, year);
  console.log(`[compute] Gathered inputs in ${((Date.now() - t0) / 1000).toFixed(1)}s (lastActualSalaryMonth=${meta.lastActualSalaryMonth || 'none'}, salary overrides=${meta.overridesApplied}).`);

  // Merge scenario knobs + fixed run params. Forces Pipeline + Last-Actual.
  Object.assign(inputs, scenarioKnobs(scenarioData), {
    activeYear: year,
    currentYear,
    now: new Date(),
    asOfDate: null,
    lastActualSalaryMonth: meta.lastActualSalaryMonth,
    ilsRevalRate: ILS_REVAL_RATE,
  });

  if (!inputs.book) console.warn('[compute] ⚠ no bank balance (book) — opening balance will fall back to year-start/0.');
  if (!inputs.prevMonthEndBalance) console.warn('[compute] ⚠ no prev-month-end balance — current month opening not anchored to NS.');

  // Optional: dump the exact inputs for a golden diff against the browser (?fccapture=1).
  if (args.dumpInputs) {
    const dump = { ...inputs, now: undefined, nowISO: inputs.now.toISOString() };
    fs.mkdirSync(path.dirname(args.dumpInputs), { recursive: true });
    fs.writeFileSync(args.dumpInputs, JSON.stringify(dump, null, 2));
    console.log(`[compute] Wrote gathered inputs → ${args.dumpInputs}`);
  }

  // Run the SHARED engine (same module the browser runs).
  const { computeCashflowForecast } = await import(pathToFileURL(path.join(REPO_ROOT, 'src', 'forecast', 'forecast-core.mjs')).href);
  const rows = computeCashflowForecast(inputs);
  if (!Array.isArray(rows) || rows.length !== 12) {
    console.error(`[compute] engine returned ${rows && rows.length} rows (expected 12) — aborting.`);
    process.exit(1);
  }
  const dec = rows[11];
  const forecastEur = Math.round(dec.closingBalance);
  const forecastIls = Math.round(dec.closingBalanceILS);

  // Report the 12 rows.
  console.log('\n[compute] month      opening        salary      vendors  collections     net       reval       closing');
  for (const r of rows) {
    const p = (n, w) => String(Math.round(Number(n) || 0).toLocaleString()).padStart(w);
    console.log(`[compute] ${r.mKey}  ${p(r.openingBalance, 12)} ${p(r.salary, 11)} ${p(r.vendors, 11)} ${p(r.collections, 12)} ${p(r.net, 10)} ${p(r.revalImpact, 10)} ${p(r.closingBalance, 13)}`);
  }
  console.log(`\n[compute] → December closing (FORECAST_EUR): ${fmtEur(forecastEur)}  (ILS ${forecastIls.toLocaleString()})`);
  if (scenarioSource.startsWith('NONE')) {
    console.warn('[compute] ⚠ scenario NOT loaded — this is the BASE plan (no savings). The real Exit-plan number is lower.');
  }

  const record = {
    date: new Date().toISOString().slice(0, 10),
    company: 'lsports',
    scenario: scenarioName,
    totalBankEur: Math.round(meta.adjEur) || 0,
    totalBankIls: Math.round(meta.adjIls) || 0,
    forecastEur,
    forecastIls,
    source: 'server-compute',
    revenueMethodology: 'pipeline',
    salaryProjectionMode: 'lastActual',
    updatedAt: new Date().toISOString(),
  };

  if (args.dryRun) {
    console.log('[compute] --dry-run: not writing data/net-cash-forecast.json. Would write:');
    console.log('[compute]   ' + JSON.stringify(record));
    process.exit(0);
  }

  const outPath = process.env.NET_CASH_SNAPSHOT_PATH
    ? path.resolve(process.env.NET_CASH_SNAPSHOT_PATH)
    : path.resolve(REPO_ROOT, 'data', 'net-cash-forecast.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(record, null, 2));
  console.log(`[compute] ✓ Wrote ${outPath} (forecastEur=${forecastEur}). The 23:00 snapshot will push it to Snowflake.`);
  process.exit(0);
}

main().catch((e) => { console.error(`[compute] FATAL: ${e && e.stack ? e.stack : e}`); process.exit(1); });
