#!/usr/bin/env node
// ============================================================================
// test-pnl-projection.cjs — checks for the P&L Projection page.
// No dependencies, no NetSuite/Snowflake access: every input is SYNTHETIC (illustrative numbers,
// not LSports figures).
//
//   node scripts/test-pnl-projection.cjs                  run all checks
//   node scripts/test-pnl-projection.cjs --write-fixture  also (re)write the synthetic UI fixture
//                                                         scripts/fixtures/pnl-projection-sample.json
//
// Covers: the account → line mapping (server/pnl-lines.cjs), the payload of computePnlProjection
// (server/pnl-projection.cjs) with stubbed inputs — actual months equal NetSuite, forecast months use
// the cash engine (parity with the New Bank Dashboard), accumulated profit and roll-forward — the
// cached handler, every cell breakdown adding up to its cell, and the UI table model
// (src/pnl-projection/model.ts, imported directly — Node strips the TypeScript types).
// Exit 0 = all checks passed; 1 = a failure.
// ============================================================================
const fs = require('fs');
const os = require('os');
const path = require('path');
const { pathToFileURL } = require('url');

const ROOT = path.resolve(__dirname, '..');
const cmp = require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs'));
const cp = require(path.join(ROOT, 'server', 'cash-projection.cjs'));
const pnl = require(path.join(ROOT, 'server', 'pnl-projection.cjs'));
const bd = require(path.join(ROOT, 'server', 'pnl-projection-breakdown.cjs'));
const lines = require(path.join(ROOT, 'server', 'pnl-lines.cjs'));

let failures = 0;
const fail = (msg) => { failures++; console.error('  ✗ ' + msg); };
const ok = (msg) => console.log('  ✓ ' + msg);
const check = (cond, msg, detail) => (cond ? ok(msg) : fail(detail ? `${msg} — ${detail}` : msg));
const near = (a, b, tol = 0.011) => Math.abs(a - b) <= tol;

async function quiet(fn) {
  const { log, warn } = console;
  console.log = () => {}; console.warn = () => {};
  try { return await fn(); } finally { console.log = log; console.warn = warn; }
}

// ── synthetic data ──────────────────────────────────────────────────────────
const Y = 2026;
const T = 2027;
const NOW = new Date('2026-10-15T12:00:00'); // Jan–Sep actual, Oct current, Nov–Dec forecast
const R = 3.7;
const mk = (y, m) => `${y}-${String(m).padStart(2, '0')}`;
const range = (a, b) => Array.from({ length: b - a + 1 }, (_, i) => a + i);
const clone = (x) => JSON.parse(JSON.stringify(x));

const CATEGORIES = { Cloud: 420_000, Marketing: 260_000, Data: 300_000, Office: 120_000, 'Professional services': 90_000 };
const PLAN = {
  currencyDefensePct: 30,
  pipelineMinProb: 100,
  adjustmentsByYear: { [String(Y)]: { salaryAdjPctByMonth: { 10: -4, 11: -4 }, collPctByMonth: { 10: 90, 11: 90 }, pipelineAdjPctByMonth: { 11: 80 }, currencyDefensePctByMonth: {} } },
  vendorCatAdj: { [mk(Y, 11)]: { Marketing: -25 } },
  salaryDeptAdj: {},
  vendorDetailAdj: {},
  fxRateByYear: {},
};
const BREAKDOWN = [
  { department: 'R&D', account: '760001', accountId: 1, name: 'Salaries', amountEUR: 1_100_000, amountILS: 4_070_000 },
  { department: 'Sales', account: '760001', accountId: 1, name: 'Salaries', amountEUR: 640_000, amountILS: 2_368_000 },
  { department: 'G&A', account: '760001', accountId: 1, name: 'Salaries', amountEUR: 440_000, amountILS: 1_628_000 },
];

// Engine inputs (the cash projection's gatherInputs, stubbed).
function syntheticGather() {
  const sfBudgetByMonth = Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { ...CATEGORIES }]));
  const catTotal = Object.values(CATEGORIES).reduce((s, v) => s + v, 0);
  const inputs = {
    salaryData: range(1, 9).map((m) => ({ month: mk(Y, m), amountEUR: 2_200_000, amountILS: 8_140_000 })),
    salaryActualsByDept: { [mk(Y, 9)]: {
      'R&D': { eur: 1_050_000, ils: 3_885_000 }, Sales: { eur: 620_000, ils: 2_294_000 }, 'G&A': { eur: 420_000, ils: 1_554_000 },
    } },
    salaryDeptBudgets: {},
    sfSalaryBudget: Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { eur: 2_300_000, ils: 8_510_000 }])),
    sfSalaryOverrides: [],
    monthlyHCImpact: { [mk(Y, 11)]: { running: 74_000 }, [mk(Y, 12)]: { running: 148_000 } },
    sfActualsSplit: {},
    vendorBills: [{ amountEUR: 420_000 }],
    vendorActuals: [],
    nsPaidVendors: { byMonth: {}, grid: {}, accounts: [] },
    vendorHistory: [],
    sfBudget: { byMonth: sfBudgetByMonth, totalByMonth: Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { eur: catTotal + 15_000, ils: Math.round((catTotal + 15_000) * R) }])) },
    nsBudget: { byMonth: {} },
    expenseCategories: { byMonth: {}, categories: [] },
    sfRevenuePaid: Object.fromEntries(range(1, 12).map((m) => {
      const revenue = 3_400_000 + (m - 1) * 20_000;
      return [mk(Y, m), { revenue, customers: 410 + m * 2, paid: m <= 9 ? revenue - 60_000 : 0, unpaid: m <= 9 ? 60_000 : revenue }];
    })),
    actualCollections: { [mk(Y, 10)]: 1_450_000 },
    sfRevenue: { budget: {}, actuals: {}, targets: {} },
    revenueActuals: [],
    customerReceipts: {},
    sfPipeline: [
      { name: 'Synthetic deal A', owner: 'Synthetic owner', probability: 60, closeDate: '2026-11-20', amount: 180_000 },
      { name: 'Synthetic deal B', owner: 'Synthetic owner', probability: 100, closeDate: '2026-12-10', amount: 40_000 },
    ],
    sfConversion: { yearly: [{ year: 2025, winRate: 36, avgWonDays: 58 }], stages: [], customers: [], projection: [] },
    pipelineMethodology: { byMonth: { [mk(Y, 10)]: { monthlyContribution: 30_000 }, [mk(Y, 11)]: { monthlyContribution: 55_000 }, [mk(Y, 12)]: { monthlyContribution: 70_000 } } },
    sfChurnQuarterly: [{ partial: false, qs: '2026-07', amount: 84_000 }],
    churnData: [],
    churnMonthlyAvg: 25_000,
    monthlyReval: { preYear: { eur: 0, ils: 0 }, byMonth: {} },
    nsBankClassified: { byMonth: {} },
    sfFinanceBudget: Object.fromEntries(range(10, 12).map((m) => [mk(Y, m), { eur: 120_000, ils: 0 }])),
    dividendExclusions: { byMonth: {} },
    book: { openingBalance: 7_900_000, currentBalance: 7_200_000, adjustedCurrentBalance: 7_200_000 },
    bookLocal: { openingBalance: 29_230_000, currentBalance: 26_640_000, adjustedCurrentBalance: 26_640_000 },
    yearStartBalance: { eur: 8_120_000, ils: Math.round(8_120_000 * R) },
    prevMonthEndBalance: { eur: 7_000_000, ils: Math.round(7_000_000 * R) },
    liveFxRate: R,
  };
  return { inputs, meta: { lastActualSalaryMonth: mk(Y, 9), overridesApplied: 0, adjEur: 7_200_000, adjIls: 26_640_000 } };
}

// NetSuite P&L by account (profit-signed), 2025-01 … 2026-09, plus a partial October that must be ignored.
const ACCOUNTS = [
  // acct, type, name, base € per month (profit-signed)
  ['400001', 'Income', 'Revenues', 3_300_000],
  ['400002', 'Income', 'Revenues From Distribution sales', 310_000],
  ['400022', 'Income', 'Service Cloud - Statscore Revenue (I/C)', 36_000],
  ['400016', 'Income', 'Revenue from Sub-Lease', 5_000],
  ['760001', 'Expense', 'Gross Salaries', -1_800_000],
  ['760023', 'Expense', 'Military reserve refund', 30_000],
  ['760017', 'Expense', 'One time payments (Bonus, grants)', -150_000],
  ['950000', 'Expense', 'Salaries CAPEX classification', 900_000],
  ['640001', 'Expense', 'Servers and cloud services', -700_000],
  ['710008', 'Expense', 'Rent', -60_000],
  ['800011', 'Expense', 'Interest income from banks USD', 500],
  ['800005', 'Expense', 'Bank fees - Commissions', -14_000],
  ['800029', 'OthExpense', 'Unrealized Gain/Loss', 120_000],
  ['780502', 'Expense', 'Depreciation Expenses', -300_000],
  ['900009', 'Expense', 'tax expenses', -40_000],
  ['780030', 'OthIncome', 'FA Gain/Loss', 2_000],
];
// Snowflake vendor budget by account (FCT_BUDGET), adding up to CATEGORIES each month.
const BUDGET_ACCOUNTS = [
  // category, acct, name, € per month
  ['Cloud', '640001', 'Servers and cloud services', 300_000],
  ['Cloud', '640002', 'Software subscriptions', 120_000],
  ['Marketing', '610005', 'Events and conferences', 260_000],
  ['Data', '620001', 'Data providers', 300_000],
  ['Office', '710008', 'Rent', 120_000],
  ['Professional services', '660004', 'Legal fees', 90_000],
];
function syntheticBudgetByAccount(year) {
  return range(1, 12).flatMap((m) => BUDGET_ACCOUNTS.map(([category, acct, name, eur]) => ({ month: mk(year, m), category, acct, name, eur, ils: eur * R })));
}
function syntheticActuals() {
  const byMonth = {};
  const months = [...range(1, 12).map((m) => mk(Y - 1, m)), ...range(1, 9).map((m) => mk(Y, m))];
  months.forEach((mKey, i) => {
    const m = {};
    for (const [acct, type, name, base] of ACCOUNTS) {
      if (acct === '950000' && mKey < mk(Y, 3)) continue;              // CAPEX booked from March 2026 only
      const wobble = ((i * 37 + Number(acct.slice(-3))) % 11 - 5) / 100; // deterministic ±5%
      const eur = Math.round(base * (1 + wobble) * 100) / 100;
      m[acct] = { acct, name, type, eur, ils: Math.round(eur * R * 100) / 100 };
    }
    byMonth[mKey] = m;
  });
  // A partial current month: must not reach the table.
  byMonth[mk(Y, 10)] = { 400001: { acct: '400001', name: 'Revenues', type: 'Income', eur: 900_000, ils: 3_330_000 } };
  // NetSuite internal ids of the P&L accounts (for the register links).
  const accountIds = Object.fromEntries([...new Set([...ACCOUNTS.map((a) => a[0]), ...BUDGET_ACCOUNTS.map((a) => a[1])])].map((acct, i) => [acct, 500 + i]));
  return { basis: 'trandate', byMonth, accountIds };
}

// Snowflake budget extras: depreciation budget for the next year only, tax budget for both years.
function syntheticBudgetRows() {
  return [
    ...range(1, 12).map((m) => ({ month: mk(T, m), acct: '780502', name: 'Depreciation Expenses', type: 'Expense', eur: 310_000, ils: 1_147_000 })),
    ...range(1, 12).map((m) => ({ month: mk(Y, m), acct: '900009', name: 'tax expenses', type: 'Expense', eur: 45_000, ils: 166_500 })),
    ...range(1, 12).map((m) => ({ month: mk(T, m), acct: '900009', name: 'tax expenses', type: 'Expense', eur: 47_000, ils: 173_900 })),
  ];
}

function fakeSf() {
  return {
    fetchBudgetByCategory: async () => ({ byMonth: Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { ...CATEGORIES }])), totalByMonth: {} }),
    fetchSalaryBudget: async () => Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { eur: 2_300_000, ils: 8_510_000 }])),
    fetchSalaryBudgetBreakdown: async () => clone(BREAKDOWN),
    fetchPnlBudgetExtras: async () => syntheticBudgetRows(),
  };
}
function fakeNs({ actuals = syntheticActuals() } = {}) {
  return { fetchPnlActuals: async ({ basis }) => ({ ...clone(actuals), basis }) };
}
function computeModuleWith(gather, scenario) {
  return { ...cmp, gatherInputs: async () => clone(gather), loadScenarioDataAsync: async () => scenario };
}
const SCENARIO = { data: PLAN, source: 'Postgres user_scenarios (name="Exit plan June26", owner=someone@example.com)' };

async function withNsEnv(fn) {
  const prev = process.env.NETSUITE_ACCOUNT_ID;
  process.env.NETSUITE_ACCOUNT_ID = prev || 'TEST_ACCOUNT';
  try { return await fn(); } finally { if (prev === undefined) delete process.env.NETSUITE_ACCOUNT_ID; else process.env.NETSUITE_ACCOUNT_ID = prev; }
}
function runPnl({ gather = syntheticGather(), ns = fakeNs(), sf = fakeSf(), now = NOW } = {}) {
  return withNsEnv(() => quiet(() => pnl.computePnlProjection({
    now, getNsClient: () => ns, getSfClient: () => sf, queueNsCall: (fn) => fn(),
    computeModule: computeModuleWith(gather, SCENARIO), includeRaw: true,
  })));
}
function runCash({ gather = syntheticGather(), sf = fakeSf() } = {}) {
  return withNsEnv(() => quiet(() => cp.computeCashProjection({
    now: NOW, getNsClient: () => ({}), getSfClient: () => sf, queueNsCall: (fn) => fn(),
    computeModule: computeModuleWith(gather, SCENARIO), includeRaw: true,
  })));
}

// ── 1. account → line mapping ───────────────────────────────────────────────
function testMapping() {
  console.log('\nMAPPING: every NetSuite P&L account lands on exactly one line (server/pnl-lines.cjs)');
  const cases = [
    ['400001', 'Income', 'revenue'], ['400002', 'Income', 'revenue'], ['400018', 'Income', 'revenue'], ['410001', 'Income', 'revenue'],
    ['400022', 'Income', 'otherRevenue'], ['400019', 'Income', 'otherRevenue'], ['660010', 'Income', 'otherRevenue'], ['400041', 'Income', 'otherRevenue'],
    ['760001', 'Expense', 'payroll'], ['76003', 'Expense', 'payroll'], ['760023', 'Expense', 'payroll'],
    ['950000', 'Expense', 'capex'],
    ['640001', 'Expense', 'opex'], ['745001', 'Expense', 'opex'], ['710013', 'Expense', 'opex'], ['800011', 'Expense', 'opex'],
    ['500001', 'COGS', 'opex'], ['COGS', 'COGS', 'opex'], ['780022', 'Expense', 'opex'],
    ['800028', 'OthExpense', 'fx'], ['800029', 'OthExpense', 'fx'], ['800030', 'OthExpense', 'fx'], ['800031', 'OthExpense', 'fx'],
    ['800005', 'Expense', 'finance'], ['800019', 'Expense', 'finance'], ['190002', 'Expense', 'finance'], ['900002', 'Expense', 'finance'],
    ['780502', 'Expense', 'depreciation'], ['780501', 'Expense', 'depreciation'], ['780503', 'Expense', 'depreciation'],
    ['900009', 'Expense', 'taxOther'], ['910000', 'Expense', 'taxOther'], ['900500', 'Expense', 'taxOther'], ['780030', 'OthIncome', 'taxOther'], ['180018', 'OthExpense', 'taxOther'],
  ];
  const wrong = cases.filter(([a, t, want]) => lines.classifyAccount(a, t) !== want).map(([a, t, want]) => `${a}/${t} → ${lines.classifyAccount(a, t)} (want ${want})`);
  check(!wrong.length, `${cases.length} accounts map to the EBITDA report's sections`, wrong.join('; '));
  check(cases.every(([a, t]) => lines.LINE_KEYS.includes(lines.classifyAccount(a, t))), 'every account maps to a known line');
  const month = syntheticActuals().byMonth[mk(Y, 5)];
  const s = lines.sumByLine(month);
  const all = Object.values(month).reduce((t, a) => t + a.eur, 0);
  const byLines = lines.LINE_KEYS.reduce((t, k) => t + s.totals[k].eur, 0);
  check(near(all, byLines), 'line totals add up to the sum of all accounts (nothing dropped)', `${all} vs ${byLines}`);
  check(s.accounts.opex.some((a) => a.acct === '800011'), '800011 sits in Operating expenses, as in the EBITDA report');
}

// ── 2. payload ──────────────────────────────────────────────────────────────
async function testPayload() {
  console.log('\nPAYLOAD: computePnlProjection (server/pnl-projection.cjs)');
  const { payload, details, raw } = await runPnl();
  const cash = await runCash();
  const actuals = syntheticActuals();
  check(payload.ok && payload.status === 'ready' && payload.schemaVersion === pnl.SCHEMA_VERSION, 'ready payload with the P&L schema version');
  check(JSON.stringify(payload.years) === JSON.stringify([Y, T]), 'years: current + next');
  check(payload.actuals.source === 'netsuite' && payload.actuals.basis === 'period' && payload.actuals.through === mk(Y, 9), 'actuals: NetSuite, by posting period (as its Profit and Loss report), through September');
  check(payload.plan.loaded && payload.plan.source === 'postgres', 'plan loaded from Postgres');

  for (const variant of ['plan', 'base']) {
    const [yb, tb] = payload.variants[variant].years;
    check(yb.rows.length === 12 && tb.rows.length === 12, `${variant}: 12 + 12 months`);
    const statuses = yb.rows.map((r) => r.status).join(',');
    check(statuses === `${'actual,'.repeat(9)}current,forecast,forecast`, `${variant}: Jan–Sep actual, Oct current, Nov–Dec forecast`, statuses);
    check(tb.rows.every((r) => r.status === 'forecast'), `${variant}: next year all forecast`);

    // Actual months equal NetSuite exactly, line by line, in both currencies.
    let mism = [];
    for (const r of yb.rows.filter((x) => x.status === 'actual')) {
      const tot = lines.sumByLine(actuals.byMonth[r.mKey]).totals;
      for (const c of ['eur', 'ils']) {
        const f = r[c];
        const want = {
          revenue: tot.revenue[c], otherRevenue: tot.otherRevenue[c], payroll: -tot.payroll[c], capex: -tot.capex[c], opex: -tot.opex[c],
          fx: tot.fx[c], finance: tot.finance[c], depreciation: tot.depreciation[c], taxOther: tot.taxOther[c],
        };
        for (const [k, v] of Object.entries(want)) if (!near(f[k], v)) mism.push(`${r.mKey} ${c} ${k}: ${f[k]} vs ${v}`);
        const all = Object.values(actuals.byMonth[r.mKey]).reduce((t, a) => t + a[c], 0);
        if (!near(f.net, all, 0.05)) mism.push(`${r.mKey} ${c} net ${f.net} vs Σ accounts ${all}`);
        const above = Object.values(actuals.byMonth[r.mKey]).filter((a) => lines.ABOVE_EBITDA.has(lines.classifyAccount(a.acct, a.type))).reduce((t, a) => t + a[c], 0);
        if (!near(f.ebitda, above, 0.05)) mism.push(`${r.mKey} ${c} ebitda ${f.ebitda} vs ${above}`);
        if (f.pipeline !== 0 || f.churn !== 0) mism.push(`${r.mKey} pipeline/churn in an actual month`);
      }
    }
    check(!mism.length, `${variant}: every actual month equals NetSuite per line, EBITDA and net profit (€ and ₪)`, mism.slice(0, 4).join('; '));

    // Row identities.
    const idErr = [];
    for (const r of [...yb.rows, ...tb.rows]) {
      for (const c of ['eur', 'ils']) {
        const f = r[c];
        if (!near(f.totalRevenue, f.revenue + f.pipeline - f.churn + f.otherRevenue, 0.02)) idErr.push(`${r.mKey} totalRevenue`);
        if (!near(f.totalCosts, f.payroll + f.capex + f.opex, 0.02)) idErr.push(`${r.mKey} totalCosts`);
        if (!near(f.ebitda, f.totalRevenue - f.totalCosts, 0.02)) idErr.push(`${r.mKey} ebitda`);
        if (!near(f.net, f.ebitda + f.fx + f.finance + f.depreciation + f.taxOther, 0.02)) idErr.push(`${r.mKey} net`);
        if (!near(f.accClosing, f.accOpening + f.net, 0.02)) idErr.push(`${r.mKey} accumulated`);
      }
    }
    check(!idErr.length, `${variant}: totals, EBITDA, net and accumulated identities hold every month`, idErr.slice(0, 4).join(', '));

    // Accumulated net profit: 0 on 1 Jan, chained across months and into the next year.
    check(yb.rows[0].eur.accOpening === 0 && yb.rows[0].ils.accOpening === 0, `${variant}: accumulated profit opens at 0 on 1 January ${Y}`);
    const chain = [...yb.rows, ...tb.rows].every((r, i, all) => i === 0 || near(r.eur.accOpening, all[i - 1].eur.accClosing, 0.02));
    check(chain, `${variant}: each month opens at the previous closing, January ${T} at December ${Y} (no reset)`);
    check(near(payload.variants[variant].rollForward.accumulated.eur, yb.rows[11].eur.accClosing), `${variant}: rollForward carries the December accumulated profit`);

    // The current month is projected as a whole month: the partial NetSuite October is ignored.
    const oct = yb.rows[9];
    check(oct.eur.revenue !== 900_000 && oct.eur.payroll > 1_000_000, `${variant}: October is a projected whole month, not the partial NetSuite postings`);
  }

  // Forecast months use the cash engine: parity with the New Bank Dashboard after the current month.
  const cashRows = cash.raw.plan.rows[Y];
  const pnlRows = raw.plan.rows[Y];
  const plan = payload.variants.plan.years[0].rows;
  const parity = [];
  for (const mi of [10, 11]) {
    if (!near(plan[mi].eur.payroll, cashRows[mi].salary, 0.01)) parity.push(`payroll ${mi + 1}: ${plan[mi].eur.payroll} vs ${cashRows[mi].salary}`);
    if (!near(plan[mi].eur.opex, cashRows[mi].vendors, 0.01)) parity.push(`opex ${mi + 1}: ${plan[mi].eur.opex} vs ${cashRows[mi].vendors}`);
    if (!near(plan[mi].eur.fx, cashRows[mi].revalImpact, 0.01)) parity.push(`fx ${mi + 1}`);
  }
  check(!parity.length, 'Plan Nov–Dec payroll, operating expenses and FX equal the cash projection\'s salary, vendors and reval', parity.join('; '));
  const baseCash = cash.raw.base.rows[Y];
  const base = payload.variants.base.years[0].rows;
  check([10, 11].every((mi) => near(base[mi].eur.revenue, baseCash[mi].collections, 0.01)), 'Base Nov–Dec customer revenue = the cash projection\'s collections at a 100% collection rate');
  check(near(plan[10].eur.revenue, pnlRows[10].collectionsRevenue + pnlRows[10].collectionsPipeline, 0.01) && plan[10].eur.revenue > cashRows[10].collections,
    'Plan revenue ignores the plan\'s collection %: revenue is recognised, not collected');
  check(plan[9].eur.pipeline === 30_000 && plan[10].eur.pipeline === 85_000 && plan[11].eur.pipeline === Math.round(155_000 * 0.8),
    'pipeline: October contributes, cumulative, with the plan\'s pipeline % (80% in December)');
  check(plan[9].eur.churn === 28_000 && plan[11].eur.churn === 84_000, 'churn: run-rate × forecast months from October (28K, 56K, 84K)');

  // Lines the cash engine does not model.
  const nsT = (mKey, line) => lines.sumByLine(actuals.byMonth[mKey]).totals[line].eur;
  const avg3 = (line) => [7, 8, 9].reduce((s, m) => s + nsT(mk(Y, m), line), 0) / 3;
  check(near(plan[10].eur.otherRevenue, avg3('otherRevenue')), 'other & intercompany revenue: average of Jul–Sep');
  check(near(plan[10].eur.finance, avg3('finance')), 'finance: average of Jul–Sep');
  check(near(plan[10].eur.capex, -nsT(mk(Y, 9), 'capex')) && plan[10].eur.capex < 0, 'Salaries CAPEX: September\'s credit carried flat (a negative cost)');
  check(near(plan[10].eur.depreciation, avg3('depreciation')), 'depreciation without a budget: average of Jul–Sep');
  const tb = payload.variants.plan.years[1].rows;
  check(near(tb[0].eur.depreciation, -310_000) && near(tb[0].ils.depreciation, -1_147_000), 'depreciation with a budget (next year): the Snowflake budget');
  check(near(plan[10].eur.taxOther, -45_000) && near(tb[0].eur.taxOther, -47_000), 'tax: the Snowflake budget of each year');
  check(tb[0].eur.fx === 0 && tb[0].eur.pipeline === 0, 'next year: no FX and no new pipeline (as on the cash page)');

  // Next year: roll-forward fed with this year's P&L months.
  const bY = payload.variants.base.years[0].rows;
  const bT = payload.variants.base.years[1].rows;
  check(near(bT[0].eur.opex, bY[0].eur.opex, 1) && near(bT[4].eur.opex, bY[4].eur.opex, 1), `base ${T} operating expenses mirror ${Y} month by month (incl. NetSuite actual months)`);
  const runRate = Math.round([9, 10, 11].reduce((s, mi) => s + bY[mi].eur.revenue + bY[mi].eur.pipeline - bY[mi].eur.churn, 0) / 3);
  check(near(bT[0].eur.revenue, runRate + 40_000, 1), `base ${T} revenue = Oct–Dec run-rate (incl. pipeline − churn) + open deals ≥ min probability`, `${bT[0].eur.revenue} vs ${runRate + 40_000}`);
  const avgPay = [9, 10, 11].reduce((s, mi) => s + bY[mi].eur.payroll, 0) / 3;
  check(near(bT[0].eur.payroll, avgPay, 3), `base ${T} payroll = the Oct–Dec ${Y} run-rate`, `${bT[0].eur.payroll} vs ${avgPay}`);

  // Details stay server-side, and hold only what the breakdowns need.
  check(!('details' in payload) && details.version === pnl.DETAILS_VERSION, 'details are returned separately (server-only)');
  check(Object.keys(details.accounts).sort().join(',') === range(1, 9).map((m) => mk(Y, m)).join(','), 'details carry NetSuite accounts for the actual months only');
  return { payload, details };
}

async function testDegraded() {
  console.log('\nFAILURES: what happens when a source is missing');
  let threw = null;
  try { await runPnl({ ns: { fetchPnlActuals: async () => { throw new Error('synthetic NetSuite outage'); } } }); } catch (e) { threw = e; }
  check(threw instanceof cp.ProjectionError && /NetSuite P&L actuals/.test(threw.message), 'no NetSuite P&L actuals → a user-safe error (actuals must equal NetSuite)', threw && threw.message);
  const sf = { ...fakeSf(), fetchPnlBudgetExtras: async () => { throw new Error('synthetic Snowflake outage'); } };
  const { payload } = await runPnl({ sf });
  check(payload.degraded && payload.warnings.some((w) => /depreciation and tax budget/.test(w)), 'no Snowflake budget → still computes, with a warning');
  const jan = await runPnl({ now: new Date('2026-01-10T12:00:00') });
  const rows = jan.payload.variants.base.years[0].rows;
  check(rows[0].status === 'current' && rows.slice(1).every((r) => r.status === 'forecast') && jan.payload.actuals.through === null, 'in January: no actual months, January projected whole');
  const a = syntheticActuals();
  const prevAvg = [10, 11, 12].reduce((s, m) => s + lines.sumByLine(a.byMonth[mk(Y - 1, m)]).totals.otherRevenue.eur, 0) / 3;
  check(near(rows[0].eur.otherRevenue, prevAvg) && rows[0].eur.capex === 0, 'in January: run-rates come from the previous year\'s closed months (no CAPEX booked in them)');
}

// ── 3. handler ──────────────────────────────────────────────────────────────
function call(handler, url = '/api/pnl-projection') {
  return new Promise((resolve) => {
    const res = {
      statusCode: 200, headers: {}, setHeader(k, v) { this.headers[k.toLowerCase()] = v; },
      end(body) { resolve({ status: this.statusCode, headers: this.headers, body: body ? JSON.parse(body) : null }); },
    };
    Promise.resolve(handler({ method: 'GET', url }, res));
  });
}

async function testHandler(computed) {
  console.log('\nHANDLER: cached GET /api/pnl-projection');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'pnl-projection-test-'));
  const cacheFile = path.join(tmp, 'pnl-cache.json');
  let t = NOW.getTime();
  let runs = 0;
  const h = pnl.createPnlProjectionHandler({ clock: () => t, cacheFile, compute: async () => { runs++; return { payload: computed.payload, details: computed.details }; } });
  const first = await call(h);
  check(first.status === 202 && first.body.status === 'computing', 'first call: 202 computing');
  await h.idle();
  const second = await call(h);
  check(second.status === 200 && second.body.status === 'ready' && runs === 1, 'then 200 ready from one computation');
  check(!('details' in second.body), 'the response never carries the server-only details');
  const saved = JSON.parse(fs.readFileSync(cacheFile, 'utf-8'));
  check(saved.schemaVersion === pnl.SCHEMA_VERSION && saved.entry.details && saved.entry.payload.actuals, 'cache file written with the P&L schema and the details');
  check(pnl.DEFAULT_CACHE_FILE !== cp.DEFAULT_CACHE_FILE, 'its own cache file, next to the cash projection\'s');
  t += 31 * 60 * 1000;
  const stale = await call(h);
  check(stale.body.cache.stale && stale.body.cache.refreshing, 'after the TTL: served stale while it refreshes');
  await h.idle();
  check(runs === 2, 'one background recompute');
  return { cacheFile };
}

// ── 4. breakdowns ───────────────────────────────────────────────────────────
async function testBreakdowns(computed, cacheFile) {
  console.log('\nBREAKDOWN: every cell\'s breakdown adds up to the cell (server/pnl-projection-breakdown.cjs)');
  const entry = cp.makeEntry(computed.payload, NOW.getTime(), computed.details, pnl.SCHEMA_VERSION);
  const actuals = syntheticActuals();
  let sfReads = 0;
  const sfx = {
    expense: async (year) => {
      sfReads++;
      // Snowflake = NetSuite cost accounts (debit-positive) with one account off by €1,000 in June.
      const out = [];
      for (const [mKey, accts] of Object.entries(actuals.byMonth)) {
        if (!mKey.startsWith(`${year}-`)) continue;
        for (const a of Object.values(accts)) {
          if (a.type === 'Income') continue;
          const off = mKey === mk(Y, 6) && a.acct === '640001' ? 1_000 : 0;
          out.push({ month: mKey, kind: 'Vendors', category: 'x', acct: a.acct, name: a.name, eur: -a.eur + off, ils: -a.ils });
        }
      }
      return out;
    },
    budget: async (year) => syntheticBudgetByAccount(year),
  };
  await withAccountId('1234_SB1', () => breakdownChecks({ computed, cacheFile, entry, sfx, sfReads: () => sfReads }));
  const bare = await withAccountId(undefined, () => bd.buildBreakdown({ entry, line: 'opex', period: mk(Y, 6), variant: 'plan', ccy: 'eur', sfx }));
  check(bare.sections[0].rows.every((r) => r.link === null), 'no NETSUITE_ACCOUNT_ID → no links');
  const old = cp.makeEntry(computed.payload, NOW.getTime(), { ...computed.details, accountIds: undefined }, pnl.SCHEMA_VERSION);
  const oldOut = await withAccountId('1234_SB1', () => bd.buildBreakdown({ entry: old, line: 'opex', period: mk(Y, 6), variant: 'plan', ccy: 'eur', sfx }));
  check(oldOut.sections[0].rows.every((r) => r.link === null && r.ref), 'a projection cached before the account ids: account numbers without links');
}

// Runs fn with NETSUITE_ACCOUNT_ID set to value (undefined = unset), then restores it.
async function withAccountId(value, fn) {
  const prev = process.env.NETSUITE_ACCOUNT_ID;
  if (value === undefined) delete process.env.NETSUITE_ACCOUNT_ID; else process.env.NETSUITE_ACCOUNT_ID = value;
  try { return await fn(); } finally { if (prev === undefined) delete process.env.NETSUITE_ACCOUNT_ID; else process.env.NETSUITE_ACCOUNT_ID = prev; }
}

async function breakdownChecks({ computed, cacheFile, entry, sfx, sfReads }) {
  const periods = [mk(Y, 3), mk(Y, 6), mk(Y, 10), mk(Y, 11), mk(Y, 12), mk(T, 1), mk(T, 7), `FY-${Y}`, `FY-${T}`];
  const bad = [];
  let built = 0;
  for (const variant of ['plan', 'base']) {
    for (const ccy of ['eur', 'ils']) {
      for (const line of bd.LINES) {
        for (const period of periods) {
          const row = period.startsWith('FY') ? null : computed.payload.variants[variant].years.flatMap((y) => y.rows).find((r) => r.mKey === period);
          const cell = row ? (line === 'churn' ? -row[ccy].churn : row[ccy][line]) : null;
          if (row && Math.abs(cell) < 0.5) continue; // the table does not open empty cells
          let out;
          try { out = await bd.buildBreakdown({ entry, line, period, variant, ccy, sfx }); } catch (e) {
            if (e instanceof bd.BreakdownError) continue; // e.g. a full year with nothing in it
            bad.push(`${variant} ${ccy} ${line} ${period}: threw ${e.message}`); continue;
          }
          built++;
          const main = out.sections[0];
          if (Math.abs(main.total - out.cell) >= 1) bad.push(`${variant} ${ccy} ${line} ${period}: ${main.total} ≠ ${out.cell}`);
          if (row && Math.abs(out.cell - cell) >= 0.01) bad.push(`${line} ${period}: cell ${out.cell} ≠ table ${cell}`);
        }
      }
    }
  }
  check(!bad.length, `${built} breakdowns (all lines, actual/current/forecast/next-year months and full years, Plan/Base, €/₪) add up to their cells`, bad.slice(0, 5).join('; '));

  const june = await bd.buildBreakdown({ entry, line: 'opex', period: mk(Y, 6), variant: 'plan', ccy: 'eur', sfx });
  check(june.sections[0].rows.some((r) => r.ref === '800011') && june.sections[0].rows.every((r) => r.kind === 'item'), 'actual month: the NetSuite accounts, nothing to adjust');
  const sfSection = june.sections.find((s) => s.id === 'snowflake');
  check(!!sfSection && sfSection.collapsed && sfSection.rows.some((r) => r.key === 'sf-diff' && Math.abs(r.amount + 1_000) < 0.01),
    'actual month: a collapsed Snowflake check shows the €1,000 difference to NetSuite');
  const nov = await bd.buildBreakdown({ entry, line: 'payroll', period: mk(Y, 11), variant: 'plan', ccy: 'eur', sfx });
  check(nov.sections[0].rows.some((r) => r.group && /September 2026 by department/.test(r.group)) && nov.sections[0].rows.some((r) => r.key === 'plan'),
    'forecast payroll: the basis month by department, hires/leavers and the plan change');
  const capex = await bd.buildBreakdown({ entry, line: 'capex', period: mk(T, 3), variant: 'base', ccy: 'eur', sfx });
  check(/September 2026, carried flat/.test(capex.sections[0].rows[0].label), 'CAPEX: the last closed month, carried flat');
  const other = await bd.buildBreakdown({ entry, line: 'otherRevenue', period: mk(Y, 12), variant: 'plan', ccy: 'ils', sfx });
  check(other.sections[0].rows.filter((r) => r.kind === 'item').length === 3, 'other revenue: the three months of the average');
  let noPipeline = null;
  try { await bd.buildBreakdown({ entry, line: 'pipeline', period: mk(Y, 4), variant: 'plan', ccy: 'eur', sfx }); } catch (e) { noPipeline = e; }
  check(noPipeline instanceof bd.BreakdownError, 'an actual month has no pipeline to break down');
  check(sfReads() > 0, 'the Snowflake check is read on demand');

  // Account numbers link to the account's register in NetSuite.
  const NS_HOST = 'https://1234-sb1.app.netsuite.com/app/reporting/reportrunner.nl?acctid=';
  const ids = computed.details.accountIds;
  const linkFor = (acct, from, to) => `${NS_HOST}${ids[acct]}&reporttype=REGISTER&subsidiary=3&combinebalance=T&startdate=${from}&enddate=${to}`;
  const juneRent = june.sections[0].rows.find((r) => r.ref === '710008');
  check(!!ids['710008'] && juneRent && juneRent.link === linkFor('710008', '6/1/2026', '6/30/2026'),
    'actual month: each account links to its NetSuite register for that month', juneRent && juneRent.link);
  check(june.sections[0].rows.every((r) => r.link && r.link.startsWith(NS_HOST)), 'actual month: every account row has a link');
  check(sfSection.rows.filter((r) => r.kind === 'item').every((r) => r.link && r.link.endsWith('startdate=6/1/2026&enddate=6/30/2026')), 'the Snowflake check rows link too');

  const novOpex = await bd.buildBreakdown({ entry, line: 'opex', period: mk(Y, 11), variant: 'plan', ccy: 'eur', sfx });
  const novRows = novOpex.sections[0].rows;
  const novItems = novRows.filter((r) => r.kind === 'item');
  check(novItems.length === BUDGET_ACCOUNTS.length && novItems.every((r) => /^\d{6}$/.test(r.ref)) && novItems.filter((r) => r.group === 'Cloud').length === 2,
    'forecast opex: the vendor budget by account, grouped by category', novItems.map((r) => `${r.group}/${r.ref}`).join(','));
  const cloud = novItems.find((r) => r.ref === '640002');
  check(cloud && cloud.link === linkFor('640002', '7/1/2026', '9/30/2026'), 'forecast month: accounts link to the last 3 closed months in NetSuite', cloud && cloud.link);
  const overrides = novRows.find((r) => r.key === 'overrides');
  check(overrides && near(overrides.amount, 15_000) && !overrides.link && novRows.some((r) => r.key === 'plan'),
    'forecast opex: budget overrides and the plan change as adjustments', overrides && overrides.amount);
  check(near(novRows.reduce((s, r) => s + r.amount, 0), novOpex.cell), 'forecast opex: account rows and adjustments add up to the cell');

  const janOpex = await bd.buildBreakdown({ entry, line: 'opex', period: mk(T, 1), variant: 'base', ccy: 'eur', sfx });
  const janItems = janOpex.sections[0].rows.filter((r) => r.kind === 'item');
  const janBooked = computed.details.accounts[mk(Y, 1)].opex;
  check(janItems.length === janBooked.length && janItems.every((r) => r.group === `Same month of ${Y} (January ${Y})`),
    `next year: the NetSuite accounts of the same month of ${Y}`, janItems.map((r) => r.ref).join(','));
  const janCloud = janItems.find((r) => r.ref === '640001');
  check(janCloud && janCloud.link === linkFor('640001', '1/1/2026', '1/31/2026'), `next year: accounts link to January ${Y}, the month they mirror`, janCloud && janCloud.link);
  const octOpex = await bd.buildBreakdown({ entry, line: 'opex', period: mk(T, 10), variant: 'base', ccy: 'eur', sfx });
  check(octOpex.sections[0].rows.some((r) => r.ref === '620001' && r.group === `Same month of ${Y} (October ${Y})`),
    `next year, mirroring a month of ${Y} not closed yet: its budget by account`);

  const capexLink = capex.sections[0].rows[0].link;
  check(capexLink === linkFor('950000', '9/1/2026', '9/30/2026'), 'CAPEX: links to 950000 in the month carried flat', capexLink);
  const fyOpex = await bd.buildBreakdown({ entry, line: 'opex', period: `FY-${Y}`, variant: 'plan', ccy: 'eur', sfx });
  const fyLinks = fyOpex.sections.flatMap((s) => s.rows).filter((r) => r.link);
  check(fyLinks.length > 0 && fyLinks.every((r) => r.link.endsWith('startdate=1/1/2026&enddate=9/30/2026')), 'full year: accounts link to the year\'s closed months');
  const fx = await bd.buildBreakdown({ entry, line: 'fx', period: mk(Y, 11), variant: 'plan', ccy: 'eur', sfx });
  check(fx.sections[0].rows.every((r) => r.link === null), 'rows that are not accounts have no link');

  // Handler contract.
  const h = bd.createPnlProjectionBreakdownHandler({ cacheFile, sfx });
  cp.writeCacheEntry(cacheFile, entry);
  const okRes = await call(h, `/api/pnl-projection/breakdown?line=opex&period=${mk(Y, 11)}&variant=plan&ccy=eur`);
  check(okRes.status === 200 && okRes.body.status === 'ready' && okRes.body.lineLabel === 'Operating expenses', 'GET breakdown → 200 ready');
  const badRes = await call(h, '/api/pnl-projection/breakdown?line=salary&period=2026-11');
  check(badRes.status === 400, 'unknown line → 400');
  const missing = await call(h, '/api/pnl-projection/breakdown?line=opex&period=2030-01');
  check(missing.status === 404, 'period outside the projection → 404');
  const cold = bd.createPnlProjectionBreakdownHandler({ cacheFile: path.join(path.dirname(cacheFile), 'none.json'), sfx });
  const coldRes = await call(cold, `/api/pnl-projection/breakdown?line=opex&period=${mk(Y, 11)}`);
  check(coldRes.status === 202, 'no cached projection yet → 202 computing');
}

// ── 5. UI model ─────────────────────────────────────────────────────────────
async function testModel(payload) {
  const file = path.join(ROOT, 'src', 'pnl-projection', 'model.ts');
  if (!fs.existsSync(file)) { console.log('\nMODEL: src/pnl-projection/model.ts not present — skipped'); return; }
  console.log('\nMODEL: table lines and KPIs (src/pnl-projection/model.ts)');
  const model = await import(pathToFileURL(file).href);
  const table = model.buildPnlTable(payload.variants.plan, 'eur', 'both');
  check(table.columns.length === 26, 'both years → 26 columns (12 months + FY, twice)');
  const keys = table.lines.map((l) => l.key);
  check(keys[0] === 'accOpening' && keys[keys.length - 1] === 'accClosing' && keys.includes('ebitda') && keys.includes('net'), 'accumulated opening first, EBITDA and net profit inside, accumulated closing last', keys.join(','));
  const line = (k) => table.lines.find((l) => l.key === k);
  const fyY = table.columns.findIndex((c) => c.id === `fy-${Y}`);
  const yRows = payload.variants.plan.years[0].rows;
  check(near(line('net').values[fyY], yRows.reduce((s, r) => s + r.eur.net, 0), 0.05), 'FY net profit = sum of the months');
  check(line('accOpening').values[fyY] === 0 && near(line('accClosing').values[fyY], yRows[11].eur.accClosing), 'FY accumulated: opening of January, closing of December');
  check(near(line('churn').values[10], -yRows[10].eur.churn), 'churn is shown as a deduction');
  const janT = table.columns.findIndex((c) => c.mKey === mk(T, 1));
  check(table.columns[janT].rollForward && near(line('accOpening').values[janT], yRows[11].eur.accClosing), `January ${T} is rolled forward: it opens at December's accumulated profit`);
  const kpis = model.computePnlKpis(payload, 'plan', 'eur');
  const ytd = yRows.filter((r) => r.status === 'actual').reduce((s, r) => s + r.eur.ebitda, 0);
  check(near(kpis.ebitdaYtd.value, ytd, 0.05) && kpis.ebitdaYtd.through === mk(Y, 9), 'KPI: EBITDA year to date from the NetSuite months');
  check(kpis.netByYear.length === 2 && near(kpis.accumulatedEnd.value, payload.variants.plan.years[1].rows[11].eur.accClosing), 'KPI: net profit per year and the accumulated profit at the end');
}

async function main() {
  console.log('=== pnl-projection checks (synthetic data) ===');
  testMapping();
  const computed = await testPayload();
  await testDegraded();
  const { cacheFile } = await testHandler(computed);
  await testBreakdowns(computed, cacheFile);
  await testModel(computed.payload);
  if (process.argv.includes('--write-fixture')) {
    const file = path.join(__dirname, 'fixtures', 'pnl-projection-sample.json');
    fs.writeFileSync(file, JSON.stringify({ ...computed.payload, cache: { ageSec: 0, stale: false, staleReason: null, refreshing: false, lastError: null } }, null, 2) + '\n');
    console.log(`\nwrote ${path.relative(ROOT, file)}`);
  }
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
