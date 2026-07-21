#!/usr/bin/env node
// ============================================================================
// test-forecast-core.cjs — correctness gate for the extracted forecast engine.
//
// Two modes:
//
//   1. GOLDEN mode (the real fidelity gate). If a fixture exists at
//      scripts/fixtures/forecast-golden.json, it must contain:
//        { "inputs": <ForecastInputs, with `nowISO` instead of a Date>,
//          "expected": <the ForecastRow[] the live dashboard displayed> }
//      The test rebuilds `now` from `nowISO`, runs computeCashflowForecast on
//      the captured inputs, and asserts EVERY month's key figures match the
//      captured dashboard values with a €0 / ₪0 diff. Any nonzero diff → exit 1.
//      (Capture the fixture from the running dashboard — see
//      docs/forecast-core-golden-test.md.)
//
//   2. SMOKE mode (always runs). A synthetic-but-representative inputs object
//      exercises every branch (past actuals, current-month anchor + proration,
//      future budget, lastActual dept projection, pipeline methodology, churn,
//      currency-defense reval, per-month scenario adjustments). It asserts the
//      engine returns 12 finite rows, the opening/net/reval/closing cascade is
//      internally consistent, and the function is deterministic. This proves
//      the module LOADS and RUNS anywhere (CI, a fresh clone) even with no
//      live NetSuite/Snowflake access.
//
// Usage:  node scripts/test-forecast-core.cjs
// Exit 0 = all checks passed; exit 1 = a mismatch or thrown error.
// ============================================================================

const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');
// The engine is an ESM module (.mjs); load it via dynamic import (works from CJS).
let computeCashflowForecast;

let failures = 0;
const fail = (msg) => { failures++; console.error('  ✗ ' + msg); };
const ok = (msg) => console.log('  ✓ ' + msg);

// Fields compared in golden mode (the finance-relevant outputs).
const GOLDEN_FIELDS = [
  'openingBalance', 'salary', 'vendors', 'other', 'collections', 'pipelineWeighted',
  'churnDeduction', 'net', 'revalImpact', 'closingBalance',
  'openingBalanceILS', 'salaryILS', 'vendorsILS', 'collectionsILS', 'netILS',
  'revalImpactILS', 'closingBalanceILS',
];

// ── Mode 1: GOLDEN ──────────────────────────────────────────────────────────
function runGolden() {
  const fixturePath = path.join(__dirname, 'fixtures', 'forecast-golden.json');
  if (!fs.existsSync(fixturePath)) {
    console.log('GOLDEN: no fixture at scripts/fixtures/forecast-golden.json — skipping.');
    console.log('        (Capture one from the running dashboard to lock the €0-diff gate;');
    console.log('         see docs/forecast-core-golden-test.md.)');
    return;
  }
  console.log('GOLDEN: scripts/fixtures/forecast-golden.json');
  const raw = JSON.parse(fs.readFileSync(fixturePath, 'utf-8'));
  const inputs = raw.inputs || {};
  const expected = raw.expected || [];
  // JSON can't carry a Date — rebuild `now` from nowISO (or now ms).
  if (inputs.nowISO) inputs.now = new Date(inputs.nowISO);
  else if (typeof inputs.now === 'number') inputs.now = new Date(inputs.now);

  const rows = computeCashflowForecast(inputs);
  if (rows.length !== expected.length) {
    fail(`row count: got ${rows.length}, expected ${expected.length}`);
    return;
  }
  let diffs = 0;
  for (let i = 0; i < rows.length; i++) {
    for (const f of GOLDEN_FIELDS) {
      const got = Math.round(Number(rows[i][f]) || 0);
      const exp = Math.round(Number(expected[i][f]) || 0);
      if (got !== exp) {
        diffs++;
        fail(`${rows[i].mKey} ${f}: got ${got.toLocaleString()}, expected ${exp.toLocaleString()} (Δ ${(got - exp).toLocaleString()})`);
      }
    }
  }
  if (diffs === 0) {
    const dec = rows[11];
    ok(`all ${rows.length} months match the captured dashboard values (€0 diff).`);
    ok(`December closing (the pushed FORECAST_EUR): €${Math.round(dec.closingBalance).toLocaleString()}`);
  } else {
    fail(`${diffs} field mismatch(es) vs the live dashboard — the port is NOT faithful.`);
  }
}

// ── Mode 2: SMOKE (synthetic fixture) ────────────────────────────────────────
function buildSyntheticInputs() {
  const mk = (y, m) => `${y}-${String(m).padStart(2, '0')}`;
  const Y = 2026;
  return {
    activeYear: Y,
    currentYear: Y,
    now: new Date('2026-04-15T12:00:00'), // Jan-Mar past, Apr current, May-Dec future
    asOfDate: null,
    ilsRevalRate: 3.59,

    book: { openingBalance: 40_000_000, currentBalance: 45_000_000, adjustedCurrentBalance: 45_000_000 },
    bookLocal: { openingBalance: 150_000_000, currentBalance: 168_750_000, adjustedCurrentBalance: 168_750_000 },
    yearStartBalance: { eur: 40_000_000, ils: 150_000_000 },
    prevMonthEndBalance: { eur: 46_000_000, ils: 172_500_000 },
    liveFxRate: 3.75,
    fxRateByYear: {},

    salaryData: [
      { month: '2026-01', amountEUR: 3_000_000, amountILS: 11_250_000 },
      { month: '2026-02', amountEUR: 3_050_000, amountILS: 11_437_500 },
      { month: '2026-03', amountEUR: 3_100_000, amountILS: 11_625_000 },
    ],
    salaryProjectionMode: 'lastActual',
    lastActualSalaryMonth: '2026-03',
    salaryActualsByDept: { '2026-03': { Sales: { eur: 1_000_000 }, 'R&D': { eur: 1_500_000 }, 'G&A': { eur: 600_000 } } },
    salaryDeptBudgets: {},
    salaryDeptAdj: { '2026-06': { Sales: -10 } },
    salaryAdjPctByMonth: { 5: 5 }, // +5% from June (index 5)
    sfSalaryOverrides: [{ mKey: '2026-07', mode: 'Add', newVal: 0, oldVal: 0, amountEUR: 50_000 }],
    sfSalaryBudget: {},
    salaryManualILS: {},
    monthlyHCImpact: {},
    sfActualsSplit: {},

    vendorBills: [{ amountEUR: 500_000 }],
    vendorActuals: [
      { month: '2026-01', amountEUR: 1_400_000 },
      { month: '2026-02', amountEUR: 1_450_000 },
      { month: '2026-03', amountEUR: 1_500_000 },
    ],
    nsPaidVendors: { byMonth: {} },
    vendorHistory: [],
    sfBudget: {
      totalByMonth: Object.fromEntries([4, 5, 6, 7, 8, 9, 10, 11].map(m => [mk(Y, m + 1), { eur: 1_600_000 }])),
      byMonth: Object.fromEntries([4, 5, 6, 7, 8, 9, 10, 11].map(m => [mk(Y, m + 1), { Marketing: 300_000, Cloud: 400_000 }])),
    },
    nsBudget: {
      byMonth: Object.fromEntries(Array.from({ length: 12 }, (_, m) => [mk(Y, m + 1), {
        salary: 3_100_000, vendors: 1_600_000, revenue: 6_000_000, categories: { 'Other (800)': 250_000 },
      }])),
    },
    expenseCategories: { byMonth: {} },
    vendorCatAdj: { '2026-06': { Marketing: -20 } },
    vendorDetailAdj: {},

    sfRevenuePaid: {
      '2026-01': { revenue: 5_200_000, customers: 260, paid: 5_000_000, unpaid: 200_000 },
      '2026-02': { revenue: 5_300_000, customers: 262, paid: 5_100_000, unpaid: 200_000 },
      '2026-03': { revenue: 5_400_000, customers: 265, paid: 5_200_000, unpaid: 200_000 },
    },
    actualCollections: { '2026-01': 5_000_000, '2026-02': 5_100_000, '2026-03': 5_200_000, '2026-04': 2_000_000 },
    collPctByMonth: {},
    sfRevenue: { budget: Object.fromEntries(Array.from({ length: 12 }, (_, m) => [mk(Y, m + 1), { eur: 6_000_000 }])) },
    revenueActuals: [],
    customerReceipts: { '2026-01': 5_050_000, '2026-02': 5_150_000, '2026-03': 5_250_000 },

    sfPipeline: [
      { probability: 80, closeDate: '2026-05-15', amount: 400_000 },
      { probability: 60, closeDate: '2026-07-10', amount: 300_000 },
      { probability: 20, closeDate: '2026-02-01', amount: 250_000 }, // low-conf
    ],
    pipelineMinProb: 50,
    sfConversion: { yearly: [{ year: 2023, winRate: 33, avgWonDays: 60 }, { year: 2024, winRate: 35, avgWonDays: 55 }] },
    pipelineAdjPctByMonth: {},
    revenueMethodology: 'pipeline',
    pipelineMethodology: { byMonth: Object.fromEntries([4, 5, 6, 7, 8, 9, 10, 11].map(m => [mk(Y, m + 1), { monthlyContribution: 120_000 }])) },

    sfChurnQuarterly: [{ partial: false, qs: '2026-Q1', amount: 150_000 }],
    churnData: [{ year: 2026, monthlyImpact: 45_000 }],
    churnMonthlyAvg: 40_000,
    churnOverride: {},

    monthlyReval: {
      preYear: { eur: 0, ils: 0 },
      byMonth: {
        '2026-02': { eur: 120_000, ils: 450_000, hasBothEnds: true },
        '2026-03': { eur: -80_000, ils: -300_000, hasBothEnds: true },
      },
    },
    nsBankClassified: { byMonth: {} },
    currencyDefensePct: 30,
    currencyDefensePctByMonth: {},
    sfFinanceBudget: Object.fromEntries([3, 4, 5, 6, 7, 8, 9, 10, 11].map(m => [mk(Y, m + 1), { eur: 300_000 }])),
  };
}

function runSmoke() {
  console.log('\nSMOKE: synthetic representative inputs');
  const inputs = buildSyntheticInputs();
  let rows;
  try {
    rows = computeCashflowForecast(inputs);
  } catch (e) {
    fail('engine threw: ' + e.stack);
    return;
  }

  // 12 rows
  if (rows.length === 12) ok('returns 12 rows'); else fail(`expected 12 rows, got ${rows.length}`);

  // all key numbers finite
  const NUMS = ['openingBalance', 'salary', 'vendors', 'other', 'collections', 'pipelineWeighted',
    'churnDeduction', 'net', 'revalImpact', 'closingBalance', 'totalOutflow',
    'openingBalanceILS', 'salaryILS', 'vendorsILS', 'collectionsILS', 'netILS', 'revalImpactILS', 'closingBalanceILS'];
  let nonFinite = 0;
  for (const r of rows) for (const f of NUMS) if (!Number.isFinite(r[f])) { nonFinite++; fail(`${r.mKey} ${f} not finite: ${r[f]}`); }
  if (nonFinite === 0) ok('every monetary field is finite (no NaN cascade)');

  // cascade: closing === opening + net + reval (exact integer arithmetic)
  let cascadeBad = 0;
  for (const r of rows) {
    if (Math.round(r.closingBalance) !== Math.round(r.openingBalance + r.net + r.revalImpact)) {
      cascadeBad++;
      fail(`${r.mKey} cascade: closing ${Math.round(r.closingBalance)} != opening+net+reval ${Math.round(r.openingBalance + r.net + r.revalImpact)}`);
    }
  }
  if (cascadeBad === 0) ok('closing = opening + net + reval for all months');

  // opening chains from prior closing, except the current-month anchor
  let chainBad = 0;
  for (let i = 1; i < rows.length; i++) {
    if (rows[i].isCurrent) continue; // April resets to prevMonthEndBalance
    if (Math.round(rows[i].openingBalance) !== Math.round(rows[i - 1].closingBalance)) {
      chainBad++;
      fail(`${rows[i].mKey} opening ${Math.round(rows[i].openingBalance)} != prior closing ${Math.round(rows[i - 1].closingBalance)}`);
    }
  }
  if (chainBad === 0) ok('opening balance chains from prior closing (bar the current-month anchor)');

  // targeted behavioral locks
  const jan = rows[0], apr = rows[3], may = rows[4];
  if (Math.round(jan.salary) === 3_000_000) ok('Jan salary = NS actual (3,000,000)'); else fail(`Jan salary ${jan.salary} != 3,000,000`);
  if (Math.round(jan.collections) === 5_050_000) ok('Jan collections = customer receipts (5,050,000)'); else fail(`Jan collections ${jan.collections} != 5,050,000`);
  if (apr.isCurrent) ok('April flagged current'); else fail('April not flagged current');
  if (Math.round(apr.openingBalance) === 46_000_000) ok('April opening anchored to prevMonthEndBalance (46,000,000)'); else fail(`April opening ${apr.openingBalance} != 46,000,000`);
  // May (future): currency-defense reval = round(300000 * 30/100) = 90000, no past reval
  if (Math.round(may.revalImpact) === 90_000) ok('May reval = currency-defense (90,000)'); else fail(`May reval ${may.revalImpact} != 90,000`);
  if (Math.round(may.revalImpactILS) === Math.round(90_000 * 3.59)) ok('May reval ILS uses ilsRevalRate 3.59 (323,100)'); else fail(`May revalILS ${may.revalImpactILS} != ${Math.round(90_000 * 3.59)}`);
  // churn is cumulative on future months: May is forecastMonthIndex 1 → 50000*1 (quarterly 150000/3)
  if (Math.round(may.churnDeduction) === 50_000) ok('May churn = quarterly run-rate × 1 (50,000)'); else fail(`May churn ${may.churnDeduction} != 50,000`);

  // determinism
  const a = JSON.stringify(computeCashflowForecast(inputs));
  const b = JSON.stringify(computeCashflowForecast(inputs));
  if (a === b) ok('deterministic (same inputs → identical rows)'); else fail('non-deterministic output');

  // human-readable dump
  console.log('\n  month     opening        salary      vendors   collections     net        reval      closing');
  for (const r of rows) {
    const p = (n, w) => String(Math.round(n).toLocaleString()).padStart(w);
    console.log(`  ${r.mKey}  ${p(r.openingBalance, 12)} ${p(r.salary, 11)} ${p(r.vendors, 11)} ${p(r.collections, 12)} ${p(r.net, 11)} ${p(r.revalImpact, 10)} ${p(r.closingBalance, 13)}`);
  }
  console.log(`\n  → December closing (pushed FORECAST_EUR): €${Math.round(rows[11].closingBalance).toLocaleString()}`);
}

// ── Mode 3: BANK-RECONCILE (closed-month cash-basis override) ────────────────
// Proves that when the bank-classified feed (bcm) is COMPLETE for a closed month, the engine drives
// salary/vendors/collections from bank-side cash (not the accrual/receipt feeds) and the month's
// closing reconciles to the NS bank delta by construction (net + reval == bcm.total). Also proves a
// BROKEN pre-close month (≈0 salary, e.g. a seed generated before the month closed) falls back to
// the accrual feeds instead of collapsing the month.
function runBankReconcile() {
  console.log('\nBANK-RECONCILE: closed-month bcm cash-basis override');
  const inputs = buildSyntheticInputs();
  // Jan/Feb: COMPLETE bcm whose cash deliberately DIFFERS from the accrual/receipt feeds
  // (salary 3.0M accrual vs 2.4M cash-paid; vendors 1.4M accrual vs 1.9M paid; receipts 5.05M vs
  // 5.10M bank). Mar: BROKEN pre-close bcm (~0 salary) → must fall back to accrual.
  const complete = (coll, sal, ven, other, reval) => ({
    collections: { eur: coll,  ils: Math.round(coll * 3.75) },
    salary:      { eur: sal,   ils: Math.round(sal  * 3.75) }, // signed cash-out (negative)
    vendors:     { eur: ven,   ils: Math.round(ven  * 3.75) },
    other:       { eur: other, ils: Math.round(other * 3.75) },
    reval:       { eur: reval, ils: Math.round(reval * 3.59) },
    total:       { eur: coll + sal + ven + other + reval, ils: 0 },
  });
  inputs.nsBankClassified = { byMonth: {
    '2026-01': complete(5_100_000, -2_400_000, -1_900_000, -50_000, 300_000),
    '2026-02': complete(5_120_000, -2_450_000, -1_950_000,  30_000,  90_000),
    '2026-03': { collections: { eur: 400_000, ils: 1_500_000 }, salary: { eur: 0, ils: 0 },
      vendors: { eur: -200_000, ils: -750_000 }, other: { eur: -1_000, ils: -3_750 },
      reval: { eur: -2_000_000, ils: -7_180_000 }, total: { eur: -1_801_000, ils: 0 } },
  } };
  // Reval both-ends so the guard can adopt bcm.reval for Jan/Feb (consistent with P&L reval).
  inputs.monthlyReval = { preYear: { eur: 0, ils: 0 }, byMonth: {
    '2026-01': { eur: 300_000, ils: 1_077_000, hasBothEnds: true },
    '2026-02': { eur: 90_000,  ils: 323_100,  hasBothEnds: true },
    '2026-03': { eur: -2_000_000, ils: -7_180_000, hasBothEnds: true },
  } };
  const rows = computeCashflowForecast(inputs);
  const jan = rows[0], feb = rows[1], mar = rows[2];

  if (Math.round(jan.salary) === 2_400_000) ok('Jan salary = bank cash-paid (2,400,000, not 3.0M accrual)'); else fail(`Jan salary ${jan.salary} != 2,400,000`);
  if (Math.round(jan.vendors) === 1_900_000) ok('Jan vendors = bank cash-paid (1,900,000, not 1.4M accrual)'); else fail(`Jan vendors ${jan.vendors} != 1,900,000`);
  if (Math.round(jan.collections) === 5_100_000) ok('Jan collections = bank cash-in (5,100,000, not 5.05M receipts)'); else fail(`Jan collections ${jan.collections} != 5,100,000`);
  const janTotal = 5_100_000 - 2_400_000 - 1_900_000 - 50_000 + 300_000; // = 1,050,000 (bcm.total)
  if (Math.round(jan.closingBalance) === Math.round(inputs.yearStartBalance.eur + janTotal)) ok(`Jan closing == opening + bank delta (${(inputs.yearStartBalance.eur + janTotal).toLocaleString()})`); else fail(`Jan closing ${Math.round(jan.closingBalance)} != ${inputs.yearStartBalance.eur + janTotal}`);
  const febTotal = 5_120_000 - 2_450_000 - 1_950_000 + 30_000 + 90_000; // = 840,000
  for (const [r, tot, lbl] of [[jan, janTotal, 'Jan'], [feb, febTotal, 'Feb']]) {
    if (Math.round(r.net + r.revalImpact) === Math.round(tot)) ok(`${lbl} net+reval == bcm.total (${Math.round(tot).toLocaleString()})`); else fail(`${lbl} net+reval ${Math.round(r.net + r.revalImpact)} != bcm.total ${Math.round(tot)}`);
  }
  // ILS twin also driven from bcm (not derived via ratio)
  if (Math.round(jan.salaryILS) === Math.round(2_400_000 * 3.75)) ok('Jan salaryILS = bank cash-paid ILS (9,000,000)'); else fail(`Jan salaryILS ${jan.salaryILS} != ${Math.round(2_400_000 * 3.75)}`);
  // Mar: BROKEN bcm (≈0 salary) → falls back to the accrual feed (3.1M), does NOT collapse
  if (Math.round(mar.salary) === 3_100_000) ok('Mar (broken bcm) salary falls back to accrual (3,100,000)'); else fail(`Mar salary ${mar.salary} != 3,100,000 (fallback broke)`);
}

// ── Mode 4: DIVIDEND-EXCLUSION (operating view) ──
// A dividend in a past month is stripped from Vendors/Other and added back to closing for that month
// and every later month (cumulative), surviving the current-month re-anchor. The cascade identity
// closing = opening + net + reval must still hold. No-op when the input is absent.
function runDividendExclusion() {
  console.log('\nDIVIDEND-EXCLUSION: strip from Vendors/Other → closing rises, carries to year-end');
  const base = buildSyntheticInputs();               // now = 2026-04-15 → Feb (idx1) past, Apr (idx3) current
  const withDiv = computeCashflowForecast(base);
  const excl = { byMonth: { '2026-02': { distributionEUR: -300_000, whtEUR: -50_000, distributionILS: -1_100_000, whtILS: -180_000 } } };
  const opView = computeCashflowForecast({ ...base, dividendExclusions: excl });
  const T = 350_000; // distribution 300k + WHT 50k
  const d = (arr, i, k) => Math.round(opView[i][k] - withDiv[i][k]);
  if (Math.round(withDiv[1].vendors - opView[1].vendors) === 300_000) ok('Feb vendors −300,000'); else fail(`Feb vendors Δ ${Math.round(withDiv[1].vendors - opView[1].vendors)} != 300,000`);
  if (Math.round(withDiv[1].other - opView[1].other) === 50_000) ok('Feb other −50,000'); else fail(`Feb other Δ ${Math.round(withDiv[1].other - opView[1].other)} != 50,000`);
  if (d(opView, 1, 'net') === T) ok('Feb net +350,000'); else fail(`Feb net Δ ${d(opView,1,'net')} != 350,000`);
  if (d(opView, 1, 'closingBalance') === T) ok('Feb closing +350,000'); else fail(`Feb closing Δ ${d(opView,1,'closingBalance')} != 350,000`);
  // dividend must drop vendorsBase too, so it is NOT mis-counted as a vendor "saving" (Σ(vendorsBase − vendors))
  if (Math.round(withDiv[1].vendorsBase - opView[1].vendorsBase) === 300_000) ok('Feb vendorsBase −300,000 (base excludes dividend)'); else fail(`Feb vendorsBase Δ ${Math.round(withDiv[1].vendorsBase - opView[1].vendorsBase)} != 300,000`);
  if (Math.round(opView[1].vendorsBase - opView[1].vendors) === 0) ok('Feb vendors === vendorsBase (no phantom saving)'); else fail(`Feb base−after ${Math.round(opView[1].vendorsBase - opView[1].vendors)} != 0`);
  if (d(opView, 2, 'closingBalance') === T) ok('Mar closing +350,000 (carries)'); else fail(`Mar closing Δ ${d(opView,2,'closingBalance')} != 350,000`);
  if (d(opView, 11, 'closingBalance') === T) ok('Dec closing +350,000 (survives Apr re-anchor → year-end)'); else fail(`Dec closing Δ ${d(opView,11,'closingBalance')} != 350,000`);
  if (d(opView, 0, 'closingBalance') === 0) ok('Jan (pre-dividend) unchanged'); else fail(`Jan closing Δ ${d(opView,0,'closingBalance')} != 0`);
  let bad = 0;
  for (const r of opView) if (Math.round(r.closingBalance) !== Math.round(r.openingBalance + r.net + r.revalImpact)) bad++;
  if (bad === 0) ok('cascade closing = opening + net + reval holds after exclusion'); else fail(`${bad} cascade break(s) after exclusion`);
  // no-op guard: absent input → identical to base
  const noop = computeCashflowForecast({ ...base, dividendExclusions: null });
  if (Math.round(noop[11].closingBalance) === Math.round(withDiv[11].closingBalance)) ok('null dividendExclusions is a no-op'); else fail('null dividendExclusions changed output');
}

// ── Mode 5: ILS-ANCHORED PROJECTION SALARY ──
// A projection year's salary is payroll-ILS anchored: EUR = source-year ILS average ÷ the
// user-set FY rate (fxRateByYear[year]). Moving the rate moves the EUR line while the ILS
// line stays fixed; entries with no ILS anchor keep the EUR carry (non-ILS subsidiaries).
// The current year is untouched (anchor applies only when forecastYear !== currentYear).
function runIlsAnchorProjection() {
  console.log('\nILS-ANCHOR: projection-year salary = ILS avg ÷ FY rate');
  const projInputs = () => {
    const base = buildSyntheticInputs();
    base.activeYear = 2027;                       // projection year (currentYear stays 2026)
    base.fxRateByYear = { 2027: 3.5 };
    // Isolate salary: clear feeds/adjustments that would layer deltas on top.
    base.salaryData = []; base.sfActualsSplit = {};
    base.salaryAdjPctByMonth = {}; base.salaryDeptAdj = {}; base.sfSalaryOverrides = [];
    base.monthlyHCImpact = {}; base.salaryManualILS = {};
    base.nsBankClassified = { byMonth: {} };
    base.monthlyReval = { preYear: { eur: 0, ils: 0 }, byMonth: {} };
    base.nsBudget = { byMonth: {} };
    base.sfBudget = { totalByMonth: {}, byMonth: {} };
    base.sfSalaryBudget = {};
    // Projection bake shape: synthetic AVG dept basis carrying BOTH eur and ils.
    // ILS total 8,400,000 → ÷3.5 = 2,400,000 ≠ the 2,200,000 EUR carry, so the
    // assertions can tell which anchor won.
    base.salaryProjectionMode = 'lastActual';
    base.lastActualSalaryMonth = '2026-AVG';
    base.salaryActualsByDept = { '2026-AVG': {
      Sales: { eur: 1_200_000, ils: 4_550_000 },
      'R&D': { eur: 1_000_000, ils: 3_850_000 },
    } };
    return base;
  };

  // 1. lastActual carry anchors on ILS ÷ rate
  const a = computeCashflowForecast(projInputs());
  if (Math.round(a[5].salary) === 2_400_000) ok('salary = ILS 8.4M ÷ 3.5 = 2,400,000 (not the 2.2M EUR carry)'); else fail(`salary ${Math.round(a[5].salary)} != 2,400,000`);
  if (Math.round(a[5].salaryILS) === 8_400_000) ok('salaryILS stays the 8,400,000 anchor'); else fail(`salaryILS ${Math.round(a[5].salaryILS)} != 8,400,000`);

  // 2. changing the FY rate moves EUR, not ILS
  const b1 = projInputs(); b1.fxRateByYear = { 2027: 4.2 };
  const b = computeCashflowForecast(b1);
  if (Math.round(b[5].salary) === 2_000_000) ok('rate 3.5→4.2: salary EUR drops to 2,000,000'); else fail(`salary at 4.2 ${Math.round(b[5].salary)} != 2,000,000`);
  if (Math.round(b[5].salaryILS) === 8_400_000) ok('rate 3.5→4.2: salaryILS unchanged (ILS is the anchor)'); else fail(`salaryILS at 4.2 ${Math.round(b[5].salaryILS)} != 8,400,000`);

  // 3. no ILS anchor → EUR carry preserved (non-ILS subsidiaries)
  const c1 = projInputs();
  c1.salaryActualsByDept = { '2026-AVG': { Sales: { eur: 1_200_000, ils: 0 }, 'R&D': { eur: 1_000_000, ils: 0 } } };
  const c = computeCashflowForecast(c1);
  if (Math.round(c[5].salary) === 2_200_000) ok('ils=0 → falls back to the 2,200,000 EUR carry'); else fail(`fallback salary ${Math.round(c[5].salary)} != 2,200,000`);

  // 3b. NO explicit FY rate set (left empty / reset) → plain EUR carry, even though ILS exists
  //     and a live/derived market rate is available. Never ILS ÷ market rate.
  const e1 = projInputs();
  e1.fxRateByYear = {}; // user never set (or reset) the FY2027 rate
  const e = computeCashflowForecast(e1);
  if (Math.round(e[5].salary) === 2_200_000) ok('no FY rate set → salary stays the 2,200,000 EUR carry (not ILS ÷ market rate)'); else fail(`no-rate salary ${Math.round(e[5].salary)} != 2,200,000`);

  // 4. sfSalaryBudget branch (consolidated bake) anchors on ILS too
  const d1 = projInputs();
  d1.salaryProjectionMode = 'budget'; d1.lastActualSalaryMonth = ''; d1.salaryActualsByDept = {};
  d1.sfSalaryBudget = Object.fromEntries(Array.from({ length: 12 }, (_, m) => [`2027-${String(m + 1).padStart(2, '0')}`, { eur: 2_300_000, ils: 8_750_000 }]));
  const d = computeCashflowForecast(d1);
  if (Math.round(d[5].salary) === 2_500_000) ok('budget path: salary = ILS 8.75M ÷ 3.5 = 2,500,000 (not the 2.3M EUR)'); else fail(`budget-path salary ${Math.round(d[5].salary)} != 2,500,000`);
}

// ── main ──────────────────────────────────────────────────────────────────
async function main() {
  const coreUrl = pathToFileURL(path.join(__dirname, '..', 'src', 'forecast', 'forecast-core.mjs')).href;
  ({ computeCashflowForecast } = await import(coreUrl));
  console.log('=== forecast-core golden/smoke test ===\n');
  runGolden();
  runSmoke();
  runBankReconcile();
  runDividendExclusion();
  runIlsAnchorProjection();
  console.log('');
  if (failures === 0) { console.log('✅ PASS — all checks green.'); process.exit(0); }
  else { console.error(`❌ FAIL — ${failures} check(s) failed.`); process.exit(1); }
}
main().catch((e) => { console.error('❌ FAIL — test harness threw:', e.stack || e); process.exit(1); });
