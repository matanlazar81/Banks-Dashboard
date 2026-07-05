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

// ── main ──────────────────────────────────────────────────────────────────
async function main() {
  const coreUrl = pathToFileURL(path.join(__dirname, '..', 'src', 'forecast', 'forecast-core.mjs')).href;
  ({ computeCashflowForecast } = await import(coreUrl));
  console.log('=== forecast-core golden/smoke test ===\n');
  runGolden();
  runSmoke();
  console.log('');
  if (failures === 0) { console.log('✅ PASS — all checks green.'); process.exit(0); }
  else { console.error(`❌ FAIL — ${failures} check(s) failed.`); process.exit(1); }
}
main().catch((e) => { console.error('❌ FAIL — test harness threw:', e.stack || e); process.exit(1); });
