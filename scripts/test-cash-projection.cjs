#!/usr/bin/env node
// ============================================================================
// test-cash-projection.cjs — checks for the New Bank Dashboard projection.
// No dependencies, no NetSuite/Snowflake access: every input is SYNTHETIC.
//
//   node scripts/test-cash-projection.cjs                  run all checks
//   node scripts/test-cash-projection.cjs --write-fixture  also (re)write the synthetic UI fixture
//                                                         scripts/fixtures/cash-projection-sample.json
//
// Covers: roll-forward helpers (src/forecast/roll-forward.mjs), the end-to-end payload from
// computeCashProjection (server/cash-projection.cjs) with a stubbed input gatherer, the handler's
// cache/polling state machine, the NetSuite queue wrapper, and the UI table model
// (src/new-dashboard/model.ts, imported directly — Node strips the TypeScript types).
// Exit 0 = all checks passed; 1 = a failure.
// ============================================================================
const fs = require('fs');
const os = require('os');
const path = require('path');
const { pathToFileURL } = require('url');

const ROOT = path.resolve(__dirname, '..');
const cmp = require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs'));
const cp = require(path.join(ROOT, 'server', 'cash-projection.cjs'));

let failures = 0;
const fail = (msg) => { failures++; console.error('  ✗ ' + msg); };
const ok = (msg) => console.log('  ✓ ' + msg);
const check = (cond, msg, detail) => (cond ? ok(msg) : fail(detail ? `${msg} — ${detail}` : msg));
const near = (a, b, tol) => Math.abs(a - b) <= tol;

// The compute module logs every feed; keep the test output readable.
async function quiet(fn) {
  const { log, warn } = console;
  console.log = () => {}; console.warn = () => {};
  try { return await fn(); } finally { console.log = log; console.warn = warn; }
}

// ── synthetic data (illustrative only, not LSports figures) ────────────────
const Y = 2026;
const T = 2027;
const NOW = new Date('2026-10-15T12:00:00'); // Jan–Sep actual, Oct current, Nov–Dec forecast
const R = 3.7;                                 // EUR→ILS used to build the ILS sides
const mk = (y, m) => `${y}-${String(m).padStart(2, '0')}`;
const range = (a, b) => Array.from({ length: b - a + 1 }, (_, i) => a + i);
const clone = (x) => JSON.parse(JSON.stringify(x));

const PAST = {
  coll: [3_300_000, 3_340_000, 3_380_000, 3_360_000, 3_420_000, 3_450_000, 3_470_000, 3_440_000, 3_520_000],
  sal: [2_100_000, 2_120_000, 2_150_000, 2_180_000, 2_200_000, 2_220_000, 2_250_000, 2_240_000, 2_260_000],
  ven: [1_050_000, 1_080_000, 1_100_000, 1_120_000, 2_650_000, 1_130_000, 1_160_000, 1_180_000, 1_200_000], // May incl. €1.5M dividend
  oth: [-40_000, -35_000, -60_000, -45_000, -200_000, -42_000, -38_000, -55_000, -47_000],                  // May incl. €150K WHT
  rev: [25_000, -18_000, 40_000, -12_000, 30_000, -22_000, 15_000, 8_000, -20_000],
};
const CATEGORIES = { Cloud: 420_000, Marketing: 260_000, Data: 300_000, Office: 120_000, 'Professional services': 90_000 };
const PLAN = {
  currencyDefensePct: 30,
  pipelineMinProb: 100,
  adjustmentsByYear: { [String(Y)]: { salaryAdjPctByMonth: { 10: -4, 11: -4 }, collPctByMonth: {}, pipelineAdjPctByMonth: {}, currencyDefensePctByMonth: {} } },
  vendorCatAdj: { [mk(Y, 11)]: { Marketing: -25 } },
  salaryDeptAdj: {},
  vendorDetailAdj: {},
  fxRateByYear: {},
};
const BREAKDOWN = [
  { department: 'R&D', account: '760001', accountId: 1, name: 'Salaries', amountEUR: 1_100_000, amountILS: 4_070_000 },
  { department: 'Sales', account: '760001', accountId: 1, name: 'Salaries', amountEUR: 640_000, amountILS: 2_368_000 },
  { department: 'G&A', account: '760001', accountId: 1, name: 'Salaries', amountEUR: 440_000, amountILS: 1_628_000 },
  { department: 'Operations', account: '760002', accountId: 2, name: 'Social benefits', amountEUR: 180_000, amountILS: 666_000 },
];

function syntheticGather() {
  const bcm = {};
  let bank = 8_120_000;
  range(1, 9).forEach((m, i) => {
    const b = (eur, rate = R) => ({ eur, ils: Math.round(eur * rate) });
    const total = PAST.coll[i] - PAST.sal[i] - PAST.ven[i] + PAST.oth[i] + PAST.rev[i];
    bank += total;
    bcm[mk(Y, m)] = {
      collections: b(PAST.coll[i]), salary: b(-PAST.sal[i]), vendors: b(-PAST.ven[i]),
      other: b(PAST.oth[i]), reval: b(PAST.rev[i], 3.59), total: b(total),
      details: [{ label: 'Synthetic bank line', bucket: 'other', eur: PAST.oth[i], ils: 0 }],
    };
  });
  const sfBudgetByMonth = Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { ...CATEGORIES }]));
  const catTotal = Object.values(CATEGORIES).reduce((s, v) => s + v, 0);
  const inputs = {
    salaryData: range(1, 9).map((m, i) => ({ month: mk(Y, m), amountEUR: PAST.sal[i], amountILS: Math.round(PAST.sal[i] * R) })),
    salaryActualsByDept: { [mk(Y, 9)]: {
      'R&D': { eur: 1_050_000, ils: 3_885_000 }, Sales: { eur: 620_000, ils: 2_294_000 },
      'G&A': { eur: 420_000, ils: 1_554_000 }, Operations: { eur: 170_000, ils: 629_000 },
    } },
    salaryDeptBudgets: {},
    sfSalaryBudget: Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { eur: 2_300_000, ils: 8_510_000 }])),
    sfSalaryOverrides: [],
    monthlyHCImpact: { [mk(Y, 11)]: { running: 74_000 }, [mk(Y, 12)]: { running: 148_000 } },
    sfActualsSplit: Object.fromEntries(range(1, 9).map((m, i) => [mk(Y, m), { salary: PAST.sal[i], salaryILS: Math.round(PAST.sal[i] * R), vendors: PAST.ven[i] - (m === 5 ? 1_500_000 : 0), vendorsILS: 0 }])),
    vendorBills: [{ amountEUR: 420_000 }],
    vendorActuals: [],
    nsPaidVendors: { byMonth: {}, grid: {}, accounts: [] },
    vendorHistory: [{ paidDate: '2026-03-12', amountEUR: 50_000, vendor: 'Synthetic vendor' }],
    sfBudget: { byMonth: sfBudgetByMonth, totalByMonth: Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { eur: catTotal, ils: Math.round(catTotal * R) }])) },
    nsBudget: { byMonth: {} },
    expenseCategories: { byMonth: {}, categories: [] },
    sfRevenuePaid: Object.fromEntries(range(1, 12).map((m) => {
      const revenue = 3_400_000 + (m - 1) * 20_000;
      return [mk(Y, m), { revenue, customers: 410 + m * 2, paid: m <= 9 ? revenue - 60_000 : 0, unpaid: m <= 9 ? 60_000 : revenue }];
    })),
    actualCollections: { ...Object.fromEntries(range(1, 9).map((m, i) => [mk(Y, m), PAST.coll[i] - 20_000])), [mk(Y, 10)]: 1_450_000 },
    sfRevenue: { budget: Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { eur: 3_600_000 }])), actuals: {}, targets: { [mk(Y, 12)]: { eur: 3_700_000 } } },
    revenueActuals: [],
    customerReceipts: Object.fromEntries(range(1, 9).map((m, i) => [mk(Y, m), PAST.coll[i] - 15_000])),
    sfPipeline: [
      { name: 'Synthetic deal A', owner: 'Synthetic owner', probability: 60, closeDate: '2026-11-20', amount: 180_000 },
      { name: 'Synthetic deal B', owner: 'Synthetic owner', probability: 100, closeDate: '2026-12-10', amount: 40_000 },
    ],
    sfConversion: { yearly: [{ year: 2024, winRate: 34, avgWonDays: 62 }, { year: 2025, winRate: 36, avgWonDays: 58 }], stages: [], customers: [], projection: [] },
    pipelineMethodology: { byMonth: { [mk(Y, 11)]: { monthlyContribution: 55_000 }, [mk(Y, 12)]: { monthlyContribution: 70_000 } } },
    sfChurnQuarterly: [{ partial: false, qs: '2026-07', amount: 84_000 }],
    churnData: [],
    churnMonthlyAvg: 25_000,
    monthlyReval: { preYear: { eur: 0, ils: 0 }, byMonth: Object.fromEntries(range(1, 9).map((m, i) => [mk(Y, m), { eur: PAST.rev[i], ils: Math.round(PAST.rev[i] * 3.59), hasBothEnds: true }])) },
    nsBankClassified: { byMonth: bcm },
    // NetSuite's currency-defense budget is not year-filtered, so it may already hold next-year keys;
    // the projection year must still show 0 reval (old-dashboard parity).
    sfFinanceBudget: Object.fromEntries([...range(10, 12).map((m) => mk(Y, m)), ...range(1, 12).map((m) => mk(T, m))].map((k) => [k, { eur: 120_000, ils: 0 }])),
    dividendExclusions: { byMonth: { [mk(Y, 5)]: { distributionEUR: -1_500_000, whtEUR: -150_000, distributionILS: -5_550_000, whtILS: -555_000 } } },
    book: { openingBalance: 7_900_000, currentBalance: 7_200_000, adjustedCurrentBalance: 7_200_000 },
    bookLocal: { openingBalance: 29_230_000, currentBalance: 26_640_000, adjustedCurrentBalance: 26_640_000 },
    yearStartBalance: { eur: 8_120_000, ils: Math.round(8_120_000 * R) },
    prevMonthEndBalance: { eur: bank + 35_000, ils: Math.round((bank + 35_000) * R) }, // late postings → small re-anchor
    liveFxRate: R,
  };
  return { inputs, meta: { lastActualSalaryMonth: mk(Y, 9), overridesApplied: 0, adjEur: 7_200_000, adjIls: 26_640_000 } };
}

function fakeSfClient({ breakdownFails = false } = {}) {
  return {
    fetchBudgetByCategory: async () => ({ byMonth: Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { ...CATEGORIES }])), totalByMonth: {} }),
    fetchSalaryBudget: async () => Object.fromEntries(range(1, 12).map((m) => [mk(Y, m), { eur: 2_300_000, ils: 8_510_000 }])),
    fetchSalaryBudgetBreakdown: async () => { if (breakdownFails) throw new Error('synthetic Snowflake outage'); return clone(BREAKDOWN); },
  };
}

function computeModuleWith(gather, scenario) {
  return {
    ...cmp,
    gatherInputs: async () => gather,
    loadScenarioDataAsync: async () => scenario,
  };
}

async function runProjection({ gather = syntheticGather(), scenario = { data: PLAN, source: 'Postgres user_scenarios (name="Exit plan June26", owner=someone@example.com)' }, sf = fakeSfClient(), snapshotFile = null } = {}) {
  const prev = process.env.NETSUITE_ACCOUNT_ID;
  process.env.NETSUITE_ACCOUNT_ID = prev || 'TEST_ACCOUNT';
  try {
    return await quiet(() => cp.computeCashProjection({
      now: NOW,
      getNsClient: () => ({}),
      getSfClient: () => sf,
      queueNsCall: (fn) => fn(),
      computeModule: computeModuleWith(gather, scenario),
      snapshotFile,
      includeRaw: true,
    }));
  } finally {
    if (prev === undefined) delete process.env.NETSUITE_ACCOUNT_ID; else process.env.NETSUITE_ACCOUNT_ID = prev;
  }
}

// Verbatim port of App.tsx:2785-2820 + setSalaryFrom 2756-2766 (the old browser code), used as the
// reference for synthesizeSalaryBasis.
function referenceSalaryBasis(results, liveCf, srcYearSal) {
  const perKey = {}; const perDept = {}; let monthsWithData = 0;
  for (const rows of results) {
    if (rows.length > 0) monthsWithData++;
    for (const row of rows) {
      const dept = row.department || 'Unassigned';
      const key = `${dept}__${row.account || ''}__${row.name || ''}`;
      if (!perKey[key]) perKey[key] = { eur: 0, ils: 0 };
      perKey[key].eur += (row.amountEUR || 0); perKey[key].ils += (row.amountILS || 0);
      if (!perDept[dept]) perDept[dept] = { eur: 0, ils: 0 };
      perDept[dept].eur += (row.amountEUR || 0); perDept[dept].ils += (row.amountILS || 0);
    }
  }
  if (monthsWithData === 0) return null;
  const tgtEur = (liveCf[9].salary + liveCf[10].salary + liveCf[11].salary) / 3;
  const tgtIls = (liveCf[9].salaryILS + liveCf[10].salaryILS + liveCf[11].salaryILS) / 3;
  const curEur = Object.values(perDept).reduce((s, v) => s + v.eur, 0) / monthsWithData;
  const curIls = Object.values(perDept).reduce((s, v) => s + v.ils, 0) / monthsWithData;
  const sE = curEur > 0 && tgtEur > 0 ? tgtEur / curEur : 1;
  const sI = curIls > 0 && tgtIls > 0 ? tgtIls / curIls : 1;
  for (const dd of Object.keys(perDept)) { perDept[dd].eur = Math.round(perDept[dd].eur * sE); perDept[dd].ils = Math.round(perDept[dd].ils * sI); }
  const div = Math.max(1, monthsWithData);
  const synth = {};
  for (const [d, v] of Object.entries(perDept)) synth[d] = { eur: Math.round(v.eur / div), ils: Math.round(v.ils / div) };
  return { [`${srcYearSal}-AVG`]: synth };
}

// ── 1. roll-forward helpers ─────────────────────────────────────────────────
function testHelpers(rf) {
  console.log('\nHELPERS: remapMonthKeys / getByMonthIdx');
  const remapped = rf.remapMonthKeys({ '2025-10': 1, '2026-10': 2, '2026-11': 3, other: 9 }, 2027);
  check(remapped['2027-10'] === 2 && remapped['2027-11'] === 3 && remapped.other === 9, 'remap re-keys months to the target year (later source year wins)', JSON.stringify(remapped));
  check(rf.getByMonthIdx({ '2025-10': 'prior', '2026-10': 'current' }, 9) === 'prior', 'getByMonthIdx keeps first-match (prior-year month wins, like the snapshot)');
  check(rf.getByMonthIdx({}, 3) === undefined && rf.getByMonthIdx(null, 3) === undefined, 'getByMonthIdx handles empty input');
}

function testInheritance(rf) {
  console.log('\nKNOBS: projection-year month maps (App.tsx:1516-1552)');
  const yb = { salaryAdjPctByMonth: { 10: -4 }, collPctByMonth: { 3: 90 }, pipelineAdjPctByMonth: { 5: 50 }, currencyDefensePctByMonth: { 1: 10 } };
  let m = rf.inheritProjectionMaps({ adjustmentsByYear: { 2026: yb } }, 2027, 2026);
  check(m.salaryAdjPctByMonth[10] === -4 && m.collPctByMonth[3] === 90 && m.currencyDefensePctByMonth[1] === 10, 'no 2027 bucket → salary/collection/defense % inherit 2026');
  check(Object.keys(m.pipelineAdjPctByMonth).length === 0, 'pipeline % never inherits');
  m = rf.inheritProjectionMaps({ adjustmentsByYear: { 2026: yb, 2027: { salaryAdjPctByMonth: { 0: 5 }, pipelineAdjPctByMonth: { 2: 80 } } } }, 2027, 2026);
  check(m.salaryAdjPctByMonth[0] === 5 && m.salaryAdjPctByMonth[10] === undefined, 'own 2027 salary map wins (no merge with 2026)');
  check(m.collPctByMonth[3] === 90, 'empty/missing 2027 map still inherits per map');
  check(m.pipelineAdjPctByMonth[2] === 80, '2027 pipeline map is used when present');
  m = rf.inheritProjectionMaps({ salaryAdjPctByMonth: { 1: -2 }, collPctByMonth: { 2: 95 }, pipelineAdjPctByMonth: { 4: 10 }, currencyDefensePctByMonth: { 6: 20 } }, 2027, 2026);
  check(m.salaryAdjPctByMonth[1] === -2 && m.collPctByMonth[2] === 95 && m.currencyDefensePctByMonth[6] === 20 && Object.keys(m.pipelineAdjPctByMonth).length === 0,
    'legacy flat scenario → flat maps carry over, pipeline % dropped');
  m = rf.inheritProjectionMaps({}, 2027, 2026);
  check(['salaryAdjPctByMonth', 'collPctByMonth', 'currencyDefensePctByMonth', 'pipelineAdjPctByMonth'].every((k) => Object.keys(m[k]).length === 0), 'empty scenario → no adjustments');
}

function testSalaryBasis(rf, rowsY) {
  console.log('\nSALARY BASIS: synthesizeSalaryBasis vs the old browser code');
  const cases = [
    ['3 months', [BREAKDOWN, BREAKDOWN.map((r) => ({ ...r, amountEUR: r.amountEUR + 7, amountILS: r.amountILS + 13 })), BREAKDOWN.slice(0, 3)]],
    ['1 month', [[], BREAKDOWN, []]],
    ['uneven departments', [BREAKDOWN.slice(1), BREAKDOWN, [{ department: '', account: '1', name: 'x', amountEUR: 333_333, amountILS: 1_000_001 }]]],
  ];
  for (const [label, results] of cases) {
    const got = rf.synthesizeSalaryBasis(results, rowsY, Y);
    const want = referenceSalaryBasis(results, rowsY, Y);
    check(got && JSON.stringify(got.salaryActualsByDept) === JSON.stringify(want), `${label}: department basis identical to the old code (euro-exact)`, JSON.stringify(got && got.salaryActualsByDept));
  }
  check(rf.synthesizeSalaryBasis([[], null, []], rowsY, Y) === null, 'no months with data → null (flat-budget fallback)');
}

// ── 2. end-to-end payload ───────────────────────────────────────────────────
function identities(payload) {
  let bad = 0;
  for (const vk of ['plan', 'base']) {
    for (const yb of payload.variants[vk].years) {
      for (const ccy of ['eur', 'ils']) {
        const rows = yb.rows;
        rows.forEach((r) => {
          const f = r[ccy];
          const inflows = f.collections + f.pipeline - f.churn;
          const outflows = f.salary + f.vendors + f.other;
          if (!near(f.closing, f.opening + f.net - f.dividend, 0.03)) { bad++; fail(`${vk} ${r.mKey} ${ccy}: closing ${f.closing} != opening + net − dividend ${f.opening + f.net - f.dividend}`); }
          if (!near(f.net, inflows - outflows + f.reval, 0.06)) { bad++; fail(`${vk} ${r.mKey} ${ccy}: net ${f.net} != inflows − outflows + reval ${inflows - outflows + f.reval}`); }
        });
        const fyNet = rows.reduce((s, r) => s + r[ccy].net - r[ccy].dividend + r[ccy].reanchor, 0);
        if (!near(rows[11][ccy].closing, rows[0][ccy].opening + fyNet, 0.5)) { bad++; fail(`${vk} FY${yb.year} ${ccy}: Dec closing != Jan opening + Σ (net − dividend) + Σ re-anchor`); }
      }
    }
  }
  return bad;
}

async function testEndToEnd(rf) {
  console.log('\nEND-TO-END: computeCashProjection on synthetic inputs');
  const { payload, raw } = await runProjection();
  const { computeCashflowForecast } = await import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'forecast-core.mjs')).href);

  check(payload.status === 'ready' && payload.years[0] === Y && payload.years[1] === T, 'ready payload for [2026, 2027]');
  check(payload.plan.loaded && payload.plan.source === 'postgres' && !JSON.stringify(payload.plan).includes('@'), 'plan loaded; source reduced to "postgres" (no owner email leaks)');
  const json = JSON.stringify(payload);
  check(!/pipelineOpps|otherDetails|Synthetic deal|Synthetic owner|Synthetic bank line|Synthetic vendor/.test(json), 'payload carries no deal, owner, vendor or bank-line names');
  check(payload.bankToday && payload.bankToday.asOf === '2026-09-30' && payload.bankToday.eur === raw.plan.inputs[Y].prevMonthEndBalance.eur, 'bank today = NetSuite balance at 30 Sep');

  const plan = payload.variants.plan;
  const base = payload.variants.base;
  const st = plan.years[0].rows.map((r) => r.status[0]).join('');
  check(st === 'aaaaaaaaacff', 'statuses: Jan–Sep actual, Oct current, Nov–Dec forecast', st);
  check(plan.years[1].rows.every((r) => r.status === 'forecast'), '2027 is all forecast');

  // Same 2026 computation as the nightly job's main(): its exact engine inputs.
  const g = syntheticGather();
  const mainKnobs = await quiet(() => cmp.scenarioKnobs(PLAN, Y));
  const mainInputs = { ...g.inputs, ...mainKnobs, activeYear: Y, currentYear: Y, now: NOW, asOfDate: null, lastActualSalaryMonth: g.meta.lastActualSalaryMonth, ilsRevalRate: cmp.ILS_REVAL_RATE };
  const mainRows = await quiet(() => computeCashflowForecast(mainInputs));
  check(JSON.stringify(mainRows) === JSON.stringify(raw.plan.rows[Y]), '2026 plan rows identical to the nightly compute\'s engine call');

  for (const [vk, v] of [['plan', plan], ['base', base]]) {
    const dec = v.years[0].rows[11];
    const jan = v.years[1].rows[0];
    check(jan.eur.opening === dec.eur.closing && v.rollForward.opening.eur === v.rollForward.closing.eur, `${vk}: Jan 2027 opening = Dec 2026 closing (${dec.eur.closing.toLocaleString()})`);
    check(near(jan.ils.opening, dec.ils.closing, 0.5) && jan.eur.reanchor === 0, `${vk}: ILS carries within rounding, no re-anchor at the year boundary`);
    check(v.years[1].rows.every((r) => r.eur.reval === 0), `${vk}: 2027 reval = 0 (finance budget not carried, as before)`);
    check(v.years[1].rows.every((r) => r.eur.pipeline === 0 && r.eur.churn === 0), `${vk}: 2027 pipeline and churn = 0`);
  }

  const rY = raw.base.rows[Y];
  const rT = raw.base.rows[T];
  check(rT.every((r, m) => r.vendors === Math.round(rY[m].vendors)), 'base 2027 vendors mirror 2026 month by month');
  const avgColl = Math.round((rY[9].collections + rY[10].collections + rY[11].collections) / 3);
  check(rT.every((r) => r.collections === avgColl + 40_000), `base 2027 collections = Oct–Dec 2026 average (${avgColl.toLocaleString()}) + the 100%-probability open deal`);
  const ref = referenceSalaryBasis([BREAKDOWN, BREAKDOWN, BREAKDOWN], rY, Y)[`${Y}-AVG`];
  const refSum = Object.values(ref).reduce((s, v) => s + v.eur, 0);
  check(rT.every((r) => r.salary === refSum), `base 2027 salary = Oct–Dec 2026 run-rate (${refSum.toLocaleString()}/month)`);

  const pY = raw.plan.rows[Y];
  const pT = raw.plan.rows[T];
  check(pY[10].salary < rY[10].salary && pY[10].vendors < rY[10].vendors, 'plan applies its Nov 2026 salary and Marketing cuts');
  check(plan.years[0].rows[11].eur.closing > base.years[0].rows[11].eur.closing, 'plan Dec 2026 closing above base (savings)');
  const planBasis = Object.values(rf.synthesizeSalaryBasis([BREAKDOWN, BREAKDOWN, BREAKDOWN], pY, Y).salaryActualsByDept[`${Y}-AVG`]).reduce((s, v) => s + v.eur, 0);
  check(pT[0].salary === planBasis && pT[10].salary === Math.round(planBasis * 0.96),
    'plan 2027: Jan at the run-rate, Nov −4% again (2026 month-% inherited, as in the old dashboard)', `${pT[0].salary} / ${pT[10].salary} vs ${planBasis}`);

  const may = plan.years[0].rows[4];
  check(may.dividendExcluded === 1_650_000 && may.eur.dividend === 1_650_000 && may.ils.dividend === 6_105_000,
    'May 2026: the €1.65M (₪6.105M) dividend + WHT is its own Dividend paid figure');
  check(may.eur.vendors === 1_150_000 && plan.years[0].rows.every((r) => r.mKey === '2026-05' || r.eur.dividend === 0),
    'the dividend stays out of Vendors, and no other month has one');
  const oct = plan.years[0].rows[9];
  check(oct.eur.opening === payload.bankToday.eur && oct.ils.opening === payload.bankToday.ils,
    'Oct 2026 (current) opens at the NetSuite bank balance itself, not bank + dividends', `${oct.eur.opening} vs ${payload.bankToday.eur}`);
  for (const [vk, v] of [['plan', plan], ['base', base]]) {
    const opRows = [...raw[vk].rows[Y], ...raw[vk].rows[T]];
    const shaped = v.years.flatMap((y) => y.rows);
    const offBy = shaped.map((r, i) => (r.mKey < '2026-05' ? 0 : 1_650_000) - (opRows[i].closingBalance - r.eur.closing));
    check(offBy.every((d) => Math.abs(d) < 0.01), `${vk}: every closing from May 2026 on (2027 included) = operating view − €1.65M, i.e. cash in the bank`);
    check(v.rollForward.closing.eur === v.years[0].rows[11].eur.closing, `${vk}: roll-forward carries the bank-cash December closing`);
  }
  const reanchored = plan.years[0].rows.filter((r) => Math.abs(r.eur.reanchor) >= 1).map((r) => r.mKey);
  check(reanchored.length === 1 && reanchored[0] === '2026-10', 'only the current month (Oct) is re-anchored to the bank', reanchored.join(','));
  check(identities(payload) === 0, 'every month and FY: closing = opening + net − dividend, net = inflows − outflows + reval (EUR and ILS)');

  // Closed-month-only feeds must not move the projection year.
  const snapshot = rf.snapshotFieldsFromLive({ sourceYear: Y, targetYear: T, srcInputs: raw.base.inputs[Y], rawSfBudget: await fakeSfClient().fetchBudgetByCategory(), rawSfSalaryBudget: await fakeSfClient().fetchSalaryBudget(), now: NOW });
  const knobs = { ...(await quiet(() => cmp.scenarioKnobs({}, T))), ...rf.inheritProjectionMaps({}, T, Y), revenueMethodology: 'pipeline', salaryProjectionMode: 'lastActual' };
  const basis = rf.synthesizeSalaryBasis([BREAKDOWN, BREAKDOWN, BREAKDOWN], rY, Y);
  const build = (snap) => quiet(() => computeCashflowForecast(rf.buildNextYearInputs({ sourceYear: Y, targetYear: T, srcInputs: raw.base.inputs[Y], srcRows: rY, snapshot: snap, salaryBasis: basis, knobs, now: NOW, ilsRevalRate: 3.59 })));
  const full = await build(snapshot);
  const stripped = await build({ ...snapshot, salary: [], vendorHistory: [], collections: {}, sfActualsSplit: {} });
  check(JSON.stringify(full) === JSON.stringify(stripped), 'closed-month feeds (salary/vendor history, collections, actuals split) do not change 2027');
  check(JSON.stringify(full) === JSON.stringify(rT), 'roll-forward built by hand equals the endpoint\'s 2027 rows');

  // Degraded: payroll breakdown unavailable → flat budget fallback + warning.
  const degraded = await runProjection({ sf: fakeSfClient({ breakdownFails: true }) });
  const dT = degraded.raw.base.rows[T];
  check(degraded.payload.degraded && degraded.payload.failedFeeds.includes('sf.fetchSalaryBudgetBreakdown'), 'failed Snowflake feed is reported');
  check(dT.every((r) => r.salary === 2_300_000) && degraded.payload.variants.base.rollForward.salaryBasis.method === 'flat-budget', 'no payroll breakdown → 2027 salary = flat Oct–Dec budget average');
  check(degraded.payload.warnings.some((w) => w.includes('flat budget average')), 'warning explains the salary fallback');

  // Parity mode: a stored snapshot file supplies the pipeline the old dashboard would use.
  const fileRun = await runProjection({ snapshotFile: { ...clone(snapshot), sfPipeline: [{ probability: 100, closeDate: '2026-12-01', amount: 100_000 }], sfBudget: { byMonth: snapshot.sfBudgetByMonth } } });
  check(fileRun.raw.base.rows[T].every((r) => r.collections === avgColl + 100_000) && fileRun.payload.variants.base.rollForward.source === 'snapshot-file', 'snapshot-file mode reads the stored snapshot\'s pipeline');

  // Missing plan → plan = base, with a warning.
  const noPlan = await runProjection({ scenario: { data: {}, source: 'NONE — base plan' } });
  check(!noPlan.payload.plan.loaded && noPlan.payload.plan.source === 'none' && JSON.stringify(noPlan.payload.variants.plan) === JSON.stringify(noPlan.payload.variants.base), 'plan not found → Plan equals Base');
  check(noPlan.payload.warnings.some((w) => w.includes('was not found')), 'warning says the plan was not found');

  // No opening anchor at all → refuse rather than show a table starting from 0.
  const g2 = syntheticGather(); g2.inputs.book = null; g2.inputs.yearStartBalance = null;
  let threw = null;
  try { await runProjection({ gather: g2 }); } catch (e) { threw = e; }
  check(threw instanceof cp.ProjectionError, 'no bank balances → ProjectionError with a user-safe message', threw && threw.message);

  return payload;
}

// ── 3. handler state machine ────────────────────────────────────────────────
function call(handler, { method = 'GET', url = '/api/cash-projection' } = {}) {
  const res = {
    statusCode: 200, headers: {}, body: null,
    setHeader(k, v) { this.headers[k.toLowerCase()] = v; },
    end(b) { this.body = b ? JSON.parse(b) : null; },
  };
  handler({ method, url, headers: {} }, res);
  return res;
}

function fakePayload(d) {
  const y = d.getFullYear();
  return { ok: true, status: 'ready', schemaVersion: cp.SCHEMA_VERSION, generatedAt: d.toISOString(), computedMonth: mk(y, d.getMonth() + 1), company: 'lsports', years: [y, y + 1], plan: { name: process.env.NET_CASH_SCENARIO_NAME || 'Exit plan June26', loaded: true, source: 'postgres' }, bankToday: null, degraded: false, failedFeeds: [], warnings: [], variants: {} };
}

// The handler starts a computation on the next microtask; let it run before counting calls.
const tick = () => new Promise((resolve) => setImmediate(resolve));

function controllableCompute() {
  const calls = [];
  const compute = (nowDate) => new Promise((resolve, reject) => calls.push({ nowDate, resolve, reject }));
  const settle = async (handler) => {
    await tick();
    const last = calls[calls.length - 1];
    last.resolve({ payload: fakePayload(last.nowDate) });
    await handler.idle();
  };
  return { calls, compute, settle };
}

async function testHandler() {
  console.log('\nHANDLER: cache, polling and refresh rules');
  delete process.env.NET_CASH_SCENARIO_NAME;
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cash-projection-test-'));
  const cacheFile = path.join(tmp, 'cache.json');
  let nowMs = new Date('2026-10-15T10:00:00').getTime();
  const clock = () => nowMs;
  const c = controllableCompute();
  const opts = { compute: c.compute, clock, cacheFile, ttlMs: 30 * 60_000, refreshCooldownMs: 60_000, failureBackoffMs: 120_000 };
  const h = cp.createCashProjectionHandler(opts);

  let r = call(h);
  check(r.statusCode === 202 && r.body.status === 'computing' && r.headers['retry-after'] === '5', 'cold cache → 202 computing with Retry-After');
  for (let i = 0; i < 4; i++) call(h);
  await tick();
  check(c.calls.length === 1, '5 concurrent requests share one computation');
  await c.settle(h);
  r = call(h);
  check(r.statusCode === 200 && r.body.status === 'ready' && r.body.cache.stale === false && r.headers['cache-control'] === 'no-store', 'after computing → 200 ready, fresh, no-store');
  check(fs.existsSync(cacheFile), 'result persisted to the cache file');

  nowMs += 31 * 60_000;
  r = call(h);
  await tick();
  check(r.statusCode === 200 && r.body.cache.stale && r.body.cache.staleReason === 'ttl' && r.body.cache.refreshing && c.calls.length === 2, 'past TTL → stale data served instantly while one refresh runs');
  await c.settle(h);

  nowMs += 61_000;
  call(h, { url: '/api/cash-projection?refresh=true' });
  call(h, { url: '/api/cash-projection?refresh=true' });
  await tick();
  check(c.calls.length === 3, 'refresh=true starts one recompute (a second click joins it)');
  await c.settle(h);
  call(h, { url: '/api/cash-projection?refresh=true' });
  await tick();
  check(c.calls.length === 3, 'refresh within a minute of the last start is ignored');

  nowMs = new Date('2026-11-01T08:00:00').getTime();
  r = call(h);
  await tick();
  check(r.body.cache.stale && r.body.cache.staleReason === 'month-rollover' && c.calls.length === 4, 'new month → stale, recomputes');
  await c.settle(h);
  check(call(h).body.computedMonth === '2026-11', 'month rolled to November');

  nowMs = new Date('2027-01-02T08:00:00').getTime();
  r = call(h);
  await tick();
  check(r.statusCode === 202 && c.calls.length === 5, 'new year → previous year\'s projection not served (202 while computing 2027/2028)');
  await c.settle(h);
  check(call(h).body.years[0] === 2027, 'window rolled to 2027 + 2028');

  process.env.NET_CASH_SCENARIO_NAME = 'Another plan';
  r = call(h);
  check(r.statusCode === 202, 'changed plan name → cached result for the old plan is not served');
  await c.settle(h);
  delete process.env.NET_CASH_SCENARIO_NAME;

  // Persistence: a new process (handler) serves the file without computing.
  const c2 = controllableCompute();
  const h2 = cp.createCashProjectionHandler({ ...opts, compute: c2.compute });
  nowMs = new Date('2027-01-02T08:10:00').getTime();
  call(h2);
  // The last entry on disk is for "Another plan"; recompute it so the default plan is on disk.
  await c2.settle(h2);
  const h3 = cp.createCashProjectionHandler({ ...opts, compute: () => { throw new Error('must not compute'); } });
  r = call(h3);
  check(r.statusCode === 200 && r.body.status === 'ready', 'fresh handler serves the persisted result without computing');
  const later = new Date('2027-01-02T08:20:00');
  cp.writeCacheEntry(cacheFile, cp.makeEntry(fakePayload(later), later.getTime()));
  const bump = Date.now() / 1000 + 5;
  fs.utimesSync(cacheFile, bump, bump);
  r = call(h3);
  check(r.body.generatedAt === later.toISOString(), 'picks up a newer cache file written by another process (CLI --write-cache)');

  // Failures: user-safe errors, backoff, generic message for unexpected errors. (The handler logs
  // every failure server-side; silenced here.)
  const logError = console.error;
  console.error = () => {};
  let attempts = 0;
  const failing = cp.createCashProjectionHandler({ clock, cacheFile: null, failureBackoffMs: 120_000, compute: async () => { attempts++; throw new cp.ProjectionError('NetSuite is not configured on this server.'); } });
  call(failing); await failing.idle();
  r = call(failing);
  check(r.statusCode === 200 && r.body.ok === false && r.body.error === 'NetSuite is not configured on this server.' && attempts === 1, 'failure → error response with the reason, no retry storm');
  nowMs += 121_000;
  call(failing); await failing.idle();
  check(attempts === 2, 'retries after the backoff');
  const leaky = cp.createCashProjectionHandler({ clock, cacheFile: null, compute: async () => { throw new Error('connect ECONNREFUSED db-internal:5432 password=hunter2'); } });
  call(leaky); await leaky.idle();
  r = call(leaky);
  check(r.body.ok === false && !/hunter2|ECONNREFUSED|db-internal/.test(r.body.error), 'unexpected errors reach the browser as a generic message');

  const hung = cp.createCashProjectionHandler({ clock: Date.now, cacheFile: null, timeoutMs: 20, compute: () => new Promise(() => {}) });
  call(hung);
  await new Promise((res) => setTimeout(res, 60));
  r = call(hung);
  console.error = logError;
  check(r.body.ok === false && /too long/.test(r.body.error), 'a hung computation times out and frees the slot');

  r = call(h3, { method: 'POST' });
  check(r.statusCode === 405 && r.headers.allow === 'GET', 'POST → 405');
  fs.rmSync(tmp, { recursive: true, force: true });
}

async function testWrapClient() {
  console.log('\nQUEUE: NetSuite calls go through the shared queue');
  const failures = [];
  let queued = 0;
  const client = { double: async (x) => x * 2, fail: async () => { throw new Error('boom'); }, failSync: () => { throw new Error('sync boom'); }, version: 42 };
  const w = cp.wrapClient(client, 'ns', failures, (fn) => { queued++; return fn(); });
  check(w.version === 42, 'non-function properties pass through');
  check((await w.double(21)) === 42 && queued === 1, 'calls run through the queue');
  await w.fail().catch(() => {});
  await w.failSync().catch(() => {});
  check(failures.join(',') === 'ns.fail,ns.failSync' && queued === 3, 'async and sync failures are recorded and rethrown');
}

// ── 4. UI table model ───────────────────────────────────────────────────────
async function testModel(payload) {
  console.log('\nMODEL: table columns, lines and formatting (src/new-dashboard/model.ts)');
  const model = await import(pathToFileURL(path.join(ROOT, 'src', 'new-dashboard', 'model.ts')).href);
  const t = model.buildTable(payload.variants.plan, 'eur', 'both');
  check(t.columns.length === 26 && t.groups.length === 2 && t.groups.every((g) => g.span === 13), 'both years: 12 months + FY each');
  check(t.columns[0].label === 'Jan 26' && t.columns[9].statusLabel === 'Current' && t.columns[12].label === 'FY 2026' && t.columns[13].rollForward && t.columns[13].label === 'Jan 27', 'labels, current month and roll-forward column');
  const line = (k) => t.lines.find((l) => l.key === k);
  check(!!line('reanchor'), 're-anchor note shown when material');
  let bad = 0;
  t.columns.forEach((col, i) => {
    const v = (k) => line(k).values[i];
    if (!near(v('inflows'), v('collections') + v('pipeline') + v('churn'), 0.05)) bad++;
    if (!near(v('outflows'), v('salary') + v('vendors') + v('other'), 0.05)) bad++;
    if (!near(v('net'), v('inflows') - v('outflows') + v('reval'), 0.1)) bad++;
    const expectedClosing = v('opening') + v('net') + v('dividend') + (col.kind === 'fy' ? v('reanchor') : 0);
    if (!near(v('closing'), expectedClosing, 0.6)) { bad++; fail(`${col.label}: closing ${v('closing')} != ${expectedClosing}`); }
  });
  check(bad === 0, 'every column adds up top to bottom (FY incl. dividend and re-anchor)');
  check(line('churn').values.every((x) => x <= 0), 'churn displays as a deduction');
  const keys = t.lines.map((l) => l.key);
  check(keys.indexOf('dividend') === keys.indexOf('net') + 1 && keys.indexOf('closing') === keys.indexOf('dividend') + 1 && line('dividend').label === 'Dividend paid',
    'Dividend paid sits between Net change and Closing balance');
  check(line('dividend').values[4] === -1_650_000 && line('dividend').values[12] === -1_650_000 && line('dividend').values.every((x, i) => x <= 0 && (i === 4 || i === 12 || x === 0) && !Object.is(x, -0)),
    'Dividend paid: (1,650) in May and FY 2026, a deduction, zero elsewhere');
  const gap = line('gap');
  check(keys[keys.length - 1] === 'gap' && gap.label === 'Monthly gap' && gap.signed === true, 'Monthly gap is the last line, shown signed');
  let gapBad = 0;
  t.columns.forEach((col, i) => {
    const v = (k) => line(k).values[i];
    if (col.kind === 'month') {
      if (!near(v('gap'), v('closing') - v('opening'), 0.05) || !near(v('gap'), v('net') + v('dividend'), 0.1)) { gapBad++; fail(`${col.label}: gap ${v('gap')} != closing − opening ${v('closing') - v('opening')}`); }
    } else {
      const sum = t.columns.reduce((s, c, j) => (c.kind === 'month' && c.year === col.year ? s + gap.values[j] : s), 0);
      if (!near(v('gap'), sum, 0.1)) { gapBad++; fail(`${col.label}: FY gap ${v('gap')} != Σ months ${sum}`); }
    }
  });
  check(gapBad === 0, 'Monthly gap = closing − opening (= net change − dividend paid) every month; FY = sum of the months');
  check(near(gap.values[4], line('net').values[4] - 1_650_000, 0.1), 'May 2026 gap includes the €1.65M dividend');
  check(model.buildTable(payload.variants.plan, 'ils', 'current').columns.length === 13, '2026 only → 13 columns');
  const next = model.buildTable(payload.variants.base, 'eur', 'next');
  check(next.columns.length === 13 && next.columns[0].rollForward && !next.lines.some((l) => l.key === 'reanchor'), '2027 only → 13 columns, no re-anchor line');
  check(next.lines.some((l) => l.key === 'dividend'), 'the Dividend paid line stays visible in the 2027-only view');
  check(next.lines[next.lines.length - 1].key === 'gap', 'the Monthly gap is the last line in the 2027-only view too');

  check(model.formatThousands(1_234_567) === '1,235' && model.formatThousands(-1_234_567) === '(1,235)' && model.formatThousands(400) === '–' && model.formatThousands(-400) === '–', 'thousands format: 1,235 / (1,235) / –');
  check(model.formatSignedThousands(1_234_567) === '+1,235' && model.formatSignedThousands(-1_234_567) === '−1,235' && model.formatSignedThousands(400) === '–' && model.formatSignedThousands(-400) === '–', 'signed format: +1,235 / −1,235 / –');
  check(model.formatMillions(7_050_000, 'eur') === '€7.1M' && model.formatMillions(-1_240_000, 'ils') === '-₪1.2M', 'millions format for KPI cards');
  check(model.formatFull(-1_200.4, 'eur') === '-€1,200', 'full amount format');
  const k = model.computeKpis(payload, 'plan', 'eur');
  const nonActual = payload.variants.plan.years.flatMap((y) => y.rows).filter((r) => r.status !== 'actual');
  const minClosing = Math.min(...nonActual.map((r) => r.eur.closing));
  check(k.closings.length === 2 && k.lowest.value === minClosing && k.bankToday.asOf === '2026-09-30', 'KPIs: year-end closings, lowest projected month-end, bank today');
}

async function writeFixture(payload) {
  const out = { _note: 'Synthetic numbers for tests and UI screenshots. Not LSports data.', ...payload, generatedAt: '2026-10-15T07:42:00.000Z', cache: { ageSec: 540, stale: false, staleReason: null, refreshing: false, lastError: null } };
  const file = path.join(__dirname, 'fixtures', 'cash-projection-sample.json');
  fs.writeFileSync(file, JSON.stringify(out, null, 2) + '\n');
  console.log(`\nWrote ${path.relative(ROOT, file)}`);
}

async function main() {
  const rf = await import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'roll-forward.mjs')).href);
  console.log('=== cash-projection checks (synthetic data) ===');
  testHelpers(rf);
  testInheritance(rf);
  const first = await runProjection();
  testSalaryBasis(rf, first.raw.base.rows[Y]);
  const payload = await testEndToEnd(rf);
  await testHandler();
  await testWrapClient();
  await testModel(payload);
  if (process.argv.includes('--write-fixture')) await writeFixture(payload);
  console.log('');
  if (failures === 0) { console.log('✅ PASS — all checks green.'); process.exit(0); }
  console.error(`❌ FAIL — ${failures} check(s) failed.`);
  process.exit(1);
}
main().catch((e) => { console.error('❌ FAIL — test harness threw:', e.stack || e); process.exit(1); });
