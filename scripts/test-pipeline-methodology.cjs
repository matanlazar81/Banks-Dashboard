// Unit test for the Column B pipeline revenue-projection methodology.
// Pure-logic verification — no Snowflake connection needed.
//   node scripts/test-pipeline-methodology.cjs
//
// Verifies the documented spec:
//   - stage weights (New/Qualified 12%, Test 17%, Negotiation 45%,
//     Contract Sent 60%, Best Case 90%)
//   - months_remaining = 12 − monthIndex (June=7, December=1)
//   - quarterly open weighted pipeline ÷ open months = projected MRR
//   - past months use real SF MR contribution (no projection, columnD=0)
//   - current month = SF actual + projected; only projected feeds columnD
//   - future months = projected × months_remaining × calibration factor
//   - footer total + columnD total formulas

const assert = require('assert');
const { assemblePipelineMethodology, pipelineStageWeight, PIPELINE_FALLBACK_FACTOR } = require('../snowflake-api.cjs');

let passed = 0;
const check = (name, fn) => { fn(); passed++; console.log(`  ✓ ${name}`); };

console.log('Pipeline methodology — stage weights');
check('Best Case → 0.90', () => assert.strictEqual(pipelineStageWeight('Best Case'), 0.90));
check('Contract Sent → 0.60', () => assert.strictEqual(pipelineStageWeight('Contract Sent'), 0.60));
check('Negotiation → 0.45', () => assert.strictEqual(pipelineStageWeight('Negotiation'), 0.45));
check('Test → 0.17', () => assert.strictEqual(pipelineStageWeight('Test'), 0.17));
check('New / Qualified → 0.12', () => assert.strictEqual(pipelineStageWeight('New / Qualified'), 0.12));
check('Qualified → 0.12', () => assert.strictEqual(pipelineStageWeight('Qualified'), 0.12));
check('unknown stage → 0', () => assert.strictEqual(pipelineStageWeight('Discovery'), 0));
check('fallback factor is 0.8381', () => assert.strictEqual(PIPELINE_FALLBACK_FACTOR, 0.8381));

console.log('\nPipeline methodology — assembly (ref date = 2026-05-15, factor 0.8381)');
// One open opp per quarter so projected MRR is easy to reason about.
// Q2 (Apr-Jun): Negotiation 100,000 × 0.45 = 45,000 weighted.
//   As of mid-May: open months in Q2 = May, Jun = 2 → projected MRR = 22,500/mo
// Q3 (Jul-Sep): Best Case 300,000 × 0.90 = 270,000 weighted.
//   All 3 months open → projected MRR = 90,000/mo
// Q4 (Oct-Dec): Contract Sent 200,000 × 0.60 = 120,000 weighted.
//   All 3 months open → projected MRR = 40,000/mo
// Q1: no open opps → 0.
const r = assemblePipelineMethodology({
  yr: 2026,
  refDate: new Date('2026-05-15T12:00:00'),
  openOpps: [
    { stage: 'Negotiation',   amount: 100000, closeDate: '2026-06-20' },
    { stage: 'Best Case',     amount: 300000, closeDate: '2026-08-10' },
    { stage: 'Contract Sent', amount: 200000, closeDate: '2026-11-05' },
  ],
  sfContribByMonth: { '2026-01': 1000, '2026-02': 2000, '2026-03': 3000, '2026-04': 4000, '2026-05': 5000 },
  actualMrrByMonth: { '2026-05': 50000 },
  calibrationFactor: 0.8381,
  calibrationSource: 'fallback',
});

const F = 0.8381;
check('quarter weighted Q2 = 45,000', () => assert.strictEqual(r.quarterWeighted[2], 45000));
check('quarter weighted Q3 = 270,000', () => assert.strictEqual(r.quarterWeighted[3], 270000));
check('quarter weighted Q4 = 120,000', () => assert.strictEqual(r.quarterWeighted[4], 120000));
check('quarter weighted Q1 = 0', () => assert.strictEqual(r.quarterWeighted[1], 0));

// Past months: real SF contribution, no projection, columnD = 0
for (const [mk, expect] of [['2026-01', 1000], ['2026-02', 2000], ['2026-03', 3000], ['2026-04', 4000]]) {
  check(`${mk} past → columnB=${expect}, columnD=0, state=past`, () => {
    const m = r.byMonth[mk];
    assert.strictEqual(m.state, 'past');
    assert.strictEqual(m.sfContribution, expect);
    assert.strictEqual(m.columnB, expect);
    assert.strictEqual(m.columnD, 0);
    assert.strictEqual(m.projectedMrr, 0);
  });
}

// months_remaining spot checks (12 − monthIndex): June idx5 → 7, December idx11 → 1
check('June months remaining = 7', () => assert.strictEqual(r.byMonth['2026-06'].monthsRemaining, 7));
check('December months remaining = 1', () => assert.strictEqual(r.byMonth['2026-12'].monthsRemaining, 1));
check('May months remaining = 8', () => assert.strictEqual(r.byMonth['2026-05'].monthsRemaining, 8));

// Current month (May): projected MRR = Q2 22,500; months remaining = 8
//   projected = round(22500 × 8 × 0.8381) = round(150,858) = 150858
//   columnB = sfContribution(5000) + projected ; columnD = projected only
check('May current → projectedMrr=22500', () => assert.strictEqual(r.byMonth['2026-05'].projectedMrr, 22500));
check('May current → state=current', () => assert.strictEqual(r.byMonth['2026-05'].state, 'current'));
const mayProjected = Math.round(22500 * 8 * F);
check(`May projected = ${mayProjected}`, () => assert.strictEqual(r.byMonth['2026-05'].projected, mayProjected));
check('May columnB = sfContribution + projected', () => assert.strictEqual(r.byMonth['2026-05'].columnB, 5000 + mayProjected));
check('May columnD = projected only (excludes sfContribution)', () => assert.strictEqual(r.byMonth['2026-05'].columnD, mayProjected));
check('May closedSoFar = actualMRR 50000 (informational)', () => assert.strictEqual(r.byMonth['2026-05'].closedSoFar, 50000));

// Future month June: projectedMrr = 22500 (Q2, 2 open months), monthsRemaining 7
const junProjected = Math.round(22500 * 7 * F);
check('June future → projectedMrr=22500', () => assert.strictEqual(r.byMonth['2026-06'].projectedMrr, 22500));
check(`June columnD = ${junProjected}`, () => assert.strictEqual(r.byMonth['2026-06'].columnD, junProjected));
check('June columnB === columnD (future, no SF actual)', () => assert.strictEqual(r.byMonth['2026-06'].columnB, junProjected));
check('June sfContribution = 0', () => assert.strictEqual(r.byMonth['2026-06'].sfContribution, 0));

// Future month August: Q3 projected 90,000/mo, monthsRemaining = 4 (idx7 → 12−7=5? Aug idx7 → 5)
check('August months remaining = 5', () => assert.strictEqual(r.byMonth['2026-08'].monthsRemaining, 5));
check('August projectedMrr = 90000 (Q3 270k / 3 open)', () => assert.strictEqual(r.byMonth['2026-08'].projectedMrr, 90000));
const augProjected = Math.round(90000 * 5 * F);
check(`August columnD = ${augProjected}`, () => assert.strictEqual(r.byMonth['2026-08'].columnD, augProjected));

// December: Q4 projected 40,000/mo, monthsRemaining = 1
check('December projectedMrr = 40000', () => assert.strictEqual(r.byMonth['2026-12'].projectedMrr, 40000));
const decProjected = Math.round(40000 * 1 * F);
check(`December columnD = ${decProjected}`, () => assert.strictEqual(r.byMonth['2026-12'].columnD, decProjected));

// Footer total = Σ past SF (1000+2000+3000+4000) + May columnB + Σ future columnB
let expectedFooter = 1000 + 2000 + 3000 + 4000 + r.byMonth['2026-05'].columnB;
for (const mk of ['2026-06','2026-07','2026-08','2026-09','2026-10','2026-11','2026-12']) expectedFooter += r.byMonth[mk].columnB;
check('footer total matches Σ(past SF + May columnB + future columnB)', () => assert.strictEqual(r.footerTotal, Math.round(expectedFooter)));

// Column D total = May projected + Σ future columnD (NO past, NO May actual)
let expectedColumnD = r.byMonth['2026-05'].columnD;
for (const mk of ['2026-06','2026-07','2026-08','2026-09','2026-10','2026-11','2026-12']) expectedColumnD += r.byMonth[mk].columnD;
check('columnD total = May projected + future columnD (excludes past + actuals)', () => assert.strictEqual(r.columnDTotal, Math.round(expectedColumnD)));
check('columnD total excludes the 10,000 of past SF contribution', () => assert.ok(r.columnDTotal < r.footerTotal));

// Monthly contribution = projectedMrr × factor (no × monthsRemaining).
// This is what feeds the cashflow Pipeline column.
check('May monthly = projectedMrr × factor', () => assert.strictEqual(r.byMonth['2026-05'].monthlyContribution, Math.round(22500 * F)));
check('Aug monthly = 90000 × factor', () => assert.strictEqual(r.byMonth['2026-08'].monthlyContribution, Math.round(90000 * F)));
check('Dec monthly = 40000 × factor', () => assert.strictEqual(r.byMonth['2026-12'].monthlyContribution, Math.round(40000 * F)));
check('past months have monthlyContribution = 0', () => {
  for (const mk of ['2026-01','2026-02','2026-03','2026-04']) assert.strictEqual(r.byMonth[mk].monthlyContribution, 0);
});
check('monthlyContribTotal = Σ monthly across all months', () => {
  let s = 0; for (const mk of Object.keys(r.byMonth)) s += r.byMonth[mk].monthlyContribution;
  assert.strictEqual(r.monthlyContribTotal, Math.round(s));
});
check('monthlyContribTotal < columnDTotal (monthly slice < annual roll-forward)', () => assert.ok(r.monthlyContribTotal < r.columnDTotal));

// Historical year: every month "past"
const hist = assemblePipelineMethodology({
  yr: 2025, refDate: new Date('2026-05-15T12:00:00'),
  openOpps: [{ stage: 'Best Case', amount: 100000, closeDate: '2025-08-01' }],
  sfContribByMonth: { '2025-08': 12345 }, actualMrrByMonth: {}, calibrationFactor: 0.8381, calibrationSource: 'fallback',
});
check('historical year → all months past, columnD total = 0', () => assert.strictEqual(hist.columnDTotal, 0));
check('historical year → Aug columnB = SF contribution 12345', () => assert.strictEqual(hist.byMonth['2025-08'].columnB, 12345));

console.log(`\nAll ${passed} assertions passed ✓`);
