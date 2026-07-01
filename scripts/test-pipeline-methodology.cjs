// Unit test for the pipeline revenue-projection methodology (FY2026 Revenue Bridge spec).
// Pure-logic — no Snowflake connection needed.  Run: node scripts/test-pipeline-methodology.cjs
//
// Verifies the documented methodology:
//   - stage weights (New/Qualified 12%, Test 17%, Negotiation 45%, Contract Sent 60%, Best Case 90%)
//   - months_remaining = 12 − monthIndex (June=7, December=1)
//   - past months use real SF MR contribution (no projection, columnD=0)
//   - current month: proj_mrr = max(0, model_mrr − closed_so_far)  [subtract this month's closes]
//   - future quarters: proj_mrr = max(model_mrr, prior_year_closed[q] × 0.80 ÷ 3)  [floor]
//   - current-quarter future months use model only (no floor)
//   - contribution = proj_mrr × months_remaining × calibration factor

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
check('fallback factor is 0.8489', () => assert.strictEqual(PIPELINE_FALLBACK_FACTOR, 0.8489));

const F = 0.8489;
console.log(`\nPipeline methodology — assembly (ref date = 2026-05-15, factor ${F})`);
// Q2 (Apr-Jun): Negotiation 100,000 × 0.45 = 45,000 weighted. Mid-May → open months May,Jun = 2 → model 22,500/mo
// Q3 (Jul-Sep): Best Case 300,000 × 0.90 = 270,000 weighted. 3 open → 90,000/mo
// Q4 (Oct-Dec): Contract Sent 200,000 × 0.60 = 120,000 weighted. 3 open → 40,000/mo
const r = assemblePipelineMethodology({
  yr: 2026,
  refDate: new Date('2026-05-15T12:00:00'),
  openOpps: [
    { stage: 'Negotiation',   amount: 100000, closeDate: '2026-06-20' },
    { stage: 'Best Case',     amount: 300000, closeDate: '2026-08-10' },
    { stage: 'Contract Sent', amount: 200000, closeDate: '2026-11-05' },
  ],
  sfContribByMonth: { '2026-01': 1000, '2026-02': 2000, '2026-03': 3000, '2026-04': 4000, '2026-05': 5000 },
  actualMrrByMonth: { '2026-05': 10000 }, // closed-so-far in the current month
  priorYearClosedByQuarter: {},           // no floor in this case
  calibrationFactor: F,
  calibrationSource: 'fallback',
});

check('quarter weighted Q2 = 45,000', () => assert.strictEqual(r.quarterWeighted[2], 45000));
check('quarter weighted Q3 = 270,000', () => assert.strictEqual(r.quarterWeighted[3], 270000));
check('quarter weighted Q4 = 120,000', () => assert.strictEqual(r.quarterWeighted[4], 120000));

// Past months: real SF contribution, no projection, columnD = 0
for (const [mk, expect] of [['2026-01', 1000], ['2026-02', 2000], ['2026-03', 3000], ['2026-04', 4000]]) {
  check(`${mk} past → columnB=${expect}, columnD=0`, () => {
    const m = r.byMonth[mk];
    assert.strictEqual(m.state, 'past');
    assert.strictEqual(m.columnB, expect);
    assert.strictEqual(m.columnD, 0);
    assert.strictEqual(m.projectedMrr, 0);
  });
}

// months_remaining (12 − monthIndex)
check('June months remaining = 7', () => assert.strictEqual(r.byMonth['2026-06'].monthsRemaining, 7));
check('December months remaining = 1', () => assert.strictEqual(r.byMonth['2026-12'].monthsRemaining, 1));

// ── Current month (May): model 22,500 − closed-so-far 10,000 = 12,500 projected MRR ──
check('May state = current', () => assert.strictEqual(r.byMonth['2026-05'].state, 'current'));
check('May projectedMrr = max(0, 22500 − 10000) = 12500', () => assert.strictEqual(r.byMonth['2026-05'].projectedMrr, 12500));
const mayProjected = Math.round(12500 * 8 * F);
check(`May projected = ${mayProjected}`, () => assert.strictEqual(r.byMonth['2026-05'].projected, mayProjected));
check('May columnB = sfContribution(5000) + projected', () => assert.strictEqual(r.byMonth['2026-05'].columnB, 5000 + mayProjected));
check('May columnD = projected only', () => assert.strictEqual(r.byMonth['2026-05'].columnD, mayProjected));
check('May monthly = projectedMrr × factor', () => assert.strictEqual(r.byMonth['2026-05'].monthlyContribution, Math.round(12500 * F)));

// Current-month clamp: when closes already exceed the model, projection → 0
const rClamp = assemblePipelineMethodology({
  yr: 2026, refDate: new Date('2026-05-15T12:00:00'),
  openOpps: [{ stage: 'Negotiation', amount: 100000, closeDate: '2026-06-20' }],
  sfContribByMonth: { '2026-05': 5000 }, actualMrrByMonth: { '2026-05': 50000 },
  priorYearClosedByQuarter: {}, calibrationFactor: F, calibrationSource: 'fallback',
});
check('May clamp → projectedMrr = 0 when closed(50k) > model(22.5k)', () => assert.strictEqual(rClamp.byMonth['2026-05'].projectedMrr, 0));

// Current-quarter future month (June, Q2): model only, no floor
check('June (current qtr) projectedMrr = 22500 model', () => assert.strictEqual(r.byMonth['2026-06'].projectedMrr, 22500));

// Future quarter, no floor data → model. August (Q3) = 90,000/mo
check('August projectedMrr = 90000 (no floor data)', () => assert.strictEqual(r.byMonth['2026-08'].projectedMrr, 90000));
const augProjected = Math.round(90000 * 5 * F);
check(`August months remaining = 5`, () => assert.strictEqual(r.byMonth['2026-08'].monthsRemaining, 5));
check(`August columnD = ${augProjected}`, () => assert.strictEqual(r.byMonth['2026-08'].columnD, augProjected));

// ── Future-quarter floor ──
// Prior-year Q4 closed 900,000 → floor = 900,000 × 0.80 ÷ 3 = 240,000/mo. Model Q4 = 40,000/mo → floor wins.
const rFloor = assemblePipelineMethodology({
  yr: 2026, refDate: new Date('2026-05-15T12:00:00'),
  openOpps: [{ stage: 'Contract Sent', amount: 200000, closeDate: '2026-11-05' }],
  sfContribByMonth: {}, actualMrrByMonth: {},
  priorYearClosedByQuarter: { 4: 900000 },
  calibrationFactor: F, calibrationSource: 'fallback',
});
check('Q4 floor → Dec projectedMrr = max(40000, 240000) = 240000', () => assert.strictEqual(rFloor.byMonth['2026-12'].projectedMrr, 240000));
check('Dec columnD uses the floored MRR', () => assert.strictEqual(rFloor.byMonth['2026-12'].columnD, Math.round(240000 * 1 * F)));

// Current quarter is NOT floored even with prior-year data present
const rFloorQ2 = assemblePipelineMethodology({
  yr: 2026, refDate: new Date('2026-05-15T12:00:00'),
  openOpps: [{ stage: 'Negotiation', amount: 100000, closeDate: '2026-06-20' }],
  sfContribByMonth: {}, actualMrrByMonth: {},
  priorYearClosedByQuarter: { 2: 900000 }, calibrationFactor: F, calibrationSource: 'fallback',
});
check('current-quarter (June) ignores floor → 22500 model', () => assert.strictEqual(rFloorQ2.byMonth['2026-06'].projectedMrr, 22500));

// Historical year: every month past, no projection
const hist = assemblePipelineMethodology({
  yr: 2025, refDate: new Date('2026-05-15T12:00:00'),
  openOpps: [{ stage: 'Best Case', amount: 100000, closeDate: '2025-08-01' }],
  sfContribByMonth: { '2025-08': 12345 }, actualMrrByMonth: {}, priorYearClosedByQuarter: {},
  calibrationFactor: F, calibrationSource: 'fallback',
});
check('historical year → columnD total = 0', () => assert.strictEqual(hist.columnDTotal, 0));
check('historical year → Aug columnB = 12345', () => assert.strictEqual(hist.byMonth['2025-08'].columnB, 12345));

console.log(`\nAll ${passed} assertions passed ✓`);
