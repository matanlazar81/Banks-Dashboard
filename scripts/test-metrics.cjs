#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// Checks for the Metrics page (server/metrics.cjs, server/metrics-settings.cjs): the pack from synthetic
// projection payloads, NRR, the cloud cap, the 2027 targets, payroll ratios, the breakdowns (NRR, churn), and the
// handlers. No network, no real figures.
//   node scripts/test-metrics.cjs
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Readable } = require('stream');
const { pathToFileURL } = require('url');

const ROOT = path.resolve(__dirname, '..');
const m = require(path.join(ROOT, 'server', 'metrics.cjs'));
const ms = require(path.join(ROOT, 'server', 'metrics-settings.cjs'));

let failures = 0;
function check(ok, label, detail) {
  if (ok) console.log(`  ✓ ${label}`);
  else { failures++; console.log(`  ✗ ${label}${detail !== undefined ? ` — ${detail}` : ''}`); }
}
const near = (a, b, tol = 0.011) => Math.abs(a - b) <= tol;

const Y = 2026;
const T = 2027;
const NOW = new Date('2026-10-15T09:00:00').getTime();
const mk = (y, mo) => `${y}-${String(mo).padStart(2, '0')}`;
const CLOUD = 'Cloud Infrastructure & DevOps';

function pnlPayload() {
  const year = (y) => ({
    year: y, kind: y === Y ? 'current' : 'projection',
    rows: Array.from({ length: 12 }, (_, i) => {
      const status = y === Y ? (i < 9 ? 'actual' : i === 9 ? 'current' : 'forecast') : 'forecast';
      const f = { revenue: 4_900_000, pipeline: status === 'actual' ? 0 : 50_000, churn: status === 'actual' ? 0 : 20_000, otherRevenue: 100_000 - (status === 'actual' ? 0 : 30_000), totalRevenue: 5_000_000, payroll: 2_000_000, capex: -1_000_000, opex: 2_000_000, totalCosts: 3_000_000, ebitda: 2_000_000, net: 1_000_000 };
      return { mKey: mk(y, i + 1), status, eur: f, ils: f };
    }),
  });
  return {
    ok: true, status: 'ready', generatedAt: '2026-10-15T08:00:00.000Z', years: [Y, T], plan: { name: 'Synthetic plan' },
    actuals: { source: 'netsuite', basis: 'period', through: mk(Y, 9) },
    variants: { plan: { years: [year(Y), year(T)] }, base: { years: [year(Y), year(T)] } },
    targetsBase: {
      year: T, categories: [CLOUD, 'Marketing', 'SW Licenses'], serverCategory: CLOUD,
      months: Array.from({ length: 12 }, (_, i) => ({ mKey: mk(T, i + 1), opexByCategory: { [CLOUD]: 220_000, Marketing: 1_000_000, 'SW Licenses': 780_000 } })),
    },
  };
}
function pnlDetails() {
  const accounts = {};
  for (let i = 1; i <= 9; i++) accounts[mk(Y, i)] = { opex: [{ acct: '640001', eur: -200_000 }, { acct: '640002', eur: -5_000 }, { acct: '620001', eur: -300_000 }] };
  const months = {};
  for (let i = 10; i <= 12; i++) months[mk(Y, i)] = { categories: { [CLOUD]: 100, Marketing: 500, 'SW Licenses': 400 } };
  return { accounts, variants: { plan: { [Y]: { months } } } };
}
function cashPayload() {
  const year = (y, open) => ({
    year: y, kind: y === Y ? 'current' : 'projection',
    rows: Array.from({ length: 12 }, (_, i) => {
      const f = { opening: open + i * 250_000, closing: open + (i + 1) * 250_000 };
      return { mKey: mk(y, i + 1), status: y === Y && i < 9 ? 'actual' : 'forecast', eur: f, ils: f };
    }),
  });
  return {
    ok: true, status: 'ready', generatedAt: '2026-10-15T08:00:00.000Z', years: [Y, T],
    bankToday: { eur: 9_100_000, ils: 0, asOf: '2026-09-30' },
    variants: { plan: { years: [year(Y, 7_000_000), year(T, 10_000_000)] } },
  };
}
const EXTRAS = {
  arr: { arr: 59_000_000, mrr: 4_916_667, liveDate: '2026-10-15', snapDate: '2026-09-30' },
  churnQuarters: [
    { qs: '2026-04-01', q: 'Q2 2026', amount: 30_000, partial: false },
    { qs: '2026-07-01', q: 'Q3 2026', amount: 40_000, partial: false },
    { qs: '2026-10-01', q: 'Q4 2026', amount: 5_000, partial: true },
    { qs: '2025-10-01', q: 'Q4 2025', amount: 99_000, partial: false },
  ],
  nrr: [{ month: mk(Y, 9), nrr: 104.5, grr: 93.2, customers: 400 }],
  // 100 employees all year; 10 more from 15 March; 5 leave on 30 June (counted at June's end); a contractor
  // from August; one starting in November (after the months shown).
  employees: [
    ...Array.from({ length: 100 }, () => ({ start: '2020-01-01', end: null, type: 'Full Time' })),
    ...Array.from({ length: 10 }, () => ({ start: '2026-03-15', end: null, type: 'Full Time' })),
    ...Array.from({ length: 5 }, () => ({ start: '2021-05-01', end: '2026-06-30', type: 'Full Time' })),
    { start: '2026-08-01', end: null, type: 'Contractor' },
    { start: '2026-11-01', end: null, type: 'Full Time' },
  ],
  company: 'LSports',
};

function build(settings = ms.emptySettings(), extras = EXTRAS, failed = []) {
  return m.buildMetrics({ nowMs: NOW, cash: cashPayload(), pnl: pnlPayload(), pnlDetails: pnlDetails(), settings, extras, failed });
}

// Revenue by customer for the NRR breakdown: A grows, B stays, C shrinks, D leaves; N and Z are new
// (Z had a zero month a year earlier).
const CUSTREV = [
  { month: '2025-09', customer: '1', name: 'Alpha', rev: 100 }, { month: '2025-09', customer: '2', name: 'Beta', rev: 100 },
  { month: '2025-09', customer: '3', name: 'Gamma', rev: 50 }, { month: '2025-09', customer: '4', name: 'Delta', rev: 30 },
  { month: '2025-09', customer: '9', name: 'Zeta', rev: 0 },
  { month: '2026-09', customer: '1', name: 'Alpha', rev: 120 }, { month: '2026-09', customer: '2', name: 'Beta', rev: 100 },
  { month: '2026-09', customer: '3', name: 'Gamma', rev: 20 }, { month: '2026-09', customer: '8', name: 'Nu', rev: 40 },
  { month: '2026-09', customer: '9', name: 'Zeta', rev: 10 },
];
// Churned opportunities matching EXTRAS.churnQuarters: Q2 30K, Q3 40K (one in USD), Q4 so far 5K.
const OPPS = [
  { month: '2026-04', opportunity: 'Opp a', customer: 'Cust A', currency: 'EUR', amount: 10_000 },
  { month: '2026-06', opportunity: 'Opp b', customer: 'Cust B', currency: 'EUR', amount: 20_000 },
  { month: '2026-07', opportunity: 'Opp c', customer: 'Cust C', currency: 'EUR', amount: 25_000 },
  { month: '2026-09', opportunity: 'Opp d', customer: 'Cust C', currency: 'USD', amount: 15_000 },
  { month: '2026-10', opportunity: 'Opp e', customer: null, currency: 'EUR', amount: 5_000 },
];
const metric = (out, key) => out.metrics.find((x) => x.key === key);

function testPack() {
  console.log('\nPACK: the same rows every month, each figure actual or forecast');
  const out = build();
  check(JSON.stringify(out.metrics.map((x) => x.key)) === JSON.stringify(['revenue', 'ebitda', 'netCash', 'arr', 'nrr', 'churn']), 'rows: revenue, EBITDA, net cash, ARR, NRR, churn');
  const rev = metric(out, 'revenue');
  check(rev.lastMonth.value === 5_000_000 && rev.lastMonth.status === 'actual' && rev.lastMonth.label === 'Sep 2026'
    && rev.ytd.value === 45_000_000 && rev.ytd.label === 'Jan–Sep 2026'
    && rev.fy[0].value === 60_000_000 && rev.fy[0].status === 'actual+forecast' && rev.fy[0].actual === 45_000_000 && rev.fy[0].forecast === 15_000_000
    && rev.fy[1].status === 'forecast', 'revenue: last month and YTD actual, FY actual + forecast, next FY forecast');
  const cash = metric(out, 'netCash');
  check(cash.lastMonth.value === 9_100_000 && cash.lastMonth.status === 'actual' && cash.ytd.value === 2_100_000
    && cash.fy[0].value === 10_000_000 && cash.fy[1].value === 13_000_000 && cash.fy[1].status === 'forecast',
  'net cash: bank at the month-end, change since 1 January, December closings');
  const arr = metric(out, 'arr');
  check(arr.lastMonth.value === 59_000_000 && arr.fy[0].value === (4_900_000 + 50_000 - 20_000) * 12 && arr.fy[0].status === 'forecast',
    'ARR: Snowflake now; December run-rate × 12 forecast');
  const nrr = metric(out, 'nrr');
  check(nrr.unit === 'pct' && nrr.lastMonth.value === 104.5 && nrr.lastMonth.grr === 93.2 && /GRR 93.2%/.test(nrr.note), 'NRR: trailing 12 months, with GRR');
  const churn = metric(out, 'churn');
  check(churn.lastMonth.value === 40_000 && churn.lastMonth.label === 'Q3 2026' && churn.ytd.value === 75_000 && churn.fy[0].value === 60_000 && churn.fy[1] === null,
    'churn: last full quarter, this year so far (in-progress quarter included), revenue lost in forecast months');
}

function testCloud() {
  console.log('\nCLOUD: against the cap on projected revenue');
  let out = build();
  const [cy, ct] = out.cloud.years;
  // Actual: 9 × 205K (640001 + 640002); forecast: 3 × 2M × 10% (budget's cloud share).
  check(cy.actual === 1_845_000 && cy.forecast === 600_000 && cy.total === 2_445_000 && cy.cap === 4_800_000 && cy.within && cy.status === 'actual+forecast',
    'this year: NetSuite 640xxx in closed months + the budget\'s cloud share of opex after', JSON.stringify(cy));
  check(ct.total === 12 * 220_000 && ct.cap === 4_800_000 && ct.within && ct.status === 'forecast' && near(ct.pctOfRevenue, 4.4), 'next year: the cloud category of the targets baseline');
  const s = ms.emptySettings();
  s.cloudCapPct = 4;
  out = build(s);
  check(!out.cloud.years[1].within && out.cloud.years[1].headroom === 2_400_000 - 2_640_000, 'a lower cap: over, with the overrun as negative headroom');
  check(out.cloud.category === CLOUD, 'the cloud category is found by name when none is set');
}

// The projection year with the targets saved on the New Bank Dashboard: the same figures as both pages'
// Targets view. The baselines are built by the real targets module, matching the payloads above.
async function testTargets() {
  console.log('\nTARGETS: the projection year follows the saved 2027 targets');
  const tl = await import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'targets.mjs')).href);
  const cats = Object.fromEntries(Array.from({ length: 12 }, (_, i) => [String(i + 1).padStart(2, '0'), { [CLOUD]: 220, Marketing: 1000, 'SW Licenses': 780 }]));
  const baseOf = (collPct) => tl.buildTargetsBase({
    year: T, serverCategory: CLOUD, deptAmounts: { 'R&D': 1 }, categoryAmountsByMonth: cats,
    months: Array.from({ length: 12 }, (_, i) => ({ mKey: mk(T, i + 1), revenue: 4_900_000, collPct, payroll: 2_000_000, opex: 2_000_000, ilsRate: 1 })),
  });
  const pnl = { ...pnlPayload(), targetsBase: baseOf(100) };
  const cash = { ...cashPayload(), targetsBase: baseOf(90) };
  const run = (years, extra = {}) => m.buildMetrics({
    nowMs: NOW, cash, pnl, pnlDetails: pnlDetails(), settings: ms.emptySettings(), extras: EXTRAS,
    targets: years ? { years, updatedAt: '2026-10-04T16:53:00.000Z', updatedBy: 'someone@example.com' } : null, targetsLib: tl, ...extra,
  });
  const fy = (out, key, i) => metric(out, key).fy[i].value;
  const sum = (rows, f) => rows.reduce((s, r) => s + f(r), 0);

  const plain = run(null);
  check(fy(plain, 'revenue', 1) === 60_000_000 && plain.cloud.years[1].total === 2_640_000 && plain.cloud.years[1].targets === null && !plain.targets.active,
    'no saved targets: the Plan as before');

  const targets = tl.validateTargets({ revenue: { mode: 'growth', growthPct: Array(12).fill(1) }, server: { enabled: true, pctOfRevenue: 8, category: CLOUD } }).targets;
  const out = run({ [String(T)]: targets });
  const deltas = tl.computeTargetDeltas(pnl.targetsBase, targets);
  const pT = tl.variantWithTargets(pnl.variants.plan, pnl.targetsBase, targets, tl.applyPnlTargets).years[1];
  const cT = tl.variantWithTargets(cash.variants.plan, cash.targetsBase, targets, tl.applyCashTargets).years[1];
  check(near(fy(out, 'revenue', 1), sum(pT.rows, (r) => r.eur.totalRevenue)) && near(fy(out, 'ebitda', 1), sum(pT.rows, (r) => r.eur.ebitda), 0.05)
    && near(fy(out, 'netCash', 1), cT.rows[11].eur.closing) && fy(out, 'revenue', 1) > 60_000_000,
    'FY 2027 revenue, EBITDA and December cash equal the pages\' Targets view');
  const dec = pT.rows[11].eur;
  check(near(fy(out, 'arr', 1), (dec.revenue + dec.pipeline - dec.churn) * 12), 'ARR December 2027 after the revenue targets');
  const ct = out.cloud.years[1];
  check(near(ct.total, sum(deltas, (d) => d.server)) && near(ct.total, sum(deltas, (d) => d.revenue) * 0.08, 0.1)
    && ct.targets.kind === 'server' && ct.targets.pct === 8 && near(ct.revenue, fy(out, 'revenue', 1)),
    'cloud 2027 = 8% of customer revenue after the targets; the cap on revenue after the targets', JSON.stringify(ct));
  check(fy(out, 'revenue', 0) === 60_000_000 && out.cloud.years[0].total === plain.cloud.years[0].total && fy(out, 'netCash', 0) === fy(plain, 'netCash', 0),
    'this year is untouched');
  check(out.targets.active && out.targets.year === T && out.targets.updatedBy === 'someone@example.com'
    && out.targets.assumptions.includes('Revenue growth 1% a month, compounding') && /2027 targets/.test(metric(out, 'revenue').note),
    'the pack says FY 2027 includes the targets, with the assumptions');

  const pctOnly = run({ [String(T)]: tl.validateTargets({ opex: { categoryPct: { [CLOUD]: 10, Marketing: -5 } } }).targets });
  check(near(pctOnly.cloud.years[1].total, 2_640_000 * 1.1) && pctOnly.cloud.years[1].targets.kind === 'category',
    'a % change of the cloud category scales it (no server %)');
  const otherYear = run({ [String(T + 1)]: targets });
  check(fy(otherYear, 'revenue', 1) === 60_000_000 && !otherYear.targets.active, 'targets saved for another year change nothing');
  const noLib = run({ [String(T)]: targets }, { targetsLib: null });
  check(fy(noLib, 'revenue', 1) === 60_000_000 && !noLib.targets.active, 'without the targets module: the Plan, not an error');
}

function testPeople() {
  console.log('\nPAYROLL / REVENUE AND REVENUE PER EMPLOYEE: through the last payroll JE');
  const hc = m.headcountByMonth(EXTRAS.employees, ['2026-02', '2026-03', '2026-06', '2026-07', '2026-08']);
  check(hc['2026-02'].total === 105 && hc['2026-03'].total === 115 && hc['2026-06'].total === 115 && hc['2026-07'].total === 110
    && hc['2026-08'].total === 111 && hc['2026-08'].byType.Contractor === 1,
  'employees at month-end: started by the last day, not left before it (a leaver on the last day still counts)', JSON.stringify(hc));

  let p = build().people;
  // Fixture: Jan–Sep closed, payroll 2M and total revenue 5M a month.
  check(p.through === mk(Y, 9) && p.months.length === 9 && p.pending.length === 0 && p.months.every((x) => x.payrollPct === 40)
    && p.ytd.payrollPct === 40 && p.ytd.label === 'Jan–Sep 2026' && p.company === 'LSports',
  'payroll / revenue each month Jan–Sep and year to date');
  check(p.months[2].revenuePerEmployee === Math.round((5_000_000 / 115) * 100) / 100 && p.months[8].headcount === 111,
    'revenue per employee: the month\'s revenue ÷ employees at its end');
  const headMonths = 105 * 2 + 115 * 4 + 110 + 111 * 2;
  check(near(p.ytd.revenuePerEmployeeMonthly, 45_000_000 / headMonths) && near(p.ytd.avgHeadcount, Math.round((headMonths / 9) * 10) / 10)
    && near(p.ytd.revenuePerEmployee, 45_000_000 / (headMonths / 9)) && near(p.ytd.revenuePerEmployeeAnnualised, (45_000_000 / (headMonths / 9)) * 12 / 9),
  'year to date: a month per employee (comparable with the months), per average employee, and at that pace a year');

  // September's payroll JE not posted yet (only a stray line): the series ends in August.
  const pnl = pnlPayload();
  pnl.variants.plan.years[0].rows[8].eur = { ...pnl.variants.plan.years[0].rows[8].eur, payroll: 40_000 };
  p = m.buildMetrics({ nowMs: NOW, cash: cashPayload(), pnl, pnlDetails: pnlDetails(), settings: ms.emptySettings(), extras: EXTRAS }).people;
  check(p.through === mk(Y, 8) && p.months.length === 8 && JSON.stringify(p.pending) === JSON.stringify([mk(Y, 9)]) && p.ytd.label === 'Jan–Aug 2026',
    'a month whose payroll JE is not posted ends the series before it, and is listed as pending');

  p = build(undefined, { ...EXTRAS, employees: null }, ['employees']).people;
  const out = build(undefined, { ...EXTRAS, employees: null }, ['employees']);
  check(p.months[0].payrollPct === 40 && p.months[0].revenuePerEmployee === null && p.ytd.revenuePerEmployee === null && out.warnings.some((w) => /HiBob/.test(w)),
    'employees unavailable: payroll / revenue still shows, revenue per employee is empty with a warning');
  const none = m.peopleMetrics(pnlPayload().variants.plan.years[1].rows, EXTRAS.employees, 'LSports');
  check(none.months.length === 0 && none.ytd === null && none.through === null, 'no closed month yet: nothing to show');
}

function testWarnings() {
  console.log('\nA SOURCE THAT FAILS');
  const noArr = build(undefined, { ...EXTRAS, arr: null }, ['arr']);
  check(metric(noArr, 'arr').lastMonth === null && noArr.warnings.length === 1 && /ARR/.test(noArr.warnings[0]), 'an empty cell and a warning, the rest still shows');
  const out = build();
  check(!('fx' in out) && !('rates' in out) && !('deposits' in out) && !('innovation' in out), 'the removed cards are not in the pack (FX conversions, rates, deposits, envelope)');
}

async function testDetails() {
  console.log('\nBREAKDOWNS: what NRR and the churn figures are made of');
  const [s] = m.nrrSeries(CUSTREV, ['2026-09']);
  const d = m.nrrDetail(CUSTREV, '2026-09');
  const line = (label) => d.summary.find((x) => x.label.startsWith(label));
  check(s.nrr === 85.71 && d.value.value === s.nrr && line('NRR').value === s.nrr && line('GRR').value === s.grr && line('Customers with revenue').value === s.customers,
    'NRR breakdown: the same NRR, GRR and base customers as the pack', JSON.stringify({ s, v: d.value }));
  check(line('Their revenue, Sep 2025').value === 280 && line('+ Expansion').value === 20 && line('− Contraction').value === -30 && line('− Churned').value === -30
    && line('= Their revenue, Sep 2026').value === 240 && 280 + 20 - 30 - 30 === 240,
    'the bridge: base + expansion − contraction − churned = their revenue now');
  const t = (title) => d.tables.find((x) => x.title.startsWith(title));
  check(t('Churned').rows[0].customer === 'Delta' && t('Contracted').rows[0].customer === 'Gamma' && t('Expanded').rows[0].customer === 'Alpha'
    && t('New since then').rows.length === 2 && /2 new customers/.test(line('Not counted').label) && line('Not counted').value === 50,
    'the customers behind each step, by name; new customers listed but not counted');
  const many = Array.from({ length: 50 }, (_, i) => [{ month: '2025-09', customer: `c${i}`, name: `C${i}`, rev: 10 + i }]).flat();
  const cut = m.nrrDetail([...many, { month: '2026-09', customer: 'x', name: 'X', rev: 1 }], '2026-09', { limit: 40 });
  check(t('Churned').total.then === 30 && cut.tables[0].rows.length === 40 && cut.tables[0].more === 10 && cut.tables[0].total.then === many.reduce((a, r) => a + r.rev, 0),
    'long lists: the largest first, the rest counted, the total over all');

  const q3 = EXTRAS.churnQuarters.find((q) => q.q === 'Q3 2026');
  const cq = m.churnQuarterDetail(q3, OPPS);
  check(cq.value.value === 40_000 && cq.tables[0].rows.length === 2 && cq.tables[0].rows[0].opportunity === 'Opp c' && cq.summary[1].value === 1
    && cq.notes.some((n) => /USD/.test(n)) && !cq.notes.some((n) => /read earlier/.test(n)),
    'churn, last full quarter: the opportunities add up to the figure; one customer with two; other currencies said');
  check(m.churnQuarterDetail({ ...q3, amount: 41_000 }, OPPS).notes.some((n) => /read earlier/.test(n)), 'a list read later than the pack says so when it differs');
  const cy = m.churnYtdDetail(Y, EXTRAS.churnQuarters, OPPS);
  check(cy.value.value === 75_000 && cy.value.value === metric(build(), 'churn').ytd.value && cy.summary.some((x) => /Q4 2026 \(in progress\)/.test(x.label))
    && cy.tables[0].rows.length === 5 && cy.tables[0].rows[0].quarter === 'Q2 2026',
    'churn, year so far: quarter by quarter (the one in progress included), the same total as the pack');
  const rows = pnlPayload().variants.plan.years[0].rows.map((r) => (r.mKey < '2026-10' ? r
    : { ...r, eur: { ...r.eur, churn: r.mKey === '2026-10' ? 40_000 / 3 : r.mKey === '2026-11' ? 80_000 / 3 : 50_000 } }));
  const months = { '2026-10': { churnIndex: 1 }, '2026-11': { churnIndex: 2 }, '2026-12': { churnIndex: 3, churnOverride: true } };
  const cf = m.churnForecastDetail(Y, rows, months, EXTRAS.churnQuarters, OPPS);
  check(near(cf.value.value, 40_000 / 3 + 80_000 / 3 + 50_000) && /Q3 2026 churned MRR ÷ 3/.test(cf.formula[0]) && near(cf.summary.find((x) => /Run-rate/.test(x.label)).value, 13_333.33)
    && cf.tables[0].rows[1].how === '2 × run-rate' && cf.tables[0].rows[2].how === 'Set in the Plan' && /context/.test(cf.tables[1].title),
    'churn, forecast: run-rate = Q3 ÷ 3, piling up month by month, a Plan override kept, Q3\'s customers as context');

  // Through the handler: each clickable cell answers with the same figure the pack shows.
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'metrics-detail-'));
  let oppReads = 0;
  let failOpps = false;
  const h = m.createMetricsHandler({
    cash: { current: () => ({ entry: { payload: cashPayload() } }) }, pnl: { current: () => ({ entry: { payload: pnlPayload(), details: pnlDetails() } }) },
    reads: {
      arr: async () => EXTRAS.arr, churnQuarters: async () => EXTRAS.churnQuarters, customerRevenue: async () => CUSTREV,
      churnedOpps: async () => { oppReads++; if (failOpps) throw new Error('Snowflake down'); return OPPS; }, employees: async () => EXTRAS.employees,
    },
    settingsFile: path.join(tmp, 's.json'), targetsFile: path.join(tmp, 't.json'), clock: () => NOW,
  });
  const pack = (await call(h)).body;
  const cellOf = (key, at) => (at === 'fy' ? metric(pack, key).fy[0] : metric(pack, key)[at]);
  const same = [];
  for (const [key, at] of [['nrr', 'lastMonth'], ['churn', 'lastMonth'], ['churn', 'ytd'], ['churn', 'fy']]) {
    const c = cellOf(key, at);
    const r = await call(h, { url: `/api/metrics?detail=${c.detail}` });
    same.push(r.status === 200 && r.body.ok && near(r.body.detail.value.value, c.value, 0.02) ? 'ok' : `${c.detail}: ${JSON.stringify(r.body).slice(0, 120)}`);
  }
  check(same.every((x) => x === 'ok') && oppReads === 1, 'the NRR and churn cells are clickable and their breakdown equals the cell (one opportunity read, cached)', same.join(', '));
  const bogus = await call(h, { url: '/api/metrics?detail=revenue' });
  failOpps = true;
  const fresh = await call(h, { url: '/api/metrics?detail=churn-ytd&refresh=true' });
  check(bogus.status === 404 && fresh.status === 200 && fresh.body.ok === false && /Snowflake/.test(fresh.body.error),
    'a figure without a breakdown → 404; a read that fails → a message, not a crash');
  fs.rmSync(tmp, { recursive: true, force: true });
}

function testNrr() {
  console.log('\nNRR: from revenue by customer');
  const rows = [
    { month: '2025-09', customer: 'A', rev: 100 }, { month: '2025-09', customer: 'B', rev: 100 }, { month: '2025-09', customer: 'Z', rev: 0 },
    { month: '2026-09', customer: 'A', rev: 120 }, { month: '2026-09', customer: 'C', rev: 50 }, { month: '2026-09', customer: 'Z', rev: 10 },
  ];
  const [s] = m.nrrSeries(rows, ['2026-09']);
  check(s.nrr === 60 && s.grr === 50 && s.customers === 2, 'NRR = what last year\'s customers pay now ÷ what they paid then; new customers excluded; GRR caps growth', JSON.stringify(s));
  check(m.nrrSeries(rows, ['2026-08']).length === 0, 'no month a year earlier: no figure');
}

function testValidation() {
  console.log('\nSETTINGS AND DEPOSITS: what can be saved');
  const ok = ms.validateSettings({ value: { cloudCapPct: 8 } });
  check(ok.ok && ok.value.cloudCategory === '' && ok.value.cloudCapPct === 8, 'valid settings are completed with defaults');
  // Settings saved while the page had the innovation envelope still save, without it.
  const legacy = ms.validateSettings({ value: { cloudCapPct: 9, innovation: { amountEur: 2_000_000, year: 2027, startMonth: 1, included: true } } });
  check(legacy.ok && legacy.value.cloudCapPct === 9 && !('innovation' in legacy.value), 'the removed innovation envelope is accepted and dropped');
  const rate = ms.validateSettings({ value: { cloudCapPct: 9, usdEurPlanningRate: 1.15 } });
  check(rate.ok && !('usdEurPlanningRate' in rate.value), 'the removed USD/EUR planning rate is accepted and dropped');
  const bads = [
    { value: { cloudCapPct: -1 } },
    { value: { other: 1 } }, { value: { cloudCategory: '__proto__' } }, {},
  ];
  check(bads.every((b) => !ms.validateSettings(b).ok), `${bads.length} kinds of bad settings are refused`);
  const dep = { id: 'x1', bank: 'Bank', amount: 10, currency: 'EUR', placedOn: '2026-09-01', maturity: null, confirmed: false, confirmedOn: null, note: '' };
  check(ms.validateDeposits({ value: [dep] }).ok, 'a valid deposit');
  const badDeps = [[dep, dep], [{ ...dep, currency: 'XYZ' }], [{ ...dep, placedOn: '2026-13-01' }], [{ ...dep, id: 'a b' }], [{ ...dep, extra: 1 }], [{ ...dep, amount: -5 }]];
  check(badDeps.every((d) => !ms.validateDeposits({ value: d }).ok), `${badDeps.length} kinds of bad deposits are refused (duplicate id, currency, date, id, field, amount)`);
}

function call(handler, { method = 'GET', url = '/api/metrics', body, headers = {} } = {}) {
  return new Promise((resolve) => {
    const raw = body === undefined ? '' : JSON.stringify(body);
    const req = Readable.from(raw ? [Buffer.from(raw)] : []);
    Object.assign(req, { method, url, headers: { host: 'finance.example', ...headers } });
    const res = {
      statusCode: 200, headers: {},
      setHeader(k, v) { this.headers[k.toLowerCase()] = v; },
      end(b) { resolve({ status: this.statusCode, body: b ? JSON.parse(b) : null }); },
    };
    handler(req, res);
  });
}

async function testHandlers() {
  console.log('\nHANDLERS: GET /api/metrics, settings, and the deposits route kept for finance-it');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'metrics-'));
  const settingsFile = path.join(tmp, 's.json');
  const depositsFile = path.join(tmp, 'd.json');
  const targetsFile = path.join(tmp, 't.json');
  const entry = (payload, details) => ({ entry: { payload, details } });
  let state = { cash: entry(cashPayload()), pnl: entry(pnlPayload(), pnlDetails()) };
  let reads = 0;
  const stubReads = {
    arr: async () => { reads++; return EXTRAS.arr; },
    churnQuarters: async () => EXTRAS.churnQuarters,
    customerRevenue: async () => [{ month: '2025-09', customer: 'A', rev: 100 }, { month: '2026-09', customer: 'A', rev: 110 }],
    churnedOpps: async () => OPPS,
    employees: async (company) => (company === 'LSports' ? EXTRAS.employees : []),
  };
  const h = m.createMetricsHandler({
    cash: { current: () => state.cash }, pnl: { current: () => state.pnl }, reads: stubReads, settingsFile, depositsFile, targetsFile, clock: () => NOW,
  });
  let r = await call(h);
  check(r.status === 200 && r.body.status === 'ready' && r.body.metrics.length === 6 && metric(r.body, 'nrr').lastMonth.value === 110,
    'GET: the pack from the cached projections and the reads');
  check(r.body.warnings.length === 0 && metric(r.body, 'churn').lastMonth.detail === 'churn-quarter', 'no warnings when every read works; the churn cells carry their breakdown');
  check(r.body.people.months.length === 9 && r.body.people.months[8].headcount === 111 && r.body.people.company === 'LSports',
    'the employees of the P&L\'s company (LSports by default) are read for revenue per employee');
  await call(h);
  check(reads === 1, 'reads are cached between requests');
  await call(h, { url: '/api/metrics?refresh=true' });
  check(reads === 2, '?refresh=true reads again');
  state = { cash: { computing: true, startedMs: NOW - 4000 }, pnl: state.pnl };
  r = await call(h);
  check(r.status === 202 && r.body.status === 'computing' && r.body.elapsedSec === 4, 'a projection still computing → 202 computing');
  state = { cash: { error: 'NetSuite is not configured on this server.' }, pnl: entry(pnlPayload(), pnlDetails()) };
  r = await call(h);
  check(r.status === 200 && r.body.status === 'error' && /NetSuite/.test(r.body.error), 'a projection that failed → its error');

  const sh = m.createMetricsSettingsHandler({ file: settingsFile, clock: () => NOW });
  r = await call(sh, { url: '/api/metrics/settings' });
  check(r.body.ok && r.body.value.cloudCapPct === 8 && r.body.updatedAt === null, 'settings GET: the defaults before any save');
  const json = { 'content-type': 'application/json' };
  r = await call(sh, { method: 'PUT', url: '/api/metrics/settings', body: { value: { cloudCapPct: 9, usdEurPlanningRate: 1.15 } }, headers: json });
  const r2 = await call(sh, { method: 'PUT', url: '/api/metrics/settings', body: { value: { cloudCapPct: 900 } }, headers: json });
  const r3 = await call(sh, { method: 'PUT', url: '/api/metrics/settings', body: { value: {} }, headers: { ...json, origin: 'https://evil.example' } });
  check(r.status === 200 && r.body.value.cloudCapPct === 9 && r2.status === 400 && r3.status === 403, 'settings PUT: saved; out of range 400; another site 403');
  state = { cash: entry(cashPayload()), pnl: entry(pnlPayload(), pnlDetails()) };
  r = await call(h);
  check(r.body.settings.cloudCapPct === 9 && r.body.cloud.capPct === 9 && !('usdEurPlanningRate' in r.body.settings), 'the pack uses the saved settings (the planning rate dropped)');
  // A settings file saved with the (removed) innovation envelope IN: it no longer changes anything.
  const before = metric(r.body, 'ebitda').fy[1].value;
  fs.writeFileSync(settingsFile, JSON.stringify({ value: { cloudCapPct: 9, usdEurPlanningRate: 1.15, cloudCategory: '', innovation: { amountEur: 1_200_000, year: T, startMonth: 1, included: true } } }));
  r = await call(h);
  check(metric(r.body, 'ebitda').fy[1].value === before && !('innovation' in r.body) && !('innovation' in r.body.settings),
    'an innovation envelope saved before its removal no longer counts');
  const dh = m.createMetricsDepositsHandler({ file: depositsFile, clock: () => NOW });
  r = await call(dh, { method: 'PUT', url: '/api/metrics/deposits', headers: json, body: { value: [{ id: 'd1', bank: 'Bank', amount: 100, currency: 'EUR', placedOn: '2026-10-01', maturity: null, confirmed: false, confirmedOn: null, note: '' }] } });
  const pack = await call(h);
  check(r.status === 200 && !('deposits' in pack.body) && fs.readFileSync(depositsFile.replace(/\.json$/, '-history.jsonl'), 'utf8').trim().split('\n').length === 1,
    'the deposits route still answers (finance-it mounts it); the pack no longer lists deposits');
  check(pack.body.targets && !pack.body.targets.active, 'no targets file: the projection year is the Plan');
  // Targets saved on the New Bank Dashboard show at the next request (no cache to wait for). The fixture's
  // baseline has no revenue, so only the server % changes something: cloud = 8% of nothing.
  fs.writeFileSync(targetsFile, JSON.stringify({ version: 1, years: { [String(T)]: { server: { enabled: true, pctOfRevenue: 8, category: CLOUD } } }, updatedAt: null, updatedBy: null }));
  const withTargets = await call(h);
  check(withTargets.body.targets.active && withTargets.body.cloud.years[1].targets.kind === 'server', 'a saved targets file applies at the next request');
  fs.rmSync(tmp, { recursive: true, force: true });
}

async function testModel() {
  console.log('\nPAGE MODEL: formatting and the exported rows (src/metrics/model.ts)');
  const model = await import(pathToFileURL(path.join(ROOT, 'src', 'metrics', 'model.ts')).href);
  check(model.formatEur(5_000_000) === '€5.00M' && model.formatEur(-40_000) === '-€40K' && model.formatEur(0) === '–' && model.formatEur(null) === '–'
    && model.formatPct(104.5) === '104.5%', 'figures: € millions / thousands, percentages, a dash for nothing');
  const rows = model.packRows(build());
  const at = (label) => rows.find((r) => r[0] === label);
  check(rows[0][0] === 'LSports metrics pack, as of Sep 2026' && at('Metric').join('|') === 'Metric|Last month|Year to date|FY 2026|FY 2027|Basis',
    'export: title and the same header every month');
  check(at('Revenue')[1] === '€5,000,000 (A)' && at('Revenue')[3] === '€60,000,000 (A+F)' && at('Revenue')[4] === '€60,000,000 (F)' && at('NRR')[1] === '104.5% (A)',
    'export: each figure tagged A, F or A+F', at('Revenue').join(' | '));
  check(rows.some((r) => /^Payroll \/ revenue and revenue per employee, through Sep 2026/.test(String(r[0])))
    && at('Jan–Sep 2026') && at('Jan–Sep 2026')[3] === '40.0%' && at('Sep 2026') && at('Sep 2026')[4] === 111,
  'export: payroll / revenue and revenue per employee by month and year to date');
  check(rows.some((r) => String(r[0]).startsWith('Cloud (')) && !rows.some((r) => /innovation|USD\/EUR|FX conversions|Deposits/i.test(String(r[0]))),
    'export: cloud; no envelope, rate, FX conversions or deposits');
  check(model.formatExplain(1234.5, 'eur') === '€1,235' && model.formatExplain(-30, 'eur', true) === '-€30' && model.formatExplain(20, 'eur', true) === '+€20'
    && model.formatExplain(85.714, 'pct') === '85.7%' && model.formatExplain(4, 'int') === '4' && model.formatExplain('Alpha', 'text') === 'Alpha' && model.formatExplain(null, 'eur') === '–',
  'breakdown figures: € in full, signed steps, % to one decimal, counts and names');
}

async function main() {
  console.log('=== metrics checks (synthetic data) ===');
  testPack();
  testCloud();
  await testTargets();
  testPeople();
  testWarnings();
  testNrr();
  testValidation();
  await testHandlers();
  await testDetails();
  await testModel();
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
