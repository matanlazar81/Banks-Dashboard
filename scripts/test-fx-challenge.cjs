#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// Checks for the FX Challenge page: the shared logic (src/forecast/fx-challenge.mjs), the saved table's
// validation, GET /api/fx-challenge and GET/PUT /api/fx-challenge/table (server/fx-challenge.cjs), and the
// NetSuite reads' mapping (netsuite-api.cjs, fetch stubbed). No network.
//   node scripts/test-fx-challenge.cjs
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Readable } = require('stream');
const { pathToFileURL } = require('url');

const ROOT = path.resolve(__dirname, '..');
const m = require(path.join(ROOT, 'server', 'metrics.cjs'));
const ms = require(path.join(ROOT, 'server', 'metrics-settings.cjs'));
const fxs = require(path.join(ROOT, 'server', 'fx-challenge.cjs'));

let failures = 0;
function check(ok, label, detail) {
  if (ok) console.log(`  ✓ ${label}`);
  else { failures++; console.log(`  ✗ ${label}${detail !== undefined ? ` — ${detail}` : ''}`); }
}
const near = (a, b, tol = 0.5) => Math.abs(a - b) <= tol;
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const TODAY = '2026-10-21';
const NOW = Date.parse('2026-10-21T09:00:00+03:00');

// The table as the user pasted it from Excel (October 2026), tab-separated.
const PASTE = [
  ['', '<3.3', '3.3-3.4', '3.4-3.49', '3.49-3.5', '3.5-3.527', '3.527-3.56', '3.56-3.6', '3.6-3.7', '3.7-3.85', '3.85-3.915', '3.915-3.93', '3.93-4.1', '4.1-4.295', '>4.295'],
  ...['10/2026', '11/2026', '12/2026'].map((mo) => [mo, '-500 / 3.62', '-800 / 3.67', '-800 / 3.67', '-1100 / 3.62', '-1100 / 3.62', '-1100 / 3.62', '-1400 / 3.61', '-1600 / 3.65', '-1800 / 3.72', '-1800 / 3.72', '-1800 / 3.72', '-2000 / 3.74', '-2200 / 3.77', '-2400 / 3.82']),
  ...Array.from({ length: 12 }, (_, i) => [`${i + 1}/2027`, '-300 / 3.49', '-850 / 3.53', '-1050 / 3.59', '-1350 / 3.57', '-1550 / 3.61', '-1550 / 3.61', '-1850 / 3.6', '-2050 / 3.63', '-2050 / 3.63', '-2250 / 3.65', '-2450 / 3.68', '-2650 / 3.69', '-2650 / 3.69', '-2650 / 3.69']),
].map((r) => r.join('\t')).join('\n');

// October 2026's "Conversion" journals (Hapoalim 583400): one spot conversion before the fixing, the fixing's
// three deals on the 6th, and two spot conversions after.
const OCT_CONVERSIONS = [
  { id: 1, tranid: 'JE10859', date: '2026-10-01', eur: 800000, ils: 2785136 },
  { id: 2, tranid: 'JE10953', date: '2026-10-06', eur: 200000, ils: 820000 },
  { id: 3, tranid: 'JE10954', date: '2026-10-06', eur: 300000, ils: 1068000 },
  { id: 4, tranid: 'JE10955', date: '2026-10-06', eur: 300000, ils: 1047000 },
  { id: 5, tranid: 'JE11019', date: '2026-10-12', eur: 800000, ils: 2747984 },
  { id: 6, tranid: 'JE11080', date: '2026-10-14', eur: 650000, ils: 2232925.5 },
];

/** A daily EUR/ILS series ending on TODAY at 3.4778, with alternating ±0.4% moves on business days, and
 * (unless clean) 6 and 7 October at their real rates. */
function series(days = 120, { clean = false } = {}) {
  const out = [];
  let rate = 3.4778;
  const end = Date.parse(`${TODAY}T00:00:00Z`);
  for (let k = 0; k < days; k++) {
    const d = new Date(end - k * 86400000).toISOString().slice(0, 10);
    out.push({ date: d, rate });
    const dow = new Date(end - k * 86400000).getUTCDay();
    if (dow !== 0 && dow !== 6) rate = rate / (k % 2 ? 1.004 : 0.996);
  }
  const s = out.reverse();
  if (!clean) {
    for (const [date, rate] of [['2026-10-06', 3.433991], ['2026-10-07', 3.424957]]) {
      const p = s.find((x) => x.date === date);
      if (p) p.rate = rate;
    }
  }
  return s;
}

async function testLogic(fx) {
  console.log('\nLOGIC: bands, fixings, risk');
  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  check(fx.tableProblems(t).length === 0 && t.rows.length === 15 && t.bounds.length === 13, 'the default table is the one received: 15 months, 14 bands, no problems');
  const labels = fx.bandLabels(t.bounds);
  check(labels.length === 14 && labels[0] === '<3.3' && labels[2] === '3.4–3.49' && labels[13] === '>4.295', 'band labels', labels.join(' '));
  const idx = [3.29, 3.3, 3.4, 3.489, 3.49, 4.294, 4.295, 5].map((r) => fx.bandIndex(t.bounds, r));
  check(same(idx, [0, 1, 2, 2, 3, 12, 13, 13]), 'bands are lower-inclusive: a rate on a bound falls in the band above', idx.join(','));

  const oct = fx.conversionAt(t.rows[0], t.bounds, 3.424957);
  check(oct.eur === 800000 && oct.rate === 3.67 && near(oct.ils, 2936000) && Math.abs(oct.ils - 2935000) / 2935000 < 0.001,
    'October at 3.425: €800K at 3.67 = ₪2,936,000, within 0.1% of the ₪2,935,000 booked on 6 Oct');
  check(near(oct.vsSpotIls, 800000 * (3.67 - 3.424957)), 'the gain against selling at that rate is € × (rate − EUR/ILS)');

  const fs_ = fx.fixings(t, TODAY);
  check(fs_[0].status === 'past' && fs_[1].status === 'next' && fs_[2].status === 'future', 'October is past, November next, the rest later');
  check(fs_[1].date === '2026-11-07' && fs_[1].weekday === 'Sat' && fs_[1].weekend && fs_[1].days === 17, '7 Nov 2026 is a Saturday, 17 days away (flagged)');
  check(fx.fixingDate('2027-02', 30) === '2027-02-28', 'a fixing day past the month end falls on its last day');

  const p = fx.bandProbabilities(t.bounds, 3.48, 0.075, 17 / 365);
  check(near(p.reduce((a, b) => a + b, 0), 1, 1e-9) && p.every((x) => x >= 0), 'band probabilities add up to 1');
  check(p[2] > 0.45 && p[3] + p[4] + p[5] > 0.3 && p[0] < 0.01 && p[13] < 1e-6, 'at 3.48 with 7.5% volatility: about half today\'s band, a third 3.49–3.56, almost no tails', p.map((x) => x.toFixed(3)).join(' '));
  const point = fx.bandProbabilities(t.bounds, 3.48, 0, 0.1);
  check(point[2] === 1 && point.reduce((a, b) => a + b, 0) === 1, 'without volatility, all of it in today\'s band');

  const r6 = fx.fixingRisk(fs_[1].row, t.bounds, 3.48, 0.06, 17 / 365);
  const r9 = fx.fixingRisk(fs_[1].row, t.bounds, 3.48, 0.09, 17 / 365);
  check(r6.p95Eur === 1100000 && r9.p95Eur === 1400000 && r6.worstEur === 2400000, 'November: 95% is €1.1M at 6% volatility, €1.4M at 9%; the worst case €2.4M');
  check(r6.expectedEur > 800000 && r6.expectedEur < r9.expectedEur && r9.expectedEur < 1100000, 'the expected euros sit between today\'s band and the next, higher with more volatility');
  check(near(r6.expectedIls / r6.expectedEur, 3.65, 0.03), 'the expected shekels follow the cells\' rates');

  const vol = fx.annualVol(series(120, { clean: true }));
  check(vol !== null && near(vol, 0.004 * Math.sqrt(252), 0.004), 'volatility: ±0.4% a business day is about 6.3% a year (weekends ignored)', vol);
  check(fx.annualVol(series(10)) === null, 'too little history → no volatility (the page assumes one)');
  check(same(fx.rateOn(series(30), '2026-10-07'), { date: '2026-10-07', rate: 3.424957 }) && fx.rateOn(series(30), '2020-01-01') === null, 'the rate on a day is the latest one on or before it');

  const conv = fx.conversionAt(fs_[1].row, t.bounds, 3.48);
  check(fx.requirementOf('today', conv, r6) === 800000 && fx.requirementOf('p95', conv, r6) === 1100000 && fx.requirementOf('worst', conv, r6) === 2400000, 'what to keep: today\'s band / 95% / worst case');
}

async function testOutcomes(fx) {
  console.log('\nOUTCOMES: what a conversion can sell, by EUR/ILS range');
  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  const nov = t.rows.find((r) => r.month === '2026-11');
  const risk = fx.fixingRisk(nov, t.bounds, 3.469, 0.086, 17 / 365);
  const o = fx.outcomeRanges(nov, t.bounds, risk.probs);
  check(same(o.map((x) => [x.lo, x.hi, x.eur]), [[null, 3.3, 500000], [3.3, 3.49, 800000], [3.49, 3.56, 1100000], [3.56, 3.6, 1400000], [3.6, 3.7, 1600000], [3.7, 3.93, 1800000], [3.93, 4.1, 2000000], [4.1, 4.295, 2200000], [4.295, null, 2400000]]),
    'neighbouring bands that sell the same are merged: 14 bands, 9 ranges', JSON.stringify(o.map((x) => [x.lo, x.hi, x.eur])));
  check(near(o.reduce((s, x) => s + x.p, 0), 1, 1e-9) && near(o.reduce((s, x) => s + x.p * x.eur, 0), risk.expectedEur, 1e-6), 'the chances add up to 1, and to the expected amount');
  check(o[1].p > 0.55 && o[1].p < 0.7 && o[2].p > 0.2 && o[2].p < 0.35 && same(o[1].bands, [1, 2]) && near(o[1].ils, 800000 * 3.67),
    'on 7 Nov from 3.469: about 6 in 10 for €800K at 3.67, about 3 in 10 for €1.1M', `${o[1].p.toFixed(3)} / ${o[2].p.toFixed(3)}`);
}

async function testPlan(fx) {
  console.log('\nPLAN: what to keep and what is free');
  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  const fixings = fx.fixings(t, TODAY).filter((f) => f.status !== 'past');
  const upcoming = fixings.map((f) => ({ month: f.month, date: f.date, days: f.days, conv: fx.conversionAt(f.row, t.bounds, 3.48), risk: fx.fixingRisk(f.row, t.bounds, 3.48, 0.09, f.days / 365) }));
  const books = [
    { bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 979709.61 },
    { bank: 'PKO', kind: 'current', currency: 'EUR', amount: 229486.53 },
    { bank: 'Payhound', kind: 'current', currency: 'EUR', amount: 65161.69 },
    { bank: 'PayPal', kind: 'current', currency: 'EUR', amount: 32792.91 },
    { bank: 'AMEX', kind: 'card', currency: 'EUR', amount: -2629.03 },
    { bank: 'Bank Hapoalim', kind: 'deposit', currency: 'EUR', amount: 4204562.36 },
    { bank: 'Bank Hapoalim', kind: 'current', currency: 'USD', amount: 500000 },
    { company: 'Bringits', bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 999999 },
  ];
  const pos = fx.eurPosition(books);
  check(near(pos.hapoalim, 979709.61) && near(pos.other, 327441.13) && pos.deposits.length === 1 && pos.deposits[0].valueDate === null,
    'euros by where they sit: Hapoalim, the other accounts (cards, USD and Bringits left out), deposits');

  const a = fx.planReserve({ upcoming, policy: 'p95', eur: pos, today: TODAY }).next;
  check(a.required === 1400000 && near(a.liquid, 1307150.74) && a.maturing === 0 && near(a.backup, 4204562.36), 'keep €1.4M; €1.31M liquid; the €4.2M deposits (no date) are backup');
  check(a.freeNow === 0 && near(a.shortfall, 92849.26) && near(a.freeNowWithBackup, 1307150.74 - 800000),
    'nothing free while the 95% reserve is kept liquid; €507K free if the backup covers a jump above today\'s band');
  check(near(a.moveToHapoalim, 327441.13), 'Hapoalim is short of the reserve: move what sits elsewhere (capped at what there is)');

  const before = { ...pos, deposits: [{ amount: 1000000, valueDate: '2026-11-03', blocked: false, label: 'EUR fixed-term' }, { amount: 3000000, valueDate: '2027-03-05', blocked: false, label: 'EUR fixed-term' }, { amount: 500000, valueDate: '2026-11-01', blocked: true, label: 'blocked' }] };
  const b = fx.planReserve({ upcoming, policy: 'p95', eur: before, today: TODAY }).next;
  check(b.maturing === 1000000 && b.backup === 3000000 && b.needLiquid === 400000 && near(b.freeNow, 907150.74),
    'a deposit maturing before the fixing covers it (€1M), a later one is backup, a blocked one counts for nothing');
  check(b.moveToHapoalim === 0, 'nothing to move when Hapoalim and the maturing deposit cover it');

  const c = fx.planReserve({ upcoming, policy: 'today', eur: pos, today: TODAY, inflowPerDay: 100000 });
  check(c.next.required === 800000 && near(c.next.freeNow, 507150.74) && c.next.inflowUntil === 1600000 && near(c.next.freeByFixing, 2107150.74),
    'today\'s band only: €507K free now; with €100K a day of inflow (16 days), €2.11M by the fixing');
  check(c.path.length === 6 && near(c.path[0].surplus, 1307150.74 + 1600000 - 800000) && near(c.path[1].available, 1307150.74 + 100000 * 46 - 800000),
    'the path: euros by each fixing, with inflows and maturing deposits, less the earlier fixings');
  const suffixMin = (p, i) => Math.max(0, Math.min(...p.slice(i).map((x) => x.surplus)));
  check(c.path.every((x, i) => near(x.convertibleBefore, suffixMin(c.path, i))) && near(c.next.freeByFixing, c.path[0].convertibleBefore),
    'what can be converted before a fixing is the smallest spare from it on (euros converted then are gone later too)');
  const dry = fx.planReserve({ upcoming, policy: 'p95', eur: pos, today: TODAY, inflowPerDay: 0 });
  check(dry.path[0].surplus > dry.path[5].surplus && dry.path[0].convertibleBefore === suffixMin(dry.path, 0) && dry.next.freeByFixing === 0,
    'with nothing coming in, a later fixing caps what can be converted before the first (here nothing)');
  const none = fx.planReserve({ upcoming, policy: 'p95', eur: pos, today: TODAY });
  check(none.path.length === 1 && none.next.freeByFixing === null && none.next.inflowUntil === null, 'without the inflow, only the next fixing is planned');
  check(fx.planReserve({ upcoming: [], eur: pos, today: TODAY }) === null, 'no fixings left → no plan');

  // Which deposits each fixing gains: those maturing after the previous fixing (after today for the first) and by it.
  const dated = { ...pos, deposits: [
    { amount: 500000, valueDate: '2026-11-07', blocked: false, label: 'on the day' },
    { amount: 300000, valueDate: '2026-10-25', blocked: false, label: 'October' },
    { amount: 200000, valueDate: '2026-11-08', blocked: false, label: 'the day after' },
    { amount: 400000, valueDate: '2026-12-01', blocked: true, label: 'blocked' },
    { amount: 900000, valueDate: '2027-06-01', blocked: false, label: 'after April' },
    { amount: 100000, valueDate: TODAY, blocked: false, label: 'today' },
  ] };
  const r = fx.planReserve({ upcoming, policy: 'today', eur: dated, today: TODAY, inflowPerDay: 0 });
  check(r.path[0].released === 800000 && same(r.path[0].releasedDeposits.map((x) => x.label), ['October', 'on the day']) && r.path[0].released === r.next.maturing,
    'the first fixing gains the deposits maturing by its day, earliest first (the same as "maturing by then")');
  check(r.path[1].released === 200000 && r.path[1].releasedDeposits[0].valueDate === '2026-11-08' && r.path.slice(2).every((p) => p.released === 0 && !p.releasedDeposits.length),
    'a deposit maturing the day after a fixing counts for the next one; a blocked one never does');
  check(r.later.length === 1 && r.later[0].label === 'after April' && r.later[0].amount === 900000, 'deposits maturing after the last fixing shown are listed apart');
  let held = r.next.liquid;
  const consistent = r.path.every((p) => { held += p.released; const ok = near(p.available, held); held -= p.required; return ok; });
  check(consistent, 'euros by each fixing = today\'s liquid euros + the deposits released by then − the earlier fixings (no inflow here)');
  const one = fx.planReserve({ upcoming, policy: 'today', eur: dated, today: TODAY });
  check(one.path.length === 1 && one.later.reduce((s, d) => s + d.amount, 0) === 1100000, 'without the inflow, the deposits after the next fixing are all "later"');

  const flows = [
    { month: '2026-07', kind: 'customer', eur: 4969632 }, { month: '2026-07', kind: 'vendor', eur: -389321 }, { month: '2026-07', kind: 'other', eur: 58695 },
    { month: '2026-07', kind: 'conversion', eur: -2646789 }, { month: '2026-07', kind: 'deposit', eur: -1400000 }, { month: '2026-07', kind: 'transfer', eur: -210194 },
    { month: '2026-08', kind: 'customer', eur: 4537969 }, { month: '2026-08', kind: 'vendor', eur: -814475 }, { month: '2026-08', kind: 'refund', eur: -49575 },
  ];
  const inflow = fx.typicalInflow(flows, ['2026-07', '2026-08']);
  check(near(inflow.months[0].net, 4969632 - 389321 + 58695) && near(inflow.perMonth, (4639006 + 3673919) / 2) && near(inflow.perDay, inflow.perMonth / 30.4, 1e-6),
    'the typical inflow leaves out conversions, deposits and transfers');
}

async function testPast(fx) {
  console.log('\nPAST: the fixing against what was booked');
  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  const oct = fx.fixings(t, TODAY)[0];
  const c = fx.checkPast({ fixing: oct, bounds: t.bounds, series: series(60), conversions: OCT_CONVERSIONS });
  check(c.booked && c.booked.date === '2026-10-06' && c.booked.n === 3 && c.booked.eur === 800000 && c.booked.ils === 2935000,
    'October: the three journals of 6 Oct (€800K → ₪2,935,000) are the fixing, not the spot conversions around it');
  check(c.match && near(c.booked.rate, 3.66875, 1e-5) && c.conv.rate === 3.67, 'they match the table (€800K at 3.67)');
  check(near(c.gainVsSpotIls, 2935000 - 800000 * 3.433991, 1), 'the gain is against that day\'s rate (₪188K)', c.gainVsSpotIls);
  const none = fx.checkPast({ fixing: oct, bounds: t.bounds, series: series(60), conversions: [] });
  check(none.booked === null && !none.match && none.gainVsSpotIls === null && none.conv.eur === 800000, 'nothing booked → the table only');
}

async function testParse(fx) {
  console.log('\nPASTE: the table from Excel');
  const p = fx.parseTable(PASTE);
  check(p.ok && same(p.table, fx.cloneTable(fx.DEFAULT_TABLE)), 'the paste as received equals the default table', p.errors.join(' | '));
  const round = fx.parseTable(fx.tableToText(fx.DEFAULT_TABLE));
  check(round.ok && same(round.table, fx.cloneTable(fx.DEFAULT_TABLE)), 'the table as Excel text reads back the same');
  const loose = fx.parseTable(PASTE.replace(/\t-/g, '\t−').replace(/3\.3-3\.4/, '3.3 – 3.4').split('\n').map((l) => `${l}\t\t`).join('\r\n').replace(/^\t/, ''));
  check(loose.ok && same(loose.table, fx.cloneTable(fx.DEFAULT_TABLE)), 'Windows line ends, trailing tabs, dashes, a missing corner cell: all fine', loose.errors.join(' | '));
  const day = fx.parseTable(PASTE, { fixingDay: 6 });
  check(day.ok && day.table.fixingDay === 6, 'the fixing day comes with it');
  const bad = [
    [PASTE.replace('3.4-3.49', '3.41-3.49'), /starts at 3\.41/],
    [PASTE.replace('-800 / 3.67\t-800', 'x\t-800'), /Line 2, band 2/],
    [PASTE.replace('11/2026', '13/2026'), /Line 3: "13\/2026"/],
    [PASTE.split('\n').map((l, i) => (i === 4 ? l.split('\t').slice(0, -1).join('\t') : l)).join('\n'), /2027-01 has 13 cells/],
    [`${PASTE}\n${PASTE.split('\n')[1]}`, /2026-10 appears twice/],
    ['', /header row/],
  ];
  const results = bad.map(([text, re]) => { const r = fx.parseTable(text); return !r.ok && r.errors.some((e) => re.test(e)) ? 'ok' : `${re}: ${r.errors.join(' | ')}`; });
  check(results.every((r) => r === 'ok'), `${bad.length} kinds of bad paste are refused with the line`, results.filter((r) => r !== 'ok').join(' || '));
}

async function testValidation(fx) {
  console.log('\nTABLE: what may be saved');
  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  const ok = fxs.validateTable({ value: t });
  check(ok.ok && same(ok.value, t), 'the table received in October 2026 saves as it is');
  const reset = fxs.validateTable({ value: null });
  check(reset.ok && reset.value === null, 'null puts the default table back');
  check(!fxs.validateTable({}).ok && !fxs.validateTable({ value: t, extra: 1 }).ok, 'a body without the table, or with more, is refused');
  const variants = [
    (x) => { x.bounds[3] = 3.4; },
    (x) => { x.rows[0].cells.pop(); },
    (x) => { x.rows[1].month = x.rows[0].month; },
    (x) => { x.rows[2].cells[4].rate = 0; },
    (x) => { x.rows[2].cells[4].eurK = 1e6; },
    (x) => { x.fixingDay = 31; },
    (x) => { x.rows[0].month = '2026-13'; },
    (x) => { x.rows = []; },
  ];
  const verdicts = variants.map((f) => { const x = fx.cloneTable(t); f(x); return { server: fxs.validateTable({ value: x }).ok, page: fx.tableProblems(x).length === 0 }; });
  check(verdicts.every((v) => !v.server && !v.page), `${variants.length} kinds of bad table are refused by the server and flagged by the page alike`, JSON.stringify(verdicts));
  const extra = fx.cloneTable(t);
  extra.rows[0].note = 'x';
  check(!fxs.validateTable({ value: extra }).ok, 'an unknown field is refused');
  const big = fx.cloneTable(t);
  big.rows = Array.from({ length: 60 }, (_, i) => ({ month: `${2026 + Math.floor(i / 12)}-${String((i % 12) + 1).padStart(2, '0')}`, cells: t.rows[3].cells }));
  check(fxs.validateTable({ value: big }).ok && JSON.stringify({ value: big }).length < 64 * 1024, 'five years of months fit the 64KB limit');
  const legacy = ms.validateSettings({ value: { cloudCapPct: 8, cloudCategory: 'Cloud', fxChallenge: t } });
  check(legacy.ok && !('fxChallenge' in legacy.value) && legacy.value.cloudCapPct === 8, 'Metrics settings saved while the table was kept there still save, without it');
}

function call(handler, { method = 'GET', url = '/api/fx-challenge', body, headers = {} } = {}) {
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

async function testHandler(fx) {
  console.log('\nHANDLERS: GET /api/fx-challenge, GET/PUT /api/fx-challenge/table');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fx-'));
  const tableFile = path.join(tmp, 'table.json');
  const counts = { rates: 0, books: 0, mail: 0, flows: 0, conversions: 0 };
  let mailMode = 'off';
  const fxReads = {
    rates: async () => { counts.rates++; return series(400); },
    books: async () => { counts.books++; return [{ number: '11001', name: '11001 Bank HPoalim Euro - 583400', eur: 979709.61 }, { number: '16003', name: '16003 Deposit - Bank Hapoalim - EURO', eur: 4204562.36 }, { number: '17026', name: '17026 4295 - AMEX - Euro', eur: -2629.03 }, { number: '11018', name: '11018 Clearing Bank Account EUR', eur: 0 }]; },
    get mail() {
      if (mailMode === 'off') return null;
      return async () => {
        counts.mail++;
        if (mailMode === 'fail') throw new Error('RESTlet 403: INSUFFICIENT_PERMISSION');
        return { ok: true, builtAt: '2026-10-21T05:00:00Z', notes: ['Leumi has no balance in its files.'], rows: [
          { company: 'LSports', bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 981000, asOf: '2026-10-20' },
          { company: 'LSports', bank: 'Hapoalim deposits', kind: 'deposit', currency: 'EUR', amount: 1500000, valueDate: '2026-11-03', label: 'EUR fixed-term' },
          { company: 'LSports', bank: 'Bank Hapoalim', kind: 'current', currency: 'ILS', amount: 12345 },
          { company: 'LSports', bank: 'Bank Hapoalim', kind: 'weird', currency: 'EUR', amount: 1 },
        ] };
      };
    },
    flows: async (from) => { counts.flows++; return from === '2026-06' ? [{ month: '2026-09', kind: 'customer', eur: 4424436 }] : []; },
    conversions: async (from) => { counts.conversions++; return from === '2026-09-20' ? OCT_CONVERSIONS : []; },
  };
  const h = fxs.createFxChallengeHandler({ reads: fxReads, tableFile, clock: () => NOW });

  const a = (await call(h)).body;
  check(a.ok && a.status === 'ready' && a.today === TODAY, 'answers on its own route');
  check(same(a.table, fx.cloneTable(fx.DEFAULT_TABLE)) && a.tableSaved === null, 'no saved table → the default');
  check(a.rates.spot.date === TODAY && near(a.rates.spot.rate, 3.4778, 1e-9) && a.rates.sigma > 0.05 && a.rates.series.length === 400, 'today\'s rate and the volatility from 400 days');
  check(a.balances.source === 'books' && a.balances.rows.length === 3 && a.balances.rows.find((r) => r.kind === 'deposit').bank === 'Bank Hapoalim'
    && a.balances.rows.find((r) => /AMEX/.test(r.account)).kind === 'card' && a.warnings.some((w) => /NS_BALANCES_RESTLET/.test(w)),
  'without the RESTlet: the books (deposit and card told apart, zero balances dropped) and a note on how to set it up');
  check(a.conversions.length === 6 && a.flows.length === 1 && same(a.inflowMonths, ['2026-07', '2026-08', '2026-09']), 'flows from four months back, conversions from before the first fixing, the inflow months');

  await call(h);
  check(counts.rates === 1 && counts.books === 1 && counts.flows === 1 && counts.conversions === 1, 'a second read is cached');

  mailMode = 'on';
  const b = (await call(h, { url: '/api/fx-challenge?refresh=true' })).body;
  check(counts.rates === 2 && b.balances.source === 'mail' && b.balances.rows.length === 3 && b.balances.asOf === '2026-10-21T05:00:00Z'
    && b.balances.rows.find((r) => r.kind === 'deposit').valueDate === '2026-11-03' && b.warnings.some((w) => /1 balance row/.test(w)) && b.balances.notes.length === 1,
  'with the RESTlet: the mail\'s rows, deposits with their dates; a row that doesn\'t fit is left out and said');

  mailMode = 'fail';
  const c = (await call(h, { url: '/api/fx-challenge?refresh=true' })).body;
  check(c.ok && c.balances.source === 'books' && c.warnings.some((w) => /could not be read/.test(w)), 'the RESTlet fails → the books, with a warning');

  const wrong = await call(h, { method: 'PUT', body: {} });
  check(wrong.status === 405, 'the page\'s data is read-only (PUT → 405)');
  const down = async () => { throw new Error('NetSuite down'); };
  const broken = fxs.createFxChallengeHandler({ reads: { rates: down, books: down, mail: null, flows: down, conversions: down }, tableFile, clock: () => NOW });
  const e = (await call(broken)).body;
  check(e.ok && e.rates.spot === null && e.balances === null && e.warnings.length === 4, 'every read failing → still an answer, with four warnings');

  const table = fxs.createFxTableHandler({ file: tableFile, clock: () => NOW });
  const json = { 'content-type': 'application/json', origin: 'https://finance.example' };
  const before = (await call(table, { url: '/api/fx-challenge/table' })).body;
  check(before.ok && before.value === null && before.updatedBy === null, 'GET the table before any save → null (the default)');
  const saved = fx.cloneTable(fx.DEFAULT_TABLE);
  saved.fixingDay = 6;
  const put = await call(table, { method: 'PUT', url: '/api/fx-challenge/table', body: { value: saved }, headers: json });
  const historyFile = path.join(tmp, 'table-history.jsonl');
  const history = () => (fs.existsSync(historyFile) ? fs.readFileSync(historyFile, 'utf8').trim().split('\n') : []);
  check(put.status === 200 && put.body.ok && put.body.value.fixingDay === 6 && history().length === 1, 'PUT saves the table, with a history line', JSON.stringify(put.body).slice(0, 200));
  const d = (await call(h)).body;
  check(d.table.fixingDay === 6 && d.tableSaved && d.tableSaved.at === new Date(NOW).toISOString(), 'a saved table is used at once (no cache), with when it was saved');
  const bad = fx.cloneTable(fx.DEFAULT_TABLE);
  bad.bounds[3] = 3.4;
  const refused = await call(table, { method: 'PUT', url: '/api/fx-challenge/table', body: { value: bad }, headers: json });
  check(refused.status === 400 && history().length === 1, 'a bad table is refused and nothing is written');
  const back = await call(table, { method: 'PUT', url: '/api/fx-challenge/table', body: { value: null }, headers: json });
  const f = (await call(h)).body;
  check(back.status === 200 && back.body.value === null && same(f.table, fx.cloneTable(fx.DEFAULT_TABLE)) && f.tableSaved === null && history().length === 2,
    'PUT null puts the default back (also in the history)');

  // The Metrics page no longer serves the FX Challenge: ?view=fx is the pack, as before the tab was added.
  const computing = { computing: true, startedMs: NOW - 5000 };
  const metrics = m.createMetricsHandler({ cash: { current: () => computing }, pnl: { current: () => computing }, reads: {}, settingsFile: path.join(tmp, 's.json'), targetsFile: path.join(tmp, 't.json'), clock: () => NOW });
  const old = await call(metrics, { url: '/api/metrics?view=fx' });
  check(old.status === 202 && old.body.status === 'computing' && !('rates' in old.body), 'GET /api/metrics?view=fx answers with the Metrics pack, not the FX data');
  fs.rmSync(tmp, { recursive: true, force: true });
}

function testServerParts() {
  console.log('\nSERVER: settings and rows');
  check(same(fxs.restletIds('customscript_ph_rl_balances:customdeploy_ph_rl_balances'), { script: 'customscript_ph_rl_balances', deploy: 'customdeploy_ph_rl_balances' })
    && same(fxs.restletIds(' 1234 : 1 '), { script: '1234', deploy: '1' }) && fxs.restletIds('') === null && fxs.restletIds('a:b:c') === null && fxs.restletIds('x;y') === null,
  'NS_BALANCES_RESTLET is "script:deploy"; anything else is "not set up"');
  const rows = ['11001 Bank HPoalim Euro - 583400', '16003 Deposit - Bank Hapoalim - EURO', '11010 PKO Euro', '12001 Crypto Banks: Payhound', '13003 PAYPAL Euro', '17033 5856 - MasterCard - EUR', '199000 LSPORTS Netting Bank Account Euro', '14003 MONEYNETINT EUR']
    .map((name) => fxs.bookRow({ name, eur: 1 }));
  check(same(rows.map((r) => `${r.bank}/${r.kind}`), ['Bank Hapoalim/current', 'Bank Hapoalim/deposit', 'PKO/current', 'Payhound/current', 'PayPal/current', '5856 - MasterCard - EUR/card', 'LSPORTS Netting Bank Account Euro/other', 'MoneyNet/current']),
    'the books\' accounts by bank and kind', rows.map((r) => `${r.bank}/${r.kind}`).join(', '));
  let threw = false;
  try { fxs.mailRows({ ok: false, error: 'no cache' }); } catch (e) { threw = /no cache/.test(e.message); }
  check(threw, 'a RESTlet answer without rows is refused, with its reason');
  check(fxs.israelDay(Date.parse('2026-10-31T22:30:00Z')) === '2026-11-01', 'today is Israel\'s day (just after midnight in Tel Aviv is already the 1st)');
}

async function testNetSuiteReads() {
  console.log('\nNETSUITE: the reads\' mapping (fetch stubbed)');
  const { createNetSuiteClient } = require(path.join(ROOT, 'netsuite-api.cjs'));
  const seen = [];
  const reply = (items) => ({ ok: true, status: 200, json: async () => ({ items, hasMore: false }), text: async () => JSON.stringify({ items }) });
  const realFetch = global.fetch;
  global.fetch = async (url, opts) => {
    seen.push({ url: String(url), method: opts.method, auth: String(opts.headers.Authorization || '') });
    if (/restlet\.nl/.test(url)) return { ok: true, status: 200, text: async () => JSON.stringify({ ok: true, rows: [] }) };
    const q = JSON.parse(opts.body).q;
    if (/FROM currencyrate/.test(q)) return reply([{ d: '2026-10-19', rate: '3.482737' }, { d: '2026-10-20', rate: '3.47' }, { d: '2026-10-20', rate: '3.4778365' }, { d: '2026-10-21', rate: null }]);
    if (/'conversion'/.test(q) && /GROUP BY/.test(q)) return reply([{ m: '2026-09', k: 'CustPymt', eur: '4424436.19' }, { m: '2026-09', k: 'VendPymt', eur: '-846093.1' }, { m: '2026-09', k: 'conversion', eur: '-2646867.75' }, { m: '2026-09', k: 'Journal', eur: '-132903.93' }, { m: '2026-09', k: 'Transfer', eur: '-47842.06' }, { m: '2026-09', k: 'CardChrg', eur: '-1' }]);
    if (/acctnumber IN/.test(q)) return reply([
      { id: 360134, tranid: 'JE10953', d: '2026-10-06', num: '11001', book: 1, amt: -200000 }, { id: 360134, tranid: 'JE10953', d: '2026-10-06', num: '10002', book: 1, amt: 200000 },
      { id: 360134, tranid: 'JE10953', d: '2026-10-06', num: '11001', book: 2, amt: -820000 }, { id: 360134, tranid: 'JE10953', d: '2026-10-06', num: '10002', book: 2, amt: 820000 },
      { id: 9, tranid: 'JE9', d: '2026-10-08', num: '10002', book: 2, amt: 5 },
    ]);
    if (/SUM\(tal\.amount\) AS eur/.test(q)) return reply([{ num: '11001', name: '11001 Bank HPoalim Euro - 583400', eur: '979709.614' }]);
    return reply([]);
  };
  try {
    const ns = createNetSuiteClient({ NETSUITE_ACCOUNT_ID: '1234567', NETSUITE_CONSUMER_KEY: 'k', NETSUITE_CONSUMER_SECRET: 's', NETSUITE_TOKEN_ID: 't', NETSUITE_TOKEN_SECRET: 'ts' }, 3);
    const rates = await ns.fetchEurIlsRates({ days: 30 });
    check(same(rates, [{ date: '2026-10-19', rate: 3.482737 }, { date: '2026-10-20', rate: 3.4778365 }]), 'rates: one per day (the last kept), empty ones dropped');
    let bad = false;
    try { await ns.fetchEurIlsRates({ provider: "x' OR 1=1" }); } catch { bad = true; }
    check(bad, 'the rate provider is checked before it reaches SuiteQL');
    const flows = await ns.fetchEurFlows({ from: '2026-06' });
    check(same(flows.map((f) => f.kind), ['customer', 'vendor', 'conversion', 'other', 'transfer', 'other']) && flows[0].eur === 4424436.19, 'flows: kinds by transaction type and memo');
    const conv = await ns.fetchHapoalimConversions({ from: '2026-09-20' });
    check(same(conv, [{ id: 360134, tranid: 'JE10953', date: '2026-10-06', eur: 200000, ils: 820000 }]), 'conversions: € from the primary book, ₪ from the ILS book; a one-sided journal is left out');
    const books = await ns.fetchEurBalancesBooks();
    check(books[0].eur === 979709.61 && books[0].number === '11001', 'books: the EUR balance per account');
    await ns.restletGet('customscript_ph_rl_balances', 'customdeploy_ph_rl_balances');
    const r = seen.find((s) => /restlet\.nl/.test(s.url));
    check(r && r.method === 'GET' && r.url === 'https://1234567.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=customscript_ph_rl_balances&deploy=customdeploy_ph_rl_balances' && /^OAuth realm="1234567"/.test(r.auth) && /oauth_signature="/.test(r.auth),
      'the RESTlet is called with the same token (OAuth, signed with its script and deploy)');
    let refused = false;
    try { await ns.restletGet('a b', '1'); } catch { refused = true; }
    check(refused, 'a malformed RESTlet id is refused');
    const sb = createNetSuiteClient({ NETSUITE_ACCOUNT_ID: '1234567_SB1', NETSUITE_CONSUMER_KEY: 'k', NETSUITE_CONSUMER_SECRET: 's', NETSUITE_TOKEN_ID: 't', NETSUITE_TOKEN_SECRET: 'ts' }, 3);
    await sb.restletGet('12', '1');
    const s = seen.filter((x) => /restlet\.nl/.test(x.url)).pop();
    check(s.url.startsWith('https://1234567-sb1.restlets.api.netsuite.com/') && /^OAuth realm="1234567_SB1"/.test(s.auth), 'a sandbox account: the host in hostname form, the realm as is');
  } finally {
    global.fetch = realFetch;
  }
}

async function main() {
  const fx = await import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'fx-challenge.mjs')).href);
  console.log('=== FX Challenge checks (synthetic data, the October 2026 fixing) ===');
  await testLogic(fx);
  await testOutcomes(fx);
  await testPlan(fx);
  await testPast(fx);
  await testParse(fx);
  await testValidation(fx);
  await testHandler(fx);
  testServerParts();
  await testNetSuiteReads();
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
