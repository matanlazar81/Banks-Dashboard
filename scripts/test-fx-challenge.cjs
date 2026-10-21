#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// Checks for the FX Challenge page: the shared logic (src/forecast/fx-challenge.mjs), the saved table's
// validation, GET /api/fx-challenge and GET/PUT /api/fx-challenge/table (server/fx-challenge.cjs), the daily mail
// (scripts/fx-challenge-mail.cjs, the mail server stubbed) and the NetSuite reads' mapping (netsuite-api.cjs, fetch
// stubbed). No network.
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
  // The parts the page shows add up to each figure, and the tightest later conversion is named.
  const parts = fx.planReserve({ upcoming, policy: 'p95', eur: dated, today: TODAY, inflowPerDay: 100000 });
  check(parts.path.every((p) => near(p.available, parts.next.liquid + p.inflow + p.depositsBy - p.earlier) && near(p.inflow, 100000 * p.inflowDays))
    && parts.path[0].earlier === 0 && near(parts.path[1].earlier, parts.path[0].required) && parts.path[0].inflowDays === 16 && parts.path[0].depositsBy === 800000,
  'euros by then = cash today + inflow (100K a day × days to the day before) + deposits by then − the earlier conversions');
  check(c.path.every((p) => p.limitedBy === p.month), 'with money coming in, each day is limited by its own spare');
  check(dry.path[0].limitedBy === dry.path[5].month && near(dry.path[0].convertibleBefore, Math.max(0, dry.path[5].surplus)),
    'with nothing coming in, the last conversion is the tight one, and it is named');
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

  // Since January 2026: months before the table show what was booked, and euros bought back count against the month.
  const rates = series(320);
  const history = [
    { date: '2026-08-03', eur: 800000, ils: 2800000 },
    { date: '2026-08-05', eur: 300000, ils: 1068000 }, { date: '2026-08-05', eur: 1200000, ils: 4234000 },
    { date: '2026-08-08', eur: 400000, ils: 1386000 },
    { date: '2026-09-02', eur: 1750000, ils: 7287750 }, { date: '2026-09-02', eur: -1750000, ils: -6593475 },
    ...OCT_CONVERSIONS,
  ];
  const past = fx.pastConversions({ table: t, today: TODAY, series: rates, conversions: history });
  check(past.from === '2026-01' && past.months.length === 10 && past.months[0].month === '2026-01' && past.months[9].month === '2026-10',
    'every month from January 2026 whose conversion day has passed (January to October)', past.months.map((p) => p.month).join(' '));
  const aug = past.months.find((p) => p.month === '2026-08');
  check(!aug.inTable && aug.conv === null && aug.booked.date === '2026-08-05' && aug.booked.eur === 1500000 && aug.booked.n === 2,
    'before the table: the day near the 7th with the best rate (5 Aug, 3.535) is the booked one, not 3 or 8 Aug (3.5, 3.465)');
  check(same(aug.others.map((o) => [o.date, o.eur]), [['2026-08-03', 800000], ['2026-08-08', 400000]]) && aug.others.every((o) => o.vsRateIls !== null),
    'the other days of the month are listed apart, each against its day\'s rate');
  const sep = past.months.find((p) => p.month === '2026-09');
  check(sep.booked === null && same(sep.others.map((o) => [o.date, o.eur, Math.round(o.rate * 1000) / 1000]), [['2026-09-02', 1750000, 4.164], ['2026-09-02', -1750000, 3.768]]),
    'euros sold and bought back on the same day stay apart (sold at 4.164, bought at 3.768)');
  const octP = past.months.find((p) => p.month === '2026-10');
  check(octP.inTable && octP.match && octP.booked.eur === 800000 && octP.others.length === 3, 'October is checked against the table as before');
  const months = past.months.filter((p) => p.booked);
  check(near(past.total.booked.eur, months.reduce((s, p) => s + p.booked.eur, 0)) && near(past.total.booked.vsRateIls, months.reduce((s, p) => s + p.gainVsSpotIls, 0), 1e-6)
    && past.total.others.bought === 1750000 && near(past.total.others.eur, past.total.others.sold - 1750000) && past.total.booked.months === 2,
  'the totals add up: booked near the days, and the rest sold less bought back');
  const early = fx.cloneTable(t);
  early.rows.unshift({ month: '2025-11', cells: t.rows[0].cells });
  check(fx.pastConversions({ table: early, today: TODAY, series: rates, conversions: [] }).from === '2025-11', 'a table that starts earlier lists from its first month');
}

async function testRateCheck(fx) {
  console.log('\nRATE: today against the last 90 days');
  const s = series(120, { clean: true });
  const rc = fx.rateCheck(s, TODAY);
  const window = s.filter((p) => p.date >= '2026-07-23' && p.date < TODAY && ![0, 6].includes(new Date(`${p.date}T00:00:00Z`).getUTCDay()));
  check(rc && rc.days === 90 && rc.n === window.length && near(rc.average, window.reduce((a, p) => a + p.rate, 0) / window.length, 1e-12),
    'the business days of the 90 days before today: their count and average', rc && `${rc.n} vs ${window.length}`);
  check(near(rc.share, window.filter((p) => p.rate < rc.rate).length / window.length, 1e-12) && rc.low <= rc.average && rc.average <= rc.high, 'the share of those days with a lower rate');
  const up = s.map((p) => (p.date === TODAY ? { ...p, rate: 9 } : p));
  const down = s.map((p) => (p.date === TODAY ? { ...p, rate: 1 } : p));
  check(fx.rateCheck(up, TODAY).level === 'high' && fx.rateCheck(up, TODAY).share === 1 && fx.rateCheck(down, TODAY).level === 'low' && fx.rateCheck(down, TODAY).share === 0,
    'above every day → high; below every day → low');
  check(['high', 'middle', 'low'].includes(rc.level) && fx.rateCheck(series(15), TODAY) === null && fx.rateCheck([], TODAY) === null, 'too little history → no check');
}

async function testPlanFromPayload(fx) {
  console.log('\nSHARED: the recommendation the page and the mail both use');
  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  const rows = [
    { bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 981000 },
    { bank: 'PKO', kind: 'current', currency: 'EUR', amount: 229000 },
    { bank: 'Hapoalim deposits', kind: 'deposit', currency: 'EUR', amount: 1500000, valueDate: '2026-11-03', label: 'EUR fixed-term' },
  ];
  const data = {
    today: TODAY, table: t, rates: { series: series(400), spot: { date: TODAY, rate: 3.4778 }, sigma: 0.08 },
    balances: { rows }, flows: [{ month: '2026-09', kind: 'customer', eur: 3000000 }], inflowMonths: ['2026-07', '2026-08', '2026-09'],
  };
  const v = fx.planFromPayload(data, 'p95');
  const left = fx.fixings(t, TODAY).filter((f) => f.status !== 'past');
  const upcoming = left.map((f) => ({ month: f.month, date: f.date, days: f.days, conv: fx.conversionAt(f.row, t.bounds, 3.4778), risk: fx.fixingRisk(f.row, t.bounds, 3.4778, 0.08, f.days / 365) }));
  const inflow = fx.typicalInflow(data.flows, data.inflowMonths);
  const direct = fx.planReserve({ upcoming, policy: 'p95', eur: fx.eurPosition(rows), inflowPerDay: inflow.perDay, today: TODAY });
  check(same(v.plan, direct) && v.schedule.length === 14 && v.schedule[0].outcomes.length === 9 && v.spotBand === 2,
    'the same plan as built by hand from the fixings, the risk and the euros');
  check(v.sigma === 0.08 && v.sigmaFromHistory && fx.planFromPayload({ ...data, rates: { ...data.rates, sigma: null } }).sigma === fx.FALLBACK_SIGMA, 'NetSuite\'s volatility, or the fallback without it');
  check(fx.planFromPayload({ ...data, balances: null }).plan === null && fx.planFromPayload({ ...data, rates: { series: [], spot: null, sigma: null } }).plan === null,
    'no balances or no rate → no plan');
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

function call(handler, { method = 'GET', url = '/api/fx-challenge', originalUrl, body, headers = {} } = {}) {
  return new Promise((resolve) => {
    const raw = body === undefined ? '' : JSON.stringify(body);
    const req = Readable.from(raw ? [Buffer.from(raw)] : []);
    Object.assign(req, { method, url, headers: { host: 'finance.example', ...headers }, ...(originalUrl ? { originalUrl } : {}) });
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
    conversions: async (from) => { counts.conversions++; return from === '2025-12-20' ? OCT_CONVERSIONS : []; },
  };
  const rateDays = [];
  const rates = fxReads.rates;
  fxReads.rates = async (days) => { rateDays.push(days); return rates(); };
  const h = fxs.createFxChallengeHandler({ reads: fxReads, tableFile, clock: () => NOW });

  const a = (await call(h)).body;
  check(a.ok && a.status === 'ready' && a.today === TODAY, 'answers on its own route');
  check(same(a.table, fx.cloneTable(fx.DEFAULT_TABLE)) && a.tableSaved === null, 'no saved table → the default');
  check(a.rates.spot.date === TODAY && near(a.rates.spot.rate, 3.4778, 1e-9) && a.rates.sigma > 0.05 && a.rates.series.length === 400 && same(rateDays, [400]),
    'today\'s rate and the volatility from 400 days (the past conversions since January need no more)');
  check(a.balances.source === 'books' && a.balances.rows.length === 3 && a.balances.rows.find((r) => r.kind === 'deposit').bank === 'Bank Hapoalim'
    && a.balances.rows.find((r) => /AMEX/.test(r.account)).kind === 'card' && a.warnings.some((w) => /NS_BALANCES_RESTLET/.test(w)),
  'without the RESTlet: the books (deposit and card told apart, zero balances dropped) and a note on how to set it up');
  check(a.conversions.length === 6 && a.flows.length === 1 && same(a.inflowMonths, ['2026-07', '2026-08', '2026-09']),
    'flows from four months back, conversions from 20 December 2025 (the past since January), the inflow months');
  const longAgo = { value: { ...fx.cloneTable(fx.DEFAULT_TABLE), rows: [{ month: '2024-11', cells: fx.DEFAULT_TABLE.rows[0].cells }, ...fx.DEFAULT_TABLE.rows] } };
  const asked = [];
  const nothing = async () => [];
  await fxs.readFxChallenge({ reads: { books: nothing, flows: nothing, mail: null, rates: async (days) => { asked.push(days); return []; }, conversions: async (from) => { asked.push(from); return []; } }, tableDoc: longAgo, clock: () => NOW });
  check(same(asked, [732, '2024-10-20']), 'a table from November 2024: conversions from 20 Oct 2024, and the rates back to then (732 days)', JSON.stringify(asked));

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

/** Runs fn with console.log / console.error captured; returns [result, lines]. */
async function quietly(fn) {
  const lines = [];
  const { log, error } = console;
  console.log = (...a) => lines.push(a.join(' '));
  console.error = (...a) => lines.push(a.join(' '));
  try { return [await fn(), lines]; } finally { console.log = log; console.error = error; }
}

async function testMail(fx) {
  console.log('\nMAIL: the daily mail (scripts/fx-challenge-mail.cjs)');
  const mail = require(path.join(ROOT, 'scripts', 'fx-challenge-mail.cjs'));
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fx-mail-'));
  const balances = { ok: true, builtAt: '2026-10-21T05:00:00Z', notes: [], rows: [
    { company: 'LSports', bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 981000, asOf: '2026-10-20' },
    { company: 'LSports', bank: 'PKO', kind: 'current', currency: 'EUR', amount: 229000, asOf: '2026-10-21' },
    { company: 'LSports', bank: 'Hapoalim deposits', kind: 'deposit', currency: 'EUR', amount: 1500000, valueDate: '2026-11-03', label: 'EUR fixed-term' },
    { company: 'LSports', bank: 'Hapoalim deposits', kind: 'deposit', currency: 'EUR', amount: 2000000, valueDate: '2027-02-15', label: 'EUR fixed-term' },
    { company: 'LSports', bank: 'Hapoalim deposits', kind: 'deposit', currency: 'EUR', amount: 700000, valueDate: '2027-09-01', label: 'EUR fixed-term' },
  ] };
  const reads = {
    rates: async () => series(400),
    books: async () => [],
    mail: async () => balances,
    flows: async () => [{ month: '2026-07', kind: 'customer', eur: 3000000 }, { month: '2026-08', kind: 'customer', eur: 2500000 }, { month: '2026-09', kind: 'customer', eur: 2800000 }, { month: '2026-09', kind: 'vendor', eur: -600000 }],
    conversions: async () => OCT_CONVERSIONS,
  };
  const data = await fxs.readFxChallenge({ reads, clock: () => NOW });
  data.warnings.push('A note with <b>markup</b> & more.');
  const m = mail.renderMail(fx, data, { policy: 'p95', link: 'https://finance.example/business-tools/fx-challenge' });
  const v = fx.planFromPayload(data, 'p95');
  const clean = (s) => !/undefined|NaN|\[object|null/.test(s);
  check(/^FX Challenge Wed 21 Oct: /.test(m.subject) && m.subject.includes('for Sat 7 Nov') && /free to convert|short for/.test(m.subject) && /, (a good|an average|a weak) day \(EUR\/ILS 3\.478\) · keep €/.test(m.subject),
    'the subject: what is free (or short), the rate verdict, what to keep and for when', m.subject);
  check(m.html.startsWith('<!doctype html>') && clean(m.html) && clean(m.text) && clean(m.subject), 'HTML and text, with no undefined or NaN in either');
  check(m.html.includes('A note with &lt;b&gt;markup&lt;/b&gt; &amp; more.') && !m.html.includes('<b>markup'), 'what comes from the data is escaped in the HTML');
  check(m.text.includes('Free to convert now:') && m.text.includes('Hapoalim EUR (583400)') && m.text.includes(`${v.rateCheck.share === 1 ? 'above 100%' : 'above'}`) && /A (GOOD|WEAK) DAY TO CONVERT|AN AVERAGE DAY TO CONVERT/.test(m.text),
    'the recommendation, then today\'s rate against the last 90 days with its verdict');
  check(m.html.includes('(today)') && m.text.includes('If EUR/ILS that day is') && m.text.includes('Can be converted before it'), 'what the next conversion can sell (today\'s range marked), and the next conversions');
  check(m.text.includes('By Sat 7 Nov: Tue 3 Nov €1.50M') && m.text.includes('By Sun 7 Mar: Mon 15 Feb €2.00M') && /a further €700K in 1 deposit\./.test(m.text),
    'each deposit on the conversion it counts for, and the ones after the last conversion shown', m.text.split('\n').filter((l) => /By |further/.test(l)).join(' | '));
  check(m.html.includes('href="https://finance.example/business-tools/fx-challenge"') && m.text.includes('95% of rate moves'), 'the page\'s link and the setting used');
  const none = mail.renderMail(fx, { ...data, balances: null }, {});
  check(/no recommendation today$/.test(none.subject) && clean(none.html) && none.text.includes('balances'), 'without balances: no recommendation, and why');
  const failed = mail.failureMail(TODAY, new Error('NetSuite down'));
  check(/not read today$/.test(failed.subject) && failed.text.includes('NetSuite down'), 'nothing read: a short mail saying so');

  const ok = mail.parseRecipients('matan.l@lsports.eu, Dotan@LSports.EU;matan.l@lsports.eu');
  const outside = mail.parseRecipients('matan.l@lsports.eu,someone@gmail.com');
  check(ok.errors.length === 0 && same(ok.to, ['matan.l@lsports.eu', 'Dotan@LSports.EU']) && outside.errors.length === 1 && /outside lsports\.eu/.test(outside.errors[0])
    && mail.parseRecipients('someone@gmail.com', { allowExternal: true }).errors.length === 0 && /not set/.test(mail.parseRecipients('').errors[0]) && /not an address/.test(mail.parseRecipients('matan').errors[0]),
  'recipients: lsports.eu only (unless allowed), each once; a missing or bad address is said');
  const cfg = mail.mailConfig({ FX_MAIL_TO: 'matan.l@lsports.eu', FX_MAIL_FROM: 'matan.l@lsports.eu', FX_MAIL_SMTP_URL: 'smtps://bot%40lsports.eu:pw-secret@smtp.gmail.com:465', FX_MAIL_HOUR: '25', FX_MAIL_POLICY: 'worst' });
  check(cfg.errors.length === 0 && cfg.from === 'matan.l@lsports.eu' && cfg.smtp.host === 'smtp.gmail.com' && cfg.smtp.transport.port === 465 && cfg.smtp.transport.secure
    && cfg.smtp.transport.auth.user === 'bot@lsports.eu' && cfg.smtp.transport.name === 'lsports.eu' && cfg.hour === 10 && cfg.policy === 'worst',
  'the mail server from FX_MAIL_SMTP_URL, EHLO the sender\'s domain, a bad hour → 10');
  const relay = mail.mailConfig({ FX_MAIL_TO: 'matan.l@lsports.eu', NET_CASH_SMTP_URL: 'smtp://smtp-relay.gmail.com:587', SMTP_HOST: 'ignored', FX_MAIL_FROM: 'matan.l@lsports.eu' });
  const backend = mail.mailConfig({ FX_MAIL_TO: 'matan.l@lsports.eu', SMTP_HOST: 'smtp.example', SMTP_PORT: '587', SMTP_USER: 'bot@lsports.eu', SMTP_PASS: 'pw-secret' });
  const bare = mail.mailConfig({ FX_MAIL_TO: 'matan.l@lsports.eu' });
  check(relay.smtp.host === 'smtp-relay.gmail.com' && !relay.smtp.transport.auth && backend.from === 'bot@lsports.eu' && backend.smtp.transport.auth.pass === 'pw-secret'
    && bare.errors.some((e) => /no mail server/.test(e)) && bare.errors.some((e) => /FX_MAIL_FROM/.test(e)) && !JSON.stringify([cfg.errors, backend.errors, bare.errors]).includes('pw-secret'),
  'else NET_CASH_SMTP_URL, else the backend\'s SMTP_*; nothing set → said, never a password');
  check(['wait', 'send', 'send', 'last', 'done'].every((want, i) => mail.scheduleState([
    { hour: 9, sendHour: 10, sentDay: null, today: TODAY }, { hour: 10, sendHour: 10, sentDay: '2026-10-20', today: TODAY }, { hour: 11, sendHour: 10, sentDay: null, today: TODAY },
    { hour: 12, sendHour: 10, sentDay: null, today: TODAY }, { hour: 15, sendHour: 10, sentDay: TODAY, today: TODAY },
  ][i]) === want), 'scheduled: wait for the hour, send once, retry an incomplete read for two hours, then send it anyway');
  check(mail.israelHour(Date.parse('2026-10-21T07:00:00Z')) === 10 && mail.israelHour(Date.parse('2026-12-01T08:00:00Z')) === 10, '10:00 in Israel, in summer time and in winter');

  const sent = [];
  const nodemailer = { createTransport: (t) => ({ sendMail: async (msg) => { sent.push({ t, msg }); return { messageId: 'x' }; } }) };
  const env = { FX_MAIL_TO: 'matan.l@lsports.eu,dotan@lsports.eu', FX_MAIL_FROM: 'matan.l@lsports.eu', FX_MAIL_SMTP_URL: 'smtp://smtp-relay.gmail.com:587' };
  const markerFile = path.join(tmp, 'last.json');
  const at = (hhmm) => () => Date.parse(`${TODAY}T${hhmm}:00+03:00`);
  const mailFile = path.join(tmp, 'mail.json');
  const deps = (clock, extra = {}) => ({ env, clock, reads, nodemailer, markerFile, mailFile, tableFile: path.join(tmp, 'none.json'), ...extra });
  const [early] = await quietly(() => mail.main(['--scheduled'], deps(at('09:00'))));
  check(early === 0 && sent.length === 0 && !fs.existsSync(markerFile), 'scheduled at 09:00: nothing yet');
  const [ten, tenLog] = await quietly(() => mail.main(['--scheduled'], deps(at('10:00'))));
  const s0 = sent[0];
  check(ten === 0 && sent.length === 1 && s0.msg.from === 'matan.l@lsports.eu' && s0.msg.to === 'matan.l@lsports.eu, dotan@lsports.eu' && s0.msg.html.startsWith('<!doctype html>')
    && s0.msg.text.includes('Free to convert now') && s0.t.name === 'lsports.eu' && JSON.parse(fs.readFileSync(markerFile, 'utf8')).day === TODAY
    && JSON.parse(fs.readFileSync(markerFile, 'utf8')).recipients === 2 && /sent to 2 recipient/.test(tenLog.join(' ')),
  'scheduled at 10:00: sent from the sender set, to both (FX_MAIL_TO), HTML and text, and marked as sent today to 2'); 
  await quietly(() => mail.main(['--scheduled'], deps(at('11:00'))));
  const [manual] = await quietly(() => mail.main([], deps(at('15:00'))));
  check(sent.length === 2 && manual === 0 && JSON.parse(fs.readFileSync(markerFile, 'utf8')).at === new Date(at('10:00')()).toISOString(), 'sent once a day; a run by hand sends a test and leaves the daily one alone');
  const partial = { ...reads, mail: null, books: async () => { throw new Error('NetSuite down'); } };
  const marker2 = path.join(tmp, 'last2.json');
  const [retry, retryLog] = await quietly(() => mail.main(['--scheduled'], deps(at('10:00'), { reads: partial, markerFile: marker2 })));
  check(retry === 0 && sent.length === 2 && !fs.existsSync(marker2) && /trying again next hour/.test(retryLog.join(' ')), 'no balances at 10:00: not sent, tried again next hour');
  await quietly(() => mail.main(['--scheduled'], deps(at('12:00'), { reads: partial, markerFile: marker2 })));
  check(sent.length === 3 && /no recommendation today/.test(sent[2].msg.subject) && fs.existsSync(marker2), 'still none at 12:00: the mail goes anyway, saying why');
  const [refused, refusedLog] = await quietly(() => mail.main([], deps(at('10:00'), { env: { ...env, FX_MAIL_TO: 'someone@gmail.com' } })));
  check(refused === 1 && sent.length === 3 && /outside lsports\.eu/.test(refusedLog.join(' ')), 'an address outside lsports.eu: nothing is sent');
  const htmlFile = path.join(tmp, 'mail.html');
  const [dry, dryLog] = await quietly(() => mail.main(['--dry-run', `--html=${htmlFile}`], deps(at('08:00'), { env: {} })));
  check(dry === 0 && sent.length === 3 && /^Subject: FX Challenge/.test(dryLog[0]) && fs.readFileSync(htmlFile, 'utf8').startsWith('<!doctype html>') && /a real send would stop/.test(dryLog.join(' ')),
    '--dry-run prints it and writes the HTML, sends nothing, says what a real send lacks');

  // The page's list, once saved, is the one used; an empty one sends to nobody.
  fs.writeFileSync(mailFile, JSON.stringify({ value: { to: ['finance@lsports.eu'] }, updatedAt: '2026-10-21T08:00:00Z', updatedBy: 'matan.l@lsports.eu' }));
  const [, pageLog] = await quietly(() => mail.main(['--dry-run'], deps(at('10:00'))));
  await quietly(() => mail.main([], deps(at('10:00'))));
  check(sent.length === 4 && sent[3].msg.to === 'finance@lsports.eu' && /goes to 1 recipient\(s\) \(the page's list\)/.test(pageLog.join(' ')), 'a list saved on the page replaces FX_MAIL_TO');
  fs.writeFileSync(mailFile, JSON.stringify({ value: { to: [] } }));
  const marker3 = path.join(tmp, 'last3.json');
  const [quiet, quietLog] = await quietly(() => mail.main(['--scheduled'], deps(at('10:00'), { markerFile: marker3 })));
  const [byHand, byHandLog] = await quietly(() => mail.main([], deps(at('10:00'))));
  check(quiet === 0 && byHand === 1 && sent.length === 4 && !fs.existsSync(marker3) && /nobody is on the page's list/.test(quietLog.join(' ')) && /nobody is on the page's list/.test(byHandLog.join(' ')),
    'an empty list on the page: no daily mail (said once at the hour), and a send by hand stops'); 
  fs.rmSync(tmp, { recursive: true, force: true });
}

async function testMailSettings() {
  console.log('\nRECIPIENTS: who gets the daily mail (GET/PUT /api/fx-challenge/mail) and the one FX router');
  const ok = fxs.validateMailSettings({ value: { to: [' Matan.L@lsports.eu', 'dotan@lsports.eu', 'matan.l@LSPORTS.eu'] } });
  check(ok.ok && same(ok.value, { to: ['matan.l@lsports.eu', 'dotan@lsports.eu'] }), 'addresses trimmed, lower-cased and kept once', JSON.stringify(ok));
  const refused = [
    { value: { to: ['someone@gmail.com'] } }, { value: { to: ['matan'] } }, { value: { to: 'matan.l@lsports.eu' } }, { value: { to: [], cc: [] } },
    { value: null, extra: 1 }, {}, { value: { to: Array.from({ length: 31 }, (_, i) => `p${i}@lsports.eu`) } }, { value: { to: [42] } },
  ].map((b) => fxs.validateMailSettings(b).ok);
  check(refused.every((r) => !r), 'refused: outside lsports.eu, not an address, not a list, unknown fields, more than 30', JSON.stringify(refused));
  check(fxs.validateMailSettings({ value: null }).ok && same(fxs.validateMailSettings({ value: { to: [] } }).value, { to: [] }), 'null goes back to FX_MAIL_TO; an empty list is allowed (nobody gets it)');
  check(same(fxs.splitAddresses('a@lsports.eu, b@lsports.eu;a@lsports.eu  c@lsports.eu'), ['a@lsports.eu', 'b@lsports.eu', 'c@lsports.eu']) && fxs.mailHourOf('7') === 7 && fxs.mailHourOf('24') === 10 && fxs.mailHourOf('') === 10,
    'FX_MAIL_TO split on commas, semicolons or spaces; FX_MAIL_HOUR 0–23, else 10');

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fx-rcpt-'));
  const file = path.join(tmp, 'mail.json');
  const sentFile = path.join(tmp, 'sent.json');
  const env = () => ({ FX_MAIL_TO: 'matan.l@lsports.eu,dotan@lsports.eu', FX_MAIL_FROM: 'matan.l@lsports.eu', FX_MAIL_HOUR: '9', SMTP_PASS: 'pw-secret' });
  const router = fxs.createFxChallengeRouter({
    reads: { rates: async () => series(400), books: async () => [], mail: null, flows: async () => [], conversions: async () => OCT_CONVERSIONS },
    tableFile: path.join(tmp, 'table.json'), clock: () => NOW, mail: { file, sentFile, env },
  });
  const json = { 'content-type': 'application/json', origin: 'https://finance.example' };
  // finance-it mounts the router with app.use('/api/fx-challenge', …): req.url loses the prefix, req.originalUrl keeps it.
  const first = (await call(router, { url: '/mail', originalUrl: '/api/fx-challenge/mail' })).body;
  check(first.ok && first.value === null && same(first.envTo, ['matan.l@lsports.eu', 'dotan@lsports.eu']) && first.from === 'matan.l@lsports.eu' && first.hour === 9 && first.lastSent === null
    && !JSON.stringify(first).includes('pw-secret'), 'GET before any save: FX_MAIL_TO, the sender and the hour (nothing else from .env)');
  const put = await call(router, { method: 'PUT', url: '/mail', originalUrl: '/api/fx-challenge/mail', body: { value: { to: ['matan.l@lsports.eu', 'dotan@lsports.eu', 'finance@lsports.eu'] } }, headers: json });
  fs.writeFileSync(sentFile, JSON.stringify({ day: TODAY, at: '2026-10-21T07:00:03.000Z', recipients: 3, subject: 'x' }));
  const after = (await call(router, { url: '/api/fx-challenge/mail' })).body;
  const history = fs.readFileSync(path.join(tmp, 'mail-history.jsonl'), 'utf8').trim().split('\n');
  check(put.status === 200 && same(after.value, { to: ['matan.l@lsports.eu', 'dotan@lsports.eu', 'finance@lsports.eu'] }) && after.updatedAt === new Date(NOW).toISOString()
    && same(after.lastSent, { day: TODAY, at: '2026-10-21T07:00:03.000Z', recipients: 3 }) && history.length === 1 && same(fxs.savedRecipients(file), after.value.to),
  'PUT saves the list for everyone, with a history line; GET shows it and when the mail last went');
  const outside = await call(router, { method: 'PUT', url: '/api/fx-challenge/mail', body: { value: { to: ['someone@gmail.com'] } }, headers: json });
  const cross = await call(router, { method: 'PUT', url: '/api/fx-challenge/mail', body: { value: { to: [] } }, headers: { ...json, origin: 'https://evil.example' } });
  check(outside.status === 400 && cross.status === 403 && same(fxs.savedRecipients(file), after.value.to), 'an address outside lsports.eu, or a save from another site, changes nothing');
  const data = (await call(router, { url: '/?refresh=true', originalUrl: '/api/fx-challenge?refresh=true' })).body;
  const direct = (await call(router, { url: '/api/fx-challenge' })).body;
  const table = (await call(router, { url: '/table', originalUrl: '/api/fx-challenge/table' })).body;
  const missing = await call(router, { url: '/nope', originalUrl: '/api/fx-challenge/nope' });
  const post = await call(router, { method: 'POST', url: '/api/fx-challenge' });
  check(data.status === 'ready' && direct.status === 'ready' && table.ok && table.value === null && missing.status === 404 && post.status === 405,
    'one router: the page\'s data (mounted or not), the table, the recipients; anything else 404');
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
      { id: 168931, tranid: 'TRN328', d: '2026-01-02', num: '10002', book: 1, amt: -1750000 }, { id: 168931, tranid: 'TRN328', d: '2026-01-02', num: '11001', book: 1, amt: 1750000 },
      { id: 168931, tranid: 'TRN328', d: '2026-01-02', num: '10002', book: 2, amt: -6593475 }, { id: 168931, tranid: 'TRN328', d: '2026-01-02', num: '11001', book: 2, amt: 6593475 },
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
    const conv = await ns.fetchHapoalimConversions({ from: '2025-12-20' });
    check(same(conv, [{ id: 168931, tranid: 'TRN328', date: '2026-01-02', eur: -1750000, ils: -6593475 }, { id: 360134, tranid: 'JE10953', date: '2026-10-06', eur: 200000, ils: 820000 }]),
      'conversions: € from the primary book, ₪ from the ILS book; euros bought back come negative; a one-sided journal is left out', JSON.stringify(conv));
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
  await testRateCheck(fx);
  await testPlanFromPayload(fx);
  await testParse(fx);
  await testValidation(fx);
  await testHandler(fx);
  await testMail(fx);
  await testMailSettings();
  testServerParts();
  await testNetSuiteReads();
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
