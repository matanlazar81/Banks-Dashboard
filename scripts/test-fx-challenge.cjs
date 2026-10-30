#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// Checks for the FX Challenge page: the shared logic (src/forecast/fx-challenge.mjs), the bank's position reports and
// the days the euros leave, the saved table's and deals' validation, GET /api/fx-challenge and GET/PUT
// /api/fx-challenge/table and /positions (server/fx-challenge.cjs), the daily mail
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

  const c = fx.planReserve({ upcoming, policy: 'today', eur: pos, today: TODAY, inflowPerBusinessDay: 100000 });
  check(c.next.required === 800000 && near(c.next.freeNow, 507150.74) && c.next.inflowUntil === 1400000 && near(c.next.freeByFixing, 1907150.74),
    'today\'s band only: €507K free now; with €100K a business day of inflow (14 of them, Sunday to Friday, to the 6th), €1.91M by the fixing');
  check(c.path.length === 6 && near(c.path[0].surplus, 1307150.74 + 1400000 - 800000) && near(c.path[1].available, 1307150.74 + 100000 * 39 - 800000),
    'the path: euros by each fixing, with inflows and maturing deposits, less the earlier fixings');
  const suffixMin = (p, i) => Math.max(0, Math.min(...p.slice(i).map((x) => x.surplus)));
  check(c.path.every((x, i) => near(x.convertibleBefore, suffixMin(c.path, i))) && near(c.next.freeByFixing, c.path[0].convertibleBefore),
    'what can be converted before a fixing is the smallest spare from it on (euros converted then are gone later too)');
  const dry = fx.planReserve({ upcoming, policy: 'p95', eur: pos, today: TODAY, inflowPerBusinessDay: 0 });
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
  const r = fx.planReserve({ upcoming, policy: 'today', eur: dated, today: TODAY, inflowPerBusinessDay: 0 });
  check(r.path[0].released === 800000 && same(r.path[0].releasedDeposits.map((x) => x.label), ['October', 'on the day']) && r.path[0].released === r.next.maturing,
    'the first fixing gains the deposits maturing by its day, earliest first (the same as "maturing by then")');
  check(r.path[1].released === 200000 && r.path[1].releasedDeposits[0].valueDate === '2026-11-08' && r.path.slice(2).every((p) => p.released === 0 && !p.releasedDeposits.length),
    'a deposit maturing the day after a fixing counts for the next one; a blocked one never does');
  check(r.later.length === 1 && r.later[0].label === 'after April' && r.later[0].amount === 900000, 'deposits maturing after the last fixing shown are listed apart');
  let held = r.next.liquid;
  const consistent = r.path.every((p) => { held += p.released; const ok = near(p.available, held); held -= p.required; return ok; });
  check(consistent, 'euros by each fixing = today\'s liquid euros + the deposits released by then − the earlier fixings (no inflow here)');
  // The parts the page shows add up to each figure, and the tightest later conversion is named.
  const parts = fx.planReserve({ upcoming, policy: 'p95', eur: dated, today: TODAY, inflowPerBusinessDay: 100000 });
  check(parts.path.every((p) => near(p.available, parts.next.liquid + p.inflow + p.depositsBy - p.earlier) && near(p.inflow, 100000 * p.inflowDays))
    && parts.path[0].earlier === 0 && near(parts.path[1].earlier, parts.path[0].required) && parts.path[0].inflowDays === 14 && parts.path[0].depositsBy === 800000,
  'euros by then = cash today + inflow (100K a business day × the business days to the day before) + deposits by then − the earlier conversions');
  check(c.path.every((p) => p.limitedBy === p.month), 'with money coming in, each day is limited by its own spare');
  check(dry.path[0].limitedBy === dry.path[5].month && near(dry.path[0].convertibleBefore, Math.max(0, dry.path[5].surplus)),
    'with nothing coming in, the last conversion is the tight one, and it is named');
  const one = fx.planReserve({ upcoming, policy: 'today', eur: dated, today: TODAY });
  check(one.path.length === 1 && one.later.reduce((s, d) => s + d.amount, 0) === 1100000, 'without the inflow, the deposits after the next fixing are all "later"');

  // The keep line: what the deposits released by the conversion hold of the amount kept, and what today's cash does.
  const words = { eur: (n) => `€${Math.round(n / 1000)}K`, day: (iso) => iso };
  const partly = fx.keepCover(fx.planReserve({ upcoming, policy: 'p95', eur: before, today: TODAY }));
  check(partly.fromDeposits === 1000000 && partly.fromCash === 400000 && !partly.covered && partly.deposits.length === 1
    && fx.describeKeep(partly, words) === 'it takes €1400K, and the deposit released on 2026-11-03 brings €1000K of it',
  'the keep line: keep only what a deposit released before the conversion doesn\'t bring (€400K of €1.4M), and why');
  const whole = fx.keepCover(fx.planReserve({ upcoming, policy: 'today', eur: before, today: TODAY }));
  check(whole.covered && whole.fromDeposits === 800000 && whole.fromCash === 0
    && fx.describeKeep(whole, words) === 'the deposit released on 2026-11-03 (€1000K) covers the €800K it takes from Hapoalim EUR (583400)',
  'a deposit released by the conversion holding all of it (€1M for €800K): nothing to keep, and why');
  check(fx.describeKeep(fx.keepCover(r), words) === 'it takes €800K, and the deposit released on 2026-10-25 brings €300K of it',
    'a deposit paying out on the conversion day itself is too late: the bank takes the euros the business day before (Thu 5 Nov for Sat 7 Nov)');
  const both = fx.keepCover(fx.planReserve({ upcoming, policy: 'today', eur: { ...dated, deposits: dated.deposits.map((d) => (d.label === 'on the day' ? { ...d, valueDate: '2026-11-04' } : d)) }, today: TODAY, inflowPerBusinessDay: 0 }));
  check(fx.describeKeep(both, words) === 'the 2 deposits released by 2026-11-04 (€800K) cover the €800K it takes from Hapoalim EUR (583400)',
    'several deposits paying out in time: added up, by the last one\'s date');
  check(fx.describeKeep(fx.keepCover(fx.planReserve({ upcoming, policy: 'p95', eur: pos, today: TODAY })), words) === '',
    'no deposit released by then (an undated one is backup): nothing more to say');

  // First in, first out, on a ladder like LSports' on 27 Oct 2026: Nov and Dec covered, Jan to Apr short.
  const dep = (amount, valueDate) => ({ amount, valueDate, blocked: false, label: 'fixed-term' });
  const LADDER = [dep(402000, '2026-11-03'), dep(202500, '2026-11-03'), dep(201500, '2026-11-03'), dep(403000, '2026-12-03'), dep(203000, '2026-12-03'),
    dep(202000, '2026-12-03'), dep(504000, '2027-01-04'), dep(404000, '2027-01-04'), dep(505000, '2027-02-01'), dep(404000, '2027-02-01'), dep(405000, '2027-03-05')];
  const rich = fx.planReserve({ upcoming, policy: 'today', eur: { hapoalim: 3e6, other: 0, deposits: LADDER }, inflowPerBusinessDay: 0, today: TODAY, eurFloor: 200000 });
  const cov = rich.coverage.map((c) => [c.date, c.required, c.released, c.carried, c.gap]);
  check(same(cov, [
    ['2026-11-07', 800000, 806000, 0, 0], ['2026-12-07', 800000, 808000, 6000, 0], ['2027-01-07', 1050000, 908000, 14000, 128000],
    ['2027-02-07', 1050000, 909000, 0, 141000], ['2027-03-07', 1050000, 0, 0, 1050000], ['2027-04-07', 1050000, 405000, 0, 645000],
  ]), 'FIFO: each deposit counts for the first conversion whose euros the bank takes after it pays out, leftovers roll on (Nov +€6K, Dec +€14K), Jan €128K short; Fri 5 Mar is after Thu 4 Mar, so it counts for April', JSON.stringify(cov));
  check(same(rich.coverage.map((c) => [c.takes, c.until]), [['2026-11-05', '2026-11-04'], ['2026-12-06', '2026-12-03'], ['2027-01-06', '2027-01-05'],
    ['2027-02-04', '2027-02-03'], ['2027-03-04', '2027-03-03'], ['2027-04-06', '2027-04-05']]),
  'the bank takes the euros the business day before the 7th (Thu 7 Jan: Wed 6 Jan; Sun 7 Feb: Thu 4 Feb); a deposit for a gap pays out the business day before that (Tue 5 Jan, Wed 3 Feb)');
  const lateOf = rich.coverage.map((c) => [c.from, c.late.map((d) => [d.valueDate, d.amount])]);
  check(same(lateOf, [['2026-10-21', []], ['2026-11-05', []], ['2026-12-06', []], ['2027-01-06', []], ['2027-02-04', [['2027-03-05', 405000]]], ['2027-03-04', []]]),
    'each conversion counts deposits paying out after the bank took the previous one\'s euros; Fri 5 Mar pays out after Thu 4 Mar, before Sun 7 Mar: March\'s late one', JSON.stringify(lateOf));
  const mail = require(path.join(ROOT, 'scripts', 'fx-challenge-mail.cjs'));
  const said = rich.coverage.map((c, i, all) => mail.coverText(c, i === 0, all[i + 1]));
  check(said[4] === 'יום א\' 7.3: €1.05M, הפיקדון שמשתחרר ביום ו\' 5.3 (€405K) מגיע אחרי שהבנק לוקח את האירו ביום ה\' 4.3, ולכן נספר להמרה של יום ד\' 7.4: חסרים €1.05M'
    && said[2] === 'יום ה\' 7.1: €1.05M, הפיקדונות מכסים €922K: חסרים €128K' && said[0] === 'שבת 7.11: €800K, מכוסה מהפיקדונות (€806K)',
  'the mail says why March gets nothing from the deposits: the one of Fri 5 Mar pays out after the bank takes the euros on Thu 4 Mar, so it counts for April', JSON.stringify(said));
  const bare = fx.planReserve({ upcoming, policy: 'today', eur: { hapoalim: 3e6, other: 0, deposits: LADDER.slice(0, -1) }, inflowPerBusinessDay: 0, today: TODAY, eurFloor: 200000 });
  const saidBare = bare.coverage.map((c, i, all) => mail.coverText(c, i === 0, all[i + 1]));
  check(saidBare[4] === 'יום א\' 7.3: €1.05M, אין פיקדון שמשתחרר בין יום ו\' 5.2 ליום ה\' 4.3 (היום שהבנק לוקח את האירו): חסרים €1.05M',
    'with no deposit at all for March, the mail says which days one would have to pay out in (the earlier ones go to the earlier conversions)', saidBare[4]);
  check(same(rich.ladder.map((l) => [l.until, l.eur]), [['2027-01-05', 130000], ['2027-02-03', 150000], ['2027-03-03', 1050000], ['2027-04-05', 650000]]),
    'with room, a deposit for each gap, earliest first, rounded up to €10K', JSON.stringify(rich.ladder));
  // Made, the ladder closes the gaps and leaves every due covered.
  const total = rich.ladder.reduce((s, l) => s + l.eur, 0);
  const made = fx.planReserve({ upcoming, policy: 'today', eur: { hapoalim: 3e6 - total, other: 0, deposits: [...LADDER, ...rich.ladder.map((l) => dep(l.eur, l.until))] }, inflowPerBusinessDay: 0, today: TODAY, eurFloor: 200000 });
  check(made.coverage.every((c) => c.gap === 0) && made.next.short === null && made.ladder.length === 0,
    'once deposited, every conversion is covered and nothing falls short');
  const tight = fx.planReserve({ upcoming, policy: 'today', eur: { hapoalim: 1e6, other: 0, deposits: LADDER }, inflowPerBusinessDay: 0, today: TODAY, eurFloor: 200000 });
  check(same(tight.ladder.map((l) => [l.until, l.eur]), [['2027-01-05', 130000], ['2027-02-03', 150000], ['2027-03-03', 520000]]),
    'with €800K above the floor: Jan and Feb in full, Mar as far as it goes, Apr waits', JSON.stringify(tight.ladder));
  const soon = fx.planReserve({ upcoming, policy: 'p95', eur: { hapoalim: 3e6, other: 0, deposits: LADDER }, inflowPerBusinessDay: 0, today: TODAY, eurFloor: 200000 });
  check(soon.coverage[0].gap > 0 && !soon.coverage[0].timed && !soon.ladder.some((l) => l.month === '2026-11') && soon.ladder[0].month === '2026-12',
    'the next conversion\'s gap is the cash to keep (the keep line), never a deposit, even two weeks out; the ladder starts with the one after');

  const flows = [
    { month: '2026-07', kind: 'customer', eur: 4969632 }, { month: '2026-07', kind: 'vendor', eur: -389321 }, { month: '2026-07', kind: 'other', eur: 58695 },
    { month: '2026-07', kind: 'conversion', eur: -2646789 }, { month: '2026-07', kind: 'deposit', eur: -1400000 }, { month: '2026-07', kind: 'transfer', eur: -210194 },
    { month: '2026-08', kind: 'customer', eur: 4537969 }, { month: '2026-08', kind: 'vendor', eur: -814475 }, { month: '2026-08', kind: 'refund', eur: -49575 },
  ];
  const inflow = fx.typicalInflow(flows, ['2026-07', '2026-08']);
  check(near(inflow.months[0].net, 4969632 - 389321 + 58695) && near(inflow.perMonth, (4639006 + 3673919) / 2) && near(inflow.perBusinessDay, (inflow.perMonth * 12) / 313, 1e-6),
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

// Hapoalim's position reports as the page reads them (a sheet's rows, dates as Excel serial days): a title, the account
// and the report's day, the header row, the deals and a total. March 2027 is fixed in two parts, like the real one: Wed
// 3 Mar (settled Fri 5 Mar) and Thu 4 Mar (settled Mon 8 Mar). Made-up deal numbers and account.
const serial = (iso) => Math.round((Date.parse(`${iso}T00:00:00Z`) - Date.UTC(1899, 11, 30)) / 86400000);
const OTC_HEAD = ['מספר עסקה', 'סוג אופציה', 'קניה/מכירה', 'למסירה', 'Put/Call', 'מטבע ראשי', 'סכום העסקה', 'מטבע נגדי', 'Strike', 'תאריך פרעון', 'שערוך במונח דולר ארהב',
  'תאריך קשירה', 'סכום הפרמיה', 'מטבע הפרמיה', 'סכום הפרמיה בדולר ארהב', 'תאריך פקיעה', 'רווח/הפסד בדולר ארהב', 'תאריך דגימה ראשון', 'תאריך דגימה אחרון', 'תדירות דגימה',
  'מקור שער הדגימות', 'טריגר 1', 'טריגר 2', 'מטבע Payoff'];
const opt = (deal, kind, side, pc, ccy, amount, strike, settle, expiry, trigger = '') => [deal, kind, side, 'כן', pc, ccy, amount, 'ILS', strike, serial(settle), 120.5,
  serial('2026-03-02'), 7000, 'EUR', 7837.39, serial(expiry), 10, null, null, null, null, trigger, '', null];
const OTC_ROWS = [
  [null], [null], ['פירוט עסקאות FX OPTION'], ['מספר חשבון  12-000-000000  תאריך הפקה  30.10.2026   '], OTC_HEAD,
  opt(1001, 'VANILLA', 'מכירה', 'CALL', 'EUR', -600000, 3.49, '2027-03-05', '2027-03-03'),
  opt(1002, 'EKO', 'קניה', 'PUT', 'EUR', 300000, 3.49, '2027-03-05', '2027-03-03', 3.3),
  opt(1003, 'EKO', 'קניה', 'PUT', 'EUR', 300000, 3.56, '2027-03-05', '2027-03-03', 3.3),
  opt(1004, 'VANILLA', 'קניה', 'PUT', 'EUR', -250000, 3.527, '2027-03-08', '2027-03-04'),
  opt(1005, 'EKO', 'קניה', 'PUT', 'EUR', 200000, 3.85, '2027-03-08', '2027-03-04', 3.4),
  opt(1006, 'VANILLA', 'מכירה', 'PUT', 'EUR', 250000, 3.3, '2027-03-08', '2027-03-04'),
  opt(1007, 'VANILLA', 'מכירה', 'CALL', 'USD', -100000, 3.2, '2027-03-08', '2027-03-04'),
  opt(1008, 'VANILLA', 'קניה', 'PUT', 'EUR', -100000, null, '2027-03-08', '2027-03-04'),
  ["סה''כ", null, null, null, null, null, null, null, null, null, 4521.3],
];
const FWD_HEAD = ["מס' עסקה", 'תאריך קשירה', 'קניה/מכירה', 'למסירה', 'מטבע ראשי', 'סכום העסקה', 'מטבע נגדי', 'תאריך פרעון', 'שער סופי', 'שערוך במונחי דולר ארהב', 'נקודות', 'סכום נגדי'];
const fwd = (deal, side, ccy, amount, settle, rate, counter) => [deal, serial('2025-04-03'), side, 'כן', ccy, amount, 'ILS', serial(settle), rate, 1000, 0.05, counter];
const FWD_ROWS = [
  [null], [null], ['פירוט עסקאות FX'], ['מספר חשבון  12-000-000000  תאריך הפקה  30.10.2026   '], FWD_HEAD,
  fwd(2001, 'מכירה', 'EUR', -1000000, '2026-11-02', 4.2, 4200000),
  fwd(2002, 'קניה', 'EUR', 1000000, '2026-11-02', 3.8, -3800000),
  fwd(2003, 'מכירה', 'USD', -500000, '2026-12-31', 3.7, 1850000),
  fwd(2004, 'קניה', 'USD', 500000, '2026-12-31', 3.24, -1620000),
  ["סה''כ", null, null, null, null, null, null, null, null, 5021.7],
];

async function testDeals(fx) {
  console.log('\nDEALS: the bank\'s position reports, and the days the euros leave');
  const o = fx.parsePositionReport(OTC_ROWS);
  check(o.kind === 'options' && o.asOf === '2026-10-30' && o.options.length === 6 && o.skipped === 1 && o.problems.length === 1 && /deal 1008/.test(o.problems[0]),
    'the options report: its day from "תאריך הפקה", the EUR/ILS options, another pair left out, an unreadable row said; the total and the account dropped', JSON.stringify([o.kind, o.asOf, o.skipped, o.problems]));
  check(same(o.options[1], { deal: '1002', kind: 'EKO', side: 'buy', type: 'put', eur: 300000, strike: 3.49, trigger: 3.3, expiry: '2027-03-03', settle: '2027-03-05' })
    && o.options[3].eur === 250000 && o.options[3].trigger === null && !JSON.stringify(o).includes('12-000-000000'),
  'each option: side, put or call, the euros (unsigned), strike, trigger, fixing and value day; not the whole account number', JSON.stringify(o.options[1]));
  const f = fx.parsePositionReport(FWD_ROWS);
  check(f.kind === 'forwards' && f.asOf === '2026-10-30' && f.forwards.length === 4
    && same(f.forwards[1], { deal: '2002', side: 'buy', currency: 'EUR', amount: 1000000, counter: 'ILS', rate: 3.8, settle: '2026-11-02', counterAmount: -3800000 }),
  'the forwards report: each forward with its value day and shekels', JSON.stringify(f.forwards[1]));
  const none = fx.parsePositionReport([['a', 'b'], [1, 2]]);
  check(none.kind === null && none.problems.length === 1, 'another sheet: not a position report, said');
  check(fx.reportDay(serial('2027-03-08')) === '2027-03-08' && fx.reportDay('08.03.2027') === '2027-03-08' && fx.reportDay('2027-03-08T00:00:00') === '2027-03-08' && fx.reportDay('soon') === null,
    'days as Excel serial days, dd.mm.yyyy or ISO');

  const [call349, eko349, , put3527, eko385, put33] = o.options;
  check(fx.optionEur(call349, 3.5) === 600000 && fx.optionEur(call349, 3.45) === 0 && fx.optionEur(eko349, 3.45) === 300000 && fx.optionEur(eko349, 3.3) === 0
    && fx.optionEur(eko349, 3.29) === 0 && fx.optionEur(put3527, 3.25) === 250000 && fx.optionEur(eko385, 3.4) === 0 && fx.optionEur(eko385, 3.41) === 200000
    && fx.optionEur(put33, 3.25) === -250000 && fx.optionEur(put33, 3.35) === 0,
  'a call sold sells above its strike; a put bought sells below it, an EKO not at or below its trigger; a put sold buys below its strike');
  const legs = fx.positionLegs({ options: o.options });
  check(same(Object.keys(legs), ['2027-03']) && same(legs['2027-03'].map((l) => [l.expiry, l.settle, l.options.length]), [['2027-03-03', '2027-03-05', 3], ['2027-03-04', '2027-03-08', 3]]),
    'March in two parts: fixed Wed 3 Mar and settled Fri 5 Mar, fixed Thu 4 Mar and settled Mon 8 Mar');
  const at = (rate) => fx.legsAt(legs['2027-03'], rate).map((l) => Math.round(l.eur));
  check(same(at(3.45), [600000, 450000]) && same(at(3.35), [600000, 250000]) && same(at(3.495), [900000, 450000]) && same(at(3.25), [0, 0]),
    'each part at a fixing rate (3.45: €600K and €450K, as the table\'s €1.05M; 3.35: €850K; 3.495: €1.35M; below 3.3 the puts sold and bought cancel)');
  const cl = fx.conversionLegs(legs['2027-03'], 3.45, 1050000);
  check(same(cl.map((l) => [l.settle, Math.round(l.eur), Math.round(l.ils)]), [['2027-03-05', 600000, 2115000], ['2027-03-08', 450000, 1651750]])
    && same(fx.conversionLegs(legs['2027-03'], 3.45, 2100000).map((l) => Math.round(l.eur)), [1200000, 900000]) && fx.conversionLegs(legs['2027-03'], 3.25, 1050000) === null
    && fx.conversionLegs(null, 3.45, 1050000) === null,
  'a conversion\'s parts with their shekels, scaled to the amount kept; null when no part sells or without deals');
  const marRow = fx.DEFAULT_TABLE.rows.find((r) => r.month === '2027-03');
  check(fx.rateSelling(marRow, fx.DEFAULT_BOUNDS, 3.45, 1050000) === 3.45 && near(fx.rateSelling(marRow, fx.DEFAULT_BOUNDS, 3.45, 1350000), 3.495, 1e-9)
    && near(fx.rateSelling(marRow, fx.DEFAULT_BOUNDS, 3.45, 2650000), 4.015, 1e-9) && fx.rateSelling(marRow, fx.DEFAULT_BOUNDS, 3.45, 123) === null,
  'the rate whose band sells the amount kept: today\'s, else the middle of the nearest band that does');
  const mism = fx.dealsVsTable(fx.cloneTable(fx.DEFAULT_TABLE), legs);
  check(mism.length === 1 && mism[0].month === '2027-03' && mism[0].bands.some((b) => b.band === 0 && b.table === 300000 && b.deals === 0) && same(fx.dealsVsTable(fx.cloneTable(fx.DEFAULT_TABLE), {}), []),
    'where the deals and the table disagree (these few options sell nothing below 3.3, the table €300K)');
  check(same(fx.forwardFlows({ forwards: f.forwards }), [{ date: '2026-11-02', eur: 0, ils: 400000 }, { date: '2026-12-31', eur: 0, ils: 230000 }]),
    'the forwards by value day: euros sold and bought back the same day net to nothing, the shekels don\'t');
  check(fx.valueDayBefore('2027-03-08') === '2027-03-05' && fx.valueDayBefore('2027-03-05') === '2027-03-04' && fx.valueDayBefore('2026-12-07') === '2026-12-04',
    'the euro value day before: Monday to Friday (a deposit for Mon 8 Mar pays out Fri 5 Mar)');

  // The plan with March's parts, on the ladder of 30 Oct 2026: €810K paid out on Fri 5 Mar (two deposits).
  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  const up = fx.fixings(t, TODAY).filter((x) => x.status !== 'past').map((x) => ({ month: x.month, date: x.date, days: x.days, conv: fx.conversionAt(x.row, t.bounds, 3.45), risk: fx.fixingRisk(x.row, t.bounds, 3.45, 0.09, x.days / 365) }));
  const mar = up.find((u) => u.month === '2027-03');
  mar.legs = fx.conversionLegs(legs['2027-03'], 3.45, mar.conv.eur);
  const dep = (amount, valueDate) => ({ amount, valueDate, blocked: false, label: 'fixed-term' });
  const LADDER = [dep(402000, '2026-11-03'), dep(202500, '2026-11-03'), dep(201500, '2026-11-03'), dep(403000, '2026-12-03'), dep(203000, '2026-12-03'),
    dep(202000, '2026-12-03'), dep(504000, '2027-01-04'), dep(404000, '2027-01-04'), dep(505000, '2027-02-01'), dep(404000, '2027-02-01'), dep(405000, '2027-03-05'), dep(405000, '2027-03-05')];
  const plan = fx.planReserve({ upcoming: up, policy: 'today', eur: { hapoalim: 3e6, other: 0, deposits: LADDER }, inflowPerBusinessDay: 0, today: TODAY, eurFloor: 200000 });
  const c = plan.coverage[4];
  check(c.deals && same(c.legs.map((l) => [l.settle, l.required, l.have, l.gap, l.until]), [['2027-03-05', 600000, 810000, 0, '2027-03-04'], ['2027-03-08', 450000, 210000, 240000, '2027-03-05']])
    && c.gap === 240000 && c.takes === '2027-03-05' && c.late.length === 0,
  'March part by part: the €810K of Fri 5 Mar covers the €600K leaving that day, and €210K rolls on to Mon 8 Mar, €240K short', JSON.stringify(c.legs.map((l) => [l.settle, l.required, l.have, l.gap])));
  check(!plan.coverage[2].deals && plan.coverage[2].legs.length === 1 && plan.coverage[2].takes === '2027-01-06' && plan.next.takes === '2026-11-05',
    'a month without deals: one part, the business day before the 7th, as before');
  const leg = plan.ladder.find((l) => l.month === '2027-03');
  check(leg && leg.settle === '2027-03-08' && leg.part && leg.until === '2027-03-05' && leg.eur === 240000 && leg.required === 450000 && leg.have === 210000,
    'a new deposit for March\'s Mon 8 Mar part, paying out on Fri 5 Mar', JSON.stringify(leg));
  const mail = require(path.join(ROOT, 'scripts', 'fx-challenge-mail.cjs'));
  const line = mail.coverText(c, false, plan.coverage[5]);
  check(line === 'יום ו\' 5.3 ויום ב\' 8.3: €1.05M (€600K ו-€450K), הפיקדונות מכסים €810K: חסרים €240K ביום ב\' 8.3' && same(c.on, ['2027-03-05', '2027-03-08']),
    'the mail\'s line for March, by its conversion days (when the euros leave): its two parts, what the deposits cover, and the part that is short', line);
  const hv = { plan: { next: {} }, rateCheck: null };
  const said = mail.actionText({ kind: 'ladder', eur: 240000, legs: [leg] }, hv, TODAY);
  check(said.head === 'להפקיד €240K בפיקדון עד יום ו\' 5.3' && said.body === 'להמרה של יום ב\' 8.3 (חלק מההמרה של מרץ): היא מוכרת €450K, והפיקדונות שמשתחררים עד אז מכסים €210K, כלומר חסרים €240K. כל התשלומים עד אז עדיין מכוסים.',
    'the mail\'s deposit for a part: until when, and the conversion day it is for', JSON.stringify(said));
  const en = fx.describeAction({ kind: 'ladder', eur: 240000, legs: [leg] }, { plan: { next: {} }, rateCheck: null }, TODAY, { eur: (n) => `€${Math.round(n / 1000)}K`, day: (d) => d });
  check(en.body.startsWith('For the 2027-03-08 conversion (part of March\'s): it takes €450K'), 'the page says the same', en.body);

  // From the payload: the saved deals give the legs, the forwards' shekels, and what the page says about them.
  const data = {
    today: TODAY, table: t, rates: { series: series(400), spot: { date: TODAY, rate: 3.45 }, sigma: 0.08 },
    balances: { rows: [{ bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 3000000 }, ...LADDER.map((d) => ({ bank: 'Hapoalim deposits', kind: 'deposit', currency: 'EUR', amount: d.amount, valueDate: d.valueDate, label: 'EUR fixed-term' }))] },
    flows: [], inflowMonths: ['2026-07', '2026-08', '2026-09'],
    positions: { asOf: { options: o.asOf, forwards: f.asOf }, options: o.options, forwards: f.forwards, savedAt: '2026-10-30T10:00:00Z', savedBy: 'finance' },
  };
  const v = fx.planFromPayload(data, 'today');
  check(v.plan.coverage[4].deals && same(v.plan.coverage[4].legs.map((l) => Math.round(l.required)), [600000, 450000]) && !v.plan.coverage[3].deals
    && v.deals.options === 6 && v.deals.forwards === 4 && v.deals.from === '2027-03' && same(v.deals.missing, ['2026-11', '2026-12', '2027-01', '2027-02', '2027-04'])
    && v.deals.flows.length === 2 && v.deals.mismatches.length === 1,
  'the saved deals give March its parts; the months without deals and where the deals and the table disagree are said', JSON.stringify(v.deals && v.deals.missing));
  check(fx.planFromPayload({ ...data, positions: null }).deals === null && !fx.planFromPayload({ ...data, positions: null }).plan.coverage[4].deals,
    'no deals saved: every conversion the business day before the 7th');

  console.log('\nDEALS: the amounts from the deals (the table no longer updated by hand)');
  const eff = fx.tableWithDeals(t, legs);
  const marEff = eff.rows.find((r) => r.month === '2027-03');
  check(same(eff.bounds, t.bounds) && eff.rows.length === t.rows.length && same(marEff.cells[2], { eurK: -1050, rate: 3.5874 }) && same(marEff.cells[1], { eurK: -850, rate: 3.5256 })
    && same(marEff.cells[0], { eurK: 0, rate: 3.25 }) && same(eff.rows.find((r) => r.month === '2027-02'), t.rows.find((r) => r.month === '2027-02')),
  'March worked out from its deals band by band (3.4–3.49: €1.05M at their blended 3.5874; below 3.3 nothing), the other months as saved', JSON.stringify(marEff.cells.slice(0, 3)));
  check(fx.tableWithDeals(t, null) === t && fx.tableWithDeals(t, {}) === t, 'without deals, the saved table as it is');
  const april = { deal: '1009', kind: 'VANILLA', side: 'buy', type: 'put', eur: 100000, strike: 3.65, trigger: null, expiry: '2027-04-05', settle: '2027-04-07' };
  const split = fx.tableWithDeals(t, fx.positionLegs({ options: [...o.options, april] }));
  const febSplit = split.rows.find((r) => r.month === '2027-02').cells;
  const febSaved = t.rows.find((r) => r.month === '2027-02').cells;
  check(split.bounds.length === t.bounds.length + 1 && split.bounds[7] === 3.65 && febSplit.length === 15 && same(febSplit[7], febSaved[7]) && same(febSplit[8], febSaved[7])
    && same(febSplit[9], febSaved[8]) && same(split.rows.find((r) => r.month === '2027-04').cells.map((c) => c.eurK), [...Array(8).fill(-100), ...Array(7).fill(0)]),
  'a new strike (3.65) splits its band: the saved months keep their cells on both sides, April sells €100K below it', JSON.stringify(split.bounds));
  const zeros = fx.outcomeRanges({ month: 'x', cells: [{ eurK: 0, rate: 3.25 }, { eurK: 0, rate: 3.35 }, { eurK: -100, rate: 3.6 }] }, [3.3, 3.4], [0.2, 0.3, 0.5]);
  check(zeros.length === 2 && zeros[0].eur === 0 && zeros[0].hi === 3.4 && near(zeros[0].p, 0.5, 1e-12), 'bands that sell nothing are one outcome, whatever their rate');
  check(same(v.table.rows.find((r) => r.month === '2027-03'), marEff) && same(fx.planFromPayload({ ...data, positions: null }).table, t)
    && v.schedule.find((s) => s.fixing.month === '2027-03').outcomes[0].eur === 0,
  'the plan uses it: March\'s outcomes from its deals (nothing below 3.3); without deals the saved table');

  console.log('\nDEALS: the days each conversion is fixed and paid');
  const fixMar = (today) => fx.fixings(t, today, legs).find((x) => x.month === '2027-03');
  const m1 = fixMar('2027-03-01');
  check(same(m1.fixedOn, ['2027-03-03', '2027-03-04']) && same(m1.paidOn, ['2027-03-05', '2027-03-08']) && m1.riskDays === 3 && m1.days === 6 && !m1.fixed && m1.status === 'next',
    'March with its deals: fixed Wed 3 and Thu 4 Mar, paid Fri 5 and Mon 8 Mar; EUR/ILS can move 3 days (to the last fixing), not the 6 to the 7th', JSON.stringify(m1));
  check(fixMar('2027-03-08').status === 'next' && fixMar('2027-03-08').fixed && fixMar('2027-03-09').status === 'past'
    && fx.fixings(t, '2027-03-08').find((x) => x.month === '2027-03').status === 'past' && fx.fixings(t, '2027-03-01').find((x) => x.month === '2027-03').fixedOn === null,
  'a month stays until its last part is paid (Mon 8 Mar, after the 7th); without deals, until the 7th');
  const early = fx.positionLegs({ options: o.options.map((x) => ({ ...x, expiry: '2027-04-01', settle: '2027-04-05' })) });
  check(fx.fixings(t, '2027-04-06', early).find((x) => x.month === '2027-04').status === 'past' && fx.fixings(t, '2027-04-06').find((x) => x.month === '2027-04').status === 'next',
    'paid on Mon 5 Apr: done from the 6th, before the 7th, so the plan no longer keeps for it');
  const onDay = (today, extra = []) => ({
    ...data, today, rates: { series: [{ date: '2027-02-26', rate: 3.45 }, ...extra.map(([date, rate]) => ({ date, rate }))], spot: { date: today, rate: 3.45 }, sigma: 0.08 },
  });
  const before = fx.planFromPayload(onDay('2027-03-01'), 'p95');
  const s1 = before.schedule[0];
  check(s1.fixing.month === '2027-03' && s1.fixedAt === null && same(s1.risk, fx.fixingRisk(s1.fixing.row, before.table.bounds, 3.45, 0.08, 3 / 365)),
    'the risk runs to Thu 4 Mar, the last fixing day (3 days)');
  const fixedDays = [['2027-03-03', 3.47], ['2027-03-04', 3.495]];
  const after = fx.planFromPayload(onDay('2027-03-05', fixedDays), 'worst');
  const s2 = after.schedule[0];
  check(s2.fixedAt && s2.fixedAt.date === '2027-03-04' && s2.fixedAt.rate === 3.495 && s2.atSpot.eur === 1350000 && s2.risk.p95Eur === 1350000 && s2.risk.worstEur === 1350000
    && after.plan.next.required === 1350000,
  'both parts fixed: at Thu 4 Mar\'s rate (3.495, €1.35M) with no risk left, whatever the setting (the worst case too)', JSON.stringify([s2.fixedAt, s2.atSpot && s2.atSpot.eur]));
  const paidOne = fx.planFromPayload(onDay('2027-03-08', fixedDays), 'p95');
  const c8 = paidOne.plan.coverage[0];
  check(paidOne.schedule[0].fixing.month === '2027-03' && paidOne.plan.next.required === 450000 && same(c8.legs.map((l) => [l.settle, Math.round(l.required)]), [['2027-03-08', 450000]]),
    'Mon 8 Mar: the €900K paid on Fri 5 Mar is gone from the balances, so only the €450K part is kept for', JSON.stringify(c8.legs.map((l) => [l.settle, l.required])));
  check(fx.planFromPayload(onDay('2027-03-09', fixedDays), 'p95').schedule[0].fixing.month === '2027-04', 'Tue 9 Mar: March is done, April is next');
  check(mail.coverText(c8, true, null).startsWith('יום ב\' 8.3: €450K, '), 'the mail\'s line by the day the euros leave, in one part too', mail.coverText(c8, true, null));

  console.log('\nDEALS: any number of report files');
  check(o.account === '000000' && f.account === '000000' && !JSON.stringify(o).includes('12-000'), 'each report\'s account by its own number (as NetSuite names it), not the bank or branch', o.account);
  const reportOf = (rows, day, account) => rows.map((r, i) => (i === 3 ? [`מספר חשבון  12-000-${account}  תאריך הפקה  ${day}   `] : r));
  const later = fx.parsePositionReport(reportOf(OTC_ROWS.filter((r) => r[0] !== 1001), '06.11.2026', '000000'));
  const other = fx.parsePositionReport(reportOf([...OTC_ROWS.slice(0, 5), opt(3001, 'VANILLA', 'קניה', 'PUT', 'EUR', 100000, 3.65, '2027-04-07', '2027-04-05')], '30.10.2026', '111111'));
  const before268 = { asOf: { options: o.asOf, forwards: f.asOf }, options: o.options, forwards: f.forwards };
  const r1 = fx.mergePositionReports(before268, [{ name: 'OTC 6 Nov.xlsx', report: later }]);
  check(r1.files[0].used && same(r1.files[0].replaces, [{ name: null, asOf: '2026-10-30' }]) && r1.value.options.length === 5 && !r1.value.options.some((x) => x.deal === '1001')
    && r1.value.forwards.length === 4 && r1.value.asOf.options === '2026-11-06' && r1.kept.length === 1 && r1.kept[0].kind === 'forwards',
  'the deals saved before: a newer options report replaces their options (deal 1001 fixed and gone), the forwards stay as saved', JSON.stringify(r1.files[0]));
  const r2 = fx.mergePositionReports(null, [
    { name: 'OTC.xlsx', report: o }, { name: 'OTC 6 Nov.xlsx', report: later }, { name: 'OTC other.xlsx', report: other }, { name: 'FX.xlsx', report: f }, { name: 'notes.xlsx', report: none },
  ]);
  const byName = Object.fromEntries(r2.files.map((x) => [x.name, x]));
  check(!byName['OTC.xlsx'].used && byName['OTC.xlsx'].why === 'OTC 6 Nov.xlsx is a newer report of account 000000' && byName['OTC 6 Nov.xlsx'].used && byName['OTC other.xlsx'].used
    && byName['FX.xlsx'].used && !byName['notes.xlsx'].used && byName['notes.xlsx'].why === "not one of the bank's position reports"
    && r2.value.options.length === 6 && r2.value.options.some((x) => x.deal === '3001') && r2.value.reports.length === 3,
  'five files at once: two accounts\' options combined, the same account\'s older report left out, another sheet said', JSON.stringify(r2.files.map((x) => [x.name, x.used, x.why])));
  const r3 = fx.mergePositionReports(r2.value, [{ name: 'OTC.xlsx', report: o }]);
  check(r3.files[0].used && r3.files[0].older === '2026-11-06' && r3.kept.some((k) => k.account === '111111') && r3.value.options.some((x) => x.deal === '1001')
    && r3.value.options.some((x) => x.deal === '3001') && r3.value.forwards.length === 4,
  'a report older than its account\'s saved one is flagged; the other account\'s report and the forwards stay as saved');
  const r4 = fx.mergePositionReports(r2.value, [{ name: 'empty.xlsx', report: fx.parsePositionReport(reportOf(OTC_ROWS.slice(0, 5), '04.12.2026', '111111')) }]);
  check(!r4.files[0].used && r4.files[0].why === 'no deal could be read from it' && r4.kept.length === 3, 'a report without deals changes nothing');
  check(same(v.deals.reports.map((r) => [r.kind, r.account, r.count]), [['options', null, 6], ['forwards', null, 4]]), 'deals saved before the reports were kept: one report per kind on the page');

  console.log('\nDEALS: the saved deals (PUT /api/fx-challenge/positions)');
  const value = { asOf: { options: o.asOf, forwards: f.asOf }, options: o.options, forwards: f.forwards };
  const ok = fxs.validatePositions({ value });
  check(ok.ok && same(ok.value, value), 'the deals as the page reads them are accepted as they are');
  check(fxs.validatePositions({ value: null }).ok, 'null clears them');
  const bad = fxs.validatePositions({ value: { ...value, options: [{ ...o.options[0], side: 'long', settle: '2027-03-01', extra: 1 }] } });
  check(!bad.ok && bad.errors.some((e) => /side/.test(e)) && bad.errors.some((e) => /settle/.test(e)) && bad.errors.some((e) => /unknown field "extra"/.test(e)),
    'a wrong side, a value day before the fixing and an unknown field are refused', JSON.stringify(bad.errors));
  check(!fxs.validatePositions({ value: { asOf: null, options: [], forwards: [] } }).ok, 'no deals at all is refused');
  const withReports = fxs.validatePositions({ value: r2.value });
  check(withReports.ok && same(withReports.value.reports, r2.value.reports), 'the deals of several reports, with the reports, are accepted as they are', JSON.stringify(withReports.errors));
  const badReport = fxs.validatePositions({ value: { ...r2.value, reports: [{ ...r2.value.reports[0], account: '12-000-000000', extra: 1 }] } });
  check(!badReport.ok && badReport.errors.some((e) => /account/.test(e)) && badReport.errors.some((e) => /unknown field "extra"/.test(e)),
    'a report with a whole account number or an unknown field is refused', JSON.stringify(badReport.errors));
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fx-pos-'));
  const positionsFile = path.join(tmp, 'positions.json');
  const nothing = async () => [];
  const router = fxs.createFxChallengeRouter({
    reads: { rates: async () => series(400), books: nothing, mail: null, flows: nothing, conversions: nothing },
    tableFile: path.join(tmp, 'table.json'), positionsFile, clock: () => NOW,
  });
  const put = await call(router, { method: 'PUT', url: '/api/fx-challenge/positions', body: { value }, headers: { 'content-type': 'application/json', origin: 'https://finance.example' }, user: { email: 'finance@lsports.eu' } });
  const got = (await call(router, { url: '/api/fx-challenge' })).body;
  check(put.status === 200 && put.body.ok && got.positions && got.positions.options.length === 6 && got.positions.forwards.length === 4 && got.positions.savedAt
    && fs.existsSync(positionsFile.replace(/\.json$/, '-history.jsonl')),
  'saved for everyone with its history, and in the page\'s data', JSON.stringify(put.body).slice(0, 200));
}

// The EUR vendor payments on their days: made-up days, with the real shape (most in the first week).
function testEurVendors(fx) {
  console.log('\nEUR VENDORS: the payments on the days they go out');
  const mail = require(path.join(ROOT, 'scripts', 'fx-challenge-mail.cjs'));
  // [day, paid, of it from Hapoalim EUR]
  const days = [['07-02', 200000, 50000], ['07-10', 60000], ['07-25', 90000], ['08-04', 500000, 150000], ['08-18', 90000], ['08-31', 120000],
    ['09-03', 620000, 180000], ['09-08', 60000], ['09-28', 150000], ['10-04', 550000, 160000], ['10-20', 85000], ['10-21', 100000]]
    .map(([d, eur, hapoalim]) => ({ date: `2026-${d}`, eur, hapoalim: hapoalim || 0 }));
  const months = ['2026-07', '2026-08', '2026-09'];
  const ev = fx.eurVendorsAhead({ days, months, today: TODAY, until: '2026-11-30' });
  check(same(ev.parts.map((p) => [p.from, p.to, Math.round(p.eur), Math.round(p.hapoalim), p.months.join()]), [
    [1, 7, 556667, 163333, '2026-08,2026-09,2026-10'], [8, 20, 70000, 0, '2026-07,2026-08,2026-09'], [21, 31, 120000, 0, '2026-07,2026-08,2026-09']]),
  'each part of the month at its average over its last three months: on 21 Oct the first week\'s August to October (October\'s is over and booked), '
    + 'the 8th–20th\'s and the rest July to September (October\'s 8th–20th ended yesterday: not booked yet), with what Hapoalim EUR paid of them', JSON.stringify(ev.parts));
  check(same(ev.dues.map((d) => [d.date, Math.round(d.eur), Math.round(d.hapoalim), d.part]), [['2026-10-21', 20000, 0, 2], ['2026-11-01', 556667, 163333, 0], ['2026-11-08', 70000, 0, 1], ['2026-11-21', 120000, 0, 2]])
    && fx.eurVendorsAhead({ days, months: [], today: TODAY, until: '2026-11-30' }) === null,
  'each on its first day; the part today is in, less what it has paid so far (€100K of €120K), today', JSON.stringify(ev.dues));
  const nov1 = fx.eurVendorsAhead({ days, months: ['2026-08', '2026-09', '2026-10'], today: '2026-11-01', until: '2026-11-30' });
  check(same(nov1.parts.map((p) => p.months.join()), ['2026-08,2026-09,2026-10', '2026-08,2026-09,2026-10', '2026-07,2026-08,2026-09']),
    'on 1 Nov, October\'s last part (to the 31st) is not booked yet: that part still takes July to September, a month before the closed months', JSON.stringify(nov1.parts.map((p) => p.months)));
  const flows = [{ month: '2026-09', kind: 'customer', eur: 3e6 }, { month: '2026-09', kind: 'vendor', eur: -6e5 }, { month: '2026-09', kind: 'conversion', eur: -2e6 }];
  check(fx.typicalInflow(flows, ['2026-09']).perMonth === 2.4e6 && fx.typicalInflow(flows, ['2026-09'], { vendors: false }).perMonth === 3e6,
    'the typical inflow leaves the vendor payments out when they are planned on their days');

  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  const up = fx.fixings(t, TODAY).filter((x) => x.status !== 'past').slice(0, 2)
    .map((x) => ({ month: x.month, date: x.date, days: x.days, conv: fx.conversionAt(x.row, t.bounds, 3.45), risk: fx.fixingRisk(x.row, t.bounds, 3.45, 0.08, x.days / 365) }));
  const input = { upcoming: up, policy: 'today', eur: { hapoalim: 2.5e6, other: 0, deposits: [] }, inflowPerBusinessDay: 0, today: TODAY };
  const without = fx.planReserve(input);
  const withV = fx.planReserve({ ...input, eurOut: [{ date: '2026-11-01', eur: 440000 }, { date: '2026-12-01', eur: 1.5e6 }] });
  check(without.next.freeNow === 1.7e6 && without.next.vendorsBefore === 0 && withV.next.vendorsBefore === 440000 && withV.next.freeNow === 1.26e6,
    'free now: today\'s euros less what the conversion needs and the EUR vendor payments before it (€440K on 1 Nov)', JSON.stringify([without.next.freeNow, withV.next.freeNow]));
  check(withV.path[0].forVendors === 440000 && withV.path[1].vendors === 1.5e6 && withV.path[1].available === 2.5e6 - 800000 - 1.94e6,
    'the path takes them by each conversion day', JSON.stringify(withV.path.map((p) => [p.forVendors, p.vendors, p.available])));
  check(withV.next.short && withV.next.short.first.vendors && withV.next.short.first.date === '2026-12-01' && withV.next.short.first.eur === 240000 && !withV.next.short.vendors,
    'a gap that opens on a vendor day says so (€240K short on 1 Dec, for the EUR vendors)', JSON.stringify(withV.next.short));

  const data = {
    today: TODAY, table: t, rates: { series: series(400), spot: { date: TODAY, rate: 3.45 }, sigma: 0.08 },
    balances: { rows: [{ bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 2.5e6 }] }, flows, inflowMonths: months,
  };
  const before = fx.planFromPayload(data, 'today');
  const v = fx.planFromPayload({ ...data, eurVendors: days }, 'today');
  check(before.eurVendors === null && near(before.inflow.perMonth, 0.8e6, 1) && near(v.inflow.perMonth, 1e6, 1) && v.eurVendors && near(v.plan.next.vendorsBefore, 576667, 1)
    && near(v.plan.next.hapoalimVendors, 163333, 1) && before.plan.next.hapoalimVendors === 0,
  'from the payload: with NetSuite\'s payments by day, the inflow leaves them out and the plan takes them on their days (Hapoalim EUR\'s apart); without, as before',
  JSON.stringify([before.inflow.perMonth, v.inflow.perMonth, v.plan.next.vendorsBefore, v.plan.next.hapoalimVendors]));

  // Receipts come in Sunday to Friday (finance's rule): none on a Saturday.
  check(fx.receiptDaysBetween('2026-10-30', '2026-11-01') === 0 && fx.receiptDaysBetween('2026-10-30', '2026-11-04') === 3 && fx.receiptDaysBetween('2026-10-31', '2026-11-02') === 1
    && fx.receiptDaysBetween('2026-11-04', '2026-11-04') === 0 && near(fx.typicalInflow([{ month: '2026-09', kind: 'customer', eur: 313000 }], ['2026-09']).perBusinessDay, 12000, 1e-6),
  'receipt days: Sunday to Friday, after today and before the day (Fri 30 Oct to Sun 1 Nov: none; to Wed 4 Nov: three); a month\'s inflow over its 26 of them');

  // 30 Oct 2026, as the balances were: €1.13M (€521K at Hapoalim), €806K of deposits by Tue 3 Nov, €683K kept, €174K a
  // business day coming in, and the first week's EUR vendors (€589K, €161K of it from Hapoalim) on Sun 1 Nov.
  const oct30 = {
    upcoming: up, policy: 'today', today: '2026-10-30', inflowPerBusinessDay: 173800, eurFloor: 683000,
    eur: { hapoalim: 521000, other: 605000, deposits: [{ amount: 806000, valueDate: '2026-11-03', label: 'EUR fixed-term' }] },
    eurOut: [{ date: '2026-10-30', eur: 6000, hapoalim: 5000 }, { date: '2026-11-01', eur: 589000, hapoalim: 161000 }, { date: '2026-11-08', eur: 94000, hapoalim: 0 }],
  };
  const dipped = fx.planReserve(oct30);
  check(dipped.next.short && dipped.next.short.date === '2026-11-01' && dipped.next.short.eur === 152000 && dipped.next.short.left === 531000 && dipped.next.short.back === '2026-11-02'
    && dipped.next.runsOut === null && dipped.next.spare === 0 && !dipped.ladder.length,
  'Sun 1 Nov: nothing comes in on the Saturday before, so the first week\'s EUR vendors take the euros €152K below the €683K kept (€531K held); '
    + 'back above it by Mon 2 Nov (Sunday\'s receipts); nothing runs out, nothing is spare, no deposit', JSON.stringify(dipped.next.short));
  const quiet = fx.todayActions({ plan: dipped, cash: null, rateCheck: null }, '2026-10-30');
  const dry = fx.planReserve({ ...oct30, eur: { ...oct30.eur, hapoalim: 100000, other: 0 } });
  const urgent = fx.todayActions({ plan: dry, cash: null, rateCheck: null }, '2026-10-30');
  check(quiet.actions.length === 0 && dry.next.runsOut && dry.next.runsOut.eur === 495000 && dry.next.runsOut.date === '2026-11-01' && dry.next.runsOut.vendors
    && same(urgent.actions.map((a) => [a.kind, a.urgent, a.eur, a.date]), [['short', true, 495000, '2026-11-01']]),
  'the dip alone is no recommendation; euros that run out (€100K held: €495K short on Sun 1 Nov, the floor spent too) are, urgent', JSON.stringify(urgent.actions));
  // Hapoalim EUR pays €161K of the first week itself, before the euros leave for the conversion: it keeps them too.
  const kept = fx.keepCover(dipped);
  const words = { eur: (x) => `€${Math.round(x / 1000)}K`, day: (iso) => iso };
  check(dipped.next.hapoalimVendors === 161000 && kept.depositsCover && !kept.covered && kept.vendors === 161000 && kept.fromCash === 161000
    && fx.describeKeep(kept, words) === 'the deposit released on 2026-11-03 (€806K) covers the €800K it takes from Hapoalim EUR (583400), but about €161K of EUR vendor payments leave Hapoalim EUR before it'
    && mail.keepLine(kept, ['2026-11-07'], 'לפי השער היום') === 'להשאיר €161K בפועלים להמרה של שבת 7.11 (לפי השער היום): הפיקדון שמשתחרר ביום ג\' 3.11 (€806K) מכסה את ה-€800K שהיא מוכרת, אבל כ-€161K יוצאים מפועלים לספקים באירו לפני כן.',
  'the deposit covers the conversion, but Hapoalim EUR\'s own EUR vendor payments in its month, before its euros leave, stay there: keep €161K (October\'s last €5K is met by the inflow before then; page and mail)', JSON.stringify(kept));
  const thin = fx.planReserve({ ...oct30, eur: { ...oct30.eur, hapoalim: 100000, deposits: [{ amount: 500000, valueDate: '2026-11-03', label: 'EUR fixed-term' }] } });
  const part = fx.keepCover(thin);
  check(thin.next.moveToHapoalim === 361000 && part.fromCash === 461000
    && fx.describeKeep(part, words) === 'it takes €800K, the deposit released on 2026-11-03 brings €500K of it, and about €161K of EUR vendor payments leave Hapoalim EUR before it'
    && mail.keepLine(part, ['2026-11-07'], 'לפי השער היום') === 'להשאיר €461K בפועלים להמרה של שבת 7.11 (לפי השער היום): היא מוכרת €800K, ו-€500K מזה מגיעים מהפיקדון שמשתחרר ביום ג\' 3.11, ועוד כ-€161K יוצאים מפועלים לספקים באירו לפני כן.'
    && mail.actionText({ kind: 'move', eur: 361000, date: '2026-11-07', on: ['2026-11-07'], by: '2026-11-05' }, { plan: thin, rateCheck: null }, '2026-10-30').body.endsWith('ועוד כ-€161K לספקים באירו שיוצאים ממנו לפני כן.'),
  'a deposit covering part of it: keep the rest and Hapoalim\'s own EUR vendors (€300K + €161K); with €100K there, move €361K from the other accounts', JSON.stringify(part));

  check(same(ev.dues.map((d) => d.rest), [true, false, false, false]) && fx.eurVendorsAhead({ days, months, today: '2026-10-23', until: '2026-10-31' }).dues[0].rest
    && !fx.eurVendorsAhead({ days, months, today: '2026-11-01', until: '2026-11-30' }).dues[0].rest,
  'a part already paying (it began before today, or paid some today) is its rest; one that starts today is whole');

  const line = mail.vendorsLine(v.eurVendors, 683000);
  const sameMonths = { months, parts: [440000, 70000, 120000].map((eur) => ({ eur, hapoalim: 0, months })) };
  check(line === 'ההמלצה מניחה שהתשלומים לספקים באירו יוצאים כמו ב-3 הפעמים האחרונות של כל חלק בחודש: כ-€557K בשבוע הראשון של החודש (1–7, אוגוסט–אוקטובר), באותם ימים שבהם הבנק לוקח את האירו להמרה, כ-€70K מה-8 עד ה-20 (יולי–ספטמבר) וכ-€120K מה-21 ועד סוף החודש (יולי–ספטמבר). בנוסף נשמרים תמיד €683K בחשבונות האירו.'
    && mail.vendorsLine(sameMonths, 0) === 'ההמלצה מניחה שהתשלומים לספקים באירו יוצאים כמו ב-3 החודשים האחרונים (יולי–ספטמבר): כ-€440K בשבוע הראשון של החודש (1–7), באותם ימים שבהם הבנק לוקח את האירו להמרה, כ-€70K מה-8 עד ה-20 וכ-€120K מה-21 ועד סוף החודש.'
    && mail.vendorsLine(null, 0) === 'התשלומים לספקים באירו לא נקראו היום לפי ימים, ולכן הם בחשבון כממוצע לאורך החודש.',
  'the mail says, in Hebrew, what the recommendation expects for the EUR vendors, each part as in its last three months (the months once when the same), or that they count as spread over the month', line);

  // Every euro expected to leave, by day: the EUR vendors, the bank's conversions (each part), the euros converted for
  // shekels and the forwards that sell euros.
  const view = {
    eurVendors: { dues: [{ date: '2026-10-21', eur: 20000, part: 2, rest: true }, { date: '2026-11-01', eur: 440000, part: 0, rest: false },
      { date: '2026-11-08', eur: 70000, part: 1, rest: false }, { date: '2026-11-21', eur: 120000, part: 2, rest: false }, { date: '2026-12-01', eur: 440000, part: 0, rest: false }] },
    plan: { coverage: [{ month: '2026-11', date: '2026-11-07', deals: true, legs: [{ settle: '2026-11-04', required: 1.1e6 }] },
      { month: '2026-12', date: '2026-12-07', deals: true, legs: [{ settle: '2026-12-04', required: 600000 }, { settle: '2026-12-07', required: 450000 }] }] },
    cash: { needs: [{ date: '2026-11-04', eur: 50000, why: ['later'] }, { date: '2026-11-05', eur: 1.13e6, why: ['net', 'vendors'] }, { date: '2026-11-10', eur: 0, why: ['cards'] }] },
    deals: { flows: [{ date: '2026-11-15', eur: -200000 }, { date: '2026-11-20', eur: 300000 }] },
  };
  const ahead = fx.eurPaymentsAhead(view, TODAY);
  check(ahead.until === '2026-11-25' && same(ahead.payments.map((p) => [p.date, p.kind, p.eur]), [
    ['2026-10-21', 'vendors', 20000], ['2026-11-01', 'vendors', 440000], ['2026-11-04', 'shekels', 50000], ['2026-11-04', 'conversion', 1.1e6],
    ['2026-11-05', 'shekels', 1.13e6], ['2026-11-08', 'vendors', 70000], ['2026-11-15', 'forward', 200000], ['2026-11-21', 'vendors', 120000]]),
  'every euro leaving in the next 35 days, by day (on one day as the plan takes them); none of nothing, a forward that buys or a day after', JSON.stringify(ahead.payments));
  check(same(ahead.byKind, { vendors: 650000, conversion: 1.1e6, shekels: 1.18e6, forward: 200000 }) && ahead.total === 3.13e6
    && same(fx.eurPaymentsAhead(view, TODAY, { days: 10 }).payments.map((p) => p.date), ['2026-10-21'])
    && fx.eurPaymentsAhead({ plan: { coverage: [{ month: '2026-11', date: '2026-11-07', deals: false, legs: [{ settle: '2026-11-05', required: 1e6 }] }] } }, TODAY).payments[0].date === '2026-11-07'
    && same(fx.eurPaymentsAhead({ plan: { coverage: [{ month: '2026-11', date: '2026-11-07', deals: true, legs: [{ settle: '2026-11-04', required: 0 }, { settle: '2026-11-06', required: 5e5 }] }] } }, TODAY).payments
      .map((p) => [p.date, p.part, p.parts]), [['2026-11-06', 0, 1]])
    && same(fx.eurPaymentsAhead({ plan: null }, TODAY), { until: '2026-11-25', payments: [], total: 0, byKind: { vendors: 0, conversion: 0, shekels: 0, forward: 0 } }),
  'in total and by kind; a shorter window; nothing to plan → nothing', JSON.stringify(ahead.byKind));
  const said = [
    { date: '2026-10-21', eur: 20000, kind: 'vendors', part: 2, rest: true }, { date: '2026-11-01', eur: 440000, kind: 'vendors', part: 0, rest: false },
    { date: '2026-11-04', eur: 1.1e6, kind: 'conversion', month: '2026-11', part: 0, parts: 1 }, { date: '2026-12-07', eur: 450000, kind: 'conversion', month: '2026-12', part: 1, parts: 2 },
    { date: '2026-11-05', eur: 1.13e6, kind: 'shekels', why: ['net', 'vendors'] }, { date: '2026-11-15', eur: 200000, kind: 'forward' },
  ].map((p) => mail.paymentText(p));
  check(same(said, ['יום ד\' 21.10: ספקים באירו (יתרת 21–31.10), כ-€20K', 'יום א\' 1.11: ספקים באירו (1–7.11), כ-€440K', 'יום ד\' 4.11: ההמרה של נובמבר בבנק, €1.10M',
    'יום ב\' 7.12: ההמרה של דצמבר בבנק (חלק 2 מ-2), €450K', 'יום ה\' 5.11: המרה לשקלים (שכר נטו, ספקים בשקלים), €1.13M', 'יום א\' 15.11: פורוורד, מכירת €200K']),
  'each payment in the mail\'s words', JSON.stringify(said));
  const pv = (next) => ({ plan: { next: { liquid: 1.8e6, eurFloor: 683000, short: null, runsOut: null, ...next } }, inflow: { perMonth: 4.53e6 },
    position: { deposits: [{ amount: 1.5e6, valueDate: '2026-11-03' }, { amount: 110000, valueDate: '2026-11-25' }, { amount: 900000, valueDate: '2026-11-26' }, { amount: 400000, valueDate: '2026-11-10', blocked: true }] } });
  const dip = { eur: 146000, date: '2026-11-01', left: 537000, back: '2026-11-02', first: { eur: 146000, date: '2026-11-01' } };
  const gone = { eur: 120000, date: '2026-12-01' };
  const totals = [mail.paymentsTotal(ahead, pv({}), TODAY), mail.paymentsTotal(ahead, pv({ short: dip }), TODAY), mail.paymentsTotal(ahead, pv({ short: { ...dip, back: null } }), TODAY),
    mail.paymentsTotal(ahead, pv({ short: dip, runsOut: gone }), TODAY, true), mail.paymentsTotal(ahead, pv({ short: dip, runsOut: { ...gone, eur: 30000 } }), TODAY),
    mail.paymentsTotal(ahead, { ...pv({ eurFloor: 0 }), inflow: null, position: { deposits: [] } }, TODAY)];
  check(totals[0] === 'סה"כ עד יום ד\' 25.11: €3.13M (ספקים באירו €650K, המרות בבנק €1.10M, המרות לשקלים €1.18M, פורוורדים €200K). מול זה: €1.80M באירו היום, €1.61M מפיקדונות שמשתחררים עד אז ותקבולים רגילים של כ-€4.53M בחודש. לפי זה כל התשלומים מכוסים ונשארים לפחות €683K.'
    && totals[1].endsWith('לפי זה התשלומים מכוסים, אבל ביום א\' 1.11 היתרה באירו יורדת €146K מתחת ל-€683K שנשמרים תמיד (נשארים €537K) וחוזרת מעליהם עד יום ב\' 2.11: עד אז לא להפקיד ולא להמיר מעבר למתוכנן.')
    && totals[2].endsWith('(נשארים €537K): בינתיים לא להפקיד ולא להמיר מעבר למתוכנן.')
    && totals[3].endsWith('לפי זה חסרים €120K ביום ג\' 1.12, גם אחרי ה-€683K השמורים: ההמלצות למעלה אומרות איך להשלים.')
    && totals[4].endsWith('לפי זה חסרים €30K ביום ג\' 1.12, גם אחרי ה-€683K השמורים (סכום קטן, ולכן אין עליו המלצה).')
    && totals[5].endsWith('מול זה: €1.80M באירו היום. לפי זה כל התשלומים מכוסים.'),
  'the total, by kind, against today\'s euros, the deposits paying out by then (not a blocked one) and the typical inflow: covered; a dip into the floor (how deep, what is held, when it is back); or euros that run out, and whether today\'s recommendations fix it', JSON.stringify(totals));
  const hv = { plan: { next: { backup: 0, eurFloor: 0 } }, rateCheck: null };
  const short = mail.actionText({ kind: 'short', eur: 240000, date: '2026-12-01', by: '2026-12-01', shekels: false, vendors: true }, hv, TODAY);
  const leg = { month: '2026-12', date: '2026-12-07', on: '2026-12-06', until: '2026-12-03', eur: 240000, gap: 240000, required: 800000, have: 560000 };
  const ladder = mail.actionText({ kind: 'ladder', eur: 240000, legs: [leg] }, { ...hv, eurVendors: v.eurVendors }, TODAY);
  const fmt = { eur: (n) => `€${Math.round(n / 1000)}K`, ils: (n) => `₪${Math.round(n / 1000)}K`, day: (d) => d, rate: (r) => r.toFixed(3) };
  check(short.body.includes('את התשלומים לספקים באירו הצפויים אז') && ladder.body.endsWith('כל התשלומים עד אז, כולל הספקים באירו, עדיין מכוסים.')
    && fx.describeAction({ kind: 'short', eur: 240000, date: '2026-12-01', by: '2026-12-01', shekels: false, vendors: true }, hv, TODAY, fmt).body.includes('the EUR vendor payments expected then')
    && fx.describeAction({ kind: 'ladder', eur: 240000, legs: [leg] }, { ...hv, eurVendors: v.eurVendors }, TODAY, fmt).body.endsWith("Every payment before then, the EUR vendors' included, stays covered."),
  'a shortfall on a vendor day, and a deposit that leaves the EUR vendors covered, say so (mail and page)', JSON.stringify([short.body, ladder.body]));
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
  const direct = fx.planReserve({ upcoming, policy: 'p95', eur: fx.eurPosition(rows), inflowPerBusinessDay: inflow.perBusinessDay, today: TODAY });
  check(same(v.plan, direct) && v.schedule.length === 14 && v.schedule[0].outcomes.length === 9 && v.spotBand === 2,
    'the same plan as built by hand from the fixings, the risk and the euros');
  check(v.sigma === 0.08 && v.sigmaFromHistory && fx.planFromPayload({ ...data, rates: { ...data.rates, sigma: null } }).sigma === fx.FALLBACK_SIGMA, 'NetSuite\'s volatility, or the fallback without it');
  check(fx.planFromPayload({ ...data, balances: null }).plan === null && fx.planFromPayload({ ...data, rates: { series: [], spot: null, sigma: null } }).plan === null,
    'no balances or no rate → no plan');
}

async function testCash(fx) {
  console.log('\nCASH: salaries, vendors and new deposits');
  // NetSuite's payroll journals (September's real split), and an accrual journal in the same month that must not count.
  const journals = [
    { id: 354114, month: '2026-09', account: '200000', name: '200000 Accounts Payable - Employees', ils: 4066621.1 },
    { id: 354114, month: '2026-09', account: '200001', name: '200001 Employees reimbursments', ils: 4600 },
    { id: 354114, month: '2026-09', account: '180013', name: '180013 Withholdind tax - Payroll', ils: 1741757.41 },
    { id: 354114, month: '2026-09', account: '180069', name: '180069 Mivtach Simon - pension', ils: 1553803.51 },
    { id: 354114, month: '2026-09', account: '180012', name: '180012 Social Insurance - Social Security - debt', ils: 1082073.04 },
    { id: 999, month: '2026-09', account: '2500', name: '2500 Accrued vacation', ils: 300000 },
    { id: 327842, month: '2026-08', account: '200000', name: '200000 Accounts Payable - Employees', ils: 3679346.39 },
    { id: 327842, month: '2026-08', account: '180013', name: '180013 Withholdind tax - Payroll', ils: 3890000 },
  ];
  const split = fx.payrollSplit(journals);
  check(split.length === 2 && near(split[1].net, 4071221.1) && near(split[1].later, 4377634) && near(split[0].later, 3890000),
    'payroll journals: net salaries (the employees) vs pension, tax and social security; an accrual journal in the same month is left out', JSON.stringify(split));
  const pay = fx.payrollToPay({ projection: { asOf: null, months: [{ month: '2026-10', ils: 9000000 }] }, split, months: ['2026-08', '2026-09'], from: '2026-09', count: 3 });
  const share = (split[0].net + split[1].net) / (split[0].total + split[1].total);
  check(near(pay.share, share, 1e-9) && same(pay.rows.map((r) => [r.month, r.source]), [['2026-09', 'journal'], ['2026-10', 'projection'], ['2026-11', 'journal']])
    && near(pay.rows[1].net, 9000000 * share) && near(pay.rows[1].later, 9000000 * (1 - share)) && near(pay.rows[0].total, split[1].total),
  'payroll to pay: the Salary Projection when it has the month, else the last journal; split as the journals of the months asked');
  check(fx.payrollToPay({ projection: null, split: [], months: [], from: '2026-09', count: 2 }).rows.length === 0
    && fx.payrollToPay({ projection: { months: [{ month: '2026-10', ils: 100 }] }, split: [], months: ['2026-09'], from: '2026-10', count: 1 }).rows[0].net === 100,
  'nothing to go on → no payroll; no journals → all of it by the 5th');
  const flows = [{ month: '2026-07', kind: 'vendor', eur: -300000 }, { month: '2026-08', kind: 'vendor', eur: -900000 }, { month: '2026-09', kind: 'customer', eur: 5e6 }];
  const v3 = fx.vendorAverages(flows, [{ month: '2026-07', ils: 3e6 }, { month: '2026-09', ils: 6e6 }, { month: '2026-05', ils: 99e6 }], ['2026-07', '2026-08', '2026-09']);
  check(near(v3.eur, 400000) && near(v3.ils, 3e6) && v3.cards === 0 && near(v3.other, 3e6) && fx.vendorAverages(flows, null, ['2026-07']).ils === null,
    'vendors: the 3 months\' averages, EUR from the flows and ILS from the ILS accounts (a month without is 0; a month without the split counts as transfers)');
  const split3 = fx.vendorAverages(flows, [{ month: '2026-07', ils: 4e6, cards: 3e6, other: 1e6 }, { month: '2026-08', ils: 5e6, cards: 3.3e6, other: 1.7e6 }, { month: '2026-09', ils: 6e6, cards: 3.6e6, other: 2.4e6 }], ['2026-07', '2026-08', '2026-09']);
  check(near(split3.cards, 3.3e6) && near(split3.other, 1.7e6) && near(split3.ils, 5e6), 'ILS vendors split: the credit cards apart from bank transfers and Masav');
  const rows = [
    { company: 'LSports', bank: 'Bank Hapoalim', kind: 'current', currency: 'ILS', amount: 2000000 },
    { company: 'LSports', bank: 'Bank Hapoalim', kind: 'deposit', currency: 'ILS', amount: 5000000 },
    { company: 'LSports', bank: 'Bank Hapoalim', kind: 'deposit', currency: 'ILS', amount: 160000, blocked: true },
    { company: 'Bringits', bank: 'Bank Hapoalim', kind: 'current', currency: 'ILS', amount: 900000 },
  ];
  check(same(fx.ilsOnHand(rows), { current: 2e6, deposits: 5e6, total: 7e6 }) && fx.ilsOnHand([{ bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 1 }]) === null,
    'shekels on hand: LSports\' ILS current accounts and ILS deposits (released the day a payment goes out; not a blocked one, not Bringits); none in the balances → not known');
  check(fx.businessDayOnOrBefore('2026-11-14') === '2026-11-12' && fx.businessDayOnOrBefore('2026-11-05') === '2026-11-05'
    && fx.businessDaysBetween('2026-10-29', '2026-11-05') === 5 && fx.businessDaysBetween('2026-11-01', '2026-11-05') === 4 && fx.businessDaysBetween('2026-11-05', '2026-11-05') === 0,
  'business days: Sunday to Thursday; a payment due on Saturday 14 Nov is ready on Thursday 12 Nov');
  const bills = fx.cardBills({ ilsVendors: [{ month: '2026-09', cards: 4.7e6 }, { month: '2026-10', cards: 2.69e6 }], average: 3.32e6, today: TODAY, until: '2026-12-31' });
  const billsNov = fx.cardBills({ ilsVendors: [{ month: '2026-10', cards: 3.5e6 }, { month: '2026-11', cards: 4e6 }], average: 3.32e6, today: '2026-11-03', until: '2026-12-31' });
  check(same(bills.map((b) => [b.date, b.ils, b.spendMonth]), [['2026-11-10', 3.32e6, '2026-10'], ['2026-12-10', 3.32e6, '2026-11']])
    && same(billsNov.map((b) => [b.date, b.ils]), [['2026-11-10', 3.5e6], ['2026-12-10', 4e6]]),
  'card bills on the 10th: this month\'s spending at least the average, a month that is over what was spent, later months the average');

  // The shekels: ₪1M on hand on 21 Oct, ₪3.1M of ILS vendor payments a month (₪1M for the rest of October), ₪4M net and
  // ₪4M more each month, ₪2M of card bills on each 10th, ₪3M from each conversion.
  const needs = fx.shekelNeeds({
    today: TODAY, ilsNow: 1e6, vendorsIls: 3.1e6, spot: 3.5, until: '2026-12-31',
    payroll: [{ month: '2026-10', net: 4e6, later: 4e6 }, { month: '2026-11', net: 4e6, later: 4e6 }],
    conversions: [{ date: '2026-11-07', ils: 3e6 }, { date: '2026-12-07', ils: 3e6 }],
    cardBills: [{ date: '2026-11-10', ils: 2e6, spendMonth: '2026-10' }, { date: '2026-12-10', ils: 2e6, spendMonth: '2026-11' }],
  });
  check(same(needs.needs.map((x) => [x.date, Math.round(x.ils)]), [['2026-11-05', 7100000], ['2026-11-14', 3000000], ['2026-12-05', 7100000], ['2026-12-14', 3000000]])
    && near(needs.needs[0].eur, 7100000 / 3.5) && same(needs.needs[0].why, ['net', 'vendors']) && same(needs.needs[1].why, ['later']),
  'by the 5th: net salaries and the month\'s vendors less the shekels on hand (the rest of October\'s vendors first); the cards on the 10th from the 7th\'s shekels; by the 14th: what is left short', JSON.stringify(needs.needs.map((x) => [x.date, x.ils, x.why])));
  const nov = needs.months.find((m) => m.month === '2026-11');
  check(same(needs.months.map((m) => m.month), ['2026-10', '2026-11', '2026-12']) && nov.payrollMonth === '2026-10' && nov.net === 4e6 && nov.later === 4e6 && nov.cards === 2e6
    && near(nov.vendors, 3.1e6) && nov.tableIls === 3e6 && near(nov.convertIls, 10.1e6) && near(needs.months[0].vendors, 1e6),
  'by month paid: October (the rest of its vendors), then each month\'s payroll, cards, vendors, the table\'s shekels and what is converted');
  const d = needs.dues;
  check(same(d.slice(0, 4).map((x) => [x.date, x.by, Math.round(x.ils), Math.round(x.before), Math.round(x.convertIls), x.after]), [
    ['2026-10-21', '2026-10-21', 1e6, 1e6, 0, null], ['2026-11-05', '2026-11-05', 7.1e6, 0, 7.1e6, null],
    ['2026-11-10', '2026-11-10', 2e6, 3e6, 0, '2026-11-07'], ['2026-11-14', '2026-11-12', 4e6, 1e6, 3e6, null],
  ]) && fx.dueWhat(d[0]) === 'the rest of October\'s ILS vendor payments' && fx.dueWhat(d[1]) === 'October\'s net salaries and November\'s ILS vendor payments'
    && fx.dueWhat(d[2]) === 'the credit card bills for October' && fx.dueWhat(d[3]) === 'October\'s pension, tax and social security',
  'each payment day: what it pays, when it has to be ready (Sat 14 Nov → Thu 12 Nov), the shekels on hand before it, what to convert, and the conversion that covers it', JSON.stringify(d.slice(0, 4)));
  const ahead = fx.duesAhead(needs);
  check(same(ahead.dues.map((x) => x.date), ['2026-10-21', '2026-11-05', '2026-11-10', '2026-11-14']) && near(ahead.eur, 10.1e6 / 3.5),
    'the payments ahead: to the first month with salaries (its 5th, 10th and 14th), and the euros for them in all');
  const rich = fx.shekelNeeds({ today: TODAY, ilsNow: 50e6, vendorsIls: 3.1e6, spot: 3.5, until: '2026-12-31', payroll: [{ month: '2026-10', net: 4e6, later: 4e6 }], conversions: [] });
  check(rich.needs.length === 0 && near(rich.ilsAfter, 50e6 - 1e6 - 4e6 - 3.1e6 - 4e6), 'enough shekels on hand → nothing to convert');
  const over = fx.shekelNeeds({ today: TODAY, ilsNow: -151457, vendorsIls: 0, spot: 3.5, until: '2026-11-30', payroll: [{ month: '2026-10', net: 1e6, later: 0 }], conversions: [] });
  check(over.needs.length === 1 && over.needs[0].date === '2026-11-05' && near(over.needs[0].ils, 1151457), 'an overdraft counts against the shekels on hand (as on 21 Oct: -₪151K)');

  // The euros: two conversions at today's band (€800K each), €3M today, nothing coming in, €500K converted for
  // shekels on each 5th, €200K kept for EUR vendors.
  const t = fx.cloneTable(fx.DEFAULT_TABLE);
  const two = fx.fixings(t, TODAY).filter((f) => f.status !== 'past').slice(0, 2)
    .map((f) => ({ month: f.month, date: f.date, days: f.days, conv: fx.conversionAt(f.row, t.bounds, 3.45), risk: fx.fixingRisk(f.row, t.bounds, 3.45, 0.08, f.days / 365) }));
  const shekels = [{ date: '2026-11-05', eur: 500000 }, { date: '2026-12-05', eur: 500000 }];
  const plan = fx.planReserve({ upcoming: two, policy: 'today', eur: { hapoalim: 3e6, other: 0, deposits: [] }, inflowPerBusinessDay: 0, today: TODAY, shekels, eurFloor: 200000 });
  check(near(plan.path[0].available, 2.5e6) && near(plan.path[0].surplus, 1.5e6) && near(plan.path[1].forShekels, 1e6) && near(plan.path[1].shekels, 500000)
    && near(plan.path[1].available, 1.2e6) && near(plan.path[1].surplus, 0.2e6) && near(plan.path[0].convertibleBefore, 0.2e6),
  'the plan takes the conversions for shekels and keeps the EUR floor: euros by then and the spare before each conversion');
  check(near(plan.next.shekelsBefore, 500000) && plan.next.eurFloor === 200000 && near(plan.next.spare, 0.2e6) && plan.next.short === null
    && plan.next.tightest.date === '2026-12-07' && plan.next.freeNow === 3e6 - 800000,
  'spare now: the smallest spare of all the dues (7 Dec), not more than today\'s euros above the floor');
  check(same(plan.deposits.map((d) => [d.months, d.until, d.eur]), [[1, '2026-11-21', 1500000], [3, '2027-01-21', 200000], [6, '2027-04-21', 200000]]),
    'new deposits: a month can hold what the dues before it leave (€1.5M), longer ones only what every due leaves', JSON.stringify(plan.deposits));
  const tight = fx.planReserve({ upcoming: two, policy: 'today', eur: { hapoalim: 1e6, other: 0, deposits: [] }, inflowPerBusinessDay: 0, today: TODAY, shekels, eurFloor: 200000 });
  check(tight.next.spare === 0 && near(tight.next.short.first.eur, 500000) && tight.next.short.first.date === '2026-11-07'
    && near(tight.next.short.eur, 1.8e6) && tight.next.short.date === '2026-12-07' && tight.deposits.every((d) => d.eur === 0),
  'short: a gap opens on 7 Nov (€500K) and grows to €1.8M by 7 Dec without inflows, so nothing is spare and no deposit');
  const plain = fx.planReserve({ upcoming: two, policy: 'today', eur: { hapoalim: 3e6, other: 0, deposits: [] }, inflowPerBusinessDay: 0, today: TODAY });
  check(plain.path[1].forShekels === 0 && near(plain.path[1].available, 3e6 - 800000) && near(plain.next.spare, 3e6 - 1.6e6), 'without shekels or a floor, the plan is as before');

  // From the payload, as the page and the mail see it.
  const data = {
    today: TODAY, table: t, rates: { series: series(400), spot: { date: TODAY, rate: 3.4778 }, sigma: 0.08 },
    balances: { rows: [{ bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 3e6 }, { bank: 'Bank Hapoalim', kind: 'current', currency: 'ILS', amount: 2e6 }] },
    flows: [...flows, { month: '2026-09', kind: 'vendor', eur: -300000 }], inflowMonths: ['2026-07', '2026-08', '2026-09'],
    payroll: { journals, projection: { asOf: '2026-10-20T08:00:00Z', months: ['2026-09', '2026-10', '2026-11', '2026-12', '2027-01', '2027-02', '2027-03', '2027-04'].map((m) => ({ month: m, ils: 8e6 })) } },
    ilsVendors: [{ month: '2026-07', ils: 3e6 }, { month: '2026-08', ils: 3e6 }, { month: '2026-09', ils: 3e6 }],
  };
  const v = fx.planFromPayload(data, 'p95');
  const nov7 = v.schedule[0];
  const kept = fx.requirementOf('p95', nov7.atSpot, nov7.risk);
  const keptRate = Math.min(...nov7.outcomes.filter((o) => o.eur === kept).map((o) => o.rate));
  const novRow = v.cash.months.find((m) => m.month === '2026-11');
  check(v.cash && v.cash.ilsNow === 2e6 && near(v.cash.vendors.ils, 3e6) && near(v.cash.vendors.eur, 500000) && v.cash.until === '2027-04-30'
    && near(novRow.tableIls, kept * keptRate) && near(novRow.net + novRow.later, 8e6) && v.cash.notes.length === 0,
  'from the payload: the shekels on hand, the vendors, and the 7th bringing the shekels of the amount kept (95%: €1.1M at its rate)');
  check(near(v.plan.next.shekelsBefore, v.cash.needs.filter((x) => x.date <= nov7.fixing.date).reduce((a, x) => a + x.eur, 0)) && v.plan.next.eurFloor === v.cash.vendors.eur
    && v.plan.deposits.length === 3 && v.plan.deposits.every((d, i, all) => i === 0 || d.eur <= all[i - 1].eur),
  'the plan carries the shekels\' conversions and the floor; longer deposits never hold more');
  const books = fx.planFromPayload({ ...data, balances: { rows: [{ bank: 'Bank Hapoalim', kind: 'current', currency: 'EUR', amount: 3e6 }] }, ilsVendors: null, payroll: { journals: [], projection: data.payroll.projection } }, 'p95');
  check(books.cash.ilsNow === null && books.cash.notes.length === 3 && books.cash.share === null, 'unknown shekels on hand, ILS vendors or split → said, and counted conservatively');
  check(fx.planFromPayload({ ...data, payroll: null }).cash === null && fx.planFromPayload({ ...data, payroll: null }).plan.next.eurFloor === 0, 'no payroll → the plan without salaries, as before');
}

async function testToday(fx) {
  console.log('\nTODAY: what to do today, from the day\'s balances, and when the mail goes');
  const { mailDecision } = require(path.join(ROOT, 'scripts', 'fx-challenge-mail.cjs'));
  // As in CASH: ₪1M on hand, ₪3.1M of ILS vendor payments a month, ₪4M net and ₪4M more, ₪2M of card bills, ₪3M from
  // each conversion; €1.31M of euros, €683K kept for EUR vendors.
  const cashOn = (day) => fx.shekelNeeds({
    today: day, ilsNow: 1e6, vendorsIls: 3.1e6, spot: 3.5, until: '2026-12-31',
    payroll: [{ month: '2026-10', net: 4e6, later: 4e6 }, { month: '2026-11', net: 4e6, later: 4e6 }],
    conversions: [{ date: '2026-11-07', ils: 3e6 }, { date: '2026-12-07', ils: 3e6 }],
    cardBills: [{ date: '2026-11-10', ils: 2e6, spendMonth: '2026-10' }, { date: '2026-12-10', ils: 2e6, spendMonth: '2026-11' }],
  });
  const NEXT = { date: '2026-11-07', liquid: 1.31e6, eurFloor: 683000, short: null, moveToHapoalim: 0, spare: 0, freeNow: 0, shortfall: 0, hapoalim: 981000, required: 1.4e6, backup: 4.2e6 };
  const view = (day, { next = {}, level = 'middle', deposits = [] } = {}) => ({
    plan: { next: { ...NEXT, ...next }, deposits }, cash: cashOn(day), rateCheck: { level, rate: 3.48, days: 90, share: 0.5, average: 3.45, low: 3.4, high: 3.55, n: 64 },
  });
  const kinds = (r) => r.actions.map((a) => `${a.kind}${a.urgent ? '!' : ''}:${a.key.split(':')[1] || ''}`);

  // A conversion for shekels is said 2 business days before it has to be ready, not earlier: finance converts at once,
  // and the figure moves until then (asked on 27 Oct 2026). Before that it is only among the payments ahead.
  const ahead21 = fx.todayActions(view(TODAY), TODAY);
  check(kinds(ahead21).length === 0 && same(ahead21.ahead.dues.filter((d) => d.eur > 0.5).map((d) => d.date), ['2026-11-05', '2026-11-14']),
    'on 21 Oct: nothing to convert yet; 5 Nov and 14 Nov are only among the payments ahead', JSON.stringify(kinds(ahead21)));
  const week = fx.todayActions(view('2026-11-01'), '2026-11-01');
  check(same(kinds(week), ['convert!:2026-11-01']),
    'on Sun 1 Nov: convert today for the rest of November\'s vendors; Thu 5 Nov (4 business days away) not yet', JSON.stringify(kinds(week)));
  const mon = fx.todayActions(view('2026-11-02'), '2026-11-02');
  const tue = fx.todayActions(view('2026-11-03'), '2026-11-03');
  check(!kinds(mon).includes('convert!:2026-11-05') && kinds(tue).includes('convert!:2026-11-05') && !kinds(tue).some((k) => k.endsWith(':2026-11-14')),
    'Thu 5 Nov\'s conversion is said from Tue 3 Nov (2 business days ahead), in red; Mon 2 Nov is too early', JSON.stringify([kinds(mon), kinds(tue)]));
  const high = fx.todayActions(view(TODAY, { level: 'high' }), TODAY);
  check(kinds(high).length === 0 && same(kinds(fx.todayActions(view('2026-11-03', { level: 'high' }), '2026-11-03')).filter((k) => k.endsWith(':2026-11-05')), ['convert!:2026-11-05']),
    'a high EUR/ILS brings no conversion forward: 5 Nov\'s is still said from Tue 3 Nov, not on 21 Oct', JSON.stringify(kinds(high)));
  const small = fx.todayActions({ ...view('2026-11-01'), cash: fx.shekelNeeds({ today: '2026-11-01', ilsNow: 2.99e6, vendorsIls: 3.1e6, spot: 3.5, until: '2026-11-30', payroll: [], conversions: [] }) }, '2026-11-01');
  check(small.actions.length === 0, 'a gap under €50K is no action (here €2K for the rest of November\'s vendors)', JSON.stringify(kinds(small)));
  const runsOut = { eur: 776000, date: '2026-11-07', shekels: false };
  const dip = { first: { eur: 300000, date: '2026-11-07', shekels: false }, eur: 300000, date: '2026-11-07', left: 383000, back: '2026-11-09' };
  check(kinds(fx.todayActions(view(TODAY, { next: { short: dip, runsOut } }), TODAY)).includes('short:2026-11-07')
    && kinds(fx.todayActions(view('2026-11-01', { next: { short: dip, runsOut } }), '2026-11-01')).includes('short!:2026-11-07')
    && !kinds(fx.todayActions(view(TODAY, { next: { short: dip, runsOut: { ...runsOut, eur: 13000 } } }), TODAY)).some((k) => k.startsWith('short')),
  'a shortfall (the euros run out): said when it opens, urgent in its last week; under €50K not at all');
  check(!kinds(fx.todayActions(view('2026-11-01', { next: { short: dip, runsOut: null } }), '2026-11-01')).some((k) => k.startsWith('short') || k.startsWith('deposit')),
    'a dip into the floor kept for EUR vendors alone is no shortfall (and no new deposit either)');
  check(same(kinds(fx.todayActions(view('2026-11-04', { next: { moveToHapoalim: 300000 } }), '2026-11-04')).filter((k) => k.startsWith('move')), ['move!:2026-11-07'])
    && !kinds(fx.todayActions(view(TODAY, { next: { moveToHapoalim: 300000 } }), TODAY)).some((k) => k.startsWith('move')),
  'euros to move to Hapoalim EUR: urgent in the last business days before the conversion (a Saturday one counts from the Thursday), not before');
  const deps = [{ months: 1, until: '2026-11-21', eur: 500000 }, { months: 3, until: '2027-01-21', eur: 0 }];
  const depo = fx.todayActions(view(TODAY, { next: { spare: 500000 }, deposits: deps }), TODAY);
  check(same(kinds(fx.todayActions(view(TODAY, { next: { spare: 500000 }, level: 'high', deposits: deps }), TODAY)), ['deposit:'])
    && same(kinds(depo), ['deposit:']) && depo.actions[0].options.length === 1,
  'spare euros: a new deposit (the terms that hold at least €50K), on a high rate too: no conversion for the rate alone');
  const fmt0 = { eur: (n) => `€${Math.round(n / 1000)}K`, ils: (n) => `₪${Math.round(n / 1000)}K`, day: (iso) => iso, rate: (r) => r.toFixed(3) };
  const allSame = { kind: 'deposit', key: 'deposit', urgent: false, eur: 430000, options: [{ months: 1, until: '2026-11-21', eur: 430000 }, { months: 3, until: '2027-01-21', eur: 430000 }, { months: 6, until: '2027-04-21', eur: 430000 }] };
  check(fx.describeAction(allSame, view(TODAY), TODAY, fmt0).body.startsWith('€430K for 1, 3 or 6 months (the longest until 2027-04-21)')
    && fx.describeAction(depo.actions[0], view(TODAY), TODAY, fmt0).body.startsWith('€500K for 1 month (until 2026-11-21)'),
  'a deposit in words: one line when every term holds the same');
  // A conversion the deposits leave short: the spare euros go into deposits timed for it, before any 1, 3 or 6 months.
  const legs = [{ month: '2027-01', date: '2027-01-07', until: '2027-01-06', eur: 130000, gap: 128000, required: 1050000, have: 922000 },
    { month: '2027-02', date: '2027-02-07', until: '2027-02-04', eur: 150000, gap: 141000, required: 1050000, have: 909000 }];
  const coverage = [{ month: '2027-01', date: '2027-01-07', gap: 128000, timed: true }, { month: '2027-02', date: '2027-02-07', gap: 141000, timed: true }];
  const withGaps = (next, level = 'middle', ladder = legs) => ({ ...view(TODAY, { next, level, deposits: deps }), plan: { next: { ...NEXT, ...next }, deposits: deps, coverage, ladder } });
  const lad = fx.todayActions(withGaps({ spare: 500000 }), TODAY);
  check(kinds(lad).includes('ladder:') && !kinds(lad).some((k) => k.startsWith('deposit')) && lad.actions.find((a) => a.kind === 'ladder').eur === 280000,
    'gaps ahead: deposits timed for them (€280K), and no 1, 3 or 6 month deposit while one is open', JSON.stringify(kinds(lad)));
  check(same(kinds(fx.todayActions(withGaps({ spare: 500000 }, 'high'), TODAY)), ['ladder:']), 'on a high rate, the same: the ladder, and nothing converted for the rate');
  check(!kinds(fx.todayActions(withGaps({ spare: 40000 }, 'middle', [{ ...legs[0], eur: 40000 }]), TODAY)).some((k) => k.startsWith('ladder') || k.startsWith('deposit')),
    'under €50K of room: no deposit at all while the gap is open');
  const lv = withGaps({ spare: 500000 });
  check(fx.describeAction({ kind: 'ladder', key: 'ladder', urgent: false, eur: 130000, legs: [legs[0]] }, lv, TODAY, fmt0).head === 'Put €130K in a deposit until 2027-01-06'
    && fx.describeAction({ kind: 'ladder', key: 'ladder', urgent: false, eur: 130000, legs: [legs[0]] }, lv, TODAY, fmt0).body === 'For the 2027-01-07 conversion: it takes €1050K, and the deposits paid out by then hold €922K, €128K short. Every payment before then stays covered.'
    && fx.describeAction(lad.actions.find((a) => a.kind === 'ladder'), lv, TODAY, fmt0).head === 'Put €280K in deposits for the January and February conversions'
    && fx.describeAction(lad.actions.find((a) => a.kind === 'ladder'), lv, TODAY, fmt0).body === '€130K until 2027-01-06 for the 2027-01-07 conversion (€128K short), €150K until 2027-02-04 for the 2027-02-07 conversion (€141K short). Every payment before then stays covered.',
  'the ladder in words: each deposit, until when, and the conversion it is for');
  const move = { kind: 'move', key: 'move:2026-11-07', urgent: true, eur: 300000, date: '2026-11-07', by: '2026-11-05' };
  const moveSaid = (next) => fx.describeAction(move, view('2026-11-04', { next }), '2026-11-04', fmt0).body;
  check(moveSaid({}) === 'The 2026-11-07 conversion takes its euros from Hapoalim EUR (583400), which holds €981K; keep €1400K there.'
    && moveSaid({ maturing: 500000 }) === 'The 2026-11-07 conversion takes its euros from Hapoalim EUR (583400), which holds €981K; keep €1400K there, €500K of it from deposits released by then.',
  'moving euros to Hapoalim: what to keep there, and what the deposits released by then bring of it');

  const fmt = { eur: (n) => `€${Math.round(n)}`, ils: (n) => `₪${Math.round(n)}`, day: (iso) => iso, rate: (r) => r.toFixed(3) };
  const v1 = view('2026-11-01');
  const said = week.actions.map((a) => fx.describeAction(a, v1, '2026-11-01', fmt));
  const nov5 = tue.actions.find((a) => a.key === 'convert:2026-11-05');
  const said5 = fx.describeAction(nov5, view('2026-11-03'), '2026-11-03', fmt);
  check(said[0].head.startsWith('Convert €') && said[0].head.endsWith(' today') && said[0].body.startsWith('The rest of November\'s ILS vendor payments: ₪2996667 goes out this month, with ₪1000000 on hand')
    && said5.head === `Convert €${Math.round(4e6 / 3.5)} by 2026-11-05` && said5.body === 'October\'s net salaries: ₪4000000 goes out on 2026-11-05, with ₪0 on hand by then.',
  'each action in words, with the balances behind it', JSON.stringify([said, said5]));
  const lowSaid = fx.describeAction(nov5, view('2026-11-03', { level: 'low' }), '2026-11-03', fmt).body;
  check(/is low against the last 90 days: if it can wait, wait, but no later than 2026-11-05\.$/.test(lowSaid), 'a low EUR/ILS: wait if it can, with the deadline', lowSaid);
  const due = cashOn(TODAY).dues;
  check(fx.describeDue(due[0], TODAY, fmt) === 'Now: the rest of October\'s ILS vendor payments, ₪1000000: covered by the shekels on hand'
    && fx.describeDue(due[2], TODAY, fmt) === '2026-11-10, in 20 days: the credit card bills for October, ₪2000000: covered by the 2026-11-07 conversion'
    && fx.describeDue(due[3], TODAY, fmt) === `2026-11-14 (ready by 2026-11-12), in 22 days: October's pension, tax and social security, ₪4000000: convert €${Math.round(3e6 / 3.5)}`,
  'the payments ahead in words: when, what, how much, and what covers it or what to convert');
  check(fx.nothingToday(fx.duesAhead(cashOn(TODAY)), TODAY, fmt) === `Nothing to do today. Next: convert €${Math.round(7.1e6 / 3.5)} by 2026-11-05 (in 15 days) for October's net salaries and November's ILS vendor payments.`,
    'nothing to do: what comes next, and when');

  // When the mail goes: any day but Saturday, whenever there is a recommendation.
  const u = [{ kind: 'convert', key: 'convert:2026-11-05', urgent: true, eur: 1e6 }];
  const n1 = [{ kind: 'deposit', key: 'deposit', urgent: false, eur: 500000 }];
  check(mailDecision(u, { today: '2026-10-23' }).send && mailDecision(n1, { today: '2026-10-22' }).send && mailDecision(n1, { today: '2026-10-23' }).send
    && mailDecision(n1, { today: '2026-10-25' }).send && mailDecision(u, { today: '2026-10-24' }).why === 'Saturday' && mailDecision([], { today: TODAY }).why === 'no recommendations',
  'a mail every day but Saturday (Friday included) when there is a recommendation, urgent or not, the same as the day before or not; none without one');
  check(mailDecision([], { today: '2026-10-24', always: true }).send, 'FX_MAIL_ALWAYS=1: every day, Saturday included');
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

function call(handler, { method = 'GET', url = '/api/fx-challenge', originalUrl, body, headers = {}, user } = {}) {
  return new Promise((resolve) => {
    const raw = body === undefined ? '' : JSON.stringify(body);
    const req = Readable.from(raw ? [Buffer.from(raw)] : []);
    Object.assign(req, { method, url, headers: { host: 'finance.example', ...headers }, ...(originalUrl ? { originalUrl } : {}), ...(user ? { user } : {}) });
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
  const clean = (s) => !/undefined|NaN|\[object|null/.test(s);
  // English words in the mail's own text, names, the rate pair, the script's path and links aside.
  const english = (s) => s.replace(/FX Challenge|EUR\/ILS|HSBC|NetSuite|scripts\/fx-challenge-mail\.cjs|https?:\/\/\S+/g, '').match(/\b[A-Za-z]{3,}\b/g);
  check(/^FX יום ד' 21\.10 · /.test(m.subject) && m.html.startsWith('<!doctype html><html dir="rtl" lang="he">') && clean(m.html) && clean(m.text) && clean(m.subject),
    'Hebrew, right to left: the subject has the day and the first recommendations', m.subject);
  check(!english(m.text) && !english(m.subject), 'no English in the mail (names, the rate pair, the script\'s path and links aside)', JSON.stringify(english(m.text)));
  check(m.text.includes('המצב היום') && m.text.includes('בחשבון האירו בפועלים') && m.text.includes('אין צורך לשמור כסף להמרה של שבת 7.11')
    && m.text.includes('פנוי להמרה עכשיו') && m.text.includes('שער EUR/ILS 3.478'),
  "today's balances in one paragraph: what is held, what to keep for the conversion, what is free, the rate against the last 90 days");
  check(!m.html.includes('<b>markup') && !m.text.includes('markup') && m.text.includes('חלק מהנתונים לא נקראו היום'), 'a read that failed is said in Hebrew; nothing from the data reaches the mail unescaped');
  check((m.html.match(/<table/g) || []).length === 2 && m.text.length < 2500 && !/If EUR\/ILS|Can be converted/.test(m.text), 'focused: no tables and no workings (they stay on the page)', String(m.text.length));
  check(m.html.includes('href="https://finance.example/business-tools/fx-challenge"') && m.text.includes('95% מתנודות השער'), 'the page\'s link and the setting used');
  const cover = fx.keepCover(fx.planFromPayload(data, 'p95').plan);
  check(cover.covered && /אין צורך לשמור כסף להמרה של שבת 7\.11: הפיקדון שמשתחרר ביום ג' 3\.11 \(€1\.50M\) מכסה את ה-€[\d.]+[KM] שהיא מוכרת מחשבון האירו בפועלים \(95% מתנודות השער\)\./.test(m.text)
    && !m.text.includes('להשאיר €'),
  'the €1.5M deposit released on Tue 3 Nov covers what Sat 7 Nov takes: the mail says there is nothing to keep, and why');
  const lines = m.text.split('\n');
  const covAt = lines.findIndex((l) => l.startsWith('ההמרות הבאות מול הפיקדונות (95% מתנודות השער)'));
  check(covAt > 0 && /^- שבת 7\.11: €[\d.]+[KM], מכוסה מהפיקדונות \(€1\.50M\)\.$/.test(lines[covAt + 1]) && /^- יום ב' 7\.12: €[\d.]+[KM], הפיקדונות מכסים €[\d.]+K: חסרים €[\d.]+[KM]\.$/.test(lines[covAt + 2])
    && lines.slice(covAt + 1, covAt + 7).every((l) => l.startsWith('- ')),
  'the mail lists the next conversions against the deposits: 7 Nov covered by the €1.5M of 3 Nov, 7 Dec short of what is left', lines.slice(covAt, covAt + 7).join(' | '));
  const hl = [{ month: '2027-01', date: '2027-01-07', until: '2027-01-06', eur: 130000, gap: 128000, required: 1050000, have: 922000 },
    { month: '2027-02', date: '2027-02-07', until: '2027-02-04', eur: 150000, gap: 141000, required: 1050000, have: 909000 }];
  const hv = { plan: { next: {} }, rateCheck: null };
  const one = mail.actionText({ kind: 'ladder', eur: 130000, legs: [hl[0]] }, hv, TODAY);
  const two = mail.actionText({ kind: 'ladder', eur: 280000, legs: hl }, hv, TODAY);
  check(one.head === 'להפקיד €130K בפיקדון עד יום ד\' 6.1' && one.body === 'להמרה של יום ה\' 7.1: היא מוכרת €1.05M, והפיקדונות שמשתחררים עד אז מכסים €922K, כלומר חסרים €128K. כל התשלומים עד אז עדיין מכוסים.'
    && two.head === 'להפקיד €280K בפיקדונות להמרות של ינואר ופברואר'
    && two.body === '€130K עד יום ד\' 6.1 להמרה של יום ה\' 7.1 (חסרים €128K), €150K עד יום ה\' 4.2 להמרה של יום א\' 7.2 (חסרים €141K). כל התשלומים עד אז עדיין מכוסים.'
    && !english(`${one.head} ${one.body} ${two.head} ${two.body}`),
  'the ladder in the mail, in Hebrew: each deposit, until when, and the conversion it is for', JSON.stringify([one, two]));
  const worst = fx.keepCover(fx.planFromPayload(data, 'worst').plan);
  const mw = mail.renderMail(fx, data, { policy: 'worst' });
  check(!worst.covered && worst.fromCash > 0
    && /להשאיר €[\d.]+[KM] בפועלים להמרה של שבת 7\.11 \(המקרה הגרוע בטבלה\): היא מוכרת €[\d.]+[KM], ו-€1\.50M מזה מגיעים מהפיקדון שמשתחרר ביום ג' 3\.11\./.test(mw.text) && !english(mw.text),
  'keeping the table\'s worst case: keep only what the deposit doesn\'t bring, and why');
  const withCash = await fxs.readFxChallenge({
    reads: {
      ...reads,
      payroll: async () => [
        { id: 1, month: '2026-09', account: '200000', name: '200000 Accounts Payable - Employees', ils: 4e6 },
        { id: 1, month: '2026-09', account: '180013', name: '180013 Withholdind tax - Payroll', ils: 4e6 },
      ],
      ilsVendors: async () => [{ month: '2026-07', ils: 3e6 }, { month: '2026-08', ils: 3e6 }, { month: '2026-09', ils: 3e6 }],
      salary: async () => ({ asOf: '2026-10-20T08:00:00Z', months: ['2026-09', '2026-10', '2026-11', '2026-12', '2027-01', '2027-02', '2027-03', '2027-04'].map((m) => ({ month: m, ils: 8e6 })) }),
    },
    clock: () => NOW,
  });
  const mc = mail.renderMail(fx, withCash, { policy: 'p95' });
  const vc = fx.planFromPayload(withCash, 'p95');
  check(/^FX יום ד' 21\.10 · להמיר €[\d.,]+[KM] היום/.test(mc.subject) && clean(mc.html) && clean(mc.text) && !english(mc.text)
    && mc.actions[0].kind === 'convert' && mc.actions[0].urgent && mc.text.includes('להמיר €') && mc.text.includes('יתרת תשלומי הספקים בשקלים של אוקטובר: ')
    && mc.text.includes('תשלומים בשקלים בתקופה הקרובה') && mc.text.includes('שכר נטו של אוקטובר ותשלומי הספקים בשקלים של נובמבר')
    && mc.text.includes('שבת 14.11 (מוכן עד יום ה\' 12.11)') && mc.text.includes('פנסיה, מס וביטוח לאומי של אוקטובר') && vc.cash && vc.cash.months.length >= 6,
  'with the payroll: today\'s recommendations lead (subject and body), each with its balances, then the payments in shekels ahead', mc.subject);
  const section = (t) => { const ls = t.split('\n'); const at = ls.indexOf('תשלומים צפויים באירו עד יום ד\' 25.11'); return at < 0 ? [] : ls.slice(at + 1, ls.indexOf('', at + 1)); };
  const ms = section(m.text);
  const mcs = section(mc.text);
  check(ms[0] === 'התשלומים לספקים באירו לא נקראו היום לפי ימים, ולכן הם בחשבון כממוצע לאורך החודש.' && /^- שבת 7\.11: ההמרה של נובמבר בבנק, €[\d.]+[KM]\.$/.test(ms[1])
    && /^סה"כ עד יום ד' 25\.11: €[\d.]+[KM] \(המרות בבנק €[\d.]+[KM]\)\. מול זה: €1\.21M באירו היום, €1\.50M מפיקדונות שמשתחררים עד אז ותקבולים רגילים של כ-€[\d.]+[KM] בחודש\. לפי זה כל התשלומים מכוסים\.$/.test(ms[2])
    && ms.length === 3,
  'the mail lists every euro expected to leave in the next 35 days, by day, with what is assumed for the EUR vendors and the total against what covers it', ms.join(' | '));
  check(/^- יום ד' 21\.10: המרה לשקלים \(ספקים בשקלים\), €[\d.]+K\.$/.test(mcs[1]) && /^- יום ה' 5\.11: המרה לשקלים \(שכר נטו, ספקים בשקלים\), €[\d.]+M\.$/.test(mcs[2])
    && /^- שבת 7\.11: ההמרה של נובמבר בבנק/.test(mcs[3]) && mc.actions.some((a) => a.kind === 'short')
    && /לפי זה חסרים €[\d.]+K ביום ב' 7\.12, גם אחרי ה-€200K השמורים: ההמלצות למעלה אומרות איך להשלים\.$/.test(mcs[mcs.length - 1]) && !english(mcs.join(' ')),
  'with the payroll: the euros converted for shekels on their days too, and a gap the recommendations above fix', mcs.join(' | '));
  const none = mail.renderMail(fx, { ...data, balances: null }, {});
  check(/· אין היום המלצה$/.test(none.subject) && clean(none.html) && none.text.includes('היתרות'), 'without balances: no recommendation, and why');
  const failed = mail.failureMail(TODAY, new Error('NetSuite down'));
  check(/· הנתונים לא נקראו היום$/.test(failed.subject) && failed.text.includes('NetSuite down'), 'nothing read: a short mail saying so');

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
  check(relay.policy === 'today' && mail.mailConfig({ FX_MAIL_TO: 'matan.l@lsports.eu', FX_MAIL_POLICY: 'p95' }).policy === 'p95'
    && /\(לפי השער היום\)/.test(mail.renderMail(fx, data).text),
  'the mail tells today\'s scenario (what today\'s rate would sell) unless FX_MAIL_POLICY asks for 95% or the worst case');
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
    && s0.msg.text.includes('פנוי להמרה עכשיו') && s0.t.name === 'lsports.eu' && JSON.parse(fs.readFileSync(markerFile, 'utf8')).day === TODAY
    && JSON.parse(fs.readFileSync(markerFile, 'utf8')).lastSent.recipients === 2 && JSON.parse(fs.readFileSync(markerFile, 'utf8')).lastSent.actions.length > 0
    && /sent to 2 recipient/.test(tenLog.join(' ')),
  'scheduled at 10:00 with something to do: sent from the sender set, to both (FX_MAIL_TO), HTML and text, and marked as sent today to 2 with its actions');
  await quietly(() => mail.main(['--scheduled'], deps(at('11:00'))));
  const [manual] = await quietly(() => mail.main([], deps(at('15:00'))));
  check(sent.length === 2 && manual === 0 && JSON.parse(fs.readFileSync(markerFile, 'utf8')).at === new Date(at('10:00')()).toISOString(), 'sent once a day; a run by hand sends a test and leaves the daily one alone');
  // Thursday 22 and Friday 23 Oct: the same recommendations as Wednesday → a mail each day; Saturday 24 Oct → none.
  const [thu, thuLog] = await quietly(() => mail.main(['--scheduled'], deps(() => Date.parse('2026-10-22T10:00:00+03:00'))));
  const [fri] = await quietly(() => mail.main(['--scheduled'], deps(() => Date.parse('2026-10-23T10:00:00+03:00'))));
  const m23 = JSON.parse(fs.readFileSync(markerFile, 'utf8'));
  check(thu === 0 && fri === 0 && sent.length === 4 && m23.day === '2026-10-23' && m23.quiet === false && m23.lastSent.day === '2026-10-23' && /sent to 2 recipient/.test(thuLog.join(' ')),
    'Thursday and Friday: a mail each day while there are recommendations, the same as the day before or not', JSON.stringify(m23));
  const [sat, satLog] = await quietly(() => mail.main(['--scheduled'], deps(() => Date.parse('2026-10-24T10:00:00+03:00'))));
  const m24 = JSON.parse(fs.readFileSync(markerFile, 'utf8'));
  check(sat === 0 && sent.length === 4 && m24.day === '2026-10-24' && m24.quiet === true && m24.why === 'Saturday' && m24.lastSent.day === '2026-10-23' && /no mail on Saturdays/.test(satLog.join(' ')),
    'a Saturday: no mail, the check kept with why, the last mail kept');
  const markerAlways = path.join(tmp, 'last-always.json');
  const [always] = await quietly(() => mail.main(['--scheduled'], deps(() => Date.parse('2026-10-24T10:00:00+03:00'), { env: { ...env, FX_MAIL_ALWAYS: '1' }, markerFile: markerAlways })));
  check(always === 0 && sent.length === 5 && JSON.parse(fs.readFileSync(markerAlways, 'utf8')).lastSent.day === '2026-10-24', 'FX_MAIL_ALWAYS=1: sent even on a Saturday');
  sent.splice(2);
  const partial = { ...reads, mail: null, books: async () => { throw new Error('NetSuite down'); } };
  const marker2 = path.join(tmp, 'last2.json');
  const [retry, retryLog] = await quietly(() => mail.main(['--scheduled'], deps(at('10:00'), { reads: partial, markerFile: marker2 })));
  check(retry === 0 && sent.length === 2 && !fs.existsSync(marker2) && /trying again next hour/.test(retryLog.join(' ')), 'no balances at 10:00: not sent, tried again next hour');
  await quietly(() => mail.main(['--scheduled'], deps(at('12:00'), { reads: partial, markerFile: marker2 })));
  check(sent.length === 3 && /אין היום המלצה/.test(sent[2].msg.subject) && fs.existsSync(marker2), 'still none at 12:00: the mail goes anyway, saying why');
  const [refused, refusedLog] = await quietly(() => mail.main([], deps(at('10:00'), { env: { ...env, FX_MAIL_TO: 'someone@gmail.com' } })));
  check(refused === 1 && sent.length === 3 && /outside lsports\.eu/.test(refusedLog.join(' ')), 'an address outside lsports.eu: nothing is sent');
  const htmlFile = path.join(tmp, 'mail.html');
  const [dry, dryLog] = await quietly(() => mail.main(['--dry-run', `--html=${htmlFile}`], deps(at('08:00'), { env: {} })));
  check(dry === 0 && sent.length === 3 && /^Subject: FX /.test(dryLog[0]) && fs.readFileSync(htmlFile, 'utf8').startsWith('<!doctype html>') && /a real send would stop/.test(dryLog.join(' ')),
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
    && same(after.lastSent, { day: TODAY, at: '2026-10-21T07:00:03.000Z', recipients: 3, manual: false }) && after.lastCheck === null && history.length === 1 && same(fxs.savedRecipients(file), after.value.to),
  'PUT saves the list for everyone, with a history line; GET shows it and when the mail last went (a marker from before the checks)');
  fs.writeFileSync(sentFile, JSON.stringify({ day: '2026-10-22', at: '2026-10-22T07:00:05.000Z', quiet: true, why: 'nothing new since the last mail',
    lastSent: { day: TODAY, at: '2026-10-21T07:00:03.000Z', recipients: 3, subject: 'x', actions: [{ key: 'deposit', eur: 500000 }] } }));
  const quietDay = (await call(router, { url: '/api/fx-challenge/mail' })).body;
  check(same(quietDay.lastSent, { day: TODAY, at: '2026-10-21T07:00:03.000Z', recipients: 3, manual: false }) && same(quietDay.lastCheck, { day: '2026-10-22', at: '2026-10-22T07:00:05.000Z', why: 'nothing new since the last mail' }),
    'a day with no mail: GET shows the last check and why, and still the last mail that went');

  // Send now (POST /mail/send): the mail at once, to the saved list, as the last mail sent; the daily check doesn't repeat it.
  const outbox = [];
  const nodemailer = { createTransport: () => ({ sendMail: async (msg) => { outbox.push(msg); return { messageId: 'x' }; } }) };
  let now = NOW;
  const sendEnv = () => ({ FX_MAIL_TO: 'matan.l@lsports.eu', FX_MAIL_FROM: 'matan.l@lsports.eu', FX_MAIL_SMTP_URL: 'smtp://smtp-relay.gmail.com:587' });
  const sendReads = { rates: async () => series(400), books: async () => [{ number: '11001', name: '11001 Bank HPoalim Euro - 583400', eur: 2e6 }], mail: null, flows: async () => [{ month: '2026-09', kind: 'customer', eur: 3e6 }], conversions: async () => OCT_CONVERSIONS };
  const sendRouter = fxs.createFxChallengeRouter({
    reads: sendReads, tableFile: path.join(tmp, 'table.json'), clock: () => now, mail: { file, sentFile, env: sendEnv, nodemailer },
  });
  const postSend = (extra = {}) => call(sendRouter, { method: 'POST', url: '/mail/send', originalUrl: '/api/fx-challenge/mail/send', body: {}, headers: json, user: { email: 'matan.l@lsports.eu' }, ...extra });
  const getSend = await call(sendRouter, { url: '/mail/send', originalUrl: '/api/fx-challenge/mail/send' });
  const notJson = await postSend({ headers: { origin: 'https://finance.example' } });
  const crossSend = await postSend({ headers: { ...json, origin: 'https://evil.example' } });
  check(getSend.status === 405 && notJson.status === 415 && crossSend.status === 403 && outbox.length === 0, 'Send now: POST only, JSON, from the dashboard itself');
  const sentNow = await postSend();
  const mk = fxs.readMailMarker(sentFile);
  check(sentNow.status === 200 && sentNow.body.recipients === 3 && /^FX יום ד' 21\.10 · /.test(sentNow.body.subject) && outbox.length === 1
    && outbox[0].to === 'matan.l@lsports.eu, dotan@lsports.eu, finance@lsports.eu' && mk.lastSent.manual && mk.lastSent.by === 'matan.l@lsports.eu'
    && mk.lastSent.at === new Date(NOW).toISOString() && mk.lastSent.actions.length > 0 && mk.day === TODAY && !mk.quiet,
  'Send now: read fresh and sent to the saved list; recorded as today\'s mail (by whom, with its recommendations)', JSON.stringify(sentNow.body));
  const again = await postSend();
  now += 61 * 1000;
  const later = await postSend();
  check(again.status === 429 && later.status === 200 && outbox.length === 2, 'Send now: once a minute at most');
  const card = (await call(sendRouter, { url: '/api/fx-challenge/mail' })).body;
  check(card.lastSent.manual === true && card.lastSent.recipients === 3, 'the card shows the last mail as sent with Send now');
  // The 10:00 check that day: Send now was today's mail, so it doesn't send it again; the next day it does.
  const { main: mailMain } = require(path.join(ROOT, 'scripts', 'fx-challenge-mail.cjs'));
  const dailyAt = (iso) => quietly(() => mailMain(['--scheduled'], {
    env: sendEnv(), clock: () => Date.parse(iso), reads: sendReads, nodemailer, markerFile: sentFile, mailFile: file, tableFile: path.join(tmp, 'table.json'),
  }));
  const [daily] = await dailyAt('2026-10-21T10:01:00+03:00');
  const after10 = fxs.readMailMarker(sentFile);
  const [nextDay] = await dailyAt('2026-10-22T10:01:00+03:00');
  check(daily === 0 && after10.day === TODAY && after10.lastSent.manual && nextDay === 0 && outbox.length === 3 && fxs.readMailMarker(sentFile).lastSent.day === '2026-10-22',
    'after Send now, the 10:00 check doesn\'t send it again that day; the next day\'s check sends as usual', JSON.stringify(after10));
  fs.writeFileSync(file, JSON.stringify({ value: { to: [] } }));
  now += 61 * 1000;
  const nobody = await postSend();
  check(nobody.status === 400 && /Nobody is on the list/.test(nobody.body.error) && outbox.length === 3, 'Send now with nobody on the list: nothing sent, and why');
  fs.writeFileSync(file, JSON.stringify({ value: { to: ['matan.l@lsports.eu', 'dotan@lsports.eu', 'finance@lsports.eu'] } }));
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

async function testServerCash(fx) {
  console.log('\nSERVER: the payroll, the ILS vendors and the Salary Projection\'s saved figures');
  const sp = require(path.join(ROOT, 'server', 'salary-projection.cjs'));
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fx-cash-'));
  const cacheFile = path.join(tmp, 'salary-projection-cache.json');
  const payload = {
    generatedAt: '2026-10-20T08:00:00.000Z', accounts: [{ acct: '760001' }, { acct: '760017' }],
    blocks: [{ year: 2026, kind: 'current', rows: [
      { mKey: '2026-09', status: 'actual', eur: {}, ils: { 760001: 7e6, 760017: 1e6 } },
      { mKey: '2026-10', status: 'current', eur: {}, ils: { 760001: 7.2e6, 760017: 0.8e6 } },
      { mKey: '2026-11', status: 'forecast', eur: {}, ils: { 760001: 7.2e6, 999999: 5e6 } },
    ] }],
    changes: { rows: [{ month: '2026-11', source: 'Open position', actual: true, eur: 0, ils: 100000 }, { month: '2026-11', source: 'Terminated', actual: false, eur: 0, ils: -50000 }] },
    overrides: { rows: [{ month: '2026-10', mode: 'Increment', eur: 0, ils: 20000 }, { month: '2026-10', mode: 'Override', eur: 0, ils: 9e6 }] },
  };
  fs.writeFileSync(cacheFile, JSON.stringify({ schemaVersion: sp.SCHEMA_VERSION, entry: { payload, generatedAtMs: NOW } }));
  const proj = await fxs.readSalaryProjection(cacheFile);
  check(proj && proj.asOf === '2026-10-20T08:00:00.000Z' && same(proj.months.map((m) => [m.month, m.ils]), [['2026-09', 8e6], ['2026-10', 8020000], ['2026-11', 7300000]]),
    'the Salary Projection\'s saved figures: Total payroll by month (accounts, increments and changes marked TRUE; not overrides, not other accounts)', JSON.stringify(proj && proj.months));
  fs.writeFileSync(cacheFile, JSON.stringify({ schemaVersion: sp.SCHEMA_VERSION - 1, entry: { payload } }));
  check((await fxs.readSalaryProjection(cacheFile)) === null && (await fxs.readSalaryProjection(path.join(tmp, 'none.json'))) === null, 'none saved, or from an older version → none');

  const asked = {};
  const reads = {
    rates: async () => series(400), books: async () => [], mail: null, flows: async () => [], conversions: async () => [],
    payroll: async (from) => { asked.payroll = from; return [{ id: 1, month: '2026-09', account: '200000', name: '200000 Accounts Payable - Employees', ils: 4e6 }]; },
    ilsVendors: async (from) => { asked.vendors = from; return [{ month: '2026-09', ils: 3e6 }]; },
    salary: async () => proj,
  };
  const d = await fxs.readFxChallenge({ reads, clock: () => NOW });
  check(d.payroll && d.payroll.journals.length === 1 && d.payroll.projection === proj && same(d.ilsVendors, [{ month: '2026-09', ils: 3e6 }])
    && asked.payroll === '2026-06' && asked.vendors === '2026-06' && !d.warnings.some((w) => /Salary Projection/.test(w)),
  'the payload carries the payroll journals (four months back), the Salary Projection and the ILS vendor payments');
  const none = await quietly(() => fxs.readFxChallenge({ reads: { ...reads, payroll: async () => { throw new Error('NetSuite down'); }, salary: async () => null, ilsVendors: async () => { throw new Error('down'); } }, clock: () => NOW }));
  check(none[0].payroll === null && none[0].ilsVendors === null && none[0].warnings.some((w) => /Neither the Salary Projection/.test(w)),
    'neither the payroll nor the Salary Projection → no payroll, said once');
  const vd = await fxs.readFxChallenge({ reads: { ...reads, eurVendors: async (from) => { asked.eurVendors = from; return [{ date: '2026-09-03', eur: 440954.95 }]; } }, clock: () => NOW });
  const vdDown = await quietly(() => fxs.readFxChallenge({ reads: { ...reads, eurVendors: async () => { throw new Error('down'); } }, clock: () => NOW }));
  check(same(vd.eurVendors, [{ date: '2026-09-03', eur: 440954.95 }]) && asked.eurVendors === '2026-06' && d.eurVendors === null && vdDown[0].eurVendors === null
    && vdDown[0].warnings.some((w) => /EUR vendor payments by day/.test(w)) && !d.warnings.some((w) => /EUR vendor payments by day/.test(w)),
  'the payload carries the EUR vendor payments by day (four months back); a failed read is said, and they are then spread over the month');
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
  let hapQuery = '';
  const reply = (items) => ({ ok: true, status: 200, json: async () => ({ items, hasMore: false }), text: async () => JSON.stringify({ items }) });
  const realFetch = global.fetch;
  global.fetch = async (url, opts) => {
    seen.push({ url: String(url), method: opts.method, auth: String(opts.headers.Authorization || '') });
    if (/restlet\.nl/.test(url)) return { ok: true, status: 200, text: async () => JSON.stringify({ ok: true, rows: [] }) };
    const q = JSON.parse(opts.body).q;
    if (/FROM currencyrate/.test(q)) return reply([{ d: '2026-10-19', rate: '3.482737' }, { d: '2026-10-20', rate: '3.47' }, { d: '2026-10-20', rate: '3.4778365' }, { d: '2026-10-21', rate: null }]);
    if (/'conversion'/.test(q) && /GROUP BY/.test(q)) return reply([{ m: '2026-09', k: 'CustPymt', eur: '4424436.19' }, { m: '2026-09', k: 'VendPymt', eur: '-846093.1' }, { m: '2026-09', k: 'conversion', eur: '-2646867.75' }, { m: '2026-09', k: 'Journal', eur: '-132903.93' }, { m: '2026-09', k: 'Transfer', eur: '-47842.06' }, { m: '2026-09', k: 'CardChrg', eur: '-1' }]);
    if (/'OthCurrLiab'/.test(q)) return reply([
      { id: 354114, m: '2026-09', num: '200000', name: '200000 Accounts Payable - Employees', ils: '-4066621.1' },
      { id: 354114, m: '2026-09', num: '180013', name: '180013 Withholdind tax - Payroll', ils: '-1741757.41' },
      { id: 5, m: '2026-09', num: '2500', name: '2500 Something debited', ils: '20' },
    ]);
    if (/AS hap/.test(q)) {
      hapQuery = q;
      return reply([{ d: '2026-10-04', eur: '-399130', hap: '0' }, { d: '2026-10-06', eur: '-156815.72', hap: '-156755.72' }, { d: '2026-10-07', eur: '12.5', hap: '0' }]);
    }
    if (/'VendPymt', 'Check'/.test(q)) {
      return /'Bank', 'CredCard'/.test(q) && /PreviousTransactionLineLink[\s\S]*accttype = 'Equity'/.test(q) ? reply([
        { m: '2026-09', name: '17005 6364 - AMEX - NIS', ils: '-2496282.81' }, { m: '2026-09', name: '17018 4491 - Isracard - NIS', ils: '-33666.12' },
        { m: '2026-09', name: '10002 Bank Poalim IL 583400', ils: '-2615679.48' }, { m: '2026-09', name: '17036 MASAV IL', ils: '-1842852.14' },
        { m: '2026-08', name: '17039 4409- AMEX - NIS', ils: '-1475947.66' }, { m: '2026-08', name: '17036 MASAV IL', ils: '-388880.74' },
      ]) : reply([]);
    }
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
    const owed = await ns.fetchPayrollLiabilities({ from: '2026-06' });
    const paid = await ns.fetchIlsVendorPayments({ from: '2026-06' });
    let badMonth = false;
    try { await ns.fetchPayrollLiabilities({ from: "2026-06' OR 1=1" }); } catch { badMonth = true; }
    check(same(owed.map((r) => [r.id, r.account, r.ils]), [[354114, '200000', 4066621.1], [354114, '180013', 1741757.41]])
      && same(paid, [{ month: '2026-08', ils: 1864828.4, cards: 1475947.66, other: 388880.74 }, { month: '2026-09', ils: 6988480.55, cards: 2529948.93, other: 4458531.62 }]) && badMonth,
    'payroll journals: what they owe (credits, positive; a debit left out); ILS vendor payments as paid, the credit cards apart from transfers and Masav (a credit card account that clears the batches), payments of equity (dividends) left out; the month is checked', JSON.stringify(paid));
    const eurDays = await ns.fetchEurVendorDays({ from: '2026-06' });
    let badAccount = false;
    try { await ns.fetchEurVendorDays({ from: '2026-06', hapoalimAccount: "11001' OR 1=1" }); } catch { badAccount = true; }
    check(same(eurDays, [{ date: '2026-10-04', eur: 399130, hapoalim: 0 }, { date: '2026-10-06', eur: 156815.72, hapoalim: 156755.72 }]) && badAccount && /acctnumber = '11001'/.test(hapQuery),
      'EUR vendor payments by day, paid (positive), with what Hapoalim EUR (11001) paid of them; a refund left out; the account number is checked', JSON.stringify(eurDays));
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
  await testDeals(fx);
  testEurVendors(fx);
  await testCash(fx);
  await testToday(fx);
  await testParse(fx);
  await testValidation(fx);
  await testHandler(fx);
  await testMail(fx);
  await testMailSettings();
  await testServerCash(fx);
  testServerParts();
  await testNetSuiteReads();
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
