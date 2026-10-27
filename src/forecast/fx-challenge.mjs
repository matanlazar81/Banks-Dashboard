// ─────────────────────────────────────────────────────────────────────────────
// FX Challenge: finance's monthly EUR→ILS conversion program at Bank Hapoalim (583400), and how much EUR to
// keep for it. Pure — no I/O — shared by the server (server/fx-challenge.cjs), its tests and the FX Challenge
// page (src/fx-challenge/).
//
// The table has one row per fixing month and one column per EUR/ILS band. On the fixing day (the 7th) that
// day's EUR/ILS picks the band, and the cell says how many euros are sold (€ thousands, negative = sold) and at
// what rate (the deals' blended rate). October 2026 checks out: EUR/ILS ≈3.43 → band 3.4–3.49 → €800K at 3.67,
// booked on 6 Oct as €200K @4.10 + €300K @3.56 + €300K @3.49 = ₪2,935,000.
//
// Bands are lower-inclusive: a rate equal to a bound falls in the band above it (the deals' documents decide the
// exact edge).
//
// A fixing's risk: EUR/ILS on the fixing day is lognormal around today's rate (no drift: today's rate is the
// expected one), with the annual volatility of NetSuite's daily rates. The band probabilities give the expected
// euros and the 95% amount (enough for 19 fixings in 20).
// ─────────────────────────────────────────────────────────────────────────────

export const DEFAULT_BOUNDS = [3.3, 3.4, 3.49, 3.5, 3.527, 3.56, 3.6, 3.7, 3.85, 3.915, 3.93, 4.1, 4.295];
const ROW_2026 = [[-500, 3.62], [-800, 3.67], [-800, 3.67], [-1100, 3.62], [-1100, 3.62], [-1100, 3.62], [-1400, 3.61],
  [-1600, 3.65], [-1800, 3.72], [-1800, 3.72], [-1800, 3.72], [-2000, 3.74], [-2200, 3.77], [-2400, 3.82]];
const ROW_2027 = [[-300, 3.49], [-850, 3.53], [-1050, 3.59], [-1350, 3.57], [-1550, 3.61], [-1550, 3.61], [-1850, 3.6],
  [-2050, 3.63], [-2050, 3.63], [-2250, 3.65], [-2450, 3.68], [-2650, 3.69], [-2650, 3.69], [-2650, 3.69]];
const pad2 = (n) => String(n).padStart(2, '0');
const cellsOf = (row) => row.map(([eurK, rate]) => ({ eurK, rate }));

/** The conversion table finance received in October 2026: fixings from October 2026 to December 2027. */
export const DEFAULT_TABLE = Object.freeze({
  pair: 'EUR/ILS',
  fixingDay: 7,
  bounds: DEFAULT_BOUNDS,
  rows: [
    ...['2026-10', '2026-11', '2026-12'].map((month) => ({ month, cells: cellsOf(ROW_2026) })),
    ...Array.from({ length: 12 }, (_, i) => ({ month: `2027-${pad2(i + 1)}`, cells: cellsOf(ROW_2027) })),
  ],
});

export function cloneTable(t) {
  return JSON.parse(JSON.stringify(t));
}

/** The volatility used when NetSuite's history is too short: about EUR/ILS's over the last two years. */
export const FALLBACK_SIGMA = 0.075;
/** The past conversions are listed from this month (before the table, what was booked only). */
export const HISTORY_FROM = '2026-01';
/** When the shekels go out, in the month after the payroll: the net salaries by the 5th; withholding tax, pension and
 * social security by the 14th. The credit cards' bills (the month before's card spending) are paid on the 10th. */
export const PAYROLL_DUE = Object.freeze({ net: 5, later: 14 });
export const CARD_BILL_DAY = 10;

// ── bands ──
export function bandLabels(bounds) {
  const out = [`<${bounds[0]}`];
  for (let i = 1; i < bounds.length; i++) out.push(`${bounds[i - 1]}–${bounds[i]}`);
  out.push(`>${bounds[bounds.length - 1]}`);
  return out;
}

/** The band a rate falls in: 0 below the first bound, bounds.length at or above the last. */
export function bandIndex(bounds, rate) {
  let i = 0;
  while (i < bounds.length && rate >= bounds[i]) i += 1;
  return i;
}

/** What a row converts at a rate: euros sold (positive), the cell's rate, the shekels received, and the shekels
 * gained against selling the same euros at that rate. */
export function conversionAt(row, bounds, rate) {
  const band = bandIndex(bounds, rate);
  const cell = row.cells[band];
  const eur = -cell.eurK * 1000;
  return { band, eur, rate: cell.rate, ils: eur * cell.rate, vsSpotIls: eur * (cell.rate - rate) };
}

// ── fixing dates ──
const DAY_MS = 86400000;
const WEEKDAY = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const isIso = (s) => /^\d{4}-\d{2}-\d{2}$/.test(String(s || ''));
const utc = (iso) => Date.UTC(+iso.slice(0, 4), +iso.slice(5, 7) - 1, +iso.slice(8, 10));
const nextMonth = (mKey) => {
  const [y, m] = mKey.split('-').map(Number);
  return m === 12 ? `${y + 1}-01` : `${y}-${pad2(m + 1)}`;
};

export function fixingDate(month, day) {
  const [y, m] = month.split('-').map(Number);
  const last = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return `${y}-${pad2(m)}-${pad2(Math.min(day, last))}`;
}

// ── business days: Sunday to Thursday in Israel (holidays are not known) ──
const offDay = (t) => { const dow = new Date(t).getUTCDay(); return dow === 5 || dow === 6; };

/** The last business day on or before a date: a payment due on a Friday or a Saturday has to be ready on the Thursday. */
export function businessDayOnOrBefore(iso) {
  let t = utc(iso);
  while (offDay(t)) t -= DAY_MS;
  return new Date(t).toISOString().slice(0, 10);
}

/** Business days after `from`, up to and including `to` (0 when `to` is not after `from`). */
export function businessDaysBetween(from, to) {
  let n = 0;
  for (let t = utc(from) + DAY_MS; t <= utc(to); t += DAY_MS) if (!offDay(t)) n += 1;
  return n;
}

/** Calendar days from `from` to `to`. */
export const daysUntil = (from, to) => Math.round((utc(to) - utc(from)) / DAY_MS);

/** Every row's fixing: date, weekday (a weekend fixing may move to a business day), days from today, and
 * whether it is past, the next one, or later. */
export function fixings(table, today) {
  const t = utc(today);
  let nextSeen = false;
  return [...table.rows].sort((a, b) => a.month.localeCompare(b.month)).map((row) => {
    const date = fixingDate(row.month, table.fixingDay);
    const d = utc(date);
    const dow = new Date(d).getUTCDay();
    let status = 'future';
    if (d < t) status = 'past';
    else if (!nextSeen) { status = 'next'; nextSeen = true; }
    return { month: row.month, date, weekday: WEEKDAY[dow], weekend: dow === 0 || dow === 6, status, days: Math.max(0, Math.round((d - t) / DAY_MS)), row };
  });
}

// ── rates ──
/** One rate per business day, oldest first (the last one kept when a day repeats). */
function businessDays(series) {
  const byDate = new Map();
  for (const p of series || []) {
    if (!p || !isIso(p.date) || !(Number(p.rate) > 0)) continue;
    const dow = new Date(utc(p.date)).getUTCDay();
    if (dow !== 0 && dow !== 6) byDate.set(p.date, Number(p.rate));
  }
  return [...byDate.entries()].sort((a, b) => a[0].localeCompare(b[0])).map(([date, rate]) => ({ date, rate }));
}

/** Annual volatility from daily log returns (business days × √252); null with fewer than 20 returns. */
export function annualVol(series, { minReturns = 20 } = {}) {
  const pts = businessDays(series);
  const rets = [];
  for (let i = 1; i < pts.length; i++) rets.push(Math.log(pts[i].rate / pts[i - 1].rate));
  if (rets.length < minReturns) return null;
  const mean = rets.reduce((s, r) => s + r, 0) / rets.length;
  const variance = rets.reduce((s, r) => s + (r - mean) ** 2, 0) / (rets.length - 1);
  return Math.sqrt(variance * 252);
}

/** The latest rate on or before a date (null before the series starts). */
export function rateOn(series, date) {
  let best = null;
  for (const p of series || []) {
    if (p && isIso(p.date) && p.date <= date && Number(p.rate) > 0 && (!best || p.date >= best.date)) best = p;
  }
  return best ? { date: best.date, rate: Number(best.rate) } : null;
}

/**
 * Today's rate against the `days` (calendar) days before it, for selling euros: the share of those business days with
 * a lower rate, their average and range, and a verdict: 'high' when the rate beats 3 days in 4 or more, 'low' when it
 * beats 1 in 4 or fewer, else 'middle'. It only looks back; where EUR/ILS goes next is not known. null with fewer
 * than `minDays` days.
 */
export function rateCheck(series, today, { days = 90, minDays = 20 } = {}) {
  const spot = rateOn(series, today);
  if (!spot) return null;
  const from = new Date(utc(spot.date) - days * DAY_MS).toISOString().slice(0, 10);
  const rates = businessDays(series).filter((p) => p.date >= from && p.date < spot.date).map((p) => p.rate);
  if (rates.length < minDays) return null;
  const share = rates.filter((r) => r < spot.rate).length / rates.length;
  return {
    date: spot.date, rate: spot.rate, days, n: rates.length,
    average: rates.reduce((s, r) => s + r, 0) / rates.length, low: Math.min(...rates), high: Math.max(...rates),
    share, level: share >= 0.75 ? 'high' : share <= 0.25 ? 'low' : 'middle',
  };
}

// Standard normal CDF (Abramowitz & Stegun 7.1.26, error below 1.5e-7).
function normCdf(x) {
  const z = Math.abs(x) / Math.SQRT2;
  const t = 1 / (1 + 0.3275911 * z);
  const poly = ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t;
  const erf = 1 - poly * Math.exp(-z * z);
  return x >= 0 ? (1 + erf) / 2 : (1 - erf) / 2;
}

/** The chance of each band on a fixing `years` away. */
export function bandProbabilities(bounds, spot, sigma, years) {
  const p = new Array(bounds.length + 1).fill(0);
  if (!(spot > 0)) return p;
  if (!(sigma > 0) || !(years > 0)) { p[bandIndex(bounds, spot)] = 1; return p; }
  const s = sigma * Math.sqrt(years);
  let prev = 0;
  for (let i = 0; i < bounds.length; i++) {
    const below = normCdf((Math.log(bounds[i] / spot) + (s * s) / 2) / s);
    p[i] = Math.max(0, below - prev);
    prev = below;
  }
  p[bounds.length] = Math.max(0, 1 - prev);
  return p;
}

/** A fixing's euros: expected (and its shekels), the amount that covers `quantile` of the outcomes, and the
 * row's worst. */
export function fixingRisk(row, bounds, spot, sigma, years, quantile = 0.95) {
  const probs = bandProbabilities(bounds, spot, sigma, years);
  const amounts = row.cells.map((c) => -c.eurK * 1000);
  let expectedEur = 0;
  let expectedIls = 0;
  probs.forEach((p, i) => { expectedEur += p * amounts[i]; expectedIls += p * amounts[i] * row.cells[i].rate; });
  const byAmount = amounts.map((a, i) => ({ a, p: probs[i] })).sort((x, y) => x.a - y.a);
  let cum = 0;
  let quantileEur = byAmount[byAmount.length - 1].a;
  for (const o of byAmount) {
    cum += o.p;
    if (cum >= quantile - 1e-9) { quantileEur = o.a; break; }
  }
  return { probs, expectedEur, expectedIls, p95Eur: quantileEur, worstEur: Math.max(...amounts) };
}

/**
 * A conversion's outcomes by EUR/ILS range: neighbouring bands that sell the same euros at the same rate are merged,
 * with their chances added (probs from fixingRisk). lo and hi are the range's edges, null where it is open-ended; a
 * rate on an edge belongs to the range above it.
 */
export function outcomeRanges(row, bounds, probs) {
  const out = [];
  row.cells.forEach((c, i) => {
    const eur = -c.eurK * 1000;
    const hi = i < bounds.length ? bounds[i] : null;
    const last = out[out.length - 1];
    if (last && last.eur === eur && last.rate === c.rate) {
      last.hi = hi;
      last.p += probs[i] || 0;
      last.bands.push(i);
      return;
    }
    out.push({ lo: i ? bounds[i - 1] : null, hi, eur, rate: c.rate, ils: eur * c.rate, p: probs[i] || 0, bands: [i] });
  });
  return out;
}

// ── what to keep, what is free ──
export const POLICIES = ['today', 'p95', 'worst'];
export const POLICY_LABEL = { today: "Today's band", p95: '95% of rate moves', worst: "The table's worst case" };

/** The euros to keep for a fixing under a policy. The 95% amount is never below what today's rate would sell. */
export function requirementOf(policy, conv, risk) {
  const today = Math.max(0, conv.eur);
  if (policy === 'worst') return Math.max(today, risk.worstEur);
  if (policy === 'today') return today;
  return Math.max(today, risk.p95Eur);
}

/** LSports' euros by where they sit, from balance rows (the mail's or the books'). */
export function eurPosition(rows) {
  const eur = (rows || []).filter((r) => r && r.currency === 'EUR' && (!r.company || /lsports/i.test(r.company)));
  const isHapoalim = (r) => /hapoalim|poalim/i.test(String(r.bank || ''));
  const sum = (xs) => xs.reduce((s, r) => s + (Number(r.amount) || 0), 0);
  return {
    hapoalim: Math.max(0, sum(eur.filter((r) => r.kind === 'current' && isHapoalim(r)))),
    other: Math.max(0, sum(eur.filter((r) => r.kind === 'current' && !isHapoalim(r)))),
    deposits: eur.filter((r) => r.kind === 'deposit' && Number(r.amount) > 0).map((r) => ({
      amount: Number(r.amount), valueDate: isIso(r.valueDate) ? r.valueDate : null, blocked: r.blocked === true, label: r.label || r.bank || 'Deposit',
    })),
  };
}

/** Net EUR coming in each month before conversions, deposits and transfers (customers less payments), averaged
 * over `months`. flows: [{ month, kind, eur }] with kind customer | vendor | conversion | deposit | transfer |
 * refund | other. Transfers are internal: between EUR accounts they net to nothing, to another currency they are
 * conversions. */
const INTERNAL = new Set(['conversion', 'deposit', 'transfer']);
export function typicalInflow(flows, months) {
  const per = months.map((m) => ({
    month: m,
    net: (flows || []).filter((f) => f.month === m && !INTERNAL.has(f.kind)).reduce((s, f) => s + (Number(f.eur) || 0), 0),
  }));
  if (!per.length) return null;
  const perMonth = per.reduce((s, p) => s + p.net, 0) / per.length;
  return { perMonth, perDay: perMonth / 30.4, months: per };
}

// ── shekels for the payroll and the vendors ──
const EMPLOYEES = /employee/i;

/**
 * Each month's payroll journal, as owed: the employees' net salaries (paid by the 5th of the next month) and the rest
 * (withholding tax, pension, social security: by the 14th). rows: the journals' credits to payables and current
 * accounts, [{ id, month, account, name, ils }] (fetchPayrollLiabilities). A month's payroll journal is the one owing
 * the employees the most, so accrual journals in the same month don't count. Returns [{ month, net, later, total }].
 */
export function payrollSplit(rows) {
  const byJournal = new Map();
  for (const r of rows || []) {
    if (!r || !/^\d{4}-\d{2}$/.test(String(r.month)) || !(Number(r.ils) > 0)) continue;
    const j = byJournal.get(r.id) || { month: r.month, net: 0, later: 0 };
    if (EMPLOYEES.test(String(r.name || ''))) j.net += Number(r.ils);
    else j.later += Number(r.ils);
    byJournal.set(r.id, j);
  }
  const byMonth = new Map();
  for (const j of byJournal.values()) {
    const best = byMonth.get(j.month);
    if (j.net > 0 && (!best || j.net > best.net)) byMonth.set(j.month, j);
  }
  return [...byMonth.values()].sort((a, b) => a.month.localeCompare(b.month)).map((j) => ({ ...j, total: j.net + j.later }));
}

/**
 * The payroll to pay, by payroll month from `from`, `count` months: the Salary Projection's Total payroll (₪) when it
 * has the month, else the last payroll journal's total; split as the payroll journals of `months` (the net salaries'
 * share), or all by the 5th without them. Returns [{ month, total, net, later, source: 'projection' | 'journal' }].
 */
export function payrollToPay({ projection, split, months, from, count }) {
  const inMonths = (split || []).filter((s) => (months || []).includes(s.month));
  const totals = inMonths.reduce((a, s) => ({ net: a.net + s.net, total: a.total + s.total }), { net: 0, total: 0 });
  const share = totals.total > 0 ? totals.net / totals.total : null;
  const last = (split || [])[(split || []).length - 1] || null;
  const proj = new Map(((projection && projection.months) || []).map((m) => [m.month, Number(m.ils) || 0]));
  const out = [];
  for (let i = 0, m = from; i < count; i++, m = nextMonth(m)) {
    const total = proj.has(m) ? proj.get(m) : last ? last.total : null;
    if (total === null) continue;
    const net = share === null ? total : total * share;
    out.push({ month: m, total, net, later: total - net, source: proj.has(m) ? 'projection' : 'journal' });
  }
  return { share, rows: out };
}

/**
 * Average monthly vendor payments over `months`: EUR from the flows (kind vendor); ILS from the ILS accounts', split
 * into the credit cards' (billed on the 10th of the next month) and the rest (bank transfers and Masav batches). A row
 * without the split (an older payload) counts as the rest. ils: both; null when they were not read.
 */
export function vendorAverages(flows, ilsVendors, months) {
  const n = (months || []).length;
  if (!n) return { eur: 0, ils: null, cards: null, other: null };
  const eur = (flows || []).filter((f) => f.kind === 'vendor' && months.includes(f.month)).reduce((s, f) => s - (Number(f.eur) || 0), 0) / n;
  if (!Array.isArray(ilsVendors)) return { eur: Math.max(0, eur), ils: null, cards: null, other: null };
  const rows = ilsVendors.filter((v) => months.includes(v.month));
  const avg = (of) => Math.max(0, rows.reduce((s, v) => s + of(v), 0) / n);
  const cards = avg(cardsOf);
  const other = avg(otherOf);
  return { eur: Math.max(0, eur), ils: cards + other, cards, other };
}
const cardsOf = (v) => (v.cards !== undefined ? Number(v.cards) || 0 : 0);
const otherOf = (v) => (v.other !== undefined ? Number(v.other) || 0 : Number(v.ils) || 0);

/**
 * The credit cards' bills from `from` (YYYY-MM, the month paid in) to `until`: on the 10th, the month before's card
 * spending. A month whose spending is over is billed what was spent; the current month at least the average (it is
 * still going), and later months the average. ilsVendors: [{ month, cards }]. Returns [{ date, ils, spendMonth }].
 */
export function cardBills({ ilsVendors, average, today, until }) {
  const spent = new Map((ilsVendors || []).map((v) => [v.month, cardsOf(v)]));
  const thisMonth = today.slice(0, 7);
  const out = [];
  for (let m = thisMonth; fixingDate(m, CARD_BILL_DAY) <= until; m = nextMonth(m)) {
    const date = fixingDate(m, CARD_BILL_DAY);
    if (date < today) continue;
    const spendMonth = prevMonth(m);
    const ils = spendMonth < thisMonth && spent.has(spendMonth) ? spent.get(spendMonth)
      : spendMonth === thisMonth ? Math.max(average || 0, spent.get(spendMonth) || 0) : average || 0;
    if (ils > 0.5) out.push({ date, ils, spendMonth });
  }
  return out;
}

/**
 * LSports' shekels: the current accounts (an overdraft counts) and the ILS deposits, which finance releases on the day a
 * payment goes out (a blocked deposit is left out). null when the balances have no shekels (NetSuite's books).
 */
export function ilsOnHand(rows) {
  const ils = (rows || []).filter((r) => r && r.currency === 'ILS' && (!r.company || /lsports/i.test(r.company)));
  const current = ils.filter((r) => r.kind === 'current');
  const deposits = ils.filter((r) => r.kind === 'deposit' && !r.blocked && Number(r.amount) > 0);
  if (!current.length && !deposits.length) return null;
  const sum = (xs) => xs.reduce((s, r) => s + (Number(r.amount) || 0), 0);
  return { current: sum(current), deposits: sum(deposits), total: sum(current) + sum(deposits) };
}

const PAY_ORDER = { net: 0, later: 1, cards: 2, vendors: 3 };

/**
 * The shekels the payroll and the vendors need from today to `until`, and the euros to convert for them at today's
 * rate. From the shekels on hand (an overdraft counts against them), as events:
 *   today      the rest of this month's ILS vendor payments (bank transfers and Masav batches, at the month's average);
 *   the 5th    the previous month's net salaries and (after this month) the month's ILS vendor payments;
 *   the 10th   the credit cards' bills (cardBills);
 *   the 14th   pension, withholding tax and social security;
 *   the 7th    the table's conversion brings its shekels (conversions).
 * Whenever the shekels on hand fall short, the gap is converted by that day.
 *   payroll    [{ month (payroll month), net, later }]; conversions: the table's [{ date, ils }]
 *   vendorsIls the ILS vendor payments a month, not the cards; cardBills: [{ date, ils, spendMonth }]
 * Returns { needs: [{ date, ils, eur, why: [...], month }], dues, months: [{ month (paid in), payrollMonth, net, later,
 * cards, vendors, tableIls, convertIls, convertEur }], ilsNow, ilsAfter }. dues: each day something is paid, with what
 * (items), when it has to be ready (by: the business day on or before), the shekels on hand just before, what to
 * convert for it (convertIls, eur; 0 when covered) and the table's conversion since the payment day before (after).
 */
export function shekelNeeds({ today, ilsNow, payroll, conversions, vendorsIls, cardBills: bills = [], spot, until }) {
  const thisMonth = today.slice(0, 7);
  const dim = new Date(Date.UTC(+today.slice(0, 4), +today.slice(5, 7), 0)).getUTCDate();
  const vendors = Math.max(0, Number(vendorsIls) || 0);
  const events = [];
  const pay = (e) => { if (e.ils < -0.5 && e.date >= today && e.date <= until) events.push(e); };
  pay({ date: today, ils: -(vendors * (dim - Number(today.slice(8, 10)))) / dim, why: 'vendors', month: thisMonth, rest: true });
  for (const p of payroll || []) {
    const paid = nextMonth(p.month);
    const dNet = fixingDate(paid, PAYROLL_DUE.net);
    pay({ date: dNet, ils: -p.net, why: 'net', month: paid, payrollMonth: p.month });
    if (paid !== thisMonth) pay({ date: dNet, ils: -vendors, why: 'vendors', month: paid });
    pay({ date: fixingDate(paid, PAYROLL_DUE.later), ils: -p.later, why: 'later', month: paid, payrollMonth: p.month });
  }
  for (const b of bills || []) pay({ date: b.date, ils: -b.ils, why: 'cards', month: b.date.slice(0, 7), spendMonth: b.spendMonth });
  for (const c of conversions || []) {
    if (c.date >= today && c.date <= until) events.push({ date: c.date, ils: c.ils, why: 'conversion', month: c.date.slice(0, 7) });
  }
  // By day; on the same day, payments before the shekels coming in.
  events.sort((a, b) => a.date.localeCompare(b.date) || a.ils - b.ils);
  let cash = Number(ilsNow) || 0;
  const byDate = new Map();
  const dues = new Map();
  let since = null;
  for (const e of events) {
    if (e.why === 'conversion') since = e.date;
    else {
      let d = dues.get(e.date);
      if (!d) { d = { date: e.date, month: e.month, ils: 0, before: cash, items: [], after: since }; dues.set(e.date, d); since = null; }
      d.ils -= e.ils;
      d.items.push({ why: e.why, month: e.month, payrollMonth: e.payrollMonth || null, spendMonth: e.spendMonth || null, rest: !!e.rest, ils: -e.ils });
    }
    cash += e.ils;
    if (cash < -0.5) {
      const n = byDate.get(e.date) || { date: e.date, ils: 0, why: [], month: e.month };
      n.ils -= cash;
      if (!n.why.includes(e.why)) n.why.push(e.why);
      byDate.set(e.date, n);
      cash = 0;
    }
  }
  const needs = [...byDate.values()].map((n) => ({ ...n, eur: spot > 0 ? n.ils / spot : 0 }));
  const needOn = new Map(needs.map((n) => [n.date, n]));
  const dueList = [...dues.values()].map((d) => {
    const n = needOn.get(d.date);
    return {
      ...d, items: d.items.sort((a, b) => PAY_ORDER[a.why] - PAY_ORDER[b.why]), by: businessDayOnOrBefore(d.date),
      convertIls: n ? n.ils : 0, eur: n ? n.eur : 0,
    };
  });
  const paidMonths = [...new Set(events.filter((e) => e.why !== 'conversion').map((e) => e.month))].sort();
  const months = paidMonths.map((m) => {
    const of = (why) => events.filter((e) => e.month === m && e.why === why).reduce((s, e) => s - e.ils, 0);
    const conv = needs.filter((n) => n.month === m);
    const pr = (payroll || []).find((p) => nextMonth(p.month) === m);
    return {
      month: m, payrollMonth: pr ? pr.month : null, net: of('net'), later: of('later'), cards: of('cards'), vendors: of('vendors'), tableIls: -of('conversion'),
      convertIls: conv.reduce((s, n) => s + n.ils, 0), convertEur: conv.reduce((s, n) => s + n.eur, 0),
    };
  });
  return { needs, dues: dueList, months, ilsNow: ilsNow == null ? null : Number(ilsNow), ilsAfter: cash };
}

/** The payments in shekels up to the first month with salaries (its 5th, 10th and 14th, and any before), and the euros
 * to convert for them in all. */
export function duesAhead(cash) {
  const first = cash ? cash.months.find((m) => m.net > 0 || m.later > 0) : null;
  const dues = first ? cash.dues.filter((d) => d.month <= first.month) : [];
  return { dues, eur: dues.reduce((s, d) => s + d.eur, 0) };
}

const MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const monthNameOf = (mKey) => MONTH_NAMES[Number(String(mKey).slice(5, 7)) - 1] || mKey;
const ITEM_TEXT = {
  net: (i) => `${monthNameOf(i.payrollMonth)}'s net salaries`,
  later: (i) => `${monthNameOf(i.payrollMonth)}'s pension, tax and social security`,
  cards: (i) => `the credit card bills for ${monthNameOf(i.spendMonth)}`,
  vendors: (i) => `${i.rest ? 'the rest of ' : ''}${monthNameOf(i.month)}'s ILS vendor payments`,
};

/** What a payment day pays, in words: "October's net salaries and November's ILS vendor payments". */
export function dueWhat(due) {
  const parts = (due.items || []).map((i) => (ITEM_TEXT[i.why] ? ITEM_TEXT[i.why](i) : i.why));
  return parts.length > 1 ? `${parts.slice(0, -1).join(', ')} and ${parts[parts.length - 1]}` : parts[0] || '';
}

const isoAddDays = (iso, n) => new Date(utc(iso) + n * DAY_MS).toISOString().slice(0, 10);
/** A new deposit for a conversion's gap pays out at least this many days from today; a nearer conversion is left to
 * the cash (the keep line). */
const LADDER_MIN_DAYS = 14;

const addMonthsIso = (iso, n) => {
  const [y, m, d] = iso.split('-').map(Number);
  const last = new Date(Date.UTC(y, m - 1 + n + 1, 0)).getUTCDate();
  const t = new Date(Date.UTC(y, m - 1 + n, Math.min(d, last)));
  return t.toISOString().slice(0, 10);
};

/**
 * What to keep for the next fixings and what is free to convert.
 *   upcoming   the next fixings, each { month, date, days, conv (at today's rate), risk }
 *   eur        { hapoalim, other, deposits: [{ amount, valueDate, blocked }] } (eurPosition)
 *   inflowPerDay  typical net EUR per day, or null when unknown
 *   shekels    the euros to convert for the payroll and the vendors, [{ date, eur }] (shekelNeeds)
 *   eurFloor   the euros kept for the EUR vendors at all times
 * Deposits maturing before a fixing count for it; later ones are the backup (broken early only if needed).
 */
export function planReserve({ upcoming, policy = 'p95', eur, inflowPerDay = null, today, horizon = 6, shekels = [], eurFloor = 0 }) {
  const fx = (upcoming || []).slice(0, horizon);
  if (!fx.length || !eur) return null;
  const hapoalim = Math.max(0, eur.hapoalim || 0);
  const other = Math.max(0, eur.other || 0);
  const liquid = hapoalim + other;
  const keep = Math.max(0, Number(eurFloor) || 0);
  const forIls = (shekels || []).filter((s) => s && s.eur > 0 && s.date >= today);
  const shekelsBy = (date) => forIls.filter((s) => s.date <= date).reduce((s, x) => s + x.eur, 0);
  const usable = (eur.deposits || []).filter((d) => d.amount > 0 && !d.blocked);
  const maturingBy = (date) => usable.filter((d) => d.valueDate && d.valueDate > today && d.valueDate <= date).reduce((s, d) => s + d.amount, 0);
  const daysTo = (date) => Math.round((utc(date) - utc(today)) / DAY_MS);
  const inflowTo = (f) => (inflowPerDay == null ? 0 : inflowPerDay * Math.max(0, f.days - 1));

  const f0 = fx[0];
  const required = requirementOf(policy, f0.conv, f0.risk);
  const todayAmount = Math.max(0, f0.conv.eur);
  const maturing = maturingBy(f0.date);
  const backup = usable.filter((d) => !d.valueDate || d.valueDate > f0.date).reduce((s, d) => s + d.amount, 0);
  const needLiquid = Math.max(0, required - maturing);
  // With the backup deposits covering the tail, the liquid euros only have to cover today's band.
  const needLiquidWithBackup = Math.max(0, todayAmount - maturing, required - maturing - backup);
  // Today's euros also carry the payroll's and the vendors' conversions due by the next fixing, and the EUR floor.
  const shekelsBefore = shekelsBy(f0.date);
  const inflowUntil = inflowPerDay == null ? null : inflowTo(f0);

  // The deposits that mature after `from` and by `to`, earliest first: what each fixing gains from them.
  const releasedBetween = (from, to) => usable
    .filter((d) => d.valueDate && d.valueDate > from && (to === null || d.valueDate <= to))
    .sort((x, y) => x.valueDate.localeCompare(y.valueDate) || y.amount - x.amount)
    .map((d) => ({ amount: d.amount, valueDate: d.valueDate, label: d.label }));

  // Euros at each fixing, if nothing else is converted: today's liquid euros, plus typical inflows and deposits
  // maturing by then, less the earlier fixings at the policy amount and the payroll's and vendors' conversions due by
  // then. What is left after the fixing and the EUR floor is its spare. Euros converted before a fixing are gone at it
  // and at every later one, so what can be converted (or deposited) before a fixing, in total, is the smallest spare
  // from that fixing on.
  let before = 0;
  let from = today;
  const path = fx.map((f) => {
    const req = requirementOf(policy, f.conv, f.risk);
    // Its parts, kept so the page can show the sum: inflow up to the day before, deposits matured by the day,
    // what the earlier conversions take, and the conversions for shekels by the day (this row's since the last).
    const inflow = inflowTo(f);
    const depositsBy = maturingBy(f.date);
    const earlier = before;
    const forShekels = shekelsBy(f.date);
    const shekelsHere = forShekels - shekelsBy(from);
    const available = liquid + inflow + depositsBy - earlier - forShekels;
    const releasedDeposits = releasedBetween(from, f.date);
    before += req;
    from = f.date;
    return {
      month: f.month, date: f.date, required: req, expected: f.risk.expectedEur,
      released: releasedDeposits.reduce((s, d) => s + d.amount, 0), releasedDeposits,
      inflow, inflowDays: inflowPerDay == null ? 0 : Math.max(0, f.days - 1), depositsBy, earlier, forShekels, shekels: shekelsHere,
      available, surplus: available - req - keep, convertibleBefore: 0, limitedBy: f.month,
    };
  });
  // limitedBy: the conversion whose spare sets the figure (the row itself unless a later one is tighter).
  let floor = Infinity;
  let floorMonth = null;
  for (let i = path.length - 1; i >= 0; i--) {
    if (path[i].surplus <= floor) { floor = path[i].surplus; floorMonth = path[i].month; }
    path[i].convertibleBefore = Math.max(0, floor);
    path[i].limitedBy = floorMonth;
  }
  const shown = inflowPerDay == null ? path.slice(0, 1) : path;

  const next = {
    month: f0.month, date: f0.date, required, todayAmount, hapoalim, other, liquid, maturing, backup,
    needLiquid, needLiquidWithBackup, shekelsBefore, eurFloor: keep,
    // On today's balances only: what is held now, less what the reserve still needs from it.
    freeNow: Math.max(0, liquid - needLiquid),
    freeNowWithBackup: Math.max(0, liquid - needLiquidWithBackup),
    shortfall: Math.max(0, needLiquid - liquid),
    inflowUntil,
    // With typical inflows: what can be converted before the fixing, in total, as the euros come in.
    freeByFixing: inflowPerDay == null ? null : path[0].convertibleBefore,
    // The euros have to be in the Hapoalim account on the day.
    moveToHapoalim: Math.min(other, Math.max(0, required - maturing - hapoalim)),
  };

  // Every due from today, in order: each fixing at the amount kept, each conversion for shekels (before a fixing on
  // the same day). At each, the euros held then (today's, the typical inflow up to the day before and the deposits
  // maturing by then, less what fell due before) less what is due and the EUR floor is that day's spare.
  const dues = [
    ...forIls.map((x) => ({ date: x.date, eur: x.eur, shekels: true })),
    ...fx.map((f) => ({ date: f.date, eur: requirementOf(policy, f.conv, f.risk), shekels: false })),
  ].sort((a, b) => a.date.localeCompare(b.date) || Number(b.shekels) - Number(a.shekels));
  let due = 0;
  const checkpoints = dues.map((d) => {
    const held = liquid + (inflowPerDay == null ? 0 : inflowPerDay * Math.max(0, daysTo(d.date) - 1)) + maturingBy(d.date) - due;
    due += d.eur;
    return { date: d.date, shekels: d.shekels, spare: held - d.eur - keep };
  });
  // Euros converted now are gone at every later due; euros in a deposit come back when it matures. So what can leave
  // the accounts for good is the smallest spare of all, and what can go into a deposit maturing on a day is the
  // smallest spare before that day; neither more than today's euros above the floor. Deposits round down to €10K.
  const cap = liquid - keep;
  const lowest = checkpoints.reduce((lo, c) => (!lo || c.spare < lo.spare ? c : lo), null);
  next.spare = Math.max(0, Math.min(cap, lowest ? lowest.spare : cap));
  next.tightest = lowest ? { date: lowest.date, spare: lowest.spare, shekels: lowest.shekels } : null;
  // Short: the largest gap, and the first day one opens (the urgent one).
  const firstGap = checkpoints.find((c) => c.spare < 0);
  next.short = lowest && lowest.spare < 0
    ? { eur: -lowest.spare, date: lowest.date, shekels: lowest.shekels, first: { eur: -firstGap.spare, date: firstGap.date, shekels: firstGap.shekels } }
    : null;
  const deposits = [1, 3, 6].map((months) => {
    const until = addMonthsIso(today, months);
    const most = Math.min(cap, ...checkpoints.filter((c) => c.date < until).map((c) => c.spare));
    return { months, until, eur: Math.max(0, Math.floor(most / 10000) * 10000) };
  });

  // First in, first out: each deposit counts for the first conversion after it pays out, and what a conversion doesn't
  // take rolls on to the next. What the deposits don't cover is the conversion's gap: euros on hand or coming in fill
  // it, or a new deposit timed for it (until: the business day before the euros have to be in Hapoalim EUR).
  // The next conversion's gap is the cash to keep (the keep line), so a deposit is timed for the later ones only.
  let carried = 0;
  let prior = today;
  const coverage = fx.map((f, i) => {
    const req = requirementOf(policy, f.conv, f.risk);
    const releasedDeposits = releasedBetween(prior, f.date);
    const released = releasedDeposits.reduce((s, d) => s + d.amount, 0);
    const have = carried + released;
    const until = businessDayOnOrBefore(isoAddDays(businessDayOnOrBefore(f.date), -1));
    const row = { month: f.month, date: f.date, required: req, released, releasedDeposits, carried, have, gap: Math.max(0, req - have), until, timed: i > 0 && daysTo(until) >= LADDER_MIN_DAYS };
    carried = Math.max(0, have - req);
    prior = f.date;
    return row;
  });
  // The new deposits for the gaps, earliest first: each holds its gap (rounded up to €10K) or as much as the dues
  // before it pays out leave room for (rounded down), never more than today's euros above the floor. A deposit locks
  // its euros away from every due before it pays out. A conversion closer than LADDER_MIN_DAYS is left to the cash.
  const ladder = [];
  const lockedAt = (date) => ladder.filter((l) => l.until > date).reduce((s, l) => s + l.eur, 0);
  let laddered = 0;
  for (const c of coverage) {
    if (c.gap < 0.5 || !c.timed) continue;
    const room = Math.min(cap - laddered, ...checkpoints.filter((x) => x.date < c.until).map((x) => x.spare - lockedAt(x.date)));
    const eur = Math.min(Math.ceil(c.gap / 10000) * 10000, Math.floor(Math.max(0, room) / 10000) * 10000);
    if (eur < 10000) break;
    ladder.push({ month: c.month, date: c.date, until: c.until, eur, gap: c.gap, required: c.required, have: c.have });
    laddered += eur;
  }
  // What can still leave for good once the ladder is deposited.
  next.spareAfterLadder = laddered > 0
    ? Math.max(0, Math.min(cap - laddered, ...checkpoints.map((x) => x.spare - lockedAt(x.date))))
    : next.spare;

  // Deposits maturing after the last fixing shown: not needed for any of them.
  return { policy, next, path: shown, later: releasedBetween(shown[shown.length - 1].date, null), deposits, coverage, ladder };
}

/**
 * The recommendation at today's rate from GET /api/fx-challenge's answer: the conversions left (each at today's rate,
 * with its risk and what it can sell by range), LSports' euros, the typical inflow and the plan. The page adds the
 * simulated rate on top of it (src/fx-challenge/fxModel.ts); the daily mail sends it (scripts/fx-challenge-mail.cjs).
 */
export function planFromPayload(data, policy = 'p95') {
  const { table, today } = data;
  const bounds = table.bounds;
  const spot = data.rates.spot ? data.rates.spot.rate : null;
  const sigmaFromHistory = !!(data.rates.sigma && data.rates.sigma > 0);
  const sigma = sigmaFromHistory ? data.rates.sigma : FALLBACK_SIGMA;
  const all = fixings(table, today);
  const schedule = all.filter((f) => f.status !== 'past').map((f) => {
    const risk = spot ? fixingRisk(f.row, bounds, spot, sigma, f.days / 365) : null;
    return { fixing: f, atSpot: spot ? conversionAt(f.row, bounds, spot) : null, risk, outcomes: risk ? outcomeRanges(f.row, bounds, risk.probs) : null };
  });
  const position = data.balances ? eurPosition(data.balances.rows) : null;
  const inflow = typicalInflow(data.flows, data.inflowMonths);
  const upcoming = schedule.flatMap((s) => (s.atSpot && s.risk
    ? [{ month: s.fixing.month, date: s.fixing.date, days: s.fixing.days, conv: s.atSpot, risk: s.risk }] : []));
  const cash = cashPlanOf(data, today, spot, upcoming, schedule, policy);
  const plan = position && upcoming.length
    ? planReserve({
      upcoming, policy, eur: position, inflowPerDay: inflow ? inflow.perDay : null, today,
      shekels: cash ? cash.needs : [], eurFloor: cash ? cash.vendors.eur : 0,
    })
    : null;
  return {
    all, spot, spotDate: data.rates.spot ? data.rates.spot.date : null, spotBand: spot ? bandIndex(bounds, spot) : null,
    sigma, sigmaFromHistory, rateCheck: rateCheck(data.rates.series, today), schedule, position, inflow, cash, plan,
  };
}

const prevMonth = (mKey) => {
  const [y, m] = mKey.split('-').map(Number);
  return m === 1 ? `${y - 1}-12` : `${y}-${pad2(m - 1)}`;
};
const monthEnd = (mKey) => fixingDate(mKey, 31);

/**
 * The shekels side of the plan, from the payload's payroll (the Salary Projection's Total payroll and NetSuite's
 * payroll journals), the ILS vendor payments and the shekels on hand, to the end of the sixth conversion's month.
 * null without a payroll figure (then the plan leaves salaries and vendors out).
 */
/** The shekels a fixing brings when it sells the amount kept (the policy's): that amount at its cell's rate (the lowest
 * rate among the cells selling it). */
function ilsKept(policy, item) {
  const { atSpot: conv, risk, outcomes } = item;
  const eur = requirementOf(policy, conv, risk);
  if (Math.abs(eur - Math.max(0, conv.eur)) < 0.5) return Math.max(0, conv.ils);
  const cells = (outcomes || []).filter((o) => Math.abs(o.eur - eur) < 0.5);
  return eur * (cells.length ? Math.min(...cells.map((o) => o.rate)) : conv.rate);
}

function cashPlanOf(data, today, spot, upcoming, schedule, policy) {
  const pr = data.payroll;
  if (!pr || !(spot > 0) || !upcoming.length) return null;
  const until = monthEnd(upcoming[Math.min(upcoming.length, 6) - 1].month);
  const from = prevMonth(today.slice(0, 7));
  let count = 1;
  for (let m = from; m < until.slice(0, 7); m = nextMonth(m)) count += 1;
  const split = payrollSplit(pr.journals || []);
  const pay = payrollToPay({ projection: pr.projection, split, months: data.inflowMonths, from, count });
  if (!pay.rows.length) return null;
  const vendors = vendorAverages(data.flows, data.ilsVendors, data.inflowMonths);
  const onHand = data.balances ? ilsOnHand(data.balances.rows) : null;
  const ilsNow = onHand ? onHand.total : null;
  const bills = cardBills({ ilsVendors: data.ilsVendors, average: vendors.cards, today, until });
  // If a fixing sells the amount kept, it also brings that amount's shekels.
  const conversions = schedule.filter((s) => s.atSpot && s.risk).slice(0, 6).map((s) => ({ date: s.fixing.date, ils: ilsKept(policy, s) }));
  const needs = shekelNeeds({ today, ilsNow, payroll: pay.rows, conversions, vendorsIls: vendors.other ?? 0, cardBills: bills, spot, until });
  const notes = [];
  if (ilsNow === null) notes.push("The shekels on hand are not known (NetSuite's books are EUR only), so they count as 0.");
  if (vendors.ils === null) notes.push('The ILS vendor payments could not be read, so no shekels are kept for vendors or the credit cards.');
  if (pay.share === null) notes.push("NetSuite's payroll journals could not be read, so the whole payroll counts by the 5th.");
  if (pay.rows.some((r) => r.source === 'journal')) {
    notes.push(pr.projection
      ? "The Salary Projection has no figure for some months: they carry the last payroll journal's total."
      : "The Salary Projection's figures are not saved on the server (open it once), so the payroll is the last payroll journal's total.");
  }
  return {
    ...needs, onHand, payroll: pay.rows, share: pay.share, split, vendors, cardBills: bills, until,
    projectionAsOf: pr.projection ? pr.projection.asOf || null : null, notes,
  };
}

// ── what to do today ──
/**
 * What to do today, from today's balances and rate (view: planFromPayload's): the recommendations, urgent first. The
 * daily mail goes on any day but Saturday when there is at least one. urgent: due within `lead` business days (in red).
 *   convert        a payment in shekels that the shekels on hand (and the conversions before it) don't cover: from
 *                  `notice` business days before it has to be ready (finance converts at once; asked on 27 Oct 2026,
 *                  as the figure moves until then), urgent. Never earlier, whatever the rate (finance asked the same
 *                  day for no early conversion on a high EUR/ILS)
 *   short          the euros held and expected fall short of a due: urgent within `lead` business days
 *   move           the conversion day is within `moveLead` business days and Hapoalim EUR holds less than to keep there
 *   ladder         a conversion the deposits don't cover (first in, first out): spare euros into deposits that pay out
 *                  just before it, earliest first
 *   convert-spare  euros spare while EUR/ILS is high (what is left after the ladder)
 *   deposit        euros spare for a new deposit of 1, 3 or 6 months, when every conversion ahead is covered
 * Amounts under `minEur` are left out. Returns { actions: [{ kind, key (the same on another day), urgent, eur, ... }],
 * ahead (duesAhead) }.
 */
export function todayActions(view, today, { lead = 5, moveLead = 3, minEur = 50000, notice = 2 } = {}) {
  const plan = view ? view.plan : null;
  const n = plan ? plan.next : null;
  const ahead = duesAhead(view ? view.cash : null);
  const actions = [];
  if (!n) return { actions, ahead };
  const high = !!view.rateCheck && view.rateCheck.level === 'high';
  for (const d of ahead.dues) {
    if (!(d.eur >= minEur)) continue;
    const left = businessDaysBetween(today, d.by);
    // Said `notice` business days ahead, not before: finance converts at once, and the figure is fresher then.
    // Earlier it is only among the payments ahead.
    if (left <= notice) actions.push({ kind: 'convert', key: `convert:${d.date}`, urgent: left < lead, eur: d.eur, by: d.by, left, due: d });
  }
  if (n.short && n.short.first.eur >= minEur) {
    const by = businessDayOnOrBefore(n.short.first.date);
    const left = businessDaysBetween(today, by);
    actions.push({ kind: 'short', key: `short:${n.short.first.date}`, urgent: left < lead, eur: n.short.first.eur, date: n.short.first.date, by, left, shekels: n.short.first.shekels });
  }
  const fixingBy = businessDayOnOrBefore(n.date);
  const fixLeft = businessDaysBetween(today, fixingBy);
  if (fixLeft < moveLead && n.moveToHapoalim >= 10000) {
    actions.push({ kind: 'move', key: `move:${n.date}`, urgent: true, eur: n.moveToHapoalim, date: n.date, by: fixingBy, left: fixLeft });
  }
  const spare = view.cash ? n.spare : n.freeNow;
  const shortNow = view.cash ? !!n.short : n.shortfall > 0;
  // A conversion the deposits don't cover (first in, first out) comes first: spare euros go into deposits timed for
  // it. They pay out before it, so a shortfall after them is no reason to wait (the ladder already leaves every due
  // before them covered). Only with no such conversion left open is there a deposit for 1, 3 or 6 months.
  const legs = plan.ladder || [];
  const laddered = legs.reduce((s, l) => s + l.eur, 0);
  if (laddered >= minEur) actions.push({ kind: 'ladder', key: 'ladder', urgent: false, eur: laddered, legs });
  const open = (plan.coverage || []).some((c) => c.timed && c.gap >= minEur);
  const left = laddered >= minEur ? Math.min(spare, n.spareAfterLadder) : spare;
  if (!shortNow && left >= minEur) {
    if (high) actions.push({ kind: 'convert-spare', key: 'spare', urgent: false, eur: left });
    else if (!open) {
      const options = (plan.deposits || []).filter((x) => x.eur >= minEur);
      if (options.length) actions.push({ kind: 'deposit', key: 'deposit', urgent: false, eur: options[0].eur, options });
    }
  }
  return { actions: [...actions.filter((a) => a.urgent), ...actions.filter((a) => !a.urgent)], ahead };
}

const LEVEL_WORD = { high: 'high', middle: 'mid-range', low: 'low' };
const capital = (s) => (s ? s[0].toUpperCase() + s.slice(1) : s);
const inDays = (from, to) => { const n = daysUntil(from, to); return n <= 0 ? 'today' : n === 1 ? 'tomorrow' : `in ${n} days`; };

/**
 * An action in words, with the page's or the mail's formats (fmt: { eur, ils, day, rate }): { head, body }. head is
 * what to do ("Convert €1.10M by Thu 5 Nov"), body why, from the balances.
 */
export function describeAction(a, view, today, fmt) {
  const { eur, ils, day } = fmt;
  const n = view.plan.next;
  const rc = view.rateCheck;
  const rateIs = rc ? `EUR/ILS ${fmt.rate(rc.rate)} is ${LEVEL_WORD[rc.level]} against the last ${rc.days} days` : '';
  if (a.kind === 'convert') {
    const d = a.due;
    const now = a.by <= today;
    const when = d.date <= today ? 'this month' : `on ${day(d.date)}${d.by !== d.date ? ` (ready by ${day(d.by)})` : ''}`;
    const held = d.before < -0.5 ? `an overdraft of ${ils(-d.before)}` : `${ils(Math.max(0, d.before))} on hand`;
    let body = `${capital(dueWhat(d))}: ${ils(d.ils)} goes out ${when}, with ${held} by then.`;
    if (rc && !now) {
      if (rc.level === 'high') body += ` ${rateIs}: a good day to do it.`;
      else if (rc.level === 'low') body += ` ${rateIs}: if it can wait, wait, but no later than ${day(a.by)}.`;
    }
    return { head: `Convert ${eur(a.eur)} ${now ? 'today' : `by ${day(a.by)}`}`, body };
  }
  if (a.kind === 'short') {
    const what = a.shekels ? 'the conversion for shekels due then' : `the ${day(a.date)} conversion`;
    const fix = n.backup > 0 ? ` Be ready to break up to ${eur(Math.min(a.eur, n.backup))} of the deposits maturing later.` : ' Keep every spare euro until then.';
    return { head: `Short ${eur(a.eur)} by ${day(a.date)}`, body: `The euros held and expected by then don't cover ${what} and the ${eur(n.eurFloor)} kept for EUR vendors.${fix}` };
  }
  if (a.kind === 'move') {
    const fromDeposits = Math.min(n.required, n.maturing || 0);
    const dep = fromDeposits > 0.5 ? `, ${eur(fromDeposits)} of it from deposits released by then` : '';
    return { head: `Move ${eur(a.eur)} to Hapoalim EUR by ${day(a.by)}`, body: `The ${day(a.date)} conversion takes its euros from Hapoalim EUR (583400), which holds ${eur(n.hapoalim)}; keep ${eur(n.required)} there${dep}.` };
  }
  if (a.kind === 'convert-spare') {
    return { head: `Convert the ${eur(a.eur)} spare today`, body: `${rateIs}. It brings ${ils(a.eur * rc.rate)}, with every conversion, salary and vendor payment ahead still covered.` };
  }
  if (a.kind === 'ladder') {
    const legs = a.legs;
    if (legs.length === 1) {
      const l = legs[0];
      return { head: `Put ${eur(l.eur)} in a deposit until ${day(l.until)}`, body: `For the ${day(l.date)} conversion: it takes ${eur(l.required)}, and the deposits paid out by then hold ${eur(l.have)}, ${eur(l.gap)} short. Every payment before then stays covered.` };
    }
    const first = monthNameOf(legs[0].month);
    const last = monthNameOf(legs[legs.length - 1].month);
    return {
      head: `Put ${eur(a.eur)} in deposits for the ${first} ${legs.length === 2 ? 'and' : 'to'} ${last} conversions`,
      body: `${legs.map((l) => `${eur(l.eur)} until ${day(l.until)} for ${day(l.date)} (${eur(l.gap)} short)`).join(', ')}. Every payment before then stays covered.`,
    };
  }
  // The same amount for every term: one line ("€430K for 1, 3 or 6 months").
  const last = a.options[a.options.length - 1];
  const options = a.options.length > 1 && a.options.every((o) => Math.abs(o.eur - a.eur) < 0.5)
    ? `${eur(a.eur)} for ${a.options.map((o) => o.months).join(', ').replace(/, (\d+)$/, ' or $1')} month${last.months === 1 ? '' : 's'} (the longest until ${day(last.until)})`
    : a.options.map((o) => `${eur(o.eur)} for ${o.months} month${o.months === 1 ? '' : 's'} (until ${day(o.until)})`).join(', or ');
  return { head: `Put ${eur(a.eur)} in a new deposit`, body: `${options}, with every payment before then still covered as the typical inflow comes in.` };
}

/** A payment day in words, for the list of what is ahead: "Thu 5 Nov, in 15 days: October's net salaries…, ₪4.80M:
 * convert €1.10M". */
export function describeDue(d, today, fmt) {
  const when = d.date <= today ? 'Now' : `${fmt.day(d.date)}${d.by !== d.date ? ` (ready by ${fmt.day(d.by)})` : ''}, ${inDays(today, d.by)}`;
  const action = d.eur > 0.5 ? `convert ${fmt.eur(d.eur)}` : d.after ? `covered by the ${fmt.day(d.after)} conversion` : 'covered by the shekels on hand';
  return `${when}: ${dueWhat(d)}, ${fmt.ils(d.ils)}: ${action}`;
}

/** When there is nothing to do today: what comes next, in words. */
export function nothingToday(ahead, today, fmt, { minEur = 50000 } = {}) {
  const next = ahead.dues.find((d) => d.eur >= minEur);
  if (next) return `Nothing to do today. Next: convert ${fmt.eur(next.eur)} by ${fmt.day(next.by)} (${inDays(today, next.by)}) for ${dueWhat(next)}.`;
  const last = ahead.dues[ahead.dues.length - 1];
  return last ? `Nothing to do today: the shekels on hand and the conversions cover the payments to ${fmt.day(last.date)}.` : 'Nothing to do today.';
}

/**
 * Where the euros kept for the next conversion come from on the day: the EUR deposits released after today and by
 * the conversion day (they count for it, paid into Hapoalim EUR), and today's cash for the rest.
 * Returns { required, deposits (earliest first), fromDeposits, fromCash, covered } (covered: the deposits alone hold it).
 */
export function keepCover(plan) {
  const n = plan.next;
  const deposits = plan.path.length ? plan.path[0].releasedDeposits : [];
  const fromDeposits = Math.min(n.required, n.maturing);
  return { required: n.required, deposits, fromDeposits, fromCash: n.needLiquid, covered: fromDeposits > 0.5 && n.needLiquid < 0.5 };
}

/** Why the amount to keep (keepCover's fromCash) is what it is, when deposits released by then hold some of the
 * conversion: "the 3 deposits released by Tue 3 Nov (€806K) cover the €800K it takes from Hapoalim EUR (583400)" (so
 * nothing to keep), or "it takes €800K, and the deposit released on Thu 5 Nov brings €500K of it" (keep the rest).
 * Empty when no deposit is released by then. */
export function describeKeep(cover, fmt) {
  const ds = cover.deposits;
  if (!(cover.fromDeposits > 0.5) || !ds.length) return '';
  const last = ds[ds.length - 1].valueDate;
  const one = ds.length === 1;
  const which = one ? `the deposit released on ${fmt.day(last)}` : `the ${ds.length} deposits released by ${fmt.day(last)}`;
  if (cover.covered) {
    const total = ds.reduce((s, d) => s + d.amount, 0);
    return `${which} (${fmt.eur(total)}) ${one ? 'covers' : 'cover'} the ${fmt.eur(cover.required)} it takes from Hapoalim EUR (583400)`;
  }
  return `it takes ${fmt.eur(cover.required)}, and ${which} ${one ? 'brings' : 'bring'} ${fmt.eur(cover.fromDeposits)} of it`;
}

// ── past fixings ──
/**
 * A past fixing against the "Conversion" journals booked near it: the table at that day's rate, what was booked, and
 * the shekels gained against that day's rate. What was booked is the day of euros sold within `windowDays` whose
 * total is closest to the table's euros; for a month the table doesn't have (fixing.row null), the day with the best
 * rate, as the monthly deals beat the market's.
 */
export function checkPast({ fixing, bounds, series, conversions, windowDays = 3 }) {
  const t = utc(fixing.date);
  const byDate = new Map();
  for (const c of conversions || []) {
    // Euros bought back (negative) are not the monthly conversion.
    if (!c || !isIso(c.date) || !(Number(c.eur) > 0) || Math.abs(utc(c.date) - t) > windowDays * DAY_MS) continue;
    const g = byDate.get(c.date) || { date: c.date, eur: 0, ils: 0, n: 0 };
    g.eur += Number(c.eur) || 0;
    g.ils += Number(c.ils) || 0;
    g.n += 1;
    byDate.set(c.date, g);
  }
  const spotPt = rateOn(series, fixing.date);
  const conv = spotPt && fixing.row ? conversionAt(fixing.row, bounds, spotPt.rate) : null;
  const days = [...byDate.values()];
  const booked = (fixing.row
    ? days.sort((a, b) => Math.abs(a.eur - (conv ? conv.eur : 0)) - Math.abs(b.eur - (conv ? conv.eur : 0)))
    : days.sort((a, b) => b.ils / b.eur - a.ils / a.eur))[0] || null;
  const bookedSpot = booked ? rateOn(series, booked.date) : null;
  const bookedRate = booked && booked.eur ? booked.ils / booked.eur : null;
  return {
    month: fixing.month,
    date: fixing.date,
    spot: spotPt,
    conv,
    booked: booked ? { ...booked, rate: bookedRate } : null,
    match: !!(booked && conv && Math.abs(booked.eur - conv.eur) <= 0.01 * Math.abs(conv.eur) && Math.abs(bookedRate - conv.rate) <= 0.01),
    gainVsSpotIls: booked && bookedSpot ? booked.ils - booked.eur * bookedSpot.rate : null,
  };
}

/**
 * The conversions month by month from `from` (YYYY-MM, or the table's first month if earlier) to the last conversion
 * day passed: each month's check (checkPast; inTable when the table has the month), and its other conversions by day,
 * euros sold or bought back (negative), each against its day's rate. Then the totals.
 */
export function pastConversions({ table, today, series, conversions, from = HISTORY_FROM, windowDays = 3 }) {
  const rows = new Map(table.rows.map((r) => [r.month, r]));
  const first = [from, ...table.rows.map((r) => r.month)].sort()[0];
  const months = [];
  for (let m = first; fixingDate(m, table.fixingDay) < today; m = nextMonth(m)) {
    const fixing = { month: m, date: fixingDate(m, table.fixingDay), row: rows.get(m) || null };
    const check = checkPast({ fixing, bounds: table.bounds, series, conversions, windowDays });
    const byDay = new Map();
    for (const c of conversions || []) {
      const eur = Number(c && c.eur) || 0;
      if (!eur || !isIso(c.date) || c.date.slice(0, 7) !== m || (eur > 0 && check.booked && c.date === check.booked.date)) continue;
      const key = `${c.date}${eur > 0 ? '+' : '-'}`;
      const o = byDay.get(key) || { date: c.date, eur: 0, ils: 0, n: 0 };
      o.eur += eur;
      o.ils += Number(c.ils) || 0;
      o.n += 1;
      byDay.set(key, o);
    }
    const others = [...byDay.values()].sort((a, b) => a.date.localeCompare(b.date) || b.eur - a.eur).map((o) => {
      const r = rateOn(series, o.date);
      return { ...o, rate: o.ils / o.eur, vsRateIls: r ? o.ils - o.eur * r.rate : null };
    });
    months.push({ ...check, inTable: !!fixing.row, others });
  }
  const sum = (xs, f) => xs.reduce((s, x) => s + (f(x) || 0), 0);
  const booked = months.filter((p) => p.booked);
  const others = months.flatMap((p) => p.others);
  return {
    from: first,
    months,
    total: {
      booked: { eur: sum(booked, (p) => p.booked.eur), ils: sum(booked, (p) => p.booked.ils), vsRateIls: sum(booked, (p) => p.gainVsSpotIls), months: booked.length },
      others: { sold: sum(others, (o) => Math.max(0, o.eur)), bought: sum(others, (o) => Math.max(0, -o.eur)), eur: sum(others, (o) => o.eur), ils: sum(others, (o) => o.ils), vsRateIls: sum(others, (o) => o.vsRateIls) },
    },
  };
}

// ── pasting the table from Excel ──
const LIMITS = { rows: 60, eurK: 100000, rate: 20 };
const number = (s) => {
  const v = Number(String(s).replace(/[,\s€₪]/g, '').replace(/[−–]/g, '-'));
  return Number.isFinite(v) ? v : NaN;
};

/** The same rules as the server (server/fx-challenge.cjs): a list of problems, empty when the table is fine. */
export function tableProblems(t) {
  const out = [];
  if (!t || !Array.isArray(t.bounds) || !Array.isArray(t.rows)) return ['The table needs bands and months.'];
  if (!(Number.isInteger(t.fixingDay) && t.fixingDay >= 1 && t.fixingDay <= 28)) out.push('The conversion day is a day from 1 to 28.');
  if (t.bounds.length < 1 || t.bounds.length > 30) out.push('Between 1 and 30 band bounds.');
  t.bounds.forEach((b, i) => {
    if (!(typeof b === 'number' && b > 0 && b < LIMITS.rate)) out.push(`Bound ${i + 1} is not a rate.`);
    else if (i && !(b > t.bounds[i - 1])) out.push(`Bound ${i + 1} (${b}) is not above the one before (${t.bounds[i - 1]}).`);
  });
  if (!t.rows.length || t.rows.length > LIMITS.rows) out.push(`Between 1 and ${LIMITS.rows} months.`);
  const seen = new Set();
  t.rows.forEach((r) => {
    if (!r || !/^\d{4}-(0[1-9]|1[0-2])$/.test(String(r.month))) { out.push(`A month is not YYYY-MM (${r && r.month}).`); return; }
    if (seen.has(r.month)) out.push(`${r.month} appears twice.`);
    seen.add(r.month);
    if (!Array.isArray(r.cells) || r.cells.length !== t.bounds.length + 1) { out.push(`${r.month} has ${r.cells ? r.cells.length : 0} cells; the bands need ${t.bounds.length + 1}.`); return; }
    r.cells.forEach((c, i) => {
      if (!(c && Number.isFinite(c.eurK) && Math.abs(c.eurK) <= LIMITS.eurK)) out.push(`${r.month}, band ${i + 1}: the amount is not € thousands.`);
      if (!(c && c.rate > 0 && c.rate < LIMITS.rate)) out.push(`${r.month}, band ${i + 1}: the rate is not a rate.`);
    });
  });
  return out;
}

/** The table as Excel text (tab-separated, the layout parseTable reads), to edit in a sheet and paste back. */
export function tableToText(t) {
  const head = ['', ...bandLabels(t.bounds).map((l) => l.replace('–', '-'))].join('\t');
  const rows = [...t.rows].sort((a, b) => a.month.localeCompare(b.month))
    .map((r) => [`${Number(r.month.slice(5))}/${r.month.slice(0, 4)}`, ...r.cells.map((c) => `${c.eurK} / ${c.rate}`)].join('\t'));
  return [head, ...rows].join('\n');
}

/**
 * The table as copied from Excel: a header row of bands ("<3.3", "3.3-3.4", …, ">4.295"), then one row per month
 * ("10/2026", then "-500 / 3.62" per band), tab-separated. Returns { ok, table, errors } (errors by line).
 */
export function parseTable(text, { fixingDay = 7, pair = 'EUR/ILS' } = {}) {
  const lines = String(text || '').replace(/\r/g, '').split('\n').map((l, i) => ({ n: i + 1, cells: l.split('\t').map((c) => c.trim()) }))
    .filter((l) => l.cells.some((c) => c));
  if (lines.length < 2) return { ok: false, table: null, errors: ['Paste the header row (the bands) and at least one month.'] };
  const errors = [];
  let head = lines[0].cells;
  while (head.length && !head[head.length - 1]) head = head.slice(0, -1);
  if (head.length && !/\d/.test(head[0])) head = head.slice(1); // the empty corner cell
  const bounds = [];
  head.forEach((label, i) => {
    const nums = (label.match(/\d+(?:\.\d+)?/g) || []).map(Number);
    if (i === 0 && nums.length === 1) bounds.push(nums[0]);
    else if (i === head.length - 1 && nums.length === 1) {
      if (nums[0] !== bounds[bounds.length - 1]) errors.push(`Line ${lines[0].n}: the last band starts at ${nums[0]}, but the band before ends at ${bounds[bounds.length - 1]}.`);
    } else if (nums.length === 2) {
      if (bounds.length && nums[0] !== bounds[bounds.length - 1]) errors.push(`Line ${lines[0].n}: band "${label}" starts at ${nums[0]}, but the band before ends at ${bounds[bounds.length - 1]}.`);
      bounds.push(nums[1]);
    } else errors.push(`Line ${lines[0].n}: "${label}" is not a band (like <3.3, 3.3-3.4 or >4.295).`);
  });
  const rows = [];
  for (const line of lines.slice(1)) {
    let cells = line.cells;
    while (cells.length && !cells[cells.length - 1]) cells = cells.slice(0, -1);
    const m = /^(\d{1,2})\/(\d{4})$/.exec(cells[0] || '');
    if (!m || +m[1] < 1 || +m[1] > 12) { errors.push(`Line ${line.n}: "${cells[0] || ''}" is not a month (MM/YYYY).`); continue; }
    const parsed = cells.slice(1).map((c, i) => {
      const [a, b] = c.split('/');
      const eurK = number(a);
      const rate = number(b);
      if (!Number.isFinite(eurK) || !Number.isFinite(rate)) errors.push(`Line ${line.n}, band ${i + 1}: "${c}" is not "amount / rate".`);
      return { eurK, rate };
    });
    rows.push({ month: `${m[2]}-${pad2(+m[1])}`, cells: parsed });
  }
  rows.sort((a, b) => a.month.localeCompare(b.month));
  const table = { pair, fixingDay, bounds, rows };
  if (!errors.length) errors.push(...tableProblems(table));
  return errors.length ? { ok: false, table: null, errors } : { ok: true, table, errors: [] };
}
