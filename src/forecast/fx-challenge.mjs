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

/**
 * What to keep for the next fixings and what is free to convert.
 *   upcoming   the next fixings, each { month, date, days, conv (at today's rate), risk }
 *   eur        { hapoalim, other, deposits: [{ amount, valueDate, blocked }] } (eurPosition)
 *   inflowPerDay  typical net EUR per day, or null when unknown
 * Deposits maturing before a fixing count for it; later ones are the backup (broken early only if needed).
 */
export function planReserve({ upcoming, policy = 'p95', eur, inflowPerDay = null, today, horizon = 6 }) {
  const fx = (upcoming || []).slice(0, horizon);
  if (!fx.length || !eur) return null;
  const hapoalim = Math.max(0, eur.hapoalim || 0);
  const other = Math.max(0, eur.other || 0);
  const liquid = hapoalim + other;
  const usable = (eur.deposits || []).filter((d) => d.amount > 0 && !d.blocked);
  const maturingBy = (date) => usable.filter((d) => d.valueDate && d.valueDate > today && d.valueDate <= date).reduce((s, d) => s + d.amount, 0);
  const inflowTo = (f) => (inflowPerDay == null ? 0 : inflowPerDay * Math.max(0, f.days - 1));

  const f0 = fx[0];
  const required = requirementOf(policy, f0.conv, f0.risk);
  const todayAmount = Math.max(0, f0.conv.eur);
  const maturing = maturingBy(f0.date);
  const backup = usable.filter((d) => !d.valueDate || d.valueDate > f0.date).reduce((s, d) => s + d.amount, 0);
  const needLiquid = Math.max(0, required - maturing);
  // With the backup deposits covering the tail, the liquid euros only have to cover today's band.
  const needLiquidWithBackup = Math.max(0, todayAmount - maturing, required - maturing - backup);
  const inflowUntil = inflowPerDay == null ? null : inflowTo(f0);

  // The deposits that mature after `from` and by `to`, earliest first: what each fixing gains from them.
  const releasedBetween = (from, to) => usable
    .filter((d) => d.valueDate && d.valueDate > from && (to === null || d.valueDate <= to))
    .sort((x, y) => x.valueDate.localeCompare(y.valueDate) || y.amount - x.amount)
    .map((d) => ({ amount: d.amount, valueDate: d.valueDate, label: d.label }));

  // Euros at each fixing, if nothing else is converted: today's liquid euros, plus typical inflows and deposits
  // maturing by then, less the earlier fixings at the policy amount. Euros converted before a fixing are gone at
  // it and at every later one, so what can be converted before a fixing, in total, is the smallest spare from
  // that fixing on.
  let before = 0;
  let from = today;
  const path = fx.map((f) => {
    const req = requirementOf(policy, f.conv, f.risk);
    // Its parts, kept so the page can show the sum: inflow up to the day before, deposits matured by the day,
    // and what the earlier conversions take.
    const inflow = inflowTo(f);
    const depositsBy = maturingBy(f.date);
    const earlier = before;
    const available = liquid + inflow + depositsBy - earlier;
    const releasedDeposits = releasedBetween(from, f.date);
    before += req;
    from = f.date;
    return {
      month: f.month, date: f.date, required: req, expected: f.risk.expectedEur,
      released: releasedDeposits.reduce((s, d) => s + d.amount, 0), releasedDeposits,
      inflow, inflowDays: inflowPerDay == null ? 0 : Math.max(0, f.days - 1), depositsBy, earlier,
      available, surplus: available - req, convertibleBefore: 0, limitedBy: f.month,
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
    needLiquid, needLiquidWithBackup,
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
  // Deposits maturing after the last fixing shown: not needed for any of them.
  return { policy, next, path: shown, later: releasedBetween(shown[shown.length - 1].date, null) };
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
  const plan = position && upcoming.length
    ? planReserve({ upcoming, policy, eur: position, inflowPerDay: inflow ? inflow.perDay : null, today })
    : null;
  return {
    all, spot, spotDate: data.rates.spot ? data.rates.spot.date : null, spotBand: spot ? bandIndex(bounds, spot) : null,
    sigma, sigmaFromHistory, rateCheck: rateCheck(data.rates.series, today), schedule, position, inflow, plan,
  };
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
