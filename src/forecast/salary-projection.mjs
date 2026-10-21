// ─────────────────────────────────────────────────────────────────────────────
// Salary projection: the forecast months of payroll by GL account, from the actual months and how each
// account is projected. Pure — no I/O — shared by the server (server/salary-projection.cjs, the CLI) and
// the Salary Projection page, which re-projects at once when someone reclassifies an account.
//
//   run-rate   permanent: every forecast month carries the run-rate month (the last closed month)
//   last-year  temporary: the current year's forecast months repeat the same month of the previous year
//              (keeps the seasonality); the next year is 0
//   none       actuals only: forecast months are 0
//
// The projection is made in ILS; forecast € = ₪ ÷ ilsPerEur. Actual months keep their own €.
//
// Planned changes add to Total payroll in the projected months: the override file's increments
// (countedIncrementRows) and Snowflake's hires, leavers and returns from leave (countedChangeRows). The
// accounts' own figures leave them out.
//
// Below the payroll: Deel (NetSuite, by account, forecast at its last six months' average: deelByMonth) and the
// headcount (HiBob's employees: headcountSeries).
// ─────────────────────────────────────────────────────────────────────────────

export const SALARY_METHODS = ['run-rate', 'last-year', 'none'];

/** What each method is called on the page. */
export const METHOD_LABEL = { 'run-rate': 'Permanent', 'last-year': 'Temporary', none: 'Actuals only' };

/** Permanent accounts carry the run-rate; temporary ones repeat last year or are not projected. */
export function groupOfMethod(method) {
  return method === 'run-rate' ? 'permanent' : 'temporary';
}

export function isSalaryMethod(m) {
  return SALARY_METHODS.includes(m);
}

const pad2 = (n) => String(n).padStart(2, '0');
const mk = (y, mi) => `${y}-${pad2(mi + 1)}`;
const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const cents = (n) => Math.round(num(n) * 100) / 100;
const shiftMonth = (mKey, n) => {
  const [y, m] = mKey.split('-').map(Number);
  const i = y * 12 + (m - 1) + n;
  return mk(Math.floor(i / 12), i % 12);
};

/**
 * The current year (actuals + forecast) and the next year (forecast), month by month and account by account.
 *   years         [Y, T]
 *   currentMonth  'YYYY-MM' (projected as a whole month)
 *   runRateMonth  'YYYY-MM' the last closed month: actual months are up to it
 *   ilsPerEur     ₪ per € of forecast months
 *   accounts      account numbers, in table order
 *   actual        { 'YYYY-MM': { [acct]: { eur, ils } } } the previous year and the current year's actual months
 *   methodOf      (acct) → 'run-rate' | 'last-year' | 'none'
 * Returns [{ year, kind: 'current' | 'projection', rows: [{ mKey, status, eur: { [acct]: n }, ils: { [acct]: n } }] }].
 */
export function projectSalary({ years, currentMonth, runRateMonth, ilsPerEur, accounts, actual, methodOf }) {
  const [Y, T] = years;
  const amount = (mKey, acct) => (actual && actual[mKey] && actual[mKey][acct]) || null;
  const isActual = (mKey) => mKey <= runRateMonth && mKey < currentMonth;
  const statusOf = (mKey) => (isActual(mKey) ? 'actual' : mKey === currentMonth ? 'current' : 'forecast');
  const rate = num(ilsPerEur) > 0 ? num(ilsPerEur) : 1;

  const memo = new Map();
  function ilsOf(year, mi, acct) {
    const mKey = mk(year, mi);
    if (year < Y || isActual(mKey)) return num((amount(mKey, acct) || {}).ils);
    const id = `${mKey}|${acct}`;
    if (memo.has(id)) return memo.get(id);
    const method = methodOf(acct);
    // Temporary payments are projected for the rest of the current year only.
    const v = method === 'run-rate' ? num((amount(runRateMonth, acct) || {}).ils)
      : method === 'none' || year > Y ? 0
        : ilsOf(year - 1, mi, acct);
    memo.set(id, v);
    return v;
  }

  const blockOf = (year, kind) => ({
    year,
    kind,
    rows: Array.from({ length: 12 }, (_, mi) => {
      const mKey = mk(year, mi);
      const status = year === Y ? statusOf(mKey) : 'forecast';
      const eur = {};
      const ils = {};
      for (const acct of accounts) {
        if (status === 'actual') {
          const a = amount(mKey, acct) || { eur: 0, ils: 0 };
          eur[acct] = cents(a.eur);
          ils[acct] = cents(a.ils);
        } else {
          const v = ilsOf(year, mi, acct);
          ils[acct] = cents(v);
          eur[acct] = cents(v / rate);
        }
      }
      return { mKey, status, eur, ils };
    }),
  });
  return [blockOf(Y, 'current'), blockOf(T, 'projection')];
}

/**
 * The override file's rows that count in Total payroll: increments in the months the projection makes (the
 * current month and forecast months). Closed months are NetSuite's actuals, which already include them.
 * Overrides replace one department's budget for an account and month, which a projection by account cannot
 * isolate, so they never count. Months outside the blocks do not count either.
 *   blocks  the projection's year blocks (month keys and statuses)
 *   rows    the override file's rows ({ month: 'YYYY-MM', mode: 'Override' | 'Increment', ... })
 * Returns the rows that count (the same objects).
 */
export function countedIncrementRows(blocks, rows) {
  const projected = projectedMonths(blocks);
  return (rows || []).filter((r) => r && r.mode === 'Increment' && projected.has(r.month));
}

/**
 * Snowflake's planned payroll changes that count in Total payroll: hires (Open position), leavers (Terminated)
 * and returns from leave (Leave end) marked TRUE (IS_SOURCE_ACTUAL), in the months the projection makes. Closed
 * months are NetSuite's actuals, which already include what happened; a change marked FALSE does not count at all.
 * Returns the rows that count (the same objects).
 */
export function countedChangeRows(blocks, rows) {
  const projected = projectedMonths(blocks);
  return (rows || []).filter((r) => r && r.actual === true && projected.has(r.month));
}

/**
 * The same changes in the months the projection makes that are not marked TRUE: the page shows them for reference,
 * and Total payroll never counts them. Returns the rows (the same objects).
 */
export function uncountedChangeRows(blocks, rows) {
  const projected = projectedMonths(blocks);
  return (rows || []).filter((r) => r && r.actual !== true && projected.has(r.month));
}

/** The months the projection makes (the current month and forecast months): where planned changes count. */
export function projectedMonths(blocks) {
  const months = new Set();
  for (const b of blocks || []) for (const r of b.rows || []) if (r.status !== 'actual') months.add(r.mKey);
  return months;
}

const CHANGE_SOURCES = new Set(['Open position', 'Terminated', 'Leave end']);

/**
 * Total payroll by month, as the Salary Projection page shows it: every payroll account (blocks, with the payload's
 * classification), plus the planned hires, leavers and returns from leave marked TRUE and the override file's
 * increments in the months the projection makes. Overrides are for reference and Deel sits below Total payroll, so
 * neither is in it. payload: GET /api/salary-projection's answer. Returns { [mKey]: { eur, ils, status } }.
 */
export function totalPayrollByMonth(payload) {
  const accounts = (payload.accounts || []).map((a) => a.acct);
  const sum = (fig) => accounts.reduce((s, a) => s + (Number(fig && fig[a]) || 0), 0);
  const out = {};
  for (const b of payload.blocks || []) for (const r of b.rows || []) out[r.mKey] = { eur: sum(r.eur), ils: sum(r.ils), status: r.status };
  const add = (rows) => {
    for (const r of rows) {
      if (!out[r.month]) continue;
      out[r.month].eur += Number(r.eur) || 0;
      out[r.month].ils += Number(r.ils) || 0;
    }
  };
  add(countedChangeRows(payload.blocks, payload.changes ? payload.changes.rows : []).filter((r) => CHANGE_SOURCES.has(r.source)));
  add(countedIncrementRows(payload.blocks, payload.overrides ? payload.overrides.rows : []));
  return out;
}

// ── Deel ─────────────────────────────────────────────────────────────────────
/** The months Deel's run-rate averages: the last six up to the run-rate month. */
export const DEEL_RUN_RATE_MONTHS = 6;

/**
 * Deel's monthly run-rate by account: the average of the last six months up to runRateMonth (from Deel's first
 * posting, when that is later), each account's sum over them ÷ their number. A month without a bill counts as 0,
 * so a bill booked a month late, and the month that then holds two, average out; a move to another account
 * leaves the total right.
 *   rows  [{ month: 'YYYY-MM', acct, eur, ils }] Deel's NetSuite costs (costs positive)
 * Returns { months: ['YYYY-MM', ...] (oldest first; none when Deel has no postings), byAcct: { [acct]: { eur, ils } } }.
 */
export function deelRunRate(rows, runRateMonth) {
  const byMonth = new Map();
  let first = null;
  for (const r of rows || []) {
    if (!r || !/^\d{4}-\d{2}$/.test(String(r.month)) || !(r.month <= runRateMonth)) continue;
    const accts = byMonth.get(r.month) || {};
    const a = accts[r.acct] || (accts[r.acct] = { eur: 0, ils: 0 });
    a.eur += num(r.eur);
    a.ils += num(r.ils);
    byMonth.set(r.month, accts);
    if (Math.abs(num(r.eur)) >= 0.5 || Math.abs(num(r.ils)) >= 0.5) first = first === null || r.month < first ? r.month : first;
  }
  const months = [];
  if (first !== null) {
    for (let i = 0, m = runRateMonth; i < DEEL_RUN_RATE_MONTHS && m >= first; i++, m = shiftMonth(m, -1)) months.push(m);
  }
  const byAcct = {};
  for (const m of months) {
    for (const [acct, a] of Object.entries(byMonth.get(m) || {})) {
      const s = byAcct[acct] || (byAcct[acct] = { eur: 0, ils: 0 });
      s.eur += a.eur / months.length;
      s.ils += a.ils / months.length;
    }
  }
  for (const s of Object.values(byAcct)) { s.eur = cents(s.eur); s.ils = cents(s.ils); }
  return { months: months.reverse(), byAcct };
}

/**
 * Deel by month and account over the projection's months: NetSuite's costs in actual months, the run-rate
 * (deelRunRate) in the months the projection makes.
 * Returns Map(mKey → { eur: { [acct]: n }, ils: { [acct]: n } }).
 */
export function deelByMonth(blocks, rows, runRateMonth) {
  const rate = deelRunRate(rows, runRateMonth);
  const actual = new Map();
  for (const r of rows || []) {
    if (!r) continue;
    const m = actual.get(r.month) || { eur: {}, ils: {} };
    m.eur[r.acct] = cents((m.eur[r.acct] || 0) + num(r.eur));
    m.ils[r.acct] = cents((m.ils[r.acct] || 0) + num(r.ils));
    actual.set(r.month, m);
  }
  const forecast = { eur: {}, ils: {} };
  for (const [acct, a] of Object.entries(rate.byAcct)) { forecast.eur[acct] = a.eur; forecast.ils[acct] = a.ils; }
  const out = new Map();
  for (const b of blocks || []) {
    for (const row of b.rows || []) {
      out.set(row.mKey, row.status === 'actual' ? actual.get(row.mKey) || { eur: {}, ils: {} } : forecast);
    }
  }
  return out;
}

// ── Headcount (HiBob) ────────────────────────────────────────────────────────
/**
 * How a HiBob event (FCT_HEADCOUNT_EVENT) shows, or null for an unknown event type. None of them moves the count,
 * which is HiBob's employees (their start and leave dates):
 *   'returns' / 'leaveStarts'  a sub-type about leave (e.g. 'Leave end'): people on leave are still employees
 *   'employee'                 an event of a known employee: HiBob's employees already hold it (their dates)
 *   'openings' / 'cuts'        an increase or decrease with no employee yet: an open position, or a planned leaver
 *                              HiBob's employees do not hold; shown, not counted
 */
export function headcountKind(type, subType, employee) {
  const t = String(type || '').toLowerCase();
  if (t !== 'increase' && t !== 'decrease') return null;
  if (/leave/i.test(String(subType || ''))) return t === 'increase' ? 'returns' : 'leaveStarts';
  if (employee) return 'employee';
  return t === 'increase' ? 'openings' : 'cuts';
}

/**
 * Employees at each month-end, and who joined and left, over the projection's months: HiBob's employees, as
 * HiBob has them.
 *   months        'YYYY-MM' in order (the projection's)
 *   currentMonth  'YYYY-MM'
 *   known         { 'YYYY-MM': { employees, hires, leavers } } HiBob's employees from their start and leave dates,
 *                 future ones included (signed hires, people leaving): every month shown and the one before the
 *                 first (null when they could not be read)
 *   events        [{ month, type, subType, employee, status, count }] HiBob's planned events (FCT_HEADCOUNT_EVENT)
 * The count is HiBob's employees only: months before the current one as they were, later ones by the dates HiBob
 * already holds (who is signed to start, who is leaving). HiBob's events are shown beside it and never move it:
 * open positions and planned leavers with no employee from the current month on (openings planned for earlier
 * months and still open, in the current month), and leave events in their month.
 * Returns [{ mKey, status, employees, hires, leavers, openings, cuts, returns, leaveStarts }] (employees, hires
 * and leavers null where HiBob's employees are missing).
 */
export function headcountSeries({ months, currentMonth, known, events }) {
  const byMonth = new Map();
  for (const e of placedHeadcountEvents(events, currentMonth)) {
    const m = byMonth.get(e.month) || { openings: 0, cuts: 0, returns: 0, leaveStarts: 0 };
    m[e.kind] += e.count;
    byMonth.set(e.month, m);
  }
  const dates = known || null;
  return (months || []).map((mKey) => {
    const p = byMonth.get(mKey) || { openings: 0, cuts: 0, returns: 0, leaveStarts: 0 };
    const k = dates ? dates[mKey] : null;
    return {
      mKey, status: mKey < currentMonth ? 'actual' : mKey === currentMonth ? 'current' : 'forecast',
      employees: k ? num(k.employees) : null, hires: k ? num(k.hires) : null, leavers: k ? num(k.leavers) : null,
      transfers: k ? num(k.transfers) : null,
      ...p,
      ...(k && k.byDept ? { byDept: k.byDept } : {}),
      ...(k && k.byType ? { byType: k.byType } : {}),
    };
  });
}

/**
 * HiBob's events placed in the month they show in, as headcountSeries shows them: leave events in their month;
 * open positions and planned leavers with no employee from the current month on, and openings planned for earlier
 * months and still open in the current month; known employees' events and unknown types not at all.
 * Returns [{ month, kind, department, position, count, carried }] in the events' order (carried: an earlier
 * month's opening shown in the current month).
 */
export function placedHeadcountEvents(events, currentMonth) {
  const out = [];
  for (const e of events || []) {
    const kind = headcountKind(e.type, e.subType, e.employee);
    const n = num(e.count);
    if (!kind || kind === 'employee' || !n || !/^\d{4}-\d{2}$/.test(String(e.month))) continue;
    let month = null;
    if (kind === 'returns' || kind === 'leaveStarts' || e.month >= currentMonth) month = e.month;
    else if (kind === 'openings' && /^open/i.test(String(e.status || ''))) month = currentMonth;
    if (month) out.push({ month, kind, department: String(e.department || ''), position: String(e.position || ''), count: n, carried: month !== e.month });
  }
  return out;
}
