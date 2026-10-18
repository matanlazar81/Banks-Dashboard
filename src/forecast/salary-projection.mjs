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
// Below the payroll: Deel (NetSuite, by account: deelByMonth) and the headcount (HiBob: headcountSeries).
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
 * and returns from leave (Leave end) in the months the projection makes. Closed months are NetSuite's actuals,
 * which already include what happened. Returns the rows that count (the same objects).
 */
export function countedChangeRows(blocks, rows) {
  const projected = projectedMonths(blocks);
  return (rows || []).filter((r) => r && projected.has(r.month));
}

/** The months the projection makes (the current month and forecast months): where planned changes count. */
export function projectedMonths(blocks) {
  const months = new Set();
  for (const b of blocks || []) for (const r of b.rows || []) if (r.status !== 'actual') months.add(r.mKey);
  return months;
}

// ── Deel ─────────────────────────────────────────────────────────────────────
/**
 * Deel's monthly run-rate by account: the last three months up to runRateMonth that have Deel postings (looking
 * back 12 months at most), each account's sum over them ÷ their number. A bill booked a month late or a move to
 * another account leaves the total right.
 *   rows  [{ month: 'YYYY-MM', acct, eur, ils }] Deel's NetSuite costs (costs positive)
 * Returns { months: ['YYYY-MM', ...] (oldest first), byAcct: { [acct]: { eur, ils } } }.
 */
export function deelRunRate(rows, runRateMonth) {
  const byMonth = new Map();
  for (const r of rows || []) {
    if (!r || !/^\d{4}-\d{2}$/.test(String(r.month)) || !(r.month <= runRateMonth)) continue;
    const accts = byMonth.get(r.month) || {};
    const a = accts[r.acct] || (accts[r.acct] = { eur: 0, ils: 0 });
    a.eur += num(r.eur);
    a.ils += num(r.ils);
    byMonth.set(r.month, accts);
  }
  const months = [];
  for (let i = 0, m = runRateMonth; i < 12 && months.length < 3; i++, m = shiftMonth(m, -1)) {
    const accts = byMonth.get(m);
    if (accts && Object.values(accts).some((a) => Math.abs(a.eur) >= 0.5 || Math.abs(a.ils) >= 0.5)) months.push(m);
  }
  const byAcct = {};
  for (const m of months) {
    for (const [acct, a] of Object.entries(byMonth.get(m))) {
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
 * The headcount line a HiBob event (FCT_HEADCOUNT_EVENT) counts in, or null for an unknown event type. A sub-type
 * about leave (e.g. 'Leave end') is a return or a leave start: people on leave are still employees, so these do
 * not move the count. Any other increase is a hire, any other decrease a leaver.
 */
export function headcountKind(type, subType) {
  const t = String(type || '').toLowerCase();
  if (t !== 'increase' && t !== 'decrease') return null;
  if (/leave/i.test(String(subType || ''))) return t === 'increase' ? 'returns' : 'leaveStarts';
  return t === 'increase' ? 'hires' : 'leavers';
}

/**
 * Employees at each month-end, and who joined and left, over the projection's months.
 *   months        'YYYY-MM' in order (the projection's)
 *   currentMonth  'YYYY-MM'
 *   actual        { 'YYYY-MM': { employees, hires, leavers } } HiBob's employees: every month before the current
 *                 one, from the month before the first month shown (or null when they could not be read)
 *   events        [{ month, type, subType, open, count }] HiBob's planned events (FCT_HEADCOUNT_EVENT)
 * Months before the current month are HiBob's: employees at month-end, hires and leavers (who joined and left
 * since the previous month-end), and the leave events HiBob has for them. From the current month on: the
 * previous month-end plus HiBob's planned hires minus its leavers; openings planned for earlier months and still
 * open count as hires in the current month. Returns [{ mKey, status, employees, hires, leavers, returns,
 * leaveStarts }] (employees, hires and leavers null where HiBob's employees are missing).
 */
export function headcountSeries({ months, currentMonth, actual, events }) {
  const byMonth = new Map();
  const add = (mKey, kind, n) => {
    const m = byMonth.get(mKey) || { hires: 0, leavers: 0, returns: 0, leaveStarts: 0 };
    m[kind] += n;
    byMonth.set(mKey, m);
  };
  for (const e of events || []) {
    const kind = headcountKind(e.type, e.subType);
    const n = num(e.count);
    if (!kind || !n || !/^\d{4}-\d{2}$/.test(String(e.month))) continue;
    if (e.month >= currentMonth || kind === 'returns' || kind === 'leaveStarts') add(e.month, kind, n);
    else if (kind === 'hires' && e.open) add(currentMonth, 'hires', n);
  }
  const known = actual || {};
  const prev = known[shiftMonth(currentMonth, -1)];
  let employees = prev ? num(prev.employees) : null;
  return (months || []).map((mKey) => {
    const p = byMonth.get(mKey) || { hires: 0, leavers: 0, returns: 0, leaveStarts: 0 };
    if (mKey < currentMonth) {
      const a = known[mKey];
      return {
        mKey, status: 'actual',
        employees: a ? num(a.employees) : null, hires: a ? num(a.hires) : null, leavers: a ? num(a.leavers) : null,
        returns: p.returns, leaveStarts: p.leaveStarts,
      };
    }
    if (employees !== null) employees += p.hires - p.leavers;
    return { mKey, status: mKey === currentMonth ? 'current' : 'forecast', employees, ...p };
  });
}
