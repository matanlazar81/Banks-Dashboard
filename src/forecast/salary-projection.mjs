// ─────────────────────────────────────────────────────────────────────────────
// Salary projection: the forecast months of payroll by GL account, from the actual months and how each
// account is projected. Pure — no I/O — shared by the server (server/salary-projection.cjs, the CLI) and
// the Salary Projection page, which re-projects at once when someone reclassifies an account.
//
//   run-rate   permanent: every forecast month carries the run-rate month (the last closed month)
//   last-year  temporary: the same month of the previous year (actual, or for the next year the current
//              year's actual or forecast), which keeps the seasonality
//   none       actuals only: forecast months are 0
//
// The projection is made in ILS; forecast € = ₪ ÷ ilsPerEur. Actual months keep their own €.
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
    const v = method === 'run-rate' ? num((amount(runRateMonth, acct) || {}).ils)
      : method === 'none' ? 0
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
