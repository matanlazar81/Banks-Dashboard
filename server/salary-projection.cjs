// ─────────────────────────────────────────────────────────────────────────────
// GET /api/salary-projection — the Salary Projection page's one data call.
//
// Payroll by GL account (every 76xxxx account, the P&L "Payroll" line) for the current year (actuals +
// forecast) and the next year, from Snowflake (sf.fetchPayrollByAccount). Each account is either
// permanent (recurring pay and its burden) or temporary (one-offs and seasonal items), per finance's
// classification (SALARY_GL_GROUPS):
//
//   • actual months   — Snowflake FCT_EXPENSE, through the last closed month: the latest month before the
//                       current one whose permanent payroll is at least half the largest of the last 12
//                       months (a just-closed month can be loaded with a token amount only)
//   • permanent       — forecast months carry the last closed month flat (run-rate)
//   • temporary       — forecast months repeat the same month of the previous year (actual, or for the
//                       next year the current year's actual or forecast), which keeps the seasonality
//   • not classified  — any other 76xxxx account with postings: projected like a temporary one, with a warning
//
// The projection is made in ILS, the currency payroll is paid in; forecast € = ₪ ÷ the ILS/EUR ratio of
// the last closed month's permanent payroll. The current month is projected as a whole month (its
// partial postings are not a month's payroll), as on the P&L Projection.
//
// Responses, caching and refresh behave exactly like /api/pnl-projection (createProjectionHandler):
// cache file data/salary-projection-cache.json, SALARY_PROJECTION_TTL_MIN / _TIMEOUT_MIN / _PREWARM
// (default: the CASH_PROJECTION_* values). The page has no plan, so the cache ignores the scenario.
// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');
const cp = require('./cash-projection.cjs');

const ROOT = path.resolve(__dirname, '..');
const SCHEMA_VERSION = 1;
const DEFAULT_CACHE_FILE = path.join(ROOT, 'data', 'salary-projection-cache.json');
const { ProjectionError } = cp;

// Finance's classification (ProfitandLoss988.xlsx: yellow = permanent, green = temporary), in its order.
// Names are fallbacks: the page shows Snowflake's account names when it has them.
const SALARY_GL_GROUPS = [
  {
    key: 'permanent',
    label: 'Permanent',
    accounts: [
      ['760001', 'Gross Salaries'],
      ['760002', 'National Insurance expenses'],
      ['760003', 'Gemel expenses'],
      ['760004', 'Severance expenses'],
      ['760005', 'Zkufut Debit'],
      ['760006', 'Zkufut Credit'],
      ['760007', 'Education fund'],
      ['760009', 'Disability Insurance'],
      ['760018', 'Gross-up expenses'],
      ['760025', 'Salaries payments during military reserve'],
    ],
  },
  {
    key: 'temporary',
    label: 'Temporary',
    accounts: [
      ['760015', 'Recuperation expenses'],
      ['760016', "Employer' tax"],
      ['760017', 'One time payments (Bonus, grants)'],
      ['760019', 'One time payments (Maternity leave benefit)'],
      ['760023', 'Military reserve refund'],
      ['760024', 'National insurance Damage fees'],
      ['760029', 'Lieu of prior notice'],
      ['760030', 'Gemel Pension expenses'],
    ],
  },
];
const GROUP_OF = new Map(SALARY_GL_GROUPS.flatMap((g) => g.accounts.map(([acct]) => [acct, g.key])));

// Below this share of the largest recent month, a closed month counts as not loaded yet.
const MATERIAL_SHARE = 0.5;
// ₪ per € when no payroll month gives a ratio (the engine's live-rate default).
const FALLBACK_ILS_PER_EUR = 3.68;

const pad2 = (n) => String(n).padStart(2, '0');
const cents = (n) => Math.round((Number(n) || 0) * 100) / 100;
const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const mk = (y, mi) => `${y}-${pad2(mi + 1)}`;
const longLabel = (mKey) => new Date(`${mKey}-15T12:00:00`).toLocaleString('en-GB', { month: 'long', year: 'numeric' });

/** The n month keys before (year, monthIdx), most recent last. */
function monthsBefore(year, monthIdx, n) {
  const out = [];
  for (let k = n; k >= 1; k--) {
    const d = new Date(year, monthIdx - k, 1);
    out.push(mk(d.getFullYear(), d.getMonth()));
  }
  return out;
}

/**
 * The payload from Snowflake's payroll rows (pure; the tests call it directly).
 *   rows  [{ month: 'YYYY-MM', acct, name, eur, ils }] for January of the previous year through December
 *   now   Date (drives the current year and month)
 */
function buildSalaryProjection({ rows, now }) {
  const Y = now.getFullYear();
  const T = Y + 1;
  const curIdx = now.getMonth();
  const currentKey = mk(Y, curIdx);

  // Snowflake amounts by month and account, and the account names.
  const actual = {};
  const names = {};
  for (const r of rows || []) {
    const mKey = String(r.month || '').slice(0, 7);
    const acct = String(r.acct || '').trim();
    if (!/^\d{4}-\d{2}$/.test(mKey) || !acct) continue;
    const m = actual[mKey] || (actual[mKey] = {});
    const a = m[acct] || (m[acct] = { eur: 0, ils: 0 });
    a.eur += num(r.eur);
    a.ils += num(r.ils);
    if (r.name && !names[acct]) names[acct] = String(r.name);
  }
  const amount = (mKey, acct) => (actual[mKey] && actual[mKey][acct]) || null;

  // Accounts: every classified one (always shown), then any other 76xxxx account with postings.
  const accounts = [];
  for (const g of SALARY_GL_GROUPS) {
    for (const [acct, name] of g.accounts) accounts.push({ acct, name: names[acct] || name, group: g.key });
  }
  const others = new Set();
  for (const m of Object.values(actual)) {
    for (const [acct, a] of Object.entries(m)) {
      if (!GROUP_OF.has(acct) && (Math.abs(a.ils) >= 1 || Math.abs(a.eur) >= 1)) others.add(acct);
    }
  }
  for (const acct of [...others].sort()) accounts.push({ acct, name: names[acct] || acct, group: 'other' });
  const groupOf = (acct) => GROUP_OF.get(acct) || 'other';
  const permanentAccts = accounts.filter((a) => a.group === 'permanent').map((a) => a.acct);

  // The last closed month: the run-rate month of the permanent accounts.
  const permTotal = (mKey) => permanentAccts.reduce((s, acct) => s + num((amount(mKey, acct) || {}).ils), 0);
  const recent = monthsBefore(Y, curIdx, 12);
  const largest = Math.max(0, ...recent.map(permTotal));
  let lastClosed = null;
  for (const mKey of recent) if (largest > 0 && permTotal(mKey) >= largest * MATERIAL_SHARE) lastClosed = mKey;
  if (!lastClosed) throw new ProjectionError('Snowflake has no payroll for the last 12 months, so there is nothing to project from.');

  const isActual = (mKey) => mKey <= lastClosed && mKey < currentKey;
  const statusOf = (mKey) => (isActual(mKey) ? 'actual' : mKey === currentKey ? 'current' : 'forecast');

  // ₪ per € of forecast months: the last closed month's permanent payroll, else this year's actual payroll.
  const ratioOver = (keys) => {
    let ils = 0;
    let eur = 0;
    for (const mKey of keys) for (const acct of permanentAccts) { const a = amount(mKey, acct); if (a) { ils += a.ils; eur += a.eur; } }
    return eur > 0 && ils > 0 ? ils / eur : null;
  };
  const yearActuals = Array.from({ length: 12 }, (_, mi) => mk(Y, mi)).filter(isActual);
  let ilsPerEur = ratioOver([lastClosed]) || ratioOver(yearActuals);
  const fxFallback = !ilsPerEur;
  if (!ilsPerEur) ilsPerEur = FALLBACK_ILS_PER_EUR;

  // ₪ of one account and month: actual (closed months and the previous year), or projected.
  const ilsMemo = new Map();
  function ilsOf(year, mi, acct) {
    const mKey = mk(year, mi);
    if (year < Y || isActual(mKey)) return num((amount(mKey, acct) || {}).ils);
    const id = `${mKey}|${acct}`;
    if (ilsMemo.has(id)) return ilsMemo.get(id);
    const v = groupOf(acct) === 'permanent' ? num((amount(lastClosed, acct) || {}).ils) : ilsOf(year - 1, mi, acct);
    ilsMemo.set(id, v);
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
      for (const { acct } of accounts) {
        if (status === 'actual') {
          const a = amount(mKey, acct) || { eur: 0, ils: 0 };
          eur[acct] = cents(a.eur);
          ils[acct] = cents(a.ils);
        } else {
          const v = ilsOf(year, mi, acct);
          ils[acct] = cents(v);
          eur[acct] = cents(v / ilsPerEur);
        }
      }
      return { mKey, status, eur, ils };
    }),
  });
  const blocks = [blockOf(Y, 'current'), blockOf(T, 'projection')];

  const warnings = [];
  const gap = Array.from({ length: curIdx }, (_, mi) => mk(Y, mi)).filter((k) => k > lastClosed);
  if (gap.length) {
    warnings.push(`Snowflake has no full payroll for ${gap.map(longLabel).join(', ')} yet, so ${gap.length === 1 ? 'it is' : 'they are'} projected from ${longLabel(lastClosed)}.`);
  }
  if (!Object.keys(actual).some((k) => k.startsWith(`${Y - 1}-`))) {
    warnings.push(`Snowflake has no ${Y - 1} payroll, so temporary accounts have nothing to repeat in ${Y}.`);
  }
  const unclassified = accounts.filter((a) => a.group === 'other');
  if (unclassified.length) {
    warnings.push(`Not classified as permanent or temporary, so projected like a temporary account (same month last year): ${unclassified.map((a) => `${a.acct} ${a.name}`).join(', ')}.`);
  }
  if (fxFallback) warnings.push(`No payroll month gives an ILS/EUR ratio; forecast € use ₪${FALLBACK_ILS_PER_EUR} per €.`);

  const payload = {
    ok: true,
    schemaVersion: SCHEMA_VERSION,
    status: 'ready',
    generatedAt: new Date().toISOString(),
    computedMonth: currentKey,
    company: cp.COMPANY,
    years: [Y, T],
    accounts,
    actuals: { source: 'snowflake', through: lastClosed.startsWith(`${Y}-`) ? lastClosed : null },
    basis: { runRateMonth: lastClosed, ilsPerEur: Math.round(ilsPerEur * 10000) / 10000 },
    degraded: false,
    failedFeeds: [],
    warnings,
    blocks,
  };
  return { payload };
}

/** Compute the payload. opts: now, getSfClient() */
async function computeSalaryProjection(opts) {
  const now = opts.now || new Date();
  const sf = opts.getSfClient();
  if (!sf) throw new ProjectionError('Snowflake is not configured on this server.');
  if (typeof sf.fetchPayrollByAccount !== 'function') throw new ProjectionError('The Snowflake client on this server is out of date; restart the server.');
  const Y = now.getFullYear();
  let rows;
  try {
    rows = await sf.fetchPayrollByAccount(Y - 1, Y);
  } catch (e) {
    console.error(`[salary-projection] Snowflake payroll failed: ${e && e.message}`);
    throw new ProjectionError('Snowflake payroll could not be loaded. Try Refresh in a minute.');
  }
  return buildSalaryProjection({ rows, now });
}

/**
 * Express/connect handler for GET /api/salary-projection. deps:
 *   getSfClient, compute(nowDate), clock(), cacheFile (null = memory only), ttlMs, timeoutMs, …
 */
function createSalaryProjectionHandler(deps = {}) {
  return cp.createProjectionHandler({
    ...deps,
    schemaVersion: SCHEMA_VERSION,
    logTag: 'salary-projection',
    envPrefix: 'SALARY_PROJECTION',
    ignoreScenario: true,
    cacheFile: deps.cacheFile === undefined ? DEFAULT_CACHE_FILE : deps.cacheFile,
    compute: deps.compute || ((nowDate) => computeSalaryProjection({
      now: nowDate,
      getSfClient: deps.getSfClient || cp.defaultGetSfClient,
    })),
  });
}

module.exports = {
  createSalaryProjectionHandler,
  computeSalaryProjection,
  buildSalaryProjection,
  SALARY_GL_GROUPS,
  SCHEMA_VERSION,
  DEFAULT_CACHE_FILE,
};
