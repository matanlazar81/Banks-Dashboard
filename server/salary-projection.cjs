// ─────────────────────────────────────────────────────────────────────────────
// GET /api/salary-projection — the Salary Projection page's one data call — and
// GET / PUT /api/salary-projection/classification — which accounts are permanent or temporary.
//
// Payroll by GL account (every 76xxxx account, the P&L "Payroll" line) for the current year (actuals +
// forecast) and the next year. Each account is projected one of three ways (src/forecast/salary-projection.mjs):
//
//   • run-rate   permanent (recurring pay and its burden): every forecast month carries the last closed month
//   • last-year  temporary (one-offs and seasonal items): the rest of the current year repeats the same month
//                of the previous year; the next year is 0
//   • none       actuals only: forecast months are 0
//
// Finance's classification (SALARY_GL_GROUPS, ACTUALS_ONLY) is the default; the page lets anyone with access
// change an account, saved for everyone in data/salary-classification.json (only the accounts that differ
// from the default). The payload carries the actual months, so the page re-projects at once while someone
// edits; an account with neither a default nor a saved choice is "not classified" (projected like a
// temporary one until someone picks).
//
//   • actual months — NetSuite GL by account and posting period (ns.fetchPnlActuals, the same read as the
//                     P&L Projection and NetSuite's Profit and Loss report), through the last closed month:
//                     the latest month before the current one whose permanent payroll (finance's default
//                     permanent accounts) is at least half the largest of the last 12 months
//
// Snowflake's FCT_EXPENSE misses some payroll accounts (760017 bonus, 760019 maternity), so actuals come
// from NetSuite; `node scripts/salary-projection.cjs --reconcile` shows when Snowflake catches up.
//
// The override file's payroll rows (Snowflake FCT_EXPENSE, SOURCE future_cost_override / _increment, from
// January of the current year, on every payroll account as the old Bank Dashboard: IS_PAYROLL, i.e. 76xxxx)
// travel with the payload, and so do Snowflake's planned hires, leavers and returns from leave on the same
// accounts (FCT_EXPENSE sources 'Open position', 'Terminated', 'Leave end' of its payroll forecast, summed by
// month: no employee detail). Only the ones marked TRUE (IS_SOURCE_ACTUAL) count; FALSE ones are listed, not added.
// The page shows them between permanent and temporary: the planned changes and the increments count in Total
// payroll from the current month on (countedChangeRows / countedIncrementRows in
// src/forecast/salary-projection.mjs); overrides are for reference. The account projection itself (blocks)
// leaves them out; a Snowflake failure only drops them, with a note.
//
// Below the payroll, two more reads travel with the payload; a failure of either only drops it, with a note:
//   • Deel       — NetSuite costs of the vendors named "deel" (ns.fetchVendorSpend), by account and posting
//                  period from January of the previous year, every account but payroll ones (those are already
//                  in Total payroll). Forecast months carry its last six months' average (deelByMonth).
//   • headcount  — HiBob (Snowflake): its employees at each month-end and today from their start and leave dates
//                  (DIM_EMPLOYEE, as the Metrics page; future dates too: signed hires, people leaving), and beside
//                  them, not counted, HiBob's open positions and leave events (FCT_HEADCOUNT_EVENT), as counts by
//                  month (headcountSeries). No names or ids leave the server.
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
const { pathToFileURL } = require('url');
const cp = require('./cash-projection.cjs');
const { readDoc, createDocHandler } = require('./json-store.cjs');

const ROOT = path.resolve(__dirname, '..');
const SCHEMA_VERSION = 12; // 3: the actual months and each account's default method; 4: the override file's payroll rows; 5: temporary 0 next year; 6: override rows on 760001 only; 7: Snowflake's planned hires, leavers and returns from leave; 8: every payroll account, as the old dashboard; 9: planned changes by department (the page's breakdowns); 10: Deel and the headcount (HiBob); 11: the headcount from HiBob's dates plus its open positions, and planned changes count only when marked TRUE (IS_SOURCE_ACTUAL); 12: the headcount is HiBob's employees only (open positions shown, not counted)
const DEFAULT_CACHE_FILE = path.join(ROOT, 'data', 'salary-projection-cache.json');
const CLASSIFICATION_FILE = path.join(ROOT, 'data', 'salary-classification.json');
const { ProjectionError } = cp;
const { employedOn, lastDayOf, sfEmployeeSpans, shiftMonth } = require('./metrics.cjs');

// Finance's classification (ProfitandLoss988.xlsx: yellow = permanent, green = temporary), in its order.
// Names are fallbacks: the page shows NetSuite's account names when it has them.
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
      ['760020', 'Reducing salary expenses-option exercise'],
      ['76003', 'Social security - Injury payment'],
      ['760008', 'Option employees expenses'],
      ['760014', 'Vacation expenses'],
    ],
  },
];
const GROUP_OF = new Map(SALARY_GL_GROUPS.flatMap((g) => g.accounts.map(([acct]) => [acct, g.key])));
// Temporary accounts shown as actuals only (forecast months 0): 2026's military reserve refunds and notice
// pay are not expected to recur; options and vacation are year-end accruals that change every year.
const ACTUALS_ONLY = new Set(['760023', '760029', '760008', '760014']);
// Snowflake's planned payroll changes: FCT_EXPENSE sources of its HR-driven payroll forecast, in table order.
const CHANGE_SOURCES = ['Open position', 'Terminated', 'Leave end'];
// The NetSuite vendors of the Deel section: names (or ids) containing this.
const DEEL_VENDOR = 'deel';

// Below this share of the largest recent month, a closed month counts as not posted yet.
const MATERIAL_SHARE = 0.5;
// ₪ per € when no payroll month gives a ratio (the engine's live-rate default).
const FALLBACK_ILS_PER_EUR = 3.68;
const MAX_SAVED_ACCOUNTS = 300;

const pad2 = (n) => String(n).padStart(2, '0');
const cents = (n) => Math.round((Number(n) || 0) * 100) / 100;
const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const mk = (y, mi) => `${y}-${pad2(mi + 1)}`;
const longLabel = (mKey) => new Date(`${mKey}-15T12:00:00`).toLocaleString('en-GB', { month: 'long', year: 'numeric' });
const isPayroll = (acct) => /^76/.test(acct);
const ACCOUNT_RE = /^76\d{3,6}$/;

let moduleP = null;
function loadSalaryModule() {
  if (!moduleP) {
    moduleP = import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'salary-projection.mjs')).href)
      .catch((e) => { moduleP = null; throw e; });
  }
  return moduleP;
}

/** The n month keys before (year, monthIdx), most recent last. */
function monthsBefore(year, monthIdx, n) {
  const out = [];
  for (let k = n; k >= 1; k--) {
    const d = new Date(year, monthIdx - k, 1);
    out.push(mk(d.getFullYear(), d.getMonth()));
  }
  return out;
}

/** Finance's default for an account: 'run-rate', 'last-year', 'none', or null (not classified). */
function defaultMethodOf(acct) {
  const group = GROUP_OF.get(acct);
  if (!group) return null;
  if (group === 'permanent') return 'run-rate';
  return ACTUALS_ONLY.has(acct) ? 'none' : 'last-year';
}

// ── the saved classification ─────────────────────────────────────────────────
/** PUT body { value: { methods: { [acct]: method } } } → { ok, value, errors }. */
function validateClassification(body) {
  const errors = [];
  const value = body && typeof body === 'object' && !Array.isArray(body) ? body.value : null;
  if (!body || typeof body !== 'object' || Object.keys(body).some((k) => k !== 'value')) errors.push('body: expected { value }');
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some((k) => k !== 'methods')) errors.push('value: expected { methods }');
  const methods = value && value.methods;
  if (!methods || typeof methods !== 'object' || Array.isArray(methods)) {
    errors.push('methods: expected an object of account → method');
    return { ok: false, value: null, errors };
  }
  const entries = Object.entries(methods);
  if (entries.length > MAX_SAVED_ACCOUNTS) errors.push(`methods: at most ${MAX_SAVED_ACCOUNTS} accounts`);
  const out = {};
  for (const [acct, method] of entries) {
    if (!ACCOUNT_RE.test(acct)) { errors.push(`${acct}: not a payroll account number (76xxxx)`); continue; }
    if (!['run-rate', 'last-year', 'none'].includes(method)) { errors.push(`${acct}: method must be run-rate, last-year or none`); continue; }
    out[acct] = method;
  }
  return errors.length ? { ok: false, value: null, errors } : { ok: true, value: { methods: out }, errors: [] };
}

/** The saved choices ({ [acct]: method }), or {} when none were saved. */
function readSavedMethods(file = CLASSIFICATION_FILE) {
  const doc = readDoc(file);
  const v = doc && doc.value ? validateClassification({ value: doc.value }) : null;
  return v && v.ok ? v.value.methods : {};
}

/** GET / PUT /api/salary-projection/classification. deps: file, clock (tests). */
function createSalaryClassificationHandler(deps = {}) {
  return createDocHandler({
    file: deps.file || CLASSIFICATION_FILE,
    label: 'salary-classification',
    validate: validateClassification,
    empty: () => ({ methods: {} }),
    clock: deps.clock,
  });
}

// ── the projection ───────────────────────────────────────────────────────────
/** Payroll rows (costs positive) from NetSuite's P&L actuals (profit-signed, every P&L account). */
function payrollRows(byMonth) {
  const rows = [];
  for (const [month, accts] of Object.entries(byMonth || {})) {
    for (const a of Object.values(accts || {})) {
      const acct = String(a.acct || '');
      if (isPayroll(acct)) rows.push({ month, acct, name: a.name, eur: -num(a.eur), ils: -num(a.ils) });
    }
  }
  return rows;
}

/**
 * The override file's payroll rows for the payload: from January of year Y, sorted by month and account, with
 * ₪ at the forecast rate when the file has € only (ilsEstimated).
 */
function overridesOf(overrides, Y, ilsPerEur) {
  if (!overrides) return { rows: [], error: 'The override file was not read.' };
  const rows = (overrides.rows || [])
    .filter((r) => /^\d{4}-\d{2}$/.test(String(r.month || '')) && String(r.month) >= `${Y}-01`)
    .map((r) => {
      const eur = cents(r.eur);
      const estimated = Math.abs(num(r.ils)) < 0.5 && Math.abs(eur) >= 0.5;
      return {
        month: String(r.month),
        acct: String(r.acct || ''),
        name: String(r.name || ''),
        department: String(r.department || ''),
        mode: r.mode === 'Override' ? 'Override' : 'Increment',
        eur,
        ils: estimated ? cents(eur * ilsPerEur) : cents(r.ils),
        ilsEstimated: estimated,
        memo: String(r.memo || '').slice(0, 300),
      };
    })
    .sort((a, b) => (a.month === b.month ? a.acct.localeCompare(b.acct) : a.month < b.month ? -1 : 1));
  return { rows, error: overrides.error || null };
}

/**
 * Snowflake's planned payroll changes for the payload, by month, account, source and department (the page's
 * breakdowns), with their TRUE / FALSE mark (actual: only TRUE ones count): from January of year Y, sorted that
 * way. When Snowflake has one currency only, the other is at the forecast rate.
 */
function changesOf(changes, Y, ilsPerEur) {
  if (!changes) return { rows: [], error: 'The planned payroll changes were not read.' };
  const order = (r) => CHANGE_SOURCES.indexOf(r.source);
  const rows = (changes.rows || [])
    .filter((r) => /^\d{4}-\d{2}$/.test(String(r.month || '')) && String(r.month) >= `${Y}-01` && CHANGE_SOURCES.includes(r.source))
    .map((r) => {
      let eur = cents(r.eur);
      let ils = cents(r.ils);
      if (Math.abs(ils) < 0.5 && Math.abs(eur) >= 0.5) ils = cents(eur * ilsPerEur);
      else if (Math.abs(eur) < 0.5 && Math.abs(ils) >= 0.5) eur = cents(ils / ilsPerEur);
      return { month: String(r.month), acct: String(r.acct || ''), name: String(r.name || ''), source: r.source, department: String(r.department || ''), actual: r.actual === true, eur, ils };
    })
    .sort((a, b) => (a.month !== b.month ? (a.month < b.month ? -1 : 1) : a.acct !== b.acct ? a.acct.localeCompare(b.acct)
      : order(a) !== order(b) ? order(a) - order(b) : a.department.localeCompare(b.department)));
  return { rows, error: changes.error || null };
}

/**
 * Deel's NetSuite costs for the payload: from January of the previous year (its run-rate can reach back), sorted
 * by month, account and vendor. Payroll accounts (76xxxx) are already in Total payroll, so their rows are left
 * out and the accounts listed in onPayroll.
 */
function deelOf(deel, Y) {
  if (!deel) return { vendors: [], rows: [], onPayroll: [], error: 'Deel was not read.' };
  const names = new Map((deel.vendors || []).map((v) => [Number(v.id), String(v.name || '')]));
  const rows = [];
  const onPayroll = new Map();
  for (const r of deel.rows || []) {
    const month = String(r.month || '');
    const acct = String(r.acct || '');
    if (!/^\d{4}-\d{2}$/.test(month) || month < `${Y - 1}-01` || !acct) continue;
    if (isPayroll(acct)) {
      if (!onPayroll.has(acct)) onPayroll.set(acct, String(r.name || ''));
      continue;
    }
    rows.push({ month, acct, name: String(r.name || ''), vendor: names.get(Number(r.vendor)) || 'Deel', eur: cents(r.eur), ils: cents(r.ils) });
  }
  rows.sort((a, b) => (a.month !== b.month ? (a.month < b.month ? -1 : 1) : a.acct !== b.acct ? a.acct.localeCompare(b.acct) : a.vendor.localeCompare(b.vendor)));
  return {
    vendors: [...new Set(names.values())].filter(Boolean).sort(),
    rows,
    onPayroll: [...onPayroll.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([acct, name]) => ({ acct, name })),
    error: deel.error || null,
  };
}

/**
 * HiBob's headcount for the payload, as counts: employees today, and over the projection's months the employees
 * at month-end with who joined and left, from their start and leave dates (future ones too: signed hires, people
 * leaving), with HiBob's open positions and leave events beside them (headcountSeries). Employee spans never leave
 * the server.
 *   headcount  { company, spans: [{ start, end, type }] | null, events: [{ month, type, subType, employee, status, count }], error }
 */
function headcountOf(headcount, blocks, currentKey, now, headcountSeries) {
  const asOf = `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())}`;
  if (!headcount) return { company: '', asOf, today: null, months: [], events: [], error: 'The headcount (HiBob) was not read.' };
  const spans = headcount.spans || null;
  const months = blocks.flatMap((b) => b.rows.map((r) => r.mKey));
  // Every month shown, and the one before the first (where the first one starts).
  let known = null;
  if (spans && months.length) {
    known = {};
    for (let m = shiftMonth(months[0], -1); m <= months[months.length - 1]; m = shiftMonth(m, 1)) {
      const day = lastDayOf(m);
      const before = lastDayOf(shiftMonth(m, -1));
      const c = { employees: 0, hires: 0, leavers: 0 };
      for (const s of spans) {
        const isIn = employedOn(s, day);
        const wasIn = employedOn(s, before);
        if (isIn) c.employees++;
        if (isIn && !wasIn) c.hires++;
        if (wasIn && !isIn) c.leavers++;
      }
      known[m] = c;
    }
  }
  const events = (headcount.events || [])
    .filter((e) => e && /^\d{4}-\d{2}$/.test(String(e.month || '')) && num(e.count) > 0)
    .map((e) => ({
      month: String(e.month), type: String(e.type || ''), subType: String(e.subType || ''),
      employee: !!e.employee, status: String(e.status || ''), count: num(e.count),
    }));
  return {
    company: String(headcount.company || ''),
    asOf,
    today: spans ? spans.filter((s) => employedOn(s, asOf)).length : null,
    months: headcountSeries({ months, currentMonth: currentKey, known, events }),
    events,
    error: headcount.error || null,
  };
}

/**
 * The payload from payroll rows (the tests call it directly).
 *   rows   [{ month: 'YYYY-MM', acct, name, eur, ils }] costs positive, January of the previous year onwards
 *   now    Date (drives the current year and month)
 *   saved      { [acct]: method } the saved classification (default: none)
 *   overrides  { rows: [{ month, acct, name, department, mode, eur, ils, memo }], error } the override file's payroll
 *              rows (Snowflake), or null when not read
 *   changes    { rows: [{ month, acct, name, source, department, actual, eur, ils }], error } Snowflake's planned
 *              hires, leavers and returns from leave (actual: IS_SOURCE_ACTUAL), or null when not read
 *   deel       { vendors: [{ id, name }], rows: [{ month, acct, name, vendor, eur, ils }], error } Deel's NetSuite
 *              costs (costs positive), or null when not read
 *   headcount  { company, spans: [{ start, end, type }] | null, events: [{ month, type, subType, employee, status,
 *              count }], error } HiBob's employees and planned events, or null when not read
 */
async function buildSalaryProjection({ rows, now, saved = {}, overrides = null, changes = null, deel = null, headcount = null }) {
  const { projectSalary, groupOfMethod, headcountSeries } = await loadSalaryModule();
  const Y = now.getFullYear();
  const T = Y + 1;
  const curIdx = now.getMonth();
  const currentKey = mk(Y, curIdx);

  // Amounts by month and account, and the account names.
  const amounts = {};
  const names = {};
  for (const r of rows || []) {
    const mKey = String(r.month || '').slice(0, 7);
    const acct = String(r.acct || '').trim();
    if (!/^\d{4}-\d{2}$/.test(mKey) || !acct) continue;
    const m = amounts[mKey] || (amounts[mKey] = {});
    const a = m[acct] || (m[acct] = { eur: 0, ils: 0 });
    a.eur += num(r.eur);
    a.ils += num(r.ils);
    if (r.name && !names[acct]) names[acct] = String(r.name);
  }
  const amount = (mKey, acct) => (amounts[mKey] && amounts[mKey][acct]) || null;

  // Accounts: every classified one (always shown), then any other 76xxxx account with postings.
  const list = [];
  for (const g of SALARY_GL_GROUPS) for (const [acct, name] of g.accounts) list.push({ acct, name: names[acct] || name });
  const others = new Set();
  for (const m of Object.values(amounts)) {
    for (const [acct, a] of Object.entries(m)) {
      if (!GROUP_OF.has(acct) && (Math.abs(a.ils) >= 1 || Math.abs(a.eur) >= 1)) others.add(acct);
    }
  }
  for (const acct of [...others].sort()) list.push({ acct, name: names[acct] || acct });
  const accounts = list.map(({ acct, name }) => {
    const defaultMethod = defaultMethodOf(acct);
    const chosen = saved[acct] || defaultMethod;
    return { acct, name, defaultMethod, method: chosen || 'last-year', group: chosen ? groupOfMethod(chosen) : 'other' };
  });
  const methodOf = new Map(accounts.map((a) => [a.acct, a.method]));

  // The last closed month (the run-rate month), from finance's default permanent accounts so that
  // reclassifying an account never moves it.
  const basisAccts = SALARY_GL_GROUPS[0].accounts.map(([acct]) => acct);
  const permTotal = (mKey) => basisAccts.reduce((s, acct) => s + num((amount(mKey, acct) || {}).ils), 0);
  const recent = monthsBefore(Y, curIdx, 12);
  const largest = Math.max(0, ...recent.map(permTotal));
  let lastClosed = null;
  for (const mKey of recent) if (largest > 0 && permTotal(mKey) >= largest * MATERIAL_SHARE) lastClosed = mKey;
  if (!lastClosed) throw new ProjectionError('NetSuite has no payroll for the last 12 months, so there is nothing to project from.');
  const isActual = (mKey) => mKey.startsWith(`${Y - 1}-`) || (mKey.startsWith(`${Y}-`) && mKey <= lastClosed && mKey < currentKey);

  // ₪ per € of forecast months: the last closed month's permanent payroll, else this year's actual payroll.
  const ratioOver = (keys) => {
    let ils = 0;
    let eur = 0;
    for (const mKey of keys) for (const acct of basisAccts) { const a = amount(mKey, acct); if (a) { ils += a.ils; eur += a.eur; } }
    return eur > 0 && ils > 0 ? ils / eur : null;
  };
  const yearActuals = Array.from({ length: 12 }, (_, mi) => mk(Y, mi)).filter(isActual);
  let ilsPerEur = ratioOver([lastClosed]) || ratioOver(yearActuals);
  const fxFallback = !ilsPerEur;
  if (!ilsPerEur) ilsPerEur = FALLBACK_ILS_PER_EUR;
  ilsPerEur = Math.round(ilsPerEur * 10000) / 10000;

  // The actual months the page re-projects from: the previous year and this year's closed months (never the
  // current month's partial postings), every listed account, rounded to cents.
  const actual = {};
  for (const mKey of Object.keys(amounts).sort()) {
    if (!isActual(mKey)) continue;
    const m = {};
    for (const { acct } of accounts) {
      const a = amount(mKey, acct);
      if (a && (a.eur || a.ils)) m[acct] = { eur: cents(a.eur), ils: cents(a.ils) };
    }
    actual[mKey] = m;
  }

  const blocks = projectSalary({
    years: [Y, T], currentMonth: currentKey, runRateMonth: lastClosed, ilsPerEur,
    accounts: accounts.map((a) => a.acct), actual, methodOf: (acct) => methodOf.get(acct) || 'last-year',
  });

  const warnings = [];
  const gap = Array.from({ length: curIdx }, (_, mi) => mk(Y, mi)).filter((k) => k > lastClosed);
  if (gap.length) {
    warnings.push(`NetSuite has no full payroll for ${gap.map(longLabel).join(', ')} yet, so ${gap.length === 1 ? 'it is' : 'they are'} projected from ${longLabel(lastClosed)}.`);
  }
  if (!Object.keys(amounts).some((k) => k.startsWith(`${Y - 1}-`))) {
    warnings.push(`NetSuite has no ${Y - 1} payroll, so temporary accounts have nothing to repeat in ${Y}.`);
  }
  if (fxFallback) warnings.push(`No payroll month gives an ILS/EUR ratio; forecast € use ₪${FALLBACK_ILS_PER_EUR} per €.`);
  if (changes && changes.error) warnings.push(changes.error);
  if (deel && deel.error) warnings.push(deel.error);
  else if (deel && !(deel.vendors || []).length) warnings.push(`NetSuite has no vendor named "${DEEL_VENDOR}", so the Deel section is empty.`);
  if (headcount && headcount.error) warnings.push(headcount.error);

  const payload = {
    ok: true,
    schemaVersion: SCHEMA_VERSION,
    status: 'ready',
    generatedAt: new Date().toISOString(),
    computedMonth: currentKey,
    company: cp.COMPANY,
    years: [Y, T],
    accounts,
    actuals: { source: 'netsuite', basis: 'period', through: lastClosed.startsWith(`${Y}-`) ? lastClosed : null },
    basis: { runRateMonth: lastClosed, ilsPerEur },
    actual,
    overrides: overridesOf(overrides, Y, ilsPerEur),
    changes: changesOf(changes, Y, ilsPerEur),
    deel: deelOf(deel, Y),
    headcount: headcountOf(headcount, blocks, currentKey, now, headcountSeries),
    degraded: false,
    failedFeeds: [],
    warnings,
    blocks,
  };
  return { payload };
}

/** The override file's payroll rows from Snowflake: { rows, error }; never throws (the page works without them). */
async function readOverrides(getSfClient, Y) {
  let sf = null;
  try { sf = getSfClient ? getSfClient() : null; } catch { sf = null; }
  if (!sf || typeof sf.fetchPayrollOverrides !== 'function') {
    return { rows: [], error: 'Snowflake is not configured on this server, so the override file is not shown.' };
  }
  try {
    return { rows: await sf.fetchPayrollOverrides(Y), error: null };
  } catch (e) {
    console.error(`[salary-projection] override file failed: ${e && e.message}`);
    return { rows: [], error: 'The override file could not be read from Snowflake. Try Refresh in a minute.' };
  }
}

/** Snowflake's planned hires, leavers and returns from leave: { rows, error }; never throws. */
async function readChanges(getSfClient, Y) {
  let sf = null;
  try { sf = getSfClient ? getSfClient() : null; } catch { sf = null; }
  if (!sf || typeof sf.fetchPayrollChanges !== 'function') {
    return { rows: [], error: 'Snowflake is not configured on this server, so planned hires and leavers are not in Total payroll.' };
  }
  try {
    return { rows: await sf.fetchPayrollChanges(Y), error: null };
  } catch (e) {
    console.error(`[salary-projection] planned payroll changes failed: ${e && e.message}`);
    return { rows: [], error: 'Planned hires and leavers could not be read from Snowflake, so Total payroll leaves them out. Try Refresh in a minute.' };
  }
}

/**
 * HiBob's employees (start and leave dates, no names) and planned events of the Metrics page's company
 * (METRICS_HEADCOUNT_COMPANY, default LSports): { company, spans, events, error }; never throws.
 */
async function readHeadcount(getSfClient, Y) {
  const company = process.env.METRICS_HEADCOUNT_COMPANY || 'LSports';
  let sf = null;
  try { sf = getSfClient ? getSfClient() : null; } catch { sf = null; }
  if (!sf || typeof sf.query !== 'function' || typeof sf.fetchHeadcountPlan !== 'function') {
    return { company, spans: null, events: [], error: 'Snowflake is not configured on this server, so the headcount (HiBob) is not shown.' };
  }
  const [spans, events] = await Promise.all([
    sfEmployeeSpans(sf, company).catch((e) => { console.error(`[salary-projection] HiBob employees failed: ${e && e.message}`); return null; }),
    sf.fetchHeadcountPlan(Y, company).catch((e) => { console.error(`[salary-projection] HiBob planned events failed: ${e && e.message}`); return null; }),
  ]);
  const failed = [!spans && 'employees', !events && 'planned events'].filter(Boolean);
  return {
    company,
    spans,
    events: events || [],
    error: failed.length ? `HiBob's ${failed.join(' and ')} could not be read from Snowflake, so the headcount is incomplete. Try Refresh in a minute.` : null,
  };
}

/** Deel's NetSuite costs, January of the previous year to December of this one: { vendors, rows, error }; never throws. */
async function readDeel(ns, queue, Y) {
  if (!ns || typeof ns.fetchVendorSpend !== 'function') {
    return { vendors: [], rows: [], error: 'The NetSuite client on this server cannot read Deel yet, so the Deel section is empty.' };
  }
  try {
    const { vendors, rows } = await queue(() => ns.fetchVendorSpend({ name: DEEL_VENDOR, fromYear: Y - 1, toYear: Y }));
    return { vendors, rows, error: null };
  } catch (e) {
    console.error(`[salary-projection] Deel failed: ${e && e.message}`);
    return { vendors: [], rows: [], error: 'Deel could not be read from NetSuite, so its section is empty. Try Refresh in a minute.' };
  }
}

/** Compute the payload. opts: now, getNsClient(sub), getSfClient(), queueNsCall(fn), classificationFile (tests) */
async function computeSalaryProjection(opts) {
  const now = opts.now || new Date();
  // The repo .env, as the other projections load it (existing variables win).
  try { require('dotenv').config({ path: path.join(ROOT, '.env'), quiet: true }); } catch { /* env may already be exported */ }
  if (!process.env.NETSUITE_ACCOUNT_ID) throw new ProjectionError('NetSuite is not configured on this server.');
  const sub = parseInt(process.env.NET_CASH_SUBSIDIARY || '3', 10) || 3;
  const ns = opts.getNsClient(sub);
  if (!ns) throw new ProjectionError('The NetSuite client is unavailable on this server.');
  const queue = opts.queueNsCall || ((fn) => fn());
  const Y = now.getFullYear();
  const [actuals, overrides, changes, deel, headcount] = await Promise.all([
    queue(() => ns.fetchPnlActuals({ fromYear: Y - 1, toYear: Y, basis: 'period' })).catch((e) => {
      console.error(`[salary-projection] NetSuite payroll failed: ${e && e.message}`);
      return null;
    }),
    readOverrides(opts.getSfClient, Y),
    readChanges(opts.getSfClient, Y),
    readDeel(ns, queue, Y),
    readHeadcount(opts.getSfClient, Y),
  ]);
  if (!actuals || !actuals.byMonth) throw new ProjectionError('NetSuite payroll could not be loaded. Try Refresh in a minute.');
  const saved = readSavedMethods(opts.classificationFile || CLASSIFICATION_FILE);
  return buildSalaryProjection({ rows: payrollRows(actuals.byMonth), now, saved, overrides, changes, deel, headcount });
}

/**
 * Express/connect handler for GET /api/salary-projection. deps:
 *   getNsClient, getSfClient, queueNsCall, compute(nowDate), clock(), cacheFile (null = memory only), ttlMs, timeoutMs, …
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
      getNsClient: deps.getNsClient || cp.defaultGetNsClient,
      getSfClient: deps.getSfClient || cp.defaultGetSfClient,
      queueNsCall: deps.queueNsCall || cp.defaultQueueNsCall,
    })),
  });
}

module.exports = {
  createSalaryProjectionHandler,
  createSalaryClassificationHandler,
  computeSalaryProjection,
  buildSalaryProjection,
  validateClassification,
  readSavedMethods,
  defaultMethodOf,
  payrollRows,
  overridesOf,
  changesOf,
  deelOf,
  headcountOf,
  loadSalaryModule,
  SALARY_GL_GROUPS,
  ACTUALS_ONLY,
  CHANGE_SOURCES,
  DEEL_VENDOR,
  SCHEMA_VERSION,
  DEFAULT_CACHE_FILE,
  CLASSIFICATION_FILE,
};
