// ─────────────────────────────────────────────────────────────────────────────
// GET /api/cash-projection — the New Bank Dashboard's one data call.
//
// Returns the finished monthly projection for the current year (actuals + forecast) and the next
// year (rolled forward from this December's closing), for two variants: the official plan (the
// scenario the nightly net-cash job uses) and the base forecast (no scenario adjustments).
// The browser only renders.
//
// Same logic as the old dashboard:
//   • inputs   — gatherInputs() from scripts/net-cash-forecast-compute.cjs, the nightly job's own
//                server-side assembly of every engine input
//   • engine   — computeCashflowForecast() from src/forecast/forecast-core.mjs (shared with the
//                browser and the nightly job); basis forced to Pipeline + Last-Actual like the
//                nightly, so the plan's December closing equals the official net-cash figure
//   • next year — src/forecast/roll-forward.mjs, the port of the old browser-only roll-forward
//
// Responses (always JSON, Cache-Control: no-store):
//   200 { ok:true,  status:'ready', … }          projection (possibly stale while it refreshes)
//   202 { ok:true,  status:'computing', … }      first computation running; poll again (Retry-After)
//   200 { ok:false, status:'error', error }      nothing cached and the last attempt failed
//   405                                           anything but GET
// ?refresh=true starts a recompute (at most once a minute) and still answers with the cached data.
//
// Caching: one entry, persisted to data/cash-projection-cache.json (also written by
// `node scripts/cash-projection.cjs --write-cache`). Fresh for CASH_PROJECTION_TTL_MIN (default 30)
// and within the same calendar month; after that it is served stale while one background recompute
// runs. Concurrent requests share one computation. NetSuite calls go through the shared queue so
// this never competes with the old dashboard for NetSuite's concurrency limit.
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');
const { captureDetails } = require('./cash-projection-breakdown.cjs');
const { loadProjectionInputs, wrapClient } = require('./projection-inputs.cjs');

const ROOT = path.resolve(__dirname, '..');
// 2: balances are cash in the bank with a separate `dividend` figure (1 was the operating view).
// 3: the entry also carries server-only `details` for GET /api/cash-projection/breakdown.
const SCHEMA_VERSION = 3;
const COMPANY = 'lsports';
const DEFAULT_SCENARIO = 'Exit plan June26';
const MIN = 60 * 1000;
const DEFAULT_CACHE_FILE = path.join(ROOT, 'data', 'cash-projection-cache.json');

// Errors whose message is safe to show to dashboard users. Anything else is logged server-side and
// replaced by a generic message.
class ProjectionError extends Error {}

function envMinutes(name, fallback) {
  const v = parseFloat(process.env[name] || '');
  return Number.isFinite(v) && v > 0 ? v : fallback;
}
const pad2 = (n) => String(n).padStart(2, '0');
const monthKeyOf = (ms) => { const d = new Date(ms); return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}`; };
const cents = (n) => Math.round((Number(n) || 0) * 100) / 100;

let modulesP = null;
function loadForecastModules() {
  if (!modulesP) {
    const load = (file) => import(pathToFileURL(path.join(ROOT, 'src', 'forecast', file)).href);
    modulesP = Promise.all([load('forecast-core.mjs'), load('roll-forward.mjs')])
      .then(([core, rf]) => ({ computeCashflowForecast: core.computeCashflowForecast, rf }))
      .catch((e) => { modulesP = null; throw e; });
  }
  return modulesP;
}

const FEED_LABELS = {
  'ns.fetchBankBalance': 'NetSuite bank balance',
  'ns.fetchBankAccountListAsOf': 'NetSuite month-end bank balances',
  'ns.fetchBankClassifiedYearly': 'NetSuite bank-classified actuals',
  'ns.fetchSalaryData': 'NetSuite payroll actuals',
  'ns.fetchDividendDistributions': 'NetSuite dividend distributions',
  'ns.fetchMonthlyRevaluation': 'NetSuite FX revaluation',
  'ns.fetchCustomerCashReceipts': 'NetSuite customer receipts',
  'ns.fetchRevenueActuals': 'NetSuite revenue actuals',
  'ns.fetchVendorActuals': 'NetSuite vendor actuals',
  'ns.fetchPaidVendorsYearly': 'NetSuite paid vendor bills',
  'ns.fetchCurrencyDefenseBudget': 'NetSuite currency-defense budget',
  'ns.suiteqlAll': 'NetSuite collections query',
  'sf.fetchSalaryActualsByDept': 'Snowflake salary actuals by department',
  'sf.fetchSalaryBudget': 'Snowflake salary budget',
  'sf.fetchBudgetByCategory': 'Snowflake vendor budget',
  'sf.fetchMonthlyRevenuePaid': 'Snowflake revenue',
  'sf.fetchPipelineMethodology': 'Snowflake pipeline methodology',
  'sf.fetchSalaryBudgetBreakdown': 'Snowflake salary budget by department',
};

function lastDayOfPreviousMonth(now) {
  const d = new Date(now.getFullYear(), now.getMonth(), 0);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

/**
 * Compute the full payload. opts:
 *   now, getNsClient(sub), getSfClient(), queueNsCall(fn)  — required in practice
 *   computeModule      — override for scripts/net-cash-forecast-compute.cjs (tests)
 *   snapshotFile       — parsed data/budgets/<year>-lsports.json: build the projection year from it
 *                        (old-dashboard parity runs) instead of from live data
 *   includeRaw         — also return the raw engine rows and inputs (CLI)
 */
async function computeCashProjection(opts) {
  const now = opts.now || new Date();
  const cmp = opts.computeModule || require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs'));
  const { computeCashflowForecast, rf } = await loadForecastModules();

  // Checked after requiring the compute module, which loads the repo .env.
  if (!process.env.NETSUITE_ACCOUNT_ID) throw new ProjectionError('NetSuite is not configured on this server.');
  const nsClient = opts.getNsClient(cmp.SUBSIDIARY);
  const sfClient = opts.getSfClient();
  if (!nsClient) throw new ProjectionError('The NetSuite client is unavailable on this server.');
  if (!sfClient) throw new ProjectionError('Snowflake is not configured on this server.');

  const Y = now.getFullYear();
  const T = Y + 1;
  const scenarioName = process.env.NET_CASH_SCENARIO_NAME || DEFAULT_SCENARIO;

  // Shared with the P&L Projection: one NetSuite/Snowflake pull serves both pages (read-only here).
  const { inputs, meta, extras, scenario, failures } = await loadProjectionInputs({
    now, cmp, nsClient, sfClient, queueNsCall: opts.queueNsCall, scenarioName,
  });

  // Without any opening anchor the whole table would start from 0 — report instead of showing it.
  if (!inputs.yearStartBalance && !inputs.book) {
    throw new ProjectionError('NetSuite bank balances are unavailable, so there is no opening balance to project from.');
  }

  const snapshot = opts.snapshotFile
    ? rf.snapshotFieldsFromFile(opts.snapshotFile)
    : rf.snapshotFieldsFromLive({
      sourceYear: Y,
      targetYear: T,
      srcInputs: inputs,
      rawSfBudget: extras.rawSfBudget || { byMonth: (inputs.sfBudget || {}).byMonth || {} },
      rawSfSalaryBudget: extras.rawSfSalaryBudget || inputs.sfSalaryBudget || {},
      now,
    });

  const planData = (scenario && scenario.data) || {};
  const planLoaded = Object.keys(planData).length > 0;
  const variants = {};
  const raw = {};
  let details = null; // server-only, for the breakdown endpoint
  for (const [variant, sd] of [['plan', planData], ['base', {}]]) {
    // Current year: exactly the nightly compute's engine inputs (net-cash-forecast-compute.cjs main()).
    const inputsY = {
      ...inputs,
      ...cmp.scenarioKnobs(sd, Y),
      activeYear: Y,
      currentYear: Y,
      now,
      asOfDate: null,
      lastActualSalaryMonth: meta.lastActualSalaryMonth,
      ilsRevalRate: cmp.ILS_REVAL_RATE,
    };
    const rowsY = computeCashflowForecast(inputsY);

    // Next year: rolled forward from rowsY.
    const salaryBasis = rf.synthesizeSalaryBasis(extras.breakdowns, rowsY, Y);
    const knobsT = {
      ...cmp.scenarioKnobs(sd, T),
      ...rf.inheritProjectionMaps(sd, T, Y),
      revenueMethodology: 'pipeline',
      salaryProjectionMode: 'lastActual',
    };
    const inputsT = rf.buildNextYearInputs({
      sourceYear: Y, targetYear: T, srcInputs: inputsY, srcRows: rowsY, snapshot, salaryBasis,
      knobs: knobsT, now, ilsRevalRate: cmp.ILS_REVAL_RATE,
    });
    const rowsT = computeCashflowForecast(inputsT);

    const dec = rowsY[11];
    // This year's dividends are still inside every next-year balance (it opens at the operating-view
    // December closing); shapeYear takes them out so both years show cash in the bank.
    const divY = rowsY.reduce((s, r) => ({ eur: s.eur + (r.dividendExcluded || 0), ils: s.ils + (r.dividendExcludedILS || 0) }), { eur: 0, ils: 0 });
    const decBank = { eur: dec.closingBalance - divY.eur, ils: dec.closingBalanceILS - divY.ils };
    variants[variant] = {
      years: [
        rf.shapeYear(rowsY, { year: Y, kind: 'current' }),
        rf.shapeYear(rowsT, { year: T, kind: 'projection', prevClosing: decBank, dividendCarry: divY }),
      ],
      rollForward: {
        from: `${Y}-12`,
        to: `${T}-01`,
        closing: { eur: cents(decBank.eur), ils: cents(decBank.ils) },
        opening: { eur: cents(rowsT[0].openingBalance - divY.eur), ils: cents(rowsT[0].openingBalanceILS - divY.ils) },
        source: opts.snapshotFile ? 'snapshot-file' : 'live',
        salaryBasis: salaryBasis
          ? { method: 'run-rate', monthsWithData: salaryBasis.monthsWithData, scale: salaryBasis.scale }
          : { method: 'flat-budget', monthsWithData: 0, scale: null },
      },
    };
    if (opts.includeRaw) raw[variant] = { rows: { [Y]: rowsY, [T]: rowsT }, inputs: { [Y]: inputsY, [T]: inputsT } };
    details = captureDetails(details, { variant, Y, T, rowsY, rowsT, inputsY, inputsT });
  }

  const warnings = [];
  if (!planLoaded) warnings.push(`The plan "${scenarioName}" was not found, so Plan shows the base forecast.`);
  if (!inputs.yearStartBalance) warnings.push(`No NetSuite bank balance for 31 Dec ${Y - 1}; January opens from the bank-balance fallback.`);
  if (!inputs.prevMonthEndBalance) warnings.push('No NetSuite bank balance for the previous month-end; the current month is not anchored to the bank.');
  if (!meta.lastActualSalaryMonth) warnings.push('No closed payroll month to project salary from; salary falls back to the budget.');
  if (!extras.breakdowns.some((b) => Array.isArray(b) && b.length > 0)) {
    warnings.push(`No Oct–Dec ${Y} payroll budget by department; ${T} salary uses the flat budget average.`);
  }
  const failedFeeds = [...new Set(failures)];
  if (failedFeeds.length) {
    warnings.push(`Some data sources failed and fell back to defaults: ${failedFeeds.map((f) => FEED_LABELS[f] || f).join(', ')}.`);
  }

  const src = String((scenario && scenario.source) || '');
  const payload = {
    ok: true,
    schemaVersion: SCHEMA_VERSION,
    status: 'ready',
    generatedAt: new Date().toISOString(),
    computedMonth: `${Y}-${pad2(now.getMonth() + 1)}`,
    company: COMPANY,
    years: [Y, T],
    plan: { name: scenarioName, loaded: planLoaded, source: !planLoaded ? 'none' : src.startsWith('Postgres') ? 'postgres' : 'file' },
    bankToday: inputs.prevMonthEndBalance
      ? { eur: cents(inputs.prevMonthEndBalance.eur), ils: cents(inputs.prevMonthEndBalance.ils), asOf: lastDayOfPreviousMonth(now) }
      : null,
    degraded: failedFeeds.length > 0,
    failedFeeds,
    warnings,
    variants,
  };
  return opts.includeRaw ? { payload, details, raw } : { payload, details };
}

// ── cache file ──────────────────────────────────────────────────────────────
// `details` (optional) stays server-side: the handler only ever sends `payload`.
function makeEntry(payload, nowMs, details = null, schemaVersion = SCHEMA_VERSION) {
  return {
    schemaVersion,
    company: payload.company,
    year: payload.years[0],
    scenarioName: payload.plan.name,
    computedMonth: payload.computedMonth,
    generatedAtMs: nowMs,
    payload: { ...payload, generatedAt: new Date(nowMs).toISOString() },
    ...(details ? { details } : {}),
  };
}

function writeCacheEntry(file, entry) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify({ schemaVersion: entry.schemaVersion, entry }));
  fs.renameSync(tmp, file); // atomic: readers never see a half-written file
}

function readCacheEntry(file, schemaVersion = SCHEMA_VERSION) {
  try {
    const saved = JSON.parse(fs.readFileSync(file, 'utf-8'));
    if (saved && saved.schemaVersion === schemaVersion && saved.entry && saved.entry.payload) return saved.entry;
  } catch { /* missing or unreadable — treated as no cache */ }
  return null;
}

// ── default clients for a standalone mount (finance-it-backend without the shared API module) ──
let nsDefault = null;
let sfDefault;
function defaultGetNsClient(sub) {
  if (!nsDefault) nsDefault = require(path.join(ROOT, 'netsuite-api.cjs')).createNetSuiteClient(process.env, sub);
  return nsDefault;
}
function defaultGetSfClient() {
  if (sfDefault === undefined) sfDefault = require(path.join(ROOT, 'snowflake-api.cjs')).createSnowflakeClient(process.env);
  return sfDefault;
}
let defaultQueueTail = Promise.resolve();
function defaultQueueNsCall(fn) {
  const next = defaultQueueTail.then(fn, fn);
  defaultQueueTail = next.catch(() => {});
  return next;
}

/**
 * The cached, background-refreshed GET handler shared by the projection pages. spec:
 *   compute(nowDate) → { payload, details? }   (required)
 *   schemaVersion, cacheFile (null = memory only), logTag ('cash-projection'), envPrefix ('CASH_PROJECTION':
 *   reads <prefix>_TTL_MIN, _TIMEOUT_MIN, _PREWARM, falling back to the CASH_PROJECTION_* values)
 *   clock() → ms, ttlMs, timeoutMs, refreshCooldownMs, failureBackoffMs
 */
function createProjectionHandler(spec) {
  const { compute, schemaVersion } = spec;
  const logTag = spec.logTag || 'cash-projection';
  const prefix = spec.envPrefix || 'CASH_PROJECTION';
  const clock = spec.clock || Date.now;
  const ttlMs = spec.ttlMs ?? envMinutes(`${prefix}_TTL_MIN`, envMinutes('CASH_PROJECTION_TTL_MIN', 30)) * MIN;
  const timeoutMs = spec.timeoutMs ?? envMinutes(`${prefix}_TIMEOUT_MIN`, envMinutes('CASH_PROJECTION_TIMEOUT_MIN', 10)) * MIN;
  const refreshCooldownMs = spec.refreshCooldownMs ?? MIN;
  const failureBackoffMs = spec.failureBackoffMs ?? 2 * MIN;
  const cacheFile = spec.cacheFile ?? null;
  const prewarm = (process.env[`${prefix}_PREWARM`] ?? process.env.CASH_PROJECTION_PREWARM) === '1';

  const state = { entry: null, fileMtimeMs: 0, inflight: null, inflightStartedMs: 0, lastStartMs: -Infinity, lastFailureMs: -Infinity, lastError: null };

  const scenarioName = () => process.env.NET_CASH_SCENARIO_NAME || DEFAULT_SCENARIO;
  const isUsable = (entry, nowMs) => !!entry
    && entry.schemaVersion === schemaVersion
    && entry.company === COMPANY
    && entry.year === new Date(nowMs).getFullYear()
    && entry.scenarioName === scenarioName();

  // Pick up a cache file written by another process (the CLI's --write-cache) or a previous run.
  function hydrateFromDisk() {
    if (!cacheFile) return;
    let mtimeMs;
    try { mtimeMs = fs.statSync(cacheFile).mtimeMs; } catch { return; }
    if (mtimeMs <= state.fileMtimeMs) return;
    state.fileMtimeMs = mtimeMs;
    const entry = readCacheEntry(cacheFile, schemaVersion);
    if (entry && (!state.entry || entry.generatedAtMs > state.entry.generatedAtMs)) state.entry = entry;
  }

  function persist(entry) {
    if (!cacheFile) return;
    try {
      writeCacheEntry(cacheFile, entry);
      state.fileMtimeMs = fs.statSync(cacheFile).mtimeMs;
    } catch (e) {
      console.warn(`[${logTag}] cache write failed: ${e.message}`);
    }
  }

  function startCompute() {
    const startedMs = clock();
    state.lastStartMs = startedMs;
    state.inflightStartedMs = startedMs;
    let timer = null;
    const timedOut = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new ProjectionError('Computing the projection took too long and was abandoned.')), timeoutMs);
      if (timer.unref) timer.unref();
    });
    const run = Promise.race([Promise.resolve().then(() => compute(new Date(startedMs))), timedOut]);
    const p = run
      .then((result) => {
        const payload = result && result.payload;
        if (!payload || payload.status !== 'ready') throw new Error('compute returned no payload');
        const entry = makeEntry(payload, clock(), result.details, schemaVersion);
        state.entry = entry;
        state.lastError = null;
        state.lastFailureMs = -Infinity;
        persist(entry);
      })
      .catch((e) => {
        state.lastFailureMs = clock();
        state.lastError = e instanceof ProjectionError ? e.message : 'The projection could not be computed. The server log has the details.';
        console.error(`[${logTag}] compute failed: ${e && e.stack ? e.stack : e}`);
      })
      .finally(() => {
        clearTimeout(timer);
        if (state.inflight === p) state.inflight = null;
      });
    state.inflight = p;
  }

  function maybeStart(nowMs, forced) {
    if (state.inflight) return;
    if (forced ? nowMs - state.lastStartMs < refreshCooldownMs : nowMs - state.lastFailureMs < failureBackoffMs) return;
    startCompute();
  }

  function send(res, status, body, headers) {
    res.statusCode = status;
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'no-store');
    for (const [k, v] of Object.entries(headers || {})) res.setHeader(k, v);
    res.end(JSON.stringify(body));
  }

  function handler(req, res) {
    if ((req.method || 'GET').toUpperCase() !== 'GET') {
      send(res, 405, { ok: false, status: 'error', error: 'Method not allowed' }, { Allow: 'GET' });
      return;
    }
    let refresh = false;
    try {
      const r = new URL(req.url || '', 'http://localhost').searchParams.get('refresh');
      refresh = r === 'true' || r === '1';
    } catch { /* malformed URL — treat as a plain read */ }

    const nowMs = clock();
    hydrateFromDisk();
    const entry = isUsable(state.entry, nowMs) ? state.entry : null;
    if (entry) {
      const ageMs = Math.max(0, nowMs - entry.generatedAtMs);
      const monthRolled = entry.computedMonth !== monthKeyOf(nowMs);
      const stale = monthRolled || ageMs >= ttlMs;
      if (refresh || stale) maybeStart(nowMs, refresh);
      send(res, 200, {
        ...entry.payload,
        cache: {
          ageSec: Math.round(ageMs / 1000),
          stale,
          staleReason: monthRolled ? 'month-rollover' : stale ? 'ttl' : null,
          refreshing: !!state.inflight,
          lastError: state.lastError,
        },
      });
      return;
    }

    maybeStart(nowMs, false);
    if (state.inflight) {
      send(res, 202, {
        ok: true,
        status: 'computing',
        startedAt: new Date(state.inflightStartedMs).toISOString(),
        elapsedSec: Math.max(0, Math.round((nowMs - state.inflightStartedMs) / 1000)),
      }, { 'Retry-After': '5' });
      return;
    }
    send(res, 200, {
      ok: false,
      status: 'error',
      error: state.lastError || 'The projection is not available yet.',
      retryAfterSec: Math.max(0, Math.ceil((state.lastFailureMs + failureBackoffMs - nowMs) / 1000)),
    });
  }

  // Resolves once no computation is running (tests, CLI).
  handler.idle = () => state.inflight || Promise.resolve();

  // Opt-in warm-up after start so the first visitor doesn't wait (no timers otherwise).
  if (prewarm) {
    const t = setTimeout(() => {
      hydrateFromDisk();
      if (!isUsable(state.entry, clock())) maybeStart(clock(), false);
    }, 30 * 1000);
    if (t.unref) t.unref();
  }

  return handler;
}

/**
 * Express/connect handler for GET /api/cash-projection. deps (all optional):
 *   getNsClient, getSfClient, queueNsCall — share the API module's clients and NetSuite queue
 *   compute(nowDate) → { payload }        — override the computation (tests)
 *   clock() → ms, cacheFile (null = memory only), ttlMs, timeoutMs, refreshCooldownMs, failureBackoffMs
 */
function createCashProjectionHandler(deps = {}) {
  return createProjectionHandler({
    ...deps,
    schemaVersion: SCHEMA_VERSION,
    logTag: 'cash-projection',
    envPrefix: 'CASH_PROJECTION',
    cacheFile: deps.cacheFile === undefined ? DEFAULT_CACHE_FILE : deps.cacheFile,
    compute: deps.compute || ((nowDate) => computeCashProjection({
      now: nowDate,
      getNsClient: deps.getNsClient || defaultGetNsClient,
      getSfClient: deps.getSfClient || defaultGetSfClient,
      queueNsCall: deps.queueNsCall || defaultQueueNsCall,
    })),
  });
}

module.exports = {
  createCashProjectionHandler,
  createProjectionHandler,
  computeCashProjection,
  wrapClient,
  makeEntry,
  writeCacheEntry,
  readCacheEntry,
  ProjectionError,
  SCHEMA_VERSION,
  DEFAULT_CACHE_FILE,
  DEFAULT_SCENARIO,
  COMPANY,
  FEED_LABELS,
  defaultGetNsClient,
  defaultGetSfClient,
  defaultQueueNsCall,
  loadForecastModules,
};
