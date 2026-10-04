// ─────────────────────────────────────────────────────────────────────────────
// GET / PUT /api/projection-targets — the shared projection-year targets of both projection pages
// (src/forecast/targets.mjs computes their effect in the browser).
//
//   GET  → { ok, years: { '2027': targets }, updatedAt, updatedBy, reference: { serverRatioYtd } }
//   PUT  { year, targets } → validated (unknown fields refused), saved, appended to the history
//
// Stored in <checkout>/data/projection-targets.json (written atomically) with every save appended to
// data/projection-targets-history.jsonl (who, when, what). Not in the Postgres plan: the old dashboard
// rebuilds a plan's data from its own state when it saves, which would drop fields it does not know.
// A PUT must be JSON (so a cross-site form cannot send it), at most 32 KB, and from this site's origin.
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');
const { readJsonBody, resolveUserEmail, PayloadTooLargeError } = require('./security.cjs');

const ROOT = path.resolve(__dirname, '..');
const DEFAULT_FILE = path.join(ROOT, 'data', 'projection-targets.json');
const MAX_BODY = 32 * 1024;

let targetsP = null;
function loadTargetsModule() {
  if (!targetsP) {
    targetsP = import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'targets.mjs')).href)
      .catch((e) => { targetsP = null; throw e; });
  }
  return targetsP;
}

function readStore(file) {
  try {
    const s = JSON.parse(fs.readFileSync(file, 'utf8'));
    return s && typeof s === 'object' && s.years && typeof s.years === 'object' ? s : null;
  } catch {
    return null;
  }
}

function writeStore(file, store) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(store, null, 2));
  fs.renameSync(tmp, file);
}

function send(res, status, body) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(body));
}

// Same-origin only: a browser always sends Origin on a cross-site PUT.
function sameOrigin(req) {
  const h = req.headers || {};
  if (!h.origin) return true;
  try {
    const host = String(h['x-forwarded-host'] || h.host || '').split(',')[0].trim().toLowerCase();
    return new URL(h.origin).host.toLowerCase() === host;
  } catch {
    return false;
  }
}

function userOf(req) {
  const u = req.user || (req.session && req.session.user) || null;
  const fromApp = u && typeof u.email === 'string' ? u.email.trim().toLowerCase() : '';
  return fromApp || resolveUserEmail(req) || null;
}

/**
 * Express/connect handler. deps (all optional, for tests):
 *   file (the store), clock(), referenceOf() → { serverRatioYtd } (default: the P&L projection's cache)
 */
function createProjectionTargetsHandler(deps = {}) {
  const file = deps.file || DEFAULT_FILE;
  const historyFile = deps.historyFile || file.replace(/\.json$/, '-history.jsonl');
  const clock = deps.clock || Date.now;
  const referenceOf = deps.referenceOf || (() => {
    try {
      const cp = require('./cash-projection.cjs');
      const pnl = require('./pnl-projection.cjs');
      const e = cp.readCacheEntry(pnl.DEFAULT_CACHE_FILE, pnl.SCHEMA_VERSION);
      const base = e && e.payload && e.payload.targetsBase;
      return { serverRatioYtd: base && Number.isFinite(base.serverRatioYtd) ? base.serverRatioYtd : null };
    } catch {
      return { serverRatioYtd: null };
    }
  });

  return async function projectionTargetsHandler(req, res) {
    const method = (req.method || 'GET').toUpperCase();
    if (method === 'GET') {
      const store = readStore(file) || { years: {} };
      send(res, 200, { ok: true, years: store.years, updatedAt: store.updatedAt || null, updatedBy: store.updatedBy || null, reference: referenceOf() });
      return;
    }
    if (method !== 'PUT') {
      res.setHeader('Allow', 'GET, PUT');
      send(res, 405, { ok: false, error: 'Method not allowed' });
      return;
    }
    if (!/^application\/json\b/i.test(String((req.headers || {})['content-type'] || ''))) {
      send(res, 415, { ok: false, error: 'Send the targets as JSON.' });
      return;
    }
    if (!sameOrigin(req)) {
      send(res, 403, { ok: false, error: 'Targets can only be saved from the dashboard itself.' });
      return;
    }
    let body;
    try {
      body = await readJsonBody(req, MAX_BODY);
    } catch (e) {
      send(res, e instanceof PayloadTooLargeError ? 413 : 400, { ok: false, error: e instanceof PayloadTooLargeError ? 'The targets are too large.' : 'The request is not valid JSON.' });
      return;
    }
    const nowYear = new Date(clock()).getFullYear();
    const year = Number(body && body.year);
    if (!Number.isInteger(year) || year < nowYear || year > nowYear + 2) {
      send(res, 400, { ok: false, error: `Targets are for ${nowYear}–${nowYear + 2}.` });
      return;
    }
    const t = await loadTargetsModule();
    const v = t.validateTargets(body.targets);
    if (!v.ok) {
      send(res, 400, { ok: false, error: 'Some targets are out of range.', details: v.errors.slice(0, 20) });
      return;
    }
    const store = readStore(file) || { version: t.TARGETS_VERSION, years: {} };
    const at = new Date(clock()).toISOString();
    const by = userOf(req);
    store.version = t.TARGETS_VERSION;
    store.years = { ...store.years, [String(year)]: v.targets };
    store.updatedAt = at;
    store.updatedBy = by;
    try {
      writeStore(file, store);
      fs.appendFileSync(historyFile, `${JSON.stringify({ at, by, year, targets: v.targets })}\n`);
    } catch (e) {
      console.error(`[projection-targets] save failed: ${e && e.message}`);
      send(res, 500, { ok: false, error: 'The targets could not be saved. The server log has the details.' });
      return;
    }
    console.log(`[projection-targets] ${year} targets saved by ${by || 'unknown user'}`);
    send(res, 200, { ok: true, years: store.years, updatedAt: at, updatedBy: by, reference: referenceOf() });
  };
}

module.exports = { createProjectionTargetsHandler, DEFAULT_FILE };
