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
const path = require('path');
const { pathToFileURL } = require('url');
const { readDoc, writeDoc, appendHistory, send, userOf, readWriteBody } = require('./json-store.cjs');

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

const readStore = (file) => {
  const s = readDoc(file);
  return s && s.years && typeof s.years === 'object' ? s : null;
};

/**
 * Express/connect handler. deps (all optional, for tests):
 *   file (the store), clock(), referenceOf() → { serverRatioYtd } (default: the P&L projection's cache)
 */
function createProjectionTargetsHandler(deps = {}) {
  const file = deps.file || DEFAULT_FILE;
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
      send(res, 405, { ok: false, error: 'Method not allowed' }, { Allow: 'GET, PUT' });
      return;
    }
    const body = await readWriteBody(req, res, MAX_BODY);
    if (body === null) return;
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
      writeDoc(file, store);
      appendHistory(file, { at, by, year, targets: v.targets });
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
