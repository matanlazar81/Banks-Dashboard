// ─────────────────────────────────────────────────────────────────────────────
// Small JSON documents the pages save for everyone (the 2027 targets, the Metrics settings and deposit
// tracker): one file each under <checkout>/data (written atomically), a history line per save (who,
// when, what), and the write rules every one of them follows: JSON only (a cross-site form cannot send
// it), same-origin, a size limit, and whoever finance-it says is signed in recorded as the author.
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');
const { readJsonBody, resolveUserEmail, PayloadTooLargeError } = require('./security.cjs');

function readDoc(file) {
  try {
    const d = JSON.parse(fs.readFileSync(file, 'utf8'));
    return d && typeof d === 'object' ? d : null;
  } catch {
    return null;
  }
}

function writeDoc(file, doc) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(doc, null, 2));
  fs.renameSync(tmp, file); // readers never see a half-written file
}

const historyFileOf = (file) => file.replace(/\.json$/, '-history.jsonl');

function appendHistory(file, record) {
  fs.appendFileSync(historyFileOf(file), `${JSON.stringify(record)}\n`);
}

function send(res, status, body, headers) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');
  for (const [k, v] of Object.entries(headers || {})) res.setHeader(k, v);
  res.end(JSON.stringify(body));
}

// A browser always sends Origin on a cross-site write; it must be this site.
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

/** The signed-in user's email: finance-it's session user, else this server's own login. */
function userOf(req) {
  const u = req.user || (req.session && req.session.user) || null;
  const fromApp = u && typeof u.email === 'string' ? u.email.trim().toLowerCase() : '';
  return fromApp || resolveUserEmail(req) || null;
}

/**
 * The JSON body of a write, or null after answering why it is refused (415 not JSON, 403 another site,
 * 413 too large, 400 not JSON).
 */
async function readWriteBody(req, res, maxBytes) {
  if (!/^application\/json\b/i.test(String((req.headers || {})['content-type'] || ''))) {
    send(res, 415, { ok: false, error: 'Send the data as JSON.' });
    return null;
  }
  if (!sameOrigin(req)) {
    send(res, 403, { ok: false, error: 'This can only be saved from the dashboard itself.' });
    return null;
  }
  try {
    return await readJsonBody(req, maxBytes);
  } catch (e) {
    const big = e instanceof PayloadTooLargeError;
    send(res, big ? 413 : 400, { ok: false, error: big ? 'The data is too large.' : 'The request is not valid JSON.' });
    return null;
  }
}

/**
 * GET / PUT handler for one document { value, updatedAt, updatedBy }.
 *   file, maxBytes, label (log), validate(body) → { ok, value, errors }, empty() → the value before any save,
 *   clock()
 */
function createDocHandler({ file, maxBytes = 32 * 1024, label, validate, empty, clock = Date.now }) {
  return async function docHandler(req, res) {
    const method = (req.method || 'GET').toUpperCase();
    if (method === 'GET') {
      const doc = readDoc(file);
      send(res, 200, { ok: true, value: doc && doc.value !== undefined ? doc.value : empty(), updatedAt: (doc && doc.updatedAt) || null, updatedBy: (doc && doc.updatedBy) || null });
      return;
    }
    if (method !== 'PUT') {
      send(res, 405, { ok: false, error: 'Method not allowed' }, { Allow: 'GET, PUT' });
      return;
    }
    const body = await readWriteBody(req, res, maxBytes);
    if (body === null) return;
    const v = validate(body);
    if (!v.ok) {
      send(res, 400, { ok: false, error: 'Some values are not valid.', details: v.errors.slice(0, 20) });
      return;
    }
    const at = new Date(clock()).toISOString();
    const by = userOf(req);
    try {
      writeDoc(file, { value: v.value, updatedAt: at, updatedBy: by });
      appendHistory(file, { at, by, value: v.value });
    } catch (e) {
      console.error(`[${label}] save failed: ${e && e.message}`);
      send(res, 500, { ok: false, error: 'This could not be saved. The server log has the details.' });
      return;
    }
    console.log(`[${label}] saved by ${by || 'unknown user'}`);
    send(res, 200, { ok: true, value: v.value, updatedAt: at, updatedBy: by });
  };
}

module.exports = { readDoc, writeDoc, appendHistory, historyFileOf, send, sameOrigin, userOf, readWriteBody, createDocHandler };
