// Shared security helpers for the standalone server and api-routes.cjs.
// Keep this module free of route registration so it can be unit-tested in isolation.
const crypto = require('crypto');

const COOKIE_NAME = 'bd_session';
const DEFAULT_MAX_BODY = 1 * 1024 * 1024; // 1 MiB
const CHAT_MAX_BODY = 1 * 1024 * 1024;
const CHAT_CONTEXT_MAX = 200_000;
const CHAT_RATE_LIMIT = 20;
const CHAT_RATE_WINDOW_MS = 10 * 60 * 1000;
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function timingSafeEqualString(a, b) {
  const left = Buffer.from(String(a || ''), 'utf8');
  const right = Buffer.from(String(b || ''), 'utf8');
  if (left.length === 0 || left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

function getAccessToken() {
  return (process.env.STANDALONE_ACCESS_TOKEN || '').trim();
}

function isLoopbackHost(host) {
  const h = String(host || '').trim().toLowerCase();
  return h === '127.0.0.1' || h === 'localhost' || h === '::1';
}

function isLoopbackReq(req) {
  const ip = (req.socket && req.socket.remoteAddress)
    || (req.connection && req.connection.remoteAddress)
    || '';
  return ip === '127.0.0.1' || ip === '::1' || ip === '::ffff:127.0.0.1';
}

function assertSafeBind(host, token) {
  if (isLoopbackHost(host)) return;
  if (token && String(token).trim()) return;
  const err = new Error(
    `Refusing to bind ${host}: set STANDALONE_ACCESS_TOKEN or keep BIND_HOST=127.0.0.1`
  );
  err.code = 'UNSAFE_BIND';
  throw err;
}

function normalizeEmail(raw) {
  const email = String(raw || '').trim().toLowerCase();
  if (!email || email.length > 120 || !EMAIL_RE.test(email)) return '';
  return email;
}

function parseCookies(req) {
  const raw = (req.headers && req.headers.cookie) || '';
  const out = {};
  for (const part of String(raw).split(';')) {
    const i = part.indexOf('=');
    if (i < 0) continue;
    const k = part.slice(0, i).trim();
    const v = part.slice(i + 1).trim();
    if (!k) continue;
    try { out[k] = decodeURIComponent(v); } catch { out[k] = v; }
  }
  return out;
}

function signSession(payload, token) {
  const secret = token || getAccessToken();
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig = crypto.createHmac('sha256', secret).update(body).digest('base64url');
  return `${body}.${sig}`;
}

function verifySession(cookieVal, token) {
  const secret = token || getAccessToken();
  if (!secret || !cookieVal) return null;
  const [body, sig] = String(cookieVal).split('.');
  if (!body || !sig) return null;
  const expected = crypto.createHmac('sha256', secret).update(body).digest('base64url');
  if (!timingSafeEqualString(sig, expected)) return null;
  let payload;
  try { payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8')); }
  catch { return null; }
  if (!payload || (payload.exp && Date.now() > payload.exp)) return null;
  return payload;
}

function getStandaloneSession(req) {
  if (!getAccessToken()) return null;
  return verifySession(parseCookies(req)[COOKIE_NAME]);
}

function hasStandaloneToken(req) {
  const token = getAccessToken();
  if (!token) return false;
  const h = req.headers || {};
  const bearer = String(h.authorization || '').replace(/^Bearer\s+/i, '');
  const given = h['x-standalone-token'] || bearer;
  return timingSafeEqualString(given, token);
}

function isStandaloneAuthed(req) {
  return hasStandaloneToken(req) || !!getStandaloneSession(req);
}

function sessionCookie(payload) {
  const token = getAccessToken();
  const value = signSession({
    email: payload.email || '',
    exp: Date.now() + SESSION_TTL_MS,
  }, token);
  const secure = process.env.COOKIE_SECURE === '1' ? '; Secure' : '';
  return `${COOKIE_NAME}=${value}; HttpOnly; Path=/; SameSite=Lax; Max-Age=${Math.floor(SESSION_TTL_MS / 1000)}${secure}`;
}

function clearSessionCookie() {
  return `${COOKIE_NAME}=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0`;
}

function resolveUserEmail(req) {
  const h = req.headers || {};
  const fromHeader = process.env.TRUST_PROXY_USER_HEADER === '1'
    ? (h['x-user-email'] || h['x-forwarded-user'] || h['x-auth-user'] || '')
    : '';
  const headerEmail = normalizeEmail(fromHeader);
  if (headerEmail) return headerEmail;

  const session = getStandaloneSession(req);
  if (session && session.email) {
    const sessionEmail = normalizeEmail(session.email);
    if (sessionEmail) return sessionEmail;
  }

  // DEV_USER_EMAIL is a local-dev fallback only: never in production, never for
  // remote clients (so `vite --host` / a shared LAN URL cannot mint sync identity).
  if (process.env.NODE_ENV !== 'production' && isLoopbackReq(req) && process.env.DEV_USER_EMAIL) {
    return normalizeEmail(process.env.DEV_USER_EMAIL);
  }
  return '';
}

function canWriteNetCash(req, email, canSync) {
  const token = (process.env.NET_CASH_WRITE_TOKEN || '').trim();
  if (token) {
    const given = (req.headers && req.headers['x-net-cash-write-token']) || '';
    if (timingSafeEqualString(given, token)) return true;
  }
  return !!canSync;
}

class PayloadTooLargeError extends Error {
  constructor(maxBytes) {
    super(`payload too large (max ${maxBytes} bytes)`);
    this.name = 'PayloadTooLargeError';
    this.statusCode = 413;
  }
}

async function readJsonBody(req, maxBytes = DEFAULT_MAX_BODY) {
  if (req.body !== undefined && req.body !== null && typeof req.body === 'object' && !Buffer.isBuffer(req.body)) {
    return req.body;
  }
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buf.length;
    if (size > maxBytes) {
      if (typeof req.resume === 'function') req.resume();
      throw new PayloadTooLargeError(maxBytes);
    }
    chunks.push(buf);
  }
  const raw = Buffer.concat(chunks).toString('utf8').trim();
  if (!raw) return {};
  return JSON.parse(raw);
}

function sendJson(res, status, obj) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(obj));
}

const chatHits = new Map();

function allowChatRequest(key, limit = CHAT_RATE_LIMIT, windowMs = CHAT_RATE_WINDOW_MS) {
  const now = Date.now();
  const entry = chatHits.get(key);
  if (!entry || now > entry.reset) {
    chatHits.set(key, { count: 1, reset: now + windowMs });
    return true;
  }
  entry.count += 1;
  return entry.count <= limit;
}

function resetChatRateLimit() {
  chatHits.clear();
}

function clipDashboardContext(text) {
  const s = String(text || '');
  if (s.length <= CHAT_CONTEXT_MAX) return s;
  return s.slice(0, CHAT_CONTEXT_MAX) + '\n[truncated]';
}

function loginPageHtml(error) {
  const err = error
    ? `<p class="err">${String(error).replace(/[<>&]/g, (c) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c]))}</p>`
    : '';
  return `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Banks Dashboard — Sign in</title>
<style>
  body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
    font-family:ui-sans-serif,system-ui,sans-serif;background:#0f172a;color:#e2e8f0}
  form{background:#1e293b;padding:28px 32px;border-radius:12px;width:min(420px,92vw);
    box-shadow:0 20px 50px rgba(0,0,0,.35)}
  h1{font-size:18px;margin:0 0 6px}
  p.sub{margin:0 0 18px;color:#94a3b8;font-size:13px}
  label{display:block;font-size:12px;margin:12px 0 4px;color:#cbd5e1}
  input{width:100%;box-sizing:border-box;padding:9px 10px;border-radius:8px;border:1px solid #334155;
    background:#0f172a;color:#e2e8f0}
  button{margin-top:18px;width:100%;padding:10px;border:0;border-radius:8px;background:#2563eb;
    color:#fff;font-weight:600;cursor:pointer}
  .err{color:#fca5a5;font-size:13px;margin:0 0 10px}
</style></head><body>
<form method="POST" action="/login" autocomplete="on">
  <h1>Banks Dashboard</h1>
  <p class="sub">Shared-server sign-in. Use your work email so chat and prefs stay private to you.</p>
  ${err}
  <label for="email">Work email</label>
  <input id="email" name="email" type="email" required maxlength="120" autocomplete="username"/>
  <label for="password">Access token</label>
  <input id="password" name="password" type="password" required autocomplete="current-password"/>
  <button type="submit">Sign in</button>
</form></body></html>`;
}

module.exports = {
  COOKIE_NAME,
  DEFAULT_MAX_BODY,
  CHAT_MAX_BODY,
  CHAT_CONTEXT_MAX,
  CHAT_RATE_LIMIT,
  CHAT_RATE_WINDOW_MS,
  SESSION_TTL_MS,
  PayloadTooLargeError,
  timingSafeEqualString,
  getAccessToken,
  isLoopbackHost,
  isLoopbackReq,
  assertSafeBind,
  normalizeEmail,
  parseCookies,
  signSession,
  verifySession,
  getStandaloneSession,
  hasStandaloneToken,
  isStandaloneAuthed,
  sessionCookie,
  clearSessionCookie,
  resolveUserEmail,
  canWriteNetCash,
  readJsonBody,
  sendJson,
  allowChatRequest,
  resetChatRateLimit,
  clipDashboardContext,
  loginPageHtml,
};
