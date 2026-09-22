#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// Banks-Dashboard — standalone production server (ONE shared host for everyone).
//
//   npm run build          # produce dist/
//   node server.cjs        # serve dist/ + all /api/* on PORT (default 8790)
//
// Every data pull (NetSuite, Snowflake), every cache and every data/*.json file
// lives in THIS process on THIS machine. Users just open http://<server>:8790 —
// no local credentials, no local fetching, and everyone sees identical data.
// The Refresh button re-pulls on the server and updates the shared cache.
//
// Routes come verbatim from server/api-routes.cjs — the same module the Vite dev
// server mounts, so dev and prod cannot drift. `npm run dev` stays development-
// only; do NOT run it on the production checkout while this server is running
// (two processes would write the same data/*.json files).
//
// Env: .env in the repo root (NetSuite/Snowflake creds, SYNC_ALLOWLIST, …), plus:
//   PORT               listen port (default 8790)
//   BIND_HOST          listen address (default 127.0.0.1). Public binds require
//                      STANDALONE_ACCESS_TOKEN (login gate).
//   STANDALONE_ACCESS_TOKEN  shared password for the login page + warm-cache header
//   CACHE_TTL_MIN      shared API cache TTL in minutes (default 5; 20 recommended
//                      here — the warm loop keeps it fresh anyway)
//   WARM_INTERVAL_MIN  keep-warm sweep interval (default 15; 0 disables)
//   WARM_SUBSIDIARIES / WARM_ENDPOINTS  see server/warm-cache.cjs
// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');
const fs = require('fs');
const express = require('express');
const { config: dotenvConfig } = require('dotenv');
dotenvConfig({ path: path.join(__dirname, '.env') });

const { registerApiRoutes } = require('./server/api-routes.cjs');
const { startWarmCache } = require('./server/warm-cache.cjs');
const {
  getAccessToken,
  assertSafeBind,
  isStandaloneAuthed,
  timingSafeEqualString,
  normalizeEmail,
  sessionCookie,
  clearSessionCookie,
  loginPageHtml,
  readJsonBody,
} = require('./server/security.cjs');

const PORT = parseInt(process.env.PORT || '8790', 10);
const DIST = path.resolve(__dirname, 'dist');

if (!fs.existsSync(path.join(DIST, 'index.html'))) {
  console.error('[server] dist/index.html not found — run `npm run build` first.');
  process.exit(1);
}

// These routes expose NetSuite and Snowflake finance data. The listener stays on
// loopback unless STANDALONE_ACCESS_TOKEN is set; reaching it from another host
// must go through this login gate or an authenticating reverse proxy.
const HOST = process.env.BIND_HOST || '127.0.0.1';
try {
  assertSafeBind(HOST, getAccessToken());
} catch (e) {
  console.error(`[server] ${e.message}`);
  process.exit(1);
}

const app = express();
const accessToken = getAccessToken();

function requestPath(req) {
  return String(req.path || (req.url || '').split('?')[0] || '');
}

app.get('/api/health', (_req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify({ ok: true }));
});

app.get('/login', (req, res) => {
  if (!accessToken) { res.redirect('/'); return; }
  if (isStandaloneAuthed(req)) { res.redirect('/'); return; }
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(loginPageHtml());
});

app.post('/login', async (req, res) => {
  if (!accessToken) { res.redirect('/'); return; }
  let password = '';
  let email = '';
  const ctype = String((req.headers && req.headers['content-type']) || '');
  try {
    if (ctype.includes('application/json')) {
      const body = await readJsonBody(req, 8 * 1024);
      password = body.password || body.token || '';
      email = body.email || '';
    } else {
      const chunks = [];
      let size = 0;
      for await (const chunk of req) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        size += buf.length;
        if (size > 8 * 1024) break;
        chunks.push(buf);
      }
      const raw = Buffer.concat(chunks).toString('utf8');
      const params = new URLSearchParams(raw);
      password = params.get('password') || '';
      email = params.get('email') || '';
    }
  } catch {
    res.statusCode = 400;
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(loginPageHtml('Invalid request.'));
    return;
  }
  const emailNorm = normalizeEmail(email);
  if (!timingSafeEqualString(password, accessToken) || !emailNorm) {
    res.statusCode = 401;
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(loginPageHtml('Invalid email or access token.'));
    return;
  }
  res.setHeader('Set-Cookie', sessionCookie({ email: emailNorm }));
  res.redirect('/');
});

app.post('/logout', (req, res) => {
  res.setHeader('Set-Cookie', clearSessionCookie());
  res.redirect('/login');
});

if (accessToken) {
  app.use((req, res, next) => {
    const p = requestPath(req);
    if (p === '/login' || p === '/logout' || p === '/api/health') return next();
    if (isStandaloneAuthed(req)) return next();
    if (p.startsWith('/api/')) {
      res.statusCode = 401;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ error: 'Authentication required' }));
      return;
    }
    res.statusCode = 302;
    res.setHeader('Location', '/login');
    res.end();
  });
}

// 1. All /api/* handlers (they stream their own request bodies — no body-parser here,
//    adding one would consume the stream before the handlers read it).
registerApiRoutes(app);

// 2. Built dashboard assets.
app.use(express.static(DIST));

// 3. SPA fallback: any other GET serves index.html (plain middleware — express 5's
//    path-to-regexp rejects the old `app.get('*')` wildcard).
app.use((req, res, next) => {
  if (req.method !== 'GET' || req.path.startsWith('/api/')) return next();
  res.sendFile(path.join(DIST, 'index.html'));
});

app.listen(PORT, HOST, () => {
  console.log(`[server] Banks-Dashboard listening on http://${HOST}:${PORT}`);
  console.log('[server] all data pulls run here — browsers only read /api/*');
  if (accessToken) {
    console.log('[server] standalone access token is set — /login required');
  } else {
    console.log('[server] loopback-only, no access token — put an authenticating proxy in front before exposing this port');
  }
  startWarmCache(PORT);
});
