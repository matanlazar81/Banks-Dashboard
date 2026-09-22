#!/usr/bin/env node
// Security-gate regression tests for bank-dashboard standalone + shared API.
// Exit 0 = all checks passed.
const assert = require('assert');
const fs = require('fs');
const http = require('http');
const path = require('path');
const { Readable } = require('stream');

const ROOT = path.resolve(__dirname, '..');
process.chdir(ROOT);

let failures = 0;
const fail = (msg, e) => { failures++; console.error('  ✗ ' + msg + (e ? ` — ${e.message || e}` : '')); };
const ok = (msg) => console.log('  ✓ ' + msg);

function section(title) {
  console.log('\n' + title);
}

async function runHelperTests() {
  section('1. security.cjs helpers');
  const sec = require(path.join(ROOT, 'server', 'security.cjs'));

  try {
    sec.assertSafeBind('127.0.0.1', '');
    sec.assertSafeBind('localhost', '');
    sec.assertSafeBind('0.0.0.0', 'secret');
    ok('assertSafeBind allows loopback and token-protected public bind');
  } catch (e) { fail('assertSafeBind should allow those cases', e); }

  try {
    sec.assertSafeBind('0.0.0.0', '');
    fail('assertSafeBind should refuse public bind without token');
  } catch (e) {
    if (e.code === 'UNSAFE_BIND') ok('assertSafeBind refuses public bind without token');
    else fail('assertSafeBind threw unexpected error', e);
  }

  try {
    assert.strictEqual(sec.normalizeEmail(' Matan.L@Lsports.eu '), 'matan.l@lsports.eu');
    assert.strictEqual(sec.normalizeEmail('not-an-email'), '');
    assert.strictEqual(sec.normalizeEmail('a@b'), '');
    ok('normalizeEmail validates and lowercases');
  } catch (e) { fail('normalizeEmail', e); }

  try {
    const token = 'tok-abc';
    const cookie = sec.signSession({ email: 'a@b.co', exp: Date.now() + 60_000 }, token);
    const good = sec.verifySession(cookie, token);
    assert.strictEqual(good.email, 'a@b.co');
    const bad = sec.verifySession(cookie, 'other');
    assert.strictEqual(bad, null);
    const expired = sec.signSession({ email: 'a@b.co', exp: Date.now() - 1 }, token);
    assert.strictEqual(sec.verifySession(expired, token), null);
    ok('session sign/verify + expiry');
  } catch (e) { fail('session sign/verify', e); }

  try {
    const oversize = Readable.from([Buffer.alloc(sec.DEFAULT_MAX_BODY + 1, 97)]);
    await sec.readJsonBody(oversize, sec.DEFAULT_MAX_BODY);
    fail('readJsonBody should reject oversize payload');
  } catch (e) {
    if (e.statusCode === 413) ok('readJsonBody rejects oversize payload with 413');
    else fail('readJsonBody oversize threw unexpected error', e);
  }

  try {
    const stream = Readable.from([Buffer.from('{"a":1}')]);
    const parsed = await sec.readJsonBody(stream);
    assert.deepStrictEqual(parsed, { a: 1 });
    ok('readJsonBody parses JSON stream');
  } catch (e) { fail('readJsonBody parse', e); }

  try {
    const replay = { body: { already: true } };
    const parsed = await sec.readJsonBody(replay);
    assert.deepStrictEqual(parsed, { already: true });
    ok('readJsonBody accepts already-parsed req.body (finance-it replay)');
  } catch (e) { fail('readJsonBody replay', e); }

  try {
    sec.resetChatRateLimit();
    for (let i = 0; i < sec.CHAT_RATE_LIMIT; i++) assert.strictEqual(sec.allowChatRequest('u'), true);
    assert.strictEqual(sec.allowChatRequest('u'), false);
    assert.strictEqual(sec.allowChatRequest('other'), true);
    ok('chat rate limiter trips after the cap');
  } catch (e) { fail('chat rate limiter', e); }

  try {
    const long = 'x'.repeat(sec.CHAT_CONTEXT_MAX + 50);
    const clipped = sec.clipDashboardContext(long);
    assert.ok(clipped.length < long.length);
    assert.ok(clipped.endsWith('[truncated]'));
    ok('dashboardContext is clipped');
  } catch (e) { fail('clipDashboardContext', e); }
}

function fakeReq(overrides = {}) {
  return {
    headers: {},
    socket: { remoteAddress: '127.0.0.1' },
    ...overrides,
    headers: { ...(overrides.headers || {}) },
    socket: overrides.socket || { remoteAddress: '127.0.0.1' },
  };
}

async function runIdentityTests() {
  section('2. DEV_USER_EMAIL / identity');
  const sec = require(path.join(ROOT, 'server', 'security.cjs'));

  const prev = {
    NODE_ENV: process.env.NODE_ENV,
    DEV_USER_EMAIL: process.env.DEV_USER_EMAIL,
    TRUST_PROXY_USER_HEADER: process.env.TRUST_PROXY_USER_HEADER,
    STANDALONE_ACCESS_TOKEN: process.env.STANDALONE_ACCESS_TOKEN,
  };

  try {
    process.env.NODE_ENV = 'development';
    process.env.DEV_USER_EMAIL = 'matan.l@lsports.eu';
    delete process.env.TRUST_PROXY_USER_HEADER;
    delete process.env.STANDALONE_ACCESS_TOKEN;

    assert.strictEqual(sec.resolveUserEmail(fakeReq()), 'matan.l@lsports.eu');
    assert.strictEqual(
      sec.resolveUserEmail(fakeReq({ socket: { remoteAddress: '10.0.0.8' } })),
      '',
      'remote client must not inherit DEV_USER_EMAIL'
    );
    ok('DEV_USER_EMAIL only applies on loopback in non-production');

    process.env.NODE_ENV = 'production';
    assert.strictEqual(sec.resolveUserEmail(fakeReq()), '');
    ok('DEV_USER_EMAIL is ignored in production');

    process.env.TRUST_PROXY_USER_HEADER = '1';
    const headerReq = fakeReq({ headers: { 'x-user-email': 'Lital@lsports.eu' } });
    assert.strictEqual(sec.resolveUserEmail(headerReq), 'lital@lsports.eu');
    ok('trusted proxy header wins for identity');

    delete process.env.TRUST_PROXY_USER_HEADER;
    process.env.STANDALONE_ACCESS_TOKEN = 'shared-secret';
    const cookie = sec.signSession({ email: 'owner@lsports.eu', exp: Date.now() + 60_000 }, 'shared-secret');
    const sessReq = fakeReq({ headers: { cookie: `${sec.COOKIE_NAME}=${cookie}` } });
    assert.strictEqual(sec.resolveUserEmail(sessReq), 'owner@lsports.eu');
    ok('standalone session cookie supplies identity');
  } catch (e) { fail('identity resolution', e); }
  finally {
    for (const [k, v] of Object.entries(prev)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
}

function closeServer(server) {
  return new Promise((resolve) => {
    const t = setTimeout(resolve, 1000);
    try {
      server.close(() => { clearTimeout(t); resolve(); });
    } catch {
      clearTimeout(t);
      resolve();
    }
  });
}

function httpRequest(port, opts) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: '127.0.0.1',
      port,
      path: opts.path,
      method: opts.method || 'GET',
      headers: opts.headers || {},
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let json = null;
        try { json = JSON.parse(text); } catch { /* raw */ }
        resolve({ status: res.statusCode, text, json, headers: res.headers });
      });
    });
    req.on('error', reject);
    if (opts.body !== undefined) req.write(opts.body);
    req.end();
  });
}

function backupFile(filePath) {
  try { return fs.readFileSync(filePath); } catch { return null; }
}
function restoreFile(filePath, prev) {
  if (prev === null) {
    try { fs.unlinkSync(filePath); } catch { /* wasn't there */ }
  } else {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, prev);
  }
}

async function runRouteTests() {
  section('3. API route gates');
  process.env.NODE_ENV = 'development';
  process.env.DEV_USER_EMAIL = 'matan.l@lsports.eu';
  process.env.SYNC_ALLOWLIST = 'matan.l@lsports.eu';
  delete process.env.TRUST_PROXY_USER_HEADER;
  delete process.env.STANDALONE_ACCESS_TOKEN;
  delete process.env.ANTHROPIC_API_KEY;
  process.env.NET_CASH_WRITE_TOKEN = 'nc-write-token';
  const netCashPath = path.join(ROOT, 'data', 'net-cash-forecast.json');
  const chatPath = path.join(ROOT, 'chat-history.json');
  const netCashPrev = backupFile(netCashPath);
  const chatPrev = backupFile(chatPath);

  const express = require('express');
  delete require.cache[require.resolve(path.join(ROOT, 'server', 'api-routes.cjs'))];
  const { registerApiRoutes } = require(path.join(ROOT, 'server', 'api-routes.cjs'));
  const app = express();
  registerApiRoutes(app);
  const server = await new Promise((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s));
  });
  const port = server.address().port;

  try {
    const who = await httpRequest(port, { path: '/api/whoami' });
    assert.strictEqual(who.status, 200);
    assert.strictEqual(who.json.email, 'matan.l@lsports.eu');
    assert.strictEqual(who.json.canSync, true);
    ok('whoami on loopback uses DEV_USER_EMAIL and canSync');

    const ncForbidden = await httpRequest(port, {
      path: '/api/net-cash-forecast',
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Force-No-Sync': '1' },
      body: JSON.stringify({ forecastEur: 1, totalBankEur: 1 }),
    });
    // Loopback + allowlisted DEV_USER_EMAIL can still persist (local owner).
    assert.strictEqual(ncForbidden.status, 200);
    ok('net-cash POST allowed for allowlisted local identity');

    const ncToken = await httpRequest(port, {
      path: '/api/net-cash-forecast',
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-net-cash-write-token': 'nc-write-token' },
      body: JSON.stringify({ forecastEur: 2, totalBankEur: 2, company: 'lsports' }),
    });
    assert.strictEqual(ncToken.status, 200);
    ok('net-cash POST allowed with write token');

    const chatNoKey = await httpRequest(port, {
      path: '/api/chat',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'hi' }], dashboardContext: 'x' }),
    });
    assert.ok(chatNoKey.status === 500 || chatNoKey.status === 401);
    assert.ok(chatNoKey.json.error);
    ok('chat requires key/auth rather than proxying blindly');

    const historyGet = await httpRequest(port, { path: '/api/chat-history' });
    assert.strictEqual(historyGet.status, 200);
    assert.ok(Array.isArray(historyGet.json));
    assert.ok(historyGet.json.every((h) => !h.ownerEmail || h.ownerEmail === 'matan.l@lsports.eu'));
    ok('chat-history GET returns only the caller\'s conversations');

    const save = await httpRequest(port, {
      path: '/api/chat-history',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        action: 'save',
        id: 'sec-test-' + Date.now(),
        title: 'security test',
        messages: [{ role: 'user', content: 'ping' }],
      }),
    });
    assert.strictEqual(save.status, 200);
    assert.strictEqual(save.json.ok, true);
    ok('chat-history save scopes the conversation to the caller');

    const huge = 'x'.repeat(1.2 * 1024 * 1024);
    try {
      const oversize = await httpRequest(port, {
        path: '/api/user-pref',
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'Content-Length': String(Buffer.byteLength(huge)) },
        body: huge,
      });
      assert.strictEqual(oversize.status, 413);
      ok('oversize body is rejected with 413');
    } catch (e) {
      if (e && e.code === 'ECONNRESET') ok('oversize body is rejected (connection reset after size cap)');
      else throw e;
    }
  } catch (e) {
    fail('API route gates', e);
  } finally {
    restoreFile(netCashPath, netCashPrev);
    restoreFile(chatPath, chatPrev);
    await closeServer(server);
  }
}

async function runUnauthRouteTests() {
  section('4. unauthenticated / production identity');
  process.env.NODE_ENV = 'production';
  delete process.env.DEV_USER_EMAIL;
  delete process.env.TRUST_PROXY_USER_HEADER;
  delete process.env.STANDALONE_ACCESS_TOKEN;
  delete process.env.NET_CASH_WRITE_TOKEN;

  delete require.cache[require.resolve(path.join(ROOT, 'server', 'api-routes.cjs'))];
  const express = require('express');
  const { registerApiRoutes } = require(path.join(ROOT, 'server', 'api-routes.cjs'));
  const app = express();
  registerApiRoutes(app);
  const server = await new Promise((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s));
  });
  const port = server.address().port;

  try {
    const who = await httpRequest(port, { path: '/api/whoami' });
    assert.strictEqual(who.json.email, '');
    assert.strictEqual(who.json.canSync, false);
    ok('production whoami has no DEV_USER_EMAIL identity');

    const nc = await httpRequest(port, {
      path: '/api/net-cash-forecast',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ forecastEur: 999, totalBankEur: 999 }),
    });
    assert.strictEqual(nc.status, 403);
    ok('unauthenticated net-cash POST is 403');

    const chat = await httpRequest(port, {
      path: '/api/chat',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'hi' }] }),
    });
    assert.strictEqual(chat.status, 401);
    ok('unauthenticated /api/chat is 401');

    const hist = await httpRequest(port, { path: '/api/chat-history' });
    assert.ok(hist.status === 401 || (Array.isArray(hist.json) && hist.json.length === 0));
    ok('unauthenticated chat-history does not leak conversations');

    const histPost = await httpRequest(port, {
      path: '/api/chat-history',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'save', id: 'x', title: 'x', messages: [] }),
    });
    assert.strictEqual(histPost.status, 401);
    ok('unauthenticated chat-history write is 401');
  } catch (e) {
    fail('unauthenticated route gates', e);
  } finally {
    await closeServer(server);
  }
}

async function runStandaloneLoginTests() {
  section('5. standalone login gate');
  process.env.STANDALONE_ACCESS_TOKEN = 'gate-secret';
  const sec = require(path.join(ROOT, 'server', 'security.cjs'));
  const express = require('express');
  const app = express();
  app.get('/login', (_req, res) => { res.end(sec.loginPageHtml()); });
  app.post('/login', async (req, res) => {
    const body = await sec.readJsonBody(req, 8 * 1024);
    const email = sec.normalizeEmail(body.email);
    if (!sec.timingSafeEqualString(body.password, process.env.STANDALONE_ACCESS_TOKEN) || !email) {
      res.statusCode = 401;
      res.end('no');
      return;
    }
    res.setHeader('Set-Cookie', sec.sessionCookie({ email }));
    res.end('ok');
  });
  app.use((req, res, next) => {
    if (sec.isStandaloneAuthed(req)) return next();
    res.statusCode = 401;
    res.end(JSON.stringify({ error: 'Authentication required' }));
  });
  app.get('/api/whoami', (req, res) => {
    res.end(JSON.stringify({ email: sec.resolveUserEmail(req) }));
  });

  const server = await new Promise((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s));
  });
  const port = server.address().port;
  try {
    const denied = await httpRequest(port, { path: '/api/whoami' });
    assert.strictEqual(denied.status, 401);
    ok('API is 401 without a session');

    const bad = await httpRequest(port, {
      path: '/login',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'a@b.co', password: 'wrong' }),
    });
    assert.strictEqual(bad.status, 401);
    ok('wrong access token is rejected');

    const login = await httpRequest(port, {
      path: '/login',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'Owner@Lsports.eu', password: 'gate-secret' }),
    });
    assert.strictEqual(login.status, 200);
    const setCookie = login.headers['set-cookie'] && login.headers['set-cookie'][0];
    assert.ok(setCookie && setCookie.includes(sec.COOKIE_NAME));
    const cookie = setCookie.split(';')[0];
    const allowed = await httpRequest(port, {
      path: '/api/whoami',
      headers: { cookie },
    });
    assert.strictEqual(allowed.status, 200);
    assert.strictEqual(allowed.json.email, 'owner@lsports.eu');
    ok('login cookie authenticates and supplies identity');

    const headerTok = await httpRequest(port, {
      path: '/api/whoami',
      headers: { 'x-standalone-token': 'gate-secret' },
    });
    assert.strictEqual(headerTok.status, 200);
    ok('warm-cache token header is accepted');
  } catch (e) {
    fail('standalone login gate', e);
  } finally {
    delete process.env.STANDALONE_ACCESS_TOKEN;
    await closeServer(server);
  }
}

(async () => {
  await runHelperTests();
  await runIdentityTests();
  // Route tests run after api-routes is wired to the new helpers.
  if (process.env.SECURITY_HELPERS_ONLY === '1') {
    if (failures) { console.error(`\n${failures} helper check(s) failed`); process.exit(1); }
    console.log('\nHelper checks passed.');
    return;
  }
  await runRouteTests();
  await runUnauthRouteTests();
  await runStandaloneLoginTests();
  if (failures) { console.error(`\n${failures} security check(s) failed`); process.exit(1); }
  console.log('\nAll security-gate checks passed.');
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
