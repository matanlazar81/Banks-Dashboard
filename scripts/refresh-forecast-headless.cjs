#!/usr/bin/env node
/**
 * Headless forecast refresh
 * ───────────────────────────────────────────────────────────────────────────
 * Runs the REAL Bank Dashboard on the server in a headless browser so the
 * Exit-plan year-end forecast is recomputed and re-persisted to
 *   data/net-cash-forecast.json
 * just before the nightly Snowflake write. Schedule at ~22:50 Asia/Jerusalem,
 * ~10 min before scripts/net-cash-snapshot.cjs (23:00).
 *
 * Why a browser: the forecast is computed client-side (pipeline/churn/reval/
 * scenario blend), so only the actual dashboard code produces the right number.
 * Running it on the server reproduces the UI exactly — no one's laptop needed.
 *
 * Flow:
 *   1. Log the bot account in  (POST /api/auth/login  {email,password} → session cookie)
 *   2. Open the dashboard      (loads the bot's remembered scenario = Exit plan June26)
 *   3. Wait for the dashboard's own POST /api/net-cash-forecast (the persist) to fire
 *   4. Exit — the 23:00 cron then reads the fresh file and writes Snowflake
 *
 * Setup (one-time):
 *   • Create a bot user with the BANK_DASHBOARD role + a password (backend
 *     create-admin/reset-admin script), and log in as it ONCE to select
 *     LSports · 2026 · "Exit plan June26" so /api/user-pref remembers it.
 *   • npm i playwright && npx playwright install chromium   (on the server)
 *   • Add to .env:  DASHBOARD_BOT_EMAIL, DASHBOARD_BOT_PASSWORD
 *
 * Env:
 *   DASHBOARD_BASE_URL     default https://finance-it.lsports.eu
 *   DASHBOARD_PATH         default /business-tools/bank-dashboard
 *   DASHBOARD_BOT_EMAIL / DASHBOARD_BOT_PASSWORD   (required; never logged)
 *   HEADLESS_TIMEOUT_MS    default 150000
 *
 * Exit: 0 = forecast persisted, 1 = setup/login error, 2 = loaded but no persist seen.
 */
const path = require('path');
try { require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') }); } catch { /* env may be exported */ }

const BASE = (process.env.DASHBOARD_BASE_URL || 'https://finance-it.lsports.eu').replace(/\/+$/, '');
const DASH_PATH = process.env.DASHBOARD_PATH || '/business-tools/bank-dashboard';
const EMAIL = process.env.DASHBOARD_BOT_EMAIL;
const PASSWORD = process.env.DASHBOARD_BOT_PASSWORD;
const TIMEOUT = parseInt(process.env.HEADLESS_TIMEOUT_MS || '150000', 10);

async function main() {
  if (!EMAIL || !PASSWORD) {
    console.error('[refresh] Missing DASHBOARD_BOT_EMAIL / DASHBOARD_BOT_PASSWORD in .env');
    process.exit(1);
  }
  let chromium;
  try { ({ chromium } = require('playwright')); }
  catch {
    console.error('[refresh] playwright not installed. On the server run:');
    console.error('[refresh]   npm i playwright && npx playwright install chromium');
    process.exit(1);
  }

  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  const ctx = await browser.newContext({ ignoreHTTPSErrors: true });
  try {
    // Prime cookies (some CSRF setups seed a cookie on first GET).
    await ctx.request.get(`${BASE}${DASH_PATH}`, { timeout: TIMEOUT }).catch(() => {});

    // 1. Log in via the passport-local endpoint (usernameField = 'email').
    console.log(`[refresh] POST ${BASE}/api/auth/login  (email=${EMAIL.replace(/(.).*(@.*)/, '$1***$2')})`);
    const login = await ctx.request.post(`${BASE}/api/auth/login`, {
      data: { email: EMAIL, password: PASSWORD },
      headers: { 'Content-Type': 'application/json' },
      timeout: TIMEOUT,
    });
    console.log(`[refresh] login → ${login.status()}`);
    if (!login.ok()) {
      const body = await login.text().catch(() => '');
      console.error(`[refresh] login failed (${login.status()}). First 300 chars: ${body.slice(0, 300)}`);
      console.error('[refresh] If 403/CSRF: the login route needs a CSRF token — tell me and I\'ll add token fetch.');
      process.exit(1);
    }

    // 2. Open the dashboard (session cookie is now in ctx). Watch for the persist POST.
    const page = await ctx.newPage();
    let persisted = false, persistStatus = null;
    page.on('requestfinished', async (req) => {
      if (req.method() === 'POST' && req.url().includes('/api/net-cash-forecast')) {
        try { const r = await req.response(); persistStatus = r ? r.status() : null; } catch {}
        persisted = true;
        console.log(`[refresh] observed persist POST /api/net-cash-forecast → ${persistStatus}`);
      }
    });
    page.on('console', (m) => { if (/error/i.test(m.type())) console.log(`[refresh][page] ${m.text()}`); });

    console.log(`[refresh] loading ${BASE}${DASH_PATH} ...`);
    await page.goto(`${BASE}${DASH_PATH}`, { waitUntil: 'networkidle', timeout: TIMEOUT }).catch((e) => {
      console.log(`[refresh] goto note: ${e.message}`);
    });

    // 3. Wait for the dashboard to compute + fire its persist (data fetch can take a while).
    const deadline = Date.now() + TIMEOUT;
    while (!persisted && Date.now() < deadline) { await page.waitForTimeout(1000); }

    if (persisted && (persistStatus === 200 || persistStatus === null)) {
      console.log('[refresh] ✓ forecast re-persisted — net-cash-forecast.json is fresh.');
      process.exit(0);
    }
    console.error('[refresh] ⚠ persist POST not observed. Check: bot logged in, active scenario = LSports/2026/Exit plan June26, and /api/net-cash-forecast route is live.');
    process.exit(2);
  } catch (e) {
    console.error(`[refresh] ERROR: ${e && e.message ? e.message : e}`);
    process.exit(1);
  } finally {
    await browser.close().catch(() => {});
  }
}
main();
