// ─────────────────────────────────────────────────────────────────────────────
// finance-it-backend: serve GET /api/cash-projection (the New Bank Dashboard's data call).
//
// ONLY NEEDED IF the shared bank-dashboard API mount (docs/backend-bank-dashboard-api.ts →
// mountBankDashboardApi) is NOT installed. That mount registers every route of
// server/api-routes.cjs, including /api/cash-projection, automatically. Check first:
//     pm2 logs finance-it-backend | grep "shared API mounted"
// If that line is there, skip this file.
//
// WHAT THIS DOES: mounts server/cash-projection.cjs from the bank-dashboard checkout. The module is
// self-contained: it loads the checkout's .env (same NetSuite/Snowflake/Postgres settings as the
// nightly net-cash job), builds its own clients, serializes its NetSuite calls, and caches the
// result in <checkout>/data/cash-projection-cache.json. GET only — no body parsing, no CSRF.
//
// HOW TO ADD:
//   1. Copy this file to src/routes/cash-projection.ts
//   2. In src/index.ts:  import { mountCashProjection } from './routes/cash-projection';
//      and call  mountCashProjection(app);  after the session/passport middleware and the /api
//      auth gate (so the route stays login-protected), before any /api catch-all or 404 handler.
//   3. npm run build && pm2 restart finance-it-backend
//
// VERIFY: pm2 logs show "[cash-projection] mounted"; in the browser (logged in) open
//   /api/cash-projection  → {"status":"computing",…} on the first call, then {"status":"ready",…}.
// ─────────────────────────────────────────────────────────────────────────────

import type express from 'express';

const BANK_DASHBOARD_DIR =
  process.env.BANK_DASHBOARD_DIR ||
  '/home/ubuntu/finance-it/extra-apps/bank-dashboard';

export function mountCashProjection(app: express.Express): void {
  try {
    // Runtime require: CommonJS module inside the checkout, resolving its own node_modules.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createCashProjectionHandler } = require(`${BANK_DASHBOARD_DIR}/server/cash-projection.cjs`);
    const handler = createCashProjectionHandler();
    app.get('/api/cash-projection', (req, res) => handler(req, res));
    console.log(`[cash-projection] mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(
      `[cash-projection] FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}. ` +
      'Is the checkout up to date and npm-installed?'
    );
  }
}
