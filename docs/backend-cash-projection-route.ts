// ─────────────────────────────────────────────────────────────────────────────
// finance-it-backend: serve the New Bank Dashboard's data calls.
//   GET /api/cash-projection             the projection (one call renders the page)
//   GET /api/cash-projection/breakdown   what makes up one cell (the movable breakdown window)
//
// ONLY NEEDED IF the shared bank-dashboard API mount (docs/backend-bank-dashboard-api.ts →
// mountBankDashboardApi) is NOT installed. That mount registers every route of
// server/api-routes.cjs, including both of these, automatically. Check first:
//     pm2 logs finance-it-backend | grep "shared API mounted"
// If that line is there, skip this file.
//
// WHAT THIS DOES: mounts server/cash-projection.cjs and server/cash-projection-breakdown.cjs from the
// bank-dashboard checkout. The modules are self-contained: they load the checkout's .env (same
// NetSuite/Snowflake/Postgres settings as the nightly net-cash job), build their own clients,
// serialize their NetSuite calls, and cache in <checkout>/data/cash-projection-cache.json.
// GET only — no body parsing, no CSRF.
//
// AUTH: finance-it-backend has no global /api auth gate (routes guard themselves), and the shared
// handlers do no identity check of their own, so both routes are gated here with the same role as the
// Bank Dashboard's static assets and API routes.
//
// HOW TO ADD:
//   1. Copy this file to src/routes/cash-projection.ts
//   2. In src/index.ts:  import { mountCashProjection } from './routes/cash-projection';
//      and call  mountCashProjection(app);  after the session/passport middleware, before any /api
//      catch-all or 404 handler.
//   3. npm run build && pm2 restart finance-it-backend
//
// VERIFY: pm2 logs show "[cash-projection] mounted" and "[cash-projection] breakdown mounted"; in the
// browser (logged in) open /api/cash-projection → {"status":"computing",…} on the first call, then
// {"status":"ready",…}; /api/cash-projection/breakdown?line=salary&period=2026-06 → {"status":"ready",…}.
// ─────────────────────────────────────────────────────────────────────────────

import type express from 'express';
import { requireRole } from '../middleware/auth';
import { UserRole } from '../../../shared/src/types';

const BANK_DASHBOARD_DIR =
  process.env.BANK_DASHBOARD_DIR ||
  '/home/ubuntu/finance-it/extra-apps/bank-dashboard';

export function mountCashProjection(app: express.Express): void {
  const bankRole = requireRole(UserRole.BANK_DASHBOARD) as any;
  try {
    // Runtime require: CommonJS module inside the checkout, resolving its own node_modules.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createCashProjectionHandler } = require(`${BANK_DASHBOARD_DIR}/server/cash-projection.cjs`);
    const handler = createCashProjectionHandler();
    app.get('/api/cash-projection', bankRole, (req, res) => handler(req, res));
    console.log(`[cash-projection] mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(
      `[cash-projection] FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}. ` +
      'Is the checkout up to date and npm-installed?'
    );
  }
  // Separately guarded: a checkout from before the breakdown keeps the projection itself working.
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createCashProjectionBreakdownHandler } = require(`${BANK_DASHBOARD_DIR}/server/cash-projection-breakdown.cjs`);
    const breakdown = createCashProjectionBreakdownHandler();
    app.get('/api/cash-projection/breakdown', bankRole, (req, res) => breakdown(req, res));
    console.log(`[cash-projection] breakdown mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(
      `[cash-projection] breakdown FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}.`
    );
  }
}
