// ─────────────────────────────────────────────────────────────────────────────
// finance-it-backend: serve the New Bank Dashboard's and the P&L Projection's data calls.
//   GET /api/cash-projection             the cash projection (one call renders the page)
//   GET /api/cash-projection/breakdown   what makes up one cell (the movable breakdown window)
//   GET /api/pnl-projection              the P&L projection (same pattern)
//   GET /api/pnl-projection/breakdown    what makes up one P&L cell
//   GET/PUT /api/projection-targets      the 2027 targets both pages apply on top of the Plan (set on the
//                                        New Bank Dashboard only: PUT needs its role)
//   GET /api/metrics (+ GET/PUT /api/metrics/settings, /api/metrics/deposits)  the Metrics page
//   GET /api/salary-projection           the Salary Projection (payroll by GL account, NetSuite)
//   GET/PUT /api/salary-projection/classification  which accounts are permanent or temporary (saved for everyone)
//   GET /api/fx-challenge                the FX Challenge (the 7th-of-month EUR→ILS conversions, NetSuite)
//   GET/PUT /api/fx-challenge/table      its conversion table (saved for everyone)
//
// ONLY NEEDED IF the shared bank-dashboard API mount (docs/backend-bank-dashboard-api.ts →
// mountBankDashboardApi) is NOT installed. That mount registers every route of
// server/api-routes.cjs, including all four of these, automatically. Check first:
//     pm2 logs finance-it-backend | grep "shared API mounted"
// If that line is there, skip this file.
//
// WHAT THIS DOES: mounts server/cash-projection.cjs, server/pnl-projection.cjs and their breakdown
// modules from the bank-dashboard checkout. The modules are self-contained: they load the checkout's
// .env (same NetSuite/Snowflake/Postgres settings as the nightly net-cash job), build their own
// clients, serialize their NetSuite calls (one shared queue, one shared input pull), and cache in
// <checkout>/data/cash-projection-cache.json and <checkout>/data/pnl-projection-cache.json.
// GET routes, plus one PUT (/api/projection-targets) that reads its own JSON body and is checked
// for same-origin; finance-it's CSRF middleware, if any, applies to it as to any write.
//
// AUTH: finance-it-backend has no global /api auth gate (routes guard themselves), and the shared
// handlers do no identity check of their own, so every route is gated here with its page's own role:
// new_bank_dashboard, pnl_projection, metrics, salary_projection, fx_challenge (finance-it's shared/src/types BUSINESS_TOOLS
// list, where each Business Tools tab has its own checkbox in Users Management). Admins always pass.
//
// HOW TO ADD:
//   1. Copy this file to src/routes/cash-projection.ts
//   2. In src/index.ts:  import { mountCashProjection } from './routes/cash-projection';
//      and call  mountCashProjection(app);  after the session/passport middleware, before any /api
//      catch-all or 404 handler.
//   3. npm run build && pm2 restart finance-it-backend
//
// VERIFY: pm2 logs show "[cash-projection] mounted", "[cash-projection] breakdown mounted",
// "[pnl-projection] mounted" and "[pnl-projection] breakdown mounted"; in the browser (logged in) open
// /api/cash-projection → {"status":"computing",…} on the first call, then {"status":"ready",…};
// /api/cash-projection/breakdown?line=salary&period=2026-06 → {"status":"ready",…}; the same for
// /api/pnl-projection and /api/pnl-projection/breakdown?line=payroll&period=2026-06; "[salary-projection]
// mounted" and /api/salary-projection the same way; "[fx-challenge] mounted" and /api/fx-challenge → {"status":"ready",…}.
// ─────────────────────────────────────────────────────────────────────────────

import type express from 'express';
import { requireRole } from '../middleware/auth';
import { UserRole } from '../../../shared/src/types';

const BANK_DASHBOARD_DIR =
  process.env.BANK_DASHBOARD_DIR ||
  '/home/ubuntu/finance-it/extra-apps/bank-dashboard';

export function mountCashProjection(app: express.Express): void {
  // Each page's data needs that page's role (finance-it: shared/src/types BUSINESS_TOOLS); admins pass.
  const cashRole = requireRole(UserRole.ADMIN, UserRole.NEW_BANK_DASHBOARD) as any;
  const pnlRole = requireRole(UserRole.ADMIN, UserRole.PNL_PROJECTION) as any;
  const targetsRole = requireRole(UserRole.ADMIN, UserRole.NEW_BANK_DASHBOARD, UserRole.PNL_PROJECTION) as any;
  const metricsRole = requireRole(UserRole.ADMIN, UserRole.METRICS) as any;
  const salaryRole = requireRole(UserRole.ADMIN, UserRole.SALARY_PROJECTION) as any;
  const fxRole = requireRole(UserRole.ADMIN, UserRole.FX_CHALLENGE) as any;
  try {
    // Runtime require: CommonJS module inside the checkout, resolving its own node_modules.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createCashProjectionHandler } = require(`${BANK_DASHBOARD_DIR}/server/cash-projection.cjs`);
    const handler = createCashProjectionHandler();
    app.get('/api/cash-projection', cashRole, (req, res) => handler(req, res));
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
    app.get('/api/cash-projection/breakdown', cashRole, (req, res) => breakdown(req, res));
    console.log(`[cash-projection] breakdown mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(
      `[cash-projection] breakdown FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}.`
    );
  }
  // P&L Projection: separately guarded, so a checkout from before it keeps the cash routes working.
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createPnlProjectionBreakdownHandler } = require(`${BANK_DASHBOARD_DIR}/server/pnl-projection-breakdown.cjs`);
    const pnlBreakdown = createPnlProjectionBreakdownHandler();
    app.get('/api/pnl-projection/breakdown', pnlRole, (req, res) => pnlBreakdown(req, res));
    console.log(`[pnl-projection] breakdown mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(
      `[pnl-projection] breakdown FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}.`
    );
  }
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createPnlProjectionHandler } = require(`${BANK_DASHBOARD_DIR}/server/pnl-projection.cjs`);
    const pnl = createPnlProjectionHandler();
    app.get('/api/pnl-projection', pnlRole, (req, res) => pnl(req, res));
    console.log(`[pnl-projection] mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(
      `[pnl-projection] FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}.`
    );
  }
  // 2027 targets of both projection pages: GET reads them (both pages), PUT saves them (the New Bank
  // Dashboard only: the P&L Projection applies the saved targets and does not edit them). JSON, same
  // origin; the handler reads the body itself when no body parser ran.
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createProjectionTargetsHandler } = require(`${BANK_DASHBOARD_DIR}/server/projection-targets.cjs`);
    const targets = createProjectionTargetsHandler();
    app.get('/api/projection-targets', targetsRole, (req, res) => targets(req, res));
    app.put('/api/projection-targets', cashRole, (req, res) => targets(req, res));
    console.log(`[projection-targets] mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(
      `[projection-targets] FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}.`
    );
  }
  // Metrics page: the pack (GET), its settings and its deposit tracker (GET/PUT). It reads the cash and
  // P&L projections from their cache files and computes them itself when there is none yet.
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const m = require(`${BANK_DASHBOARD_DIR}/server/metrics.cjs`);
    const settings = m.createMetricsSettingsHandler();
    const deposits = m.createMetricsDepositsHandler();
    const metrics = m.createMetricsHandler();
    app.get('/api/metrics/settings', metricsRole, (req, res) => settings(req, res));
    app.put('/api/metrics/settings', metricsRole, (req, res) => settings(req, res));
    app.get('/api/metrics/deposits', metricsRole, (req, res) => deposits(req, res));
    app.put('/api/metrics/deposits', metricsRole, (req, res) => deposits(req, res));
    app.get('/api/metrics', metricsRole, (req, res) => metrics(req, res));
    console.log(`[metrics] mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(`[metrics] FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}.`);
  }
  // Salary Projection: payroll by GL account from Snowflake, permanent and temporary accounts. Separately
  // guarded, so a checkout from before it keeps the other routes working.
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createSalaryProjectionHandler, createSalaryClassificationHandler } = require(`${BANK_DASHBOARD_DIR}/server/salary-projection.cjs`);
    const salary = createSalaryProjectionHandler();
    app.get('/api/salary-projection', salaryRole, (req, res) => salary(req, res));
    // Which accounts are permanent or temporary: read and saved by anyone with the page's role (JSON, same origin).
    if (createSalaryClassificationHandler) {
      const classification = createSalaryClassificationHandler();
      app.get('/api/salary-projection/classification', salaryRole, (req, res) => classification(req, res));
      app.put('/api/salary-projection/classification', salaryRole, (req, res) => classification(req, res));
    }
    console.log(`[salary-projection] mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(`[salary-projection] FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}.`);
  }
  // FX Challenge: the monthly EUR→ILS conversions and what to keep for them (NetSuite, and the daily balances
  // mail's RESTlet when NS_BALANCES_RESTLET is set). Its table is read and saved by anyone with the page's role
  // (JSON, same origin). Separately guarded, so a checkout from before it keeps the other routes working.
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { createFxChallengeHandler, createFxTableHandler } = require(`${BANK_DASHBOARD_DIR}/server/fx-challenge.cjs`);
    const fx = createFxChallengeHandler();
    const table = createFxTableHandler();
    app.get('/api/fx-challenge/table', fxRole, (req, res) => table(req, res));
    app.put('/api/fx-challenge/table', fxRole, (req, res) => table(req, res));
    app.get('/api/fx-challenge', fxRole, (req, res) => fx(req, res));
    console.log(`[fx-challenge] mounted from ${BANK_DASHBOARD_DIR}`);
  } catch (e) {
    console.error(`[fx-challenge] FAILED to mount from ${BANK_DASHBOARD_DIR}: ${e instanceof Error ? e.message : String(e)}.`);
  }
}
