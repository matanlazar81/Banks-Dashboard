// ─────────────────────────────────────────────────────────────────────────────
// finance-it-backend: mount the ENTIRE bank-dashboard API in one line.
//
// WHY: In production the bank-dashboard frontend is static files served by
// finance-it-backend, and every /api/* call hits finance-it-backend. Until now
// each dashboard route was hand-copied from a docs/backend-*.ts template — a
// partial, drifting copy. The dashboard repo now ships ALL ~57 /api/* handlers
// as one shared CommonJS module (server/api-routes.cjs — the exact module its
// dev server mounts), including the caching upgrades: disk-persisted shared
// cache, stale-while-revalidate, and a keep-warm loop. Mounting that module
// here replaces every copied route and keeps prod automatically in sync with
// the dashboard repo on each deploy.
//
// The module is self-contained: it loads the SUBMODULE's own .env
// (<bank-dashboard>/.env — same credentials the crons already use), resolves
// data/ and the SQLite db against the submodule root, and its dependencies
// resolve from the submodule's node_modules (installed by the deploy script).
// finance-it-backend needs NO new env vars or packages.
//
// HOW TO ADD:
//   1. Copy this file into finance-it-backend (e.g. src/routes/bank-dashboard-api.ts).
//   2. In the backend's app bootstrap, call:
//         mountBankDashboardApi(app);
//      ORDER IS CRITICAL — register it:
//        • AFTER the auth / X-User-Email-injection middleware (the dashboard's
//          Sync allowlist and per-user prefs read that header), and
//        • BEFORE express.json() / any body parser (the dashboard handlers
//          stream their own request bodies; a body parser that runs first
//          breaks every dashboard POST — scenario saves, budget edits, chat), and
//        • BEFORE any previously copied bank-dashboard routes (Express is
//          first-match-wins, so this mount supersedes them).
//   3. npm run build && pm2 restart finance-it-backend
//   4. Cleanup (optional but recommended): delete the now-shadowed copies of
//      the old per-route templates — net-cash-forecast, forecast-actuals,
//      ns-actuals, salary-breakdown, pipeline-methodology,
//      budget-snapshot-salary-breakdown, budget-targets.
//
// VERIFY after restart:
//   curl -s https://finance-it.lsports.eu/api/ns-config   → {"accountId":"..."}
//   Save a scenario from the dashboard (proves body-parser ordering is right).
//   Backend logs show "[cache] hydrated ..." and "[warm] keep-warm loop armed".
//
// ENV KNOBS (all optional, read by the shared module / warm loop):
//   BANK_DASHBOARD_DIR   submodule checkout dir (default below)
//   BACKEND_PORT / PORT  the port THIS backend listens on — the warm loop
//                        self-fetches http://127.0.0.1:<port>/api/... on it
//   WARM_INTERVAL_MIN    keep-warm sweep interval, minutes (default 15; 0 = off)
//   WARM_SUBSIDIARIES    default '3,6' (LSports + Statscore)
//   CACHE_TTL_MIN        shared cache TTL, minutes (default 5; 20 recommended)
// ─────────────────────────────────────────────────────────────────────────────

import type express from 'express';

const BANK_DASHBOARD_DIR =
  process.env.BANK_DASHBOARD_DIR ||
  '/home/ubuntu/finance-it/extra-apps/bank-dashboard';

export function mountBankDashboardApi(app: express.Express): void {
  try {
    // Runtime require (not import): the module is CommonJS inside the submodule
    // checkout and must resolve its own dependencies from the submodule's
    // node_modules, not from finance-it-backend's build.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { registerApiRoutes } = require(`${BANK_DASHBOARD_DIR}/server/api-routes.cjs`);
    registerApiRoutes(app);
    console.log('[bank-dashboard] API routes mounted from', BANK_DASHBOARD_DIR);
  } catch (e: any) {
    console.error(
      `[bank-dashboard] FAILED to mount API routes from ${BANK_DASHBOARD_DIR}: ` +
      `${e?.message || e}. Is the submodule checked out and npm-installed?`
    );
    return; // don't start the warm loop against routes that aren't there
  }

  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { startWarmCache } = require(`${BANK_DASHBOARD_DIR}/server/warm-cache.cjs`);
    const port = parseInt(process.env.BACKEND_PORT || process.env.PORT || '3001', 10);
    startWarmCache(port);
  } catch (e: any) {
    console.warn(`[bank-dashboard] warm-cache loop not started: ${e?.message || e}`);
  }
}
