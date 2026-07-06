// ─────────────────────────────────────────────────────────────────────────────
// finance-it-backend: mount the shared bank-dashboard API module.
//
// TOPOLOGY (as discovered on the prod box): finance-it-backend serves the built
// dashboard as static files AND serves every dashboard /api/* route from a large
// hand-maintained copy, src/routes/bankDashboardApi.ts, registered as
//     app.use('/api', bankDashboardApiRoutes);        // ~line 450 of src/index.ts
// That copy drifts from the dashboard repo and lacks the caching upgrades
// (disk-persisted shared cache, stale-while-revalidate, request coalescing).
//
// WHAT THIS DOES: mounts the dashboard repo's canonical route module
// (<submodule>/server/api-routes.cjs — the exact code its dev server runs) so it
// takes precedence for the READ-ONLY NetSuite/Snowflake data routes, while the
// EXCLUDED_ROUTES below stay with the backend's own implementations (Postgres
// scenarios with sharing/impersonation, session-based whoami, backend-owned
// stores). Express is first-match-wins, so:
//   • routes in the shared module and not excluded  → served here (cached, fresh)
//   • excluded routes                               → fall through to bankDashboardApiRoutes
//   • routes only the backend copy has              → unchanged
//
// The module is self-contained: it loads the SUBMODULE's .env (same creds the
// crons use), resolves data/ and SQLite against the submodule root, and its deps
// come from the submodule's node_modules. No new env vars or packages here.
//
// BODY REPLAY: index.ts registers express.json() long before this mount, so
// request bodies arrive already parsed and the stream consumed. The shared
// handlers read their own bodies, so withBodyReplay() re-streams req.body.
//
// KEEP-WARM LOOP: OFF by default here — the backend's /api auth gate rejects the
// loop's cookie-less localhost self-fetches with 401. Stale-while-revalidate +
// the disk-persisted cache already make loads instant after the first hit.
// Opt in with WARM_INTERVAL_MIN=<minutes> only if the gate exempts localhost.
//
// HOW TO ADD:
//   1. Copy this file to src/routes/bank-dashboard-api.ts (overwrite older copy).
//   2. In src/index.ts add the import, and insert
//          mountBankDashboardApi(app);
//      IMMEDIATELY BEFORE the line:
//          app.use('/api', bankDashboardApiRoutes);
//      (i.e. after session/passport/the /api auth gate — routes stay
//      login-protected — and before the backend's dashboard copy).
//   3. npm run build && pm2 restart finance-it-backend
//
// VERIFY: pm2 logs show "[bank-dashboard] shared API mounted", "[cache] hydrated";
// in the BROWSER (curl gets 401 from the auth gate — that's correct) hard-reload
// the dashboard twice: second load is instant; saving a scenario still works
// (scenarios stay on the backend's Postgres implementation).
// ─────────────────────────────────────────────────────────────────────────────

import type express from 'express';

const BANK_DASHBOARD_DIR =
  process.env.BANK_DASHBOARD_DIR ||
  '/home/ubuntu/finance-it/extra-apps/bank-dashboard';

// Stateful / user-aware routes that stay with the backend's bankDashboardApi.ts.
// Everything else in the shared module (the read-only data routes) mounts here.
const EXCLUDED_ROUTES = new Set<string>([
  '/api/scenarios',            // backend: Postgres per-user + sharing + impersonation
  '/api/whoami',               // backend: real session identity
  '/api/user-pref',            // backend-owned user prefs
  '/api/chat',                 // backend-owned chat
  '/api/chat-history',         // backend-owned chat history store
  '/api/net-cash-forecast',    // backend copy writes the cron-read snapshot (NET_CASH_FILE)
  '/api/budget-targets',       // backend-owned budget-targets store (SQLite)
  '/api/budget-target-edits',  // reads that same store's edit log
  '/api/sync-budget-targets',  // Snowflake write + allowlist — keep backend's
  '/api/budget-snapshot',      // writes data/budgets/*.json — keep backend's
  '/api/budget-snapshot-patch',// writes those same snapshot files
  '/api/budget-years',         // lists those same snapshot files
  '/api/arr-history',          // reads arr-history.json — backend-owned store
  '/api/arr-current',          // WRITES arr-history.json on every read (auto-snapshot)
]);

// Re-stream the already-parsed body for handlers that read the raw request.
// Covers both patterns the shared module uses:
//   for await (const chunk of req) ...   and   req.on('data'/'end', ...)
function withBodyReplay(handler: (req: any, res: any) => void) {
  return (req: any, res: any) => {
    if (req.body !== undefined && req.readableEnded) {
      const raw = Buffer.from(typeof req.body === 'string' ? req.body : JSON.stringify(req.body));
      req[Symbol.asyncIterator] = async function* () { yield raw; };
      const origOn = req.on.bind(req);
      req.on = (ev: string, fn: any) => {
        if (ev === 'data') { process.nextTick(() => fn(raw)); return req; }
        if (ev === 'end') { process.nextTick(() => fn()); return req; }
        return origOn(ev, fn);
      };
    }
    handler(req, res);
  };
}

export function mountBankDashboardApi(app: express.Express): void {
  let registerApiRoutes: (target: any) => void;
  try {
    // Runtime require (not import): the module is CommonJS inside the submodule
    // checkout and resolves its own deps from the submodule's node_modules.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    ({ registerApiRoutes } = require(`${BANK_DASHBOARD_DIR}/server/api-routes.cjs`));
  } catch (e: any) {
    console.error(
      `[bank-dashboard] FAILED to load shared API from ${BANK_DASHBOARD_DIR}: ` +
      `${e?.message || e}. Is the submodule checked out and npm-installed?`
    );
    return;
  }

  const skipped: string[] = [];
  // Facade: the shared module only ever calls target.use(route, handler).
  const facade = {
    use(route: string, handler: any) {
      if (EXCLUDED_ROUTES.has(route)) { skipped.push(route); return; }
      app.use(route, withBodyReplay(handler));
    },
  };
  registerApiRoutes(facade);
  console.log(
    `[bank-dashboard] shared API mounted from ${BANK_DASHBOARD_DIR} ` +
    `(kept on backend: ${skipped.join(', ') || 'none'})`
  );

  const warmMin = parseInt(process.env.WARM_INTERVAL_MIN || '0', 10);
  if (warmMin > 0) {
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { startWarmCache } = require(`${BANK_DASHBOARD_DIR}/server/warm-cache.cjs`);
      startWarmCache(parseInt(process.env.BACKEND_PORT || process.env.PORT || '3001', 10));
    } catch (e: any) {
      console.warn(`[bank-dashboard] warm-cache loop not started: ${e?.message || e}`);
    }
  }
}
