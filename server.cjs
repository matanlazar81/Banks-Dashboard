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
//   CACHE_TTL_MIN      shared API cache TTL in minutes (default 5; 20 recommended
//                      here — the warm loop keeps it fresh anyway)
//   WARM_INTERVAL_MIN  keep-warm sweep interval (default 15; 0 disables)
//   WARM_SUBSIDIARIES / WARM_ENDPOINTS  see server/warm-cache.cjs
// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');
const fs = require('fs');
const express = require('express');
const { registerApiRoutes } = require('./server/api-routes.cjs');
const { startWarmCache } = require('./server/warm-cache.cjs');

const PORT = parseInt(process.env.PORT || '8790', 10);
const DIST = path.resolve(__dirname, 'dist');

if (!fs.existsSync(path.join(DIST, 'index.html'))) {
  console.error('[server] dist/index.html not found — run `npm run build` first.');
  process.exit(1);
}

const app = express();

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

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[server] Banks-Dashboard listening on http://0.0.0.0:${PORT}`);
  console.log('[server] all data pulls run here — browsers only read /api/*');
  startWarmCache(PORT);
});
