# Standalone shared server — all data operations on ONE host

Every data pull (NetSuite, Snowflake), cache, scenario save and cron in this app runs
**server-side**. The browser only calls relative `/api/*` endpoints. What used to be the
problem: in dev mode the "server" is whoever runs `npm run dev` — each laptop becomes its own
isolated host with its own `.env`, cache and `data/` files, so people saw different data.

`server.cjs` fixes that: one machine hosts the built dashboard **and** all `/api/*` routes.
Everyone opens the same URL and sees the same shared data instantly, on first load and after
any Refresh.

```
┌─ your laptop ──────────┐        ┌─ shared server ───────────────────────────────┐
│ browser                │  HTTP  │ server.cjs (express, port 8790)               │
│ http://<server>:8790 ──┼───────►│  ├─ server/api-routes.cjs   ← ALL /api/*      │
│ (no creds, no fetching)│        │  ├─ dist/                   ← built dashboard │
└────────────────────────┘        │  ├─ shared cache (data/api-cache.json)        │
                                  │  ├─ data/*.json, SQLite     ← shared state    │
                                  │  └─ crons (net-cash compute + snapshot)       │
                                  │ .env lives ONLY here                          │
                                  └───────────────────────────────────────────────┘
```

## Files

| File | Role |
|---|---|
| `server/api-routes.cjs` | **Single home of every `/api/*` handler** — mounted by both the Vite dev server and `server.cjs`, so dev and prod cannot drift. Edit routes here (dev auto-restarts on change). |
| `server.cjs` | Production entry: express app → API routes → static `dist/` → SPA fallback. |
| `server/warm-cache.cjs` | Keep-warm loop: re-pulls the hot endpoints every `WARM_INTERVAL_MIN` minutes so the cache is always fresh. |
| `ecosystem.config.cjs` | pm2 config (`pm2 start ecosystem.config.cjs`). |

## Deploy / run

```bash
# on the shared server (e.g. the finance-it Ubuntu box), repo checked out with .env in place
npm install
npm run build          # → dist/
node server.cjs        # or: pm2 start ecosystem.config.cjs && pm2 save
```

Users browse `http://<server>:8790`. `npm run dev` remains for development on your own
machine only — do **not** run it on the production checkout while pm2 is running (two
processes would write the same `data/*.json` files).

## How the data stays fresh and identical for everyone

- **Shared cache with disk persistence** — API responses cache in-process
  (`CACHE_TTL_MIN`, default 5 min; 20 recommended in production) and persist to
  `data/api-cache.json`, so even a restarted server serves the last known data instantly.
- **Stale-while-revalidate** — an expired cache entry is served immediately while the
  server refreshes it in the background; nobody waits on NetSuite after first prime.
- **Keep-warm loop** — every `WARM_INTERVAL_MIN` (default 15) the server re-pulls the hot
  endpoints (`bank-balance`, `bank-accounts`, `vendor-bills`, `salary-data`,
  `vendor-history`, `banks-collection-data`, `consolidated-data`) for subsidiaries 3+6.
- **Refresh button** — forces a server-side re-pull (`refresh=true`) that updates the
  shared cache, so one person's Refresh refreshes it for everyone.
- **Crons unchanged** — `scripts/net-cash-forecast-compute.cjs` (06:00) and
  `scripts/net-cash-snapshot.cjs --refresh` (23:00) never depended on the web server; run
  them on the same box so they share `data/` (see `docs/net-cash-snapshot-setup.md`).

## Env knobs (server-only, in `.env` or pm2 env)

| Var | Default | Meaning |
|---|---|---|
| `PORT` | `8790` | Listen port |
| `CACHE_TTL_MIN` | `5` | Shared API cache TTL (minutes) |
| `WARM_INTERVAL_MIN` | `15` | Keep-warm sweep interval; `0` disables |
| `WARM_SUBSIDIARIES` | `3,6` | Subsidiaries to warm |
| `WARM_ENDPOINTS` | built-in set | Override the warmed endpoint list (`{sub}` placeholder) |
| `DEV_USER_EMAIL` | — | Identity fallback when no proxy injects `X-User-Email` |
| `SYNC_ALLOWLIST` | `matan.l@lsports.eu` | Who may use the Sync button |

## Known trade-off: user identity

Without the finance-it parent proxy, nothing injects `X-User-Email`, so all users share the
`DEV_USER_EMAIL` identity (affects per-user prefs and the Sync-button allowlist). If per-user
identity matters, front this server with the existing finance-it proxy (it already injects
the header) — the routes trust `X-User-Email` either way.
