# Daily Net Cash snapshot → Snowflake — setup

Appends one row per run into `RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST`
(append-only history — every update is a new row, never overwritten). See
`scripts/net-cash-snapshot.cjs`.

## Data flow

```
Server compute (cron 06:00 & 23:00, NO browser) — the primary path
   └─ node scripts/net-cash-forecast-compute.cjs
        • gathers NetSuite + Snowflake feeds; loads "Exit plan June26" from Postgres (user_scenarios)
        • runs the shared engine (src/forecast/forecast-core.mjs — the exact module the dashboard runs)
        └─ writes data/net-cash-forecast.json   { forecastEur, scenario, ... }
Dashboard (same engine, client-side) — also persists the file whenever it's opened/refreshed
Cron @ 23:00 Asia/Jerusalem
   └─ node scripts/net-cash-snapshot.cjs --refresh
        • runs the compute above first, then:
        • TOTAL_BANK_EUR ← live NetSuite balance as of the last CLOSED month-end
        •                  (reval-closed guard: steps back if the month-end FX reval isn't posted)
        • FORECAST_EUR   ← the freshly computed data/net-cash-forecast.json (or carry-forward)
        • MODEL_CLOSING_EUR ← the model's flow-forward closing of the last completed month
        └─ INSERT one row into Snowflake  →  Slack summary
```

- `DATE` = the **sync timestamp incl. the hour**, in Asia/Jerusalem (e.g. `2026-07-01 23:00:07`).
- `TOTAL_BANK_EUR` = NetSuite bank balance (all BANK/CredCard accounts, EUR primary book)
  **as of the end of the previous month** — every day in July reports the Jun 30 balance,
  every day in August reports Jul 31, etc. Fetched live from NetSuite
  (`fetchBankAccountListAsOf`), so it needs no dashboard. Override with `NET_CASH_TOTAL_BANK_EUR`.
  **Reval-closed guard**: a month-end is only trusted once NetSuite carries the **posted**
  month-end FX revaluation dated on that day (`hasPostedMonthEndReval`). If June 30's reval
  isn't posted yet, the job steps back to May 31 (max 2 steps) and flags the fallback in the
  Slack/email summary — so a partially-posted month is never reported as final.
- `MODEL_CLOSING_EUR` = the dashboard model's **flow-forward closing of the last completed
  month** (from `data/net-cash-forecast.json`, field `modelClosingEur`, written by the compute).
  This is the model-side twin of `TOTAL_BANK_EUR`: the same month-end seen through the
  cashflow model instead of the posted ledger — the difference is bookkeeping not yet posted.
  Written only if the column exists; add it once with
  `ALTER TABLE RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST ADD COLUMN MODEL_CLOSING_EUR FLOAT;`
- `FORECAST_EUR` = the year-end (December) closing balance, after savings. Computed client-side
  on the **Revenue:Pipeline + Salary:Actual** basis (the agreed methodology), so it is persisted
  from the UI. The persist is **gated to that basis** — a dashboard viewed in Win-rate or Budget
  mode does **not** overwrite the file (it logs `persist skipped` and moves on), so the shipped
  figure never drifts from Pipeline/Actual. Fallback chain: env override → persisted snapshot →
  **carry-forward** (reuse the last row's `FORECAST_EUR`). The first run has no prior row, so seed
  it once with `NET_CASH_FORECAST_EUR`.
- `SRC_UPDATED_AT` = `CURRENT_TIMESTAMP()` at insert (the load time; the live column is spelled
  `SRC_UPDATED_AT`, with a "D"). `IS_APPROVED_UPDATED_AT` = **NOT written** — set by the external
  approval automation.
- The INSERT **adapts to the table's actual columns**: it always writes `DATE` /
  `TOTAL_BANK_EUR` / `FORECAST_EUR`, and adds `MODEL_CLOSING_EUR` and `IS_APPROVED=FALSE`
  only if those columns exist.

## 1. Snowflake write access

The app's own Snowflake client (`snowflake-api.cjs`) is **read-only by design** and points
at `DL_PRODUCTION`. This job uses its **own** connection to the `RAW` database. The service
user needs `INSERT` on the table (and `USAGE` on db/schema/warehouse). If the table does not
exist yet, it also needs `CREATE TABLE` on the schema (or create it once manually).

```sql
-- run as a role that owns RAW.LANDING_FINANCE (adjust role/user names)
GRANT USAGE ON DATABASE RAW TO ROLE <WRITE_ROLE>;
GRANT USAGE ON SCHEMA RAW.LANDING_FINANCE TO ROLE <WRITE_ROLE>;
GRANT INSERT, SELECT ON TABLE RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST TO ROLE <WRITE_ROLE>;
GRANT USAGE ON WAREHOUSE finance_wh TO ROLE <WRITE_ROLE>;
GRANT ROLE <WRITE_ROLE> TO USER <SERVICE_USER>;
```

Table DDL (matches the agreed schema — the script's `--create-table` runs exactly this):

```sql
CREATE TABLE IF NOT EXISTS RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST (
  DATE                   TIMESTAMP_NTZ,
  TOTAL_BANK_EUR         FLOAT,
  FORECAST_EUR           FLOAT,
  MODEL_CLOSING_EUR      FLOAT,
  SRC_UPDATE_AT          TIMESTAMP_NTZ,
  IS_APPROVED            BOOLEAN,
  IS_APPROVED_UPDATED_AT TIMESTAMP_NTZ
);
```

## 2. Environment

The script reads `.env` in the repo root (or the exported environment). It reuses the
existing Snowflake vars; add write-specific overrides only if the write user differs:

```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PRIVATE_KEY_PATH=/abs/path/to/key.pem
SNOWFLAKE_WAREHOUSE=finance_wh
# optional overrides (fall back to the above):
# SNOWFLAKE_WRITE_USER=...
# SNOWFLAKE_WRITE_PRIVATE_KEY_PATH=/abs/path/to/write-key.pem
# SNOWFLAKE_WRITE_ROLE=<WRITE_ROLE>

# on/off switch + email summary (see §7):
# NET_CASH_SYNC_ENABLED=true            # set false to pause the write; email still fires
# NET_CASH_EMAIL_TO=matan.l@lsports.eu
# NET_CASH_SMTP_URL=smtp://smtp-relay.gmail.com:587   # IP-allowlisted relay (no creds) OR smtps://user:app_pw@smtp.gmail.com:465
# NET_CASH_EMAIL_FROM=finance-bot@lsports.eu          # optional; defaults to NET_CASH_EMAIL_TO
```

## 3. Verify before scheduling

```bash
cd /home/ubuntu/finance-it/extra-apps/bank-dashboard

# 1. See the table's actual columns.
node scripts/net-cash-snapshot.cjs --describe

# 2. Dry run (credential-free). Bank auto-fetches from NetSuite (prev month-end);
#    seed the forecast for the first run only.
NET_CASH_FORECAST_EUR=7048154 node scripts/net-cash-snapshot.cjs --dry-run

# 3. First real insert. --create-table only if the table doesn't exist yet.
NET_CASH_FORECAST_EUR=7048154 node scripts/net-cash-snapshot.cjs            # or add: --create-table
```

- The bank balance auto-fetches from NetSuite as of the **previous month-end** — no need to
  pass it. Only the forecast needs a one-time seed; after that it carries forward.
- The insert **adapts to the table's actual columns** (writes `DATE` / `TOTAL_BANK_EUR` /
  `FORECAST_EUR`, plus `SRC_UPDATED_AT=CURRENT_TIMESTAMP()` and `IS_APPROVED=FALSE` if those
  columns exist).
- `--dry-run` needs **no** Snowflake credentials.
- `--show` prints the last 10 rows in the table (to inspect what's there).
- **Append-only**: every run inserts a **new** row — no dedupe, no overwrite, no delete. Each
  update is preserved as its own row (keyed by the DATE sync timestamp), so the table holds the
  full history of every sync. To correct a value, just run again with the right figure; the
  latest row (by DATE / SRC_UPDATED_AT) is the current one.
- `TOTAL_BANK_EUR` is fetched live from NetSuite if not in env/snapshot — no need to paste it.
- After the first row exists, later runs with no env/snapshot **carry forward** the last
  `FORECAST_EUR`, so the daily job keeps working; the number refreshes once the dashboard
  persists a new snapshot (see backend route below).

## 4. Schedule the cron (23:00 Asia/Jerusalem)

Use `CRON_TZ` so it fires at 23:00 Israel time regardless of the server timezone:

```cron
CRON_TZ=Asia/Jerusalem
0 23 * * * cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && /usr/bin/node scripts/net-cash-snapshot.cjs >> /var/log/net-cash-snapshot.log 2>&1
```

Install with `crontab -e` (adjust the node path via `which node`). If your cron doesn't
support `CRON_TZ`, schedule in UTC instead: 20:00 UTC during IDT (summer, UTC+3) or 21:00 UTC
during IST (winter, UTC+2).

## 5. Full auto-persist (auto-refresh the forecast)

With just the cron, the **bank** figure is fully automatic (live from NetSuite) and the
**forecast** carries forward from the last row. To also **auto-refresh the forecast** as the
Exit-plan-June26 plan changes, the production dashboard must be able to persist it — which
needs the `/api/net-cash-forecast` route on the prod `/api/*` host. **This route ships inside
the shared module `server/api-routes.cjs`**, so it is already included if you either
(a) deploy the repo's own standalone server (`server.cjs` — see `docs/standalone-server.md`),
or (b) mount the shared module in `finance-it-backend` via
`docs/backend-bank-dashboard-api.ts` (one `mountBankDashboardApi(app)` call — the preferred
path for the finance-it.lsports.eu setup, replacing all per-route copies including this one).
The single-route copy below remains only as the legacy/manual fallback.

Install it (you are root on the server):

```bash
# 1. Copy the ready-made route into finance-it-backend's routes dir (adjust path to your layout).
cp /home/ubuntu/finance-it/extra-apps/bank-dashboard/docs/backend-net-cash-forecast-route.ts \
   /home/ubuntu/finance-it/backend/src/routes/net-cash-forecast.ts

# 2. Register it in the backend's app (wherever other routers are mounted), e.g.:
#      import netCashForecast from './routes/net-cash-forecast';
#      app.use(netCashForecast);
#    The route defaults NET_CASH_FILE to
#      /home/ubuntu/finance-it/extra-apps/bank-dashboard/data/net-cash-forecast.json
#    which is exactly where the cron reads — keep it, or set NET_CASH_FILE to match.

# 3. Rebuild + restart the backend.
cd /home/ubuntu/finance-it/backend && npm run build && pm2 restart finance-it-backend
```

Once mounted: whenever anyone loads the LSports current-year dashboard **on the Exit plan
June26 scenario**, the frontend POSTs `forecastEur` (currently €7,048,154) to that route, which
writes `data/net-cash-forecast.json`. The nightly cron then picks it up automatically — no more
seeding. Other scenarios do not overwrite the file.

## 6. Server-side forecast recompute (no browser) — `scripts/net-cash-forecast-compute.cjs`

Without this, `FORECAST_EUR` only changes when someone opens the dashboard on Exit plan June26
(the persist). To recompute it automatically with **no browser and no login**, this script
fetches fresh NetSuite + Snowflake data, runs the **shared forecast engine**
(`src/forecast/forecast-core.mjs` — the exact module the dashboard runs), loads the "Exit plan
June26" scenario knobs, forces **Revenue: Pipeline + Salary: Last-Actual**, and rewrites
`data/net-cash-forecast.json`. Because it runs the same engine on the same inputs the browser
builds, the number cannot diverge from the on-screen forecast.

The scenario knobs (dept salary cuts, vendor cuts, currency-defense %) are NOT data feeds — they
come from the saved scenario. In prod, scenarios live in **Postgres** (`user_scenarios.data`), which
the job reads **automatically** via the backend's `DATABASE_URL` (already in `.env`) using `psql` —
**no configuration needed**. Resolution order (first match wins):
```bash
#   NET_CASH_SCENARIO_FILE=/path/to/exit-plan.json   # (test override) a single ScenarioData or {data}/record
#   (default) Postgres user_scenarios via DATABASE_URL — matched by name; NO config needed
#   NET_CASH_SCENARIOS_PATH=/path/to/scenarios.json  # (dev only) a scenarios-array file, matched by name
#   NET_CASH_SCENARIO_NAME="Exit plan June26"        # name to match (default)
#   NET_CASH_SCENARIO_OWNER=<owner-email>            # optional: pin the owner if a name is shared across users
# If none resolves, the job runs the BASE plan (no savings) and says so — the number will be too high.
```
The connection string is never logged (any `postgres://…` URI is redacted from errors). NetSuite feeds
are throttled to stay under NetSuite's concurrency governor (HTTP 429); tune with
**`NET_CASH_NS_CONCURRENCY`** (default 3 — lower to 2 if you still see repeated 429 retries).

Verify it (credential-full run, on the server):
```bash
cd /home/ubuntu/finance-it/extra-apps/bank-dashboard
node scripts/net-cash-forecast-compute.cjs --dry-run       # fetch + compute + print 12 rows, no write
node scripts/net-cash-forecast-compute.cjs --dump-inputs   # also write data/net-cash-inputs.json
node scripts/net-cash-forecast-compute.cjs                 # compute + write data/net-cash-forecast.json
```
Compare the printed **December closing** to a live dashboard load of Exit plan June26 (Pipeline +
Last-Actual). To debug any gap, open the dashboard with `?fccapture=1`, copy `window.__fcInputs` from
the console, and diff it against the `--dump-inputs` output field by field (see
`docs/forecast-core-golden-test.md`).

### Schedule — 06:00 recompute (silent) + 23:00 recompute-and-push

```cron
CRON_TZ=Asia/Jerusalem
# 06:00 — recompute only. Refreshes data/net-cash-forecast.json. NO Snowflake write, NO Slack.
0 6 * * * cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && /usr/bin/node scripts/net-cash-forecast-compute.cjs >> /var/log/net-cash.log 2>&1
# 23:00 — recompute → push to Snowflake → Slack summary (ONE command; no file race, no NS concurrency clash).
0 23 * * * cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && /usr/bin/node scripts/net-cash-snapshot.cjs --refresh >> /var/log/net-cash.log 2>&1
```
`--refresh` runs the compute script first, then reads the freshly written
`data/net-cash-forecast.json` and pushes it. If the compute can't run (e.g. a feed is down),
`--refresh` logs it and still pushes the last persisted forecast, so the nightly row is never
skipped. A **manual browser refresh keeps working** and produces the identical number (same engine).

> The old headless-browser refresh (`scripts/refresh-forecast-headless.cjs`) is **superseded** by
> this and no longer needed — no bot user, no Playwright, no login.

## 7. Run summary (email and/or Slack) + on/off switch

Every run sends a one-page summary of exactly what it pushed to **email and/or Slack** — whichever
is configured, independently. You get a nightly receipt and an easy kill-switch. Nothing is
hard-coded — tokens and recipients live in `.env`.

### Slack (recommended here — reuses the backend's bot token, no SMTP/Google auth)

```
SLACK_BOT_TOKEN=...                 # already present in the backend .env
NET_CASH_SLACK_CHANNEL=cash_flow_sync   # channel name or ID; default is cash_flow_sync
```

The bot must be a **member of the channel** — in Slack, open `#cash_flow_sync` and
`/invite @<bot>` (or add it via the channel's Integrations). If a post fails with
`channel_not_found` / `not_in_channel`, invite the bot or set `NET_CASH_SLACK_CHANNEL` to the
channel **ID**.

### Email (optional)

Every run emails a one-page summary of exactly what it pushed, so you get a nightly receipt and
an easy kill-switch. Nothing is hard-coded — the transport and recipient live in `.env`.

Transport is resolved in this order:
1. `NET_CASH_SMTP_URL` — an explicit connection URL, if you set one.
2. **else the app's existing `SMTP_HOST` / `SMTP_PORT` / `SMTP_USER` / `SMTP_PASS`** — reuse the
   backend's already-working mailer, so you add **no new credentials**. This is the recommended path.

```
# .env — recommended: just set the recipient and let it reuse the backend's SMTP_* creds
NET_CASH_EMAIL_TO=matan.l@lsports.eu
# NET_CASH_EMAIL_FROM=finance@lsports.eu     # optional; defaults to SMTP_USER, then NET_CASH_EMAIL_TO

# OR override with an explicit URL instead of SMTP_*:
# NET_CASH_SMTP_URL=smtp://smtp-relay.gmail.com:587                                   # relay, no password
# NET_CASH_SMTP_URL=smtps://finance%40lsports.eu:APP_PASSWORD@smtp.gmail.com:465      # or an app password
# NET_CASH_SMTP_HELO=lsports.eu     # EHLO name — required by the Workspace relay; defaults to the sender's domain
```

**Workspace SMTP relay:** if you use `smtp-relay.gmail.com` with no auth, Google requires the server
to present one of your domains in the EHLO greeting and to send from that domain. The script sets the
EHLO name to the sender's domain automatically (from `NET_CASH_EMAIL_TO`/`FROM`), so
`NET_CASH_EMAIL_TO=matan.l@lsports.eu` → EHLO `lsports.eu`. Override with `NET_CASH_SMTP_HELO` if needed.
The relay only accepts senders in your registered domain, and only from the allowlisted server IP.

Install the mailer once (added to `package.json`): `npm i nodemailer`.

The email contains: run timestamp, the **ACTIVE flag (TRUE/FALSE)**, status (row written / disabled /
failed), `DATE` / `TOTAL_BANK_EUR` / `FORECAST_EUR`, and the basis (Revenue: Pipeline, Salary: Actual).
If neither `NET_CASH_SMTP_URL` nor `SMTP_HOST` is set (or `NET_CASH_EMAIL_TO` is missing), the job
just **logs** the body (no send) and still writes Snowflake — so email is strictly additive.

**Stop / activate whenever you want** — no code change, no restart (the cron re-reads `.env` each run):

```bash
# pause the sync (no Snowflake write; you still get a "DISABLED" email as confirmation):
#   set in .env →  NET_CASH_SYNC_ENABLED=false
# resume:
#   set in .env →  NET_CASH_SYNC_ENABLED=true   (or delete the line — default is on)
```

A disabled run also doubles as an email test: it sends the summary without writing a row.

## Notes

- `IS_APPROVED` is written `FALSE` and `SRC_UPDATED_AT` is stamped with the insert time (both if
  the columns exist). `IS_APPROVED_UPDATED_AT` is **not** written — the external Workato/n8n
  approval automation manages it.
- Append-only: the job never updates or deletes; every day adds one row.
- The bank figure is anchored to the previous month-end, so it is stable for the whole month
  (every day in July shows the Jun 30 balance); it steps once at each month boundary.
