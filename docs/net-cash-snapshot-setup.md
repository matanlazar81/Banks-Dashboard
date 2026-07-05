# Daily Net Cash snapshot → Snowflake — setup

Appends one row per run into `RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST`
(append-only history — every update is a new row, never overwritten). See
`scripts/net-cash-snapshot.cjs`.

## Data flow

```
Dashboard (LSports, current year, "Exit plan June26" scenario, on load)
   └─ POST /api/net-cash-forecast  →  data/net-cash-forecast.json   { forecastEur, scenario, ... }
Cron @ 23:00 Asia/Jerusalem
   └─ node scripts/net-cash-snapshot.cjs
        • TOTAL_BANK_EUR ← live NetSuite balance as of previous month-end
        • FORECAST_EUR   ← persisted snapshot (or carry-forward)
        └─ INSERT one row into Snowflake
```

- `DATE` = the **sync timestamp incl. the hour**, in Asia/Jerusalem (e.g. `2026-07-01 23:00:07`).
- `TOTAL_BANK_EUR` = NetSuite bank balance (all BANK/CredCard accounts, EUR primary book)
  **as of the end of the previous month** — every day in July reports the Jun 30 balance,
  every day in August reports Jul 31, etc. Fetched live from NetSuite
  (`fetchBankAccountListAsOf`), so it needs no dashboard. Override with `NET_CASH_TOTAL_BANK_EUR`.
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
  `TOTAL_BANK_EUR` / `FORECAST_EUR`, and adds `IS_APPROVED=FALSE` only if that column exists.

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
needs the `/api/net-cash-forecast` route in `finance-it-backend` (the prod `/api/*` host;
the vite route only serves local dev).

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

## 6. Same-night forecast refresh (headless, optional) — `scripts/refresh-forecast-headless.cjs`

Without this, `FORECAST_EUR` only changes when someone opens the dashboard on Exit plan June26
(the persist). To capture plan changes automatically each night with **no one's laptop open**,
run a headless browser on the server at **22:50** (10 min before the 23:00 write). It runs the
real dashboard, so the number matches the UI exactly.

One-time setup:
```bash
# a) A bot user with the BANK_DASHBOARD role + a password (finance-it-backend has
#    create-admin/reset-admin scripts). Then log in as the bot ONCE and select
#    LSports · 2026 · "Exit plan June26" so /api/user-pref remembers it for headless loads.
# b) Playwright + Chromium on the server:
cd /home/ubuntu/finance-it/extra-apps/bank-dashboard
npm i playwright && npx playwright install chromium
# c) Add the bot creds to .env (never committed):
#    DASHBOARD_BOT_EMAIL=...      DASHBOARD_BOT_PASSWORD=...
```

Test it, then schedule:
```bash
node scripts/refresh-forecast-headless.cjs        # expect: login → 200, "forecast re-persisted"
# crontab -e — add BEFORE the 23:00 line:
# 50 22 * * * cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && /usr/bin/node scripts/refresh-forecast-headless.cjs >> /var/log/net-cash-refresh.log 2>&1
```

Login is `POST /api/auth/login` with `{email, password}` (passport-local, `usernameField:'email'`).
If that returns 403 (CSRF), the script needs a token-fetch step — see its header.

## 7. Email summary + on/off switch

Every run emails a one-page summary of exactly what it pushed, so you get a nightly receipt and
an easy kill-switch. Nothing is hard-coded — the transport and recipient live in `.env`.

```
# .env — email transport (pick ONE style for NET_CASH_SMTP_URL)
NET_CASH_EMAIL_TO=matan.l@lsports.eu
NET_CASH_SMTP_URL=smtp://smtp-relay.gmail.com:587            # Workspace relay, server IP allowlisted → NO password
# NET_CASH_SMTP_URL=smtps://finance-bot%40lsports.eu:APP_PASSWORD@smtp.gmail.com:465   # or an app password
NET_CASH_EMAIL_FROM=finance-bot@lsports.eu                  # optional; defaults to NET_CASH_EMAIL_TO
```

Install the mailer once (added to `package.json`): `npm i nodemailer`.

The email contains: run timestamp, the **ACTIVE flag (TRUE/FALSE)**, status (row written / disabled /
failed), `DATE` / `TOTAL_BANK_EUR` / `FORECAST_EUR`, and the basis (Revenue: Pipeline, Salary: Actual).
If `NET_CASH_EMAIL_TO` + `NET_CASH_SMTP_URL` aren't set, the job just **logs** the body (no send) and
still writes Snowflake — so email is strictly additive.

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
