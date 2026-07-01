# Daily Net Cash snapshot → Snowflake — setup

Writes one row per day into `RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST`
(append-only history). See `scripts/net-cash-snapshot.cjs`.

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
- `FORECAST_EUR` = the **"Exit plan June26"** year-end (December) closing, after savings
  (currently €7,048,154). Computed client-side, so it is persisted from the UI, **gated to the
  Exit-plan-June26 scenario** (other scenarios don't overwrite it). Fallback chain: env override
  → persisted snapshot → **carry-forward** (reuse the last row's `FORECAST_EUR`). The first run
  has no prior row, so seed it once with `NET_CASH_FORECAST_EUR`.
- `SRC_UPDATE_AT` / `IS_APPROVED_UPDATED_AT` = **NOT written by this job** — populated by a
  separate process / the external approval automation.
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
  `FORECAST_EUR`, plus `IS_APPROVED=FALSE` if that column exists). It never writes
  `SRC_UPDATE_AT`.
- `--dry-run` needs **no** Snowflake credentials.
- The real insert skips if a row for today's `DATE` already exists (use `--force` to override).
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

## Notes

- `IS_APPROVED` is written `FALSE` (if the column exists). `SRC_UPDATE_AT` and
  `IS_APPROVED_UPDATED_AT` are **not** written by this job — a separate process / the external
  Workato/n8n approval automation manages them.
- Append-only: the job never updates or deletes; every day adds one row.
- The bank figure is anchored to the previous month-end, so it is stable for the whole month
  (every day in July shows the Jun 30 balance); it steps once at each month boundary.
