# Daily Net Cash snapshot → Snowflake — setup

Writes one row per day into `RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST`
(append-only history). See `scripts/net-cash-snapshot.cjs`.

## Data flow

```
Dashboard (LSports, current year, on load)
   └─ POST /api/net-cash-forecast  →  data/net-cash-forecast.json
                                         { date, totalBankEur, totalBankIls, forecastEur, forecastIls, updatedAt }
Cron @ 23:00 Asia/Jerusalem
   └─ node scripts/net-cash-snapshot.cjs  →  reads that JSON  →  INSERT into Snowflake
```

- `TOTAL_BANK_EUR` = the dashboard's displayed total bank balance (all BANK-category
  accounts, raw NS balance — not reval-adjusted).
- `FORECAST_EUR` = the dashboard's year-end (December) closing balance, **after savings**
  (the €8,278,814 figure). This is computed client-side (it includes pipeline / churn /
  unpaid-carry that the server-side calc omits), which is why it is persisted from the UI.

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

# 1. Confirm the persisted figures look right (should show ~8,278,814 forecast).
cat data/net-cash-forecast.json

# 2. Dry run — prints the row + SQL, writes nothing.
node scripts/net-cash-snapshot.cjs --dry-run

# 3. First real insert (add --create-table only if the table doesn't exist yet).
node scripts/net-cash-snapshot.cjs            # or: --create-table
```

The job skips insert if a row for today's `DATE` already exists (use `--force` to override).

## 4. Schedule the cron (23:00 Asia/Jerusalem)

Use `CRON_TZ` so it fires at 23:00 Israel time regardless of the server timezone:

```cron
CRON_TZ=Asia/Jerusalem
0 23 * * * cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && /usr/bin/node scripts/net-cash-snapshot.cjs >> /var/log/net-cash-snapshot.log 2>&1
```

Install with `crontab -e` (adjust the node path via `which node`). If your cron doesn't
support `CRON_TZ`, schedule in UTC instead: 20:00 UTC during IDT (summer, UTC+3) or 21:00 UTC
during IST (winter, UTC+2).

## Notes

- `IS_APPROVED` is written `FALSE`; `IS_APPROVED_UPDATED_AT` is left `NULL` — both are managed
  later by the external Workato/n8n approval automation.
- Append-only: the job never updates or deletes; every day adds one row.
- If nobody opened the dashboard on a given day, the persisted figures are the last known
  values; the row still inserts under today's `DATE`, and the script logs the as-of date.
- **Production forecast persistence** requires the `/api/net-cash-forecast` route in
  `finance-it-backend` (the production `/api/*` host). See
  `docs/backend-net-cash-forecast-route.ts`. Until it is added, seed the file manually or
  pass `NET_CASH_TOTAL_BANK_EUR` / `NET_CASH_FORECAST_EUR` env overrides.
