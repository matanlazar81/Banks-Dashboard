#!/usr/bin/env node
/**
 * Daily Net Cash snapshot → Snowflake
 * ───────────────────────────────────────────────────────────────────────────
 * Inserts ONE append-only row into
 *   RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST
 * from the latest dashboard-persisted net-cash snapshot.
 *
 * Intended to run once per day at 23:00 Asia/Jerusalem via cron (see
 * docs/net-cash-snapshot-setup.md).
 *
 * Data source
 * ───────────
 * The accurate forecast (after-savings year-end closing, incl. pipeline / churn /
 * unpaid-carry) only exists in the dashboard frontend, so it is persisted to
 *   data/net-cash-forecast.json
 * by the dashboard (POST /api/net-cash-forecast) each time an LSports current-year
 * view is loaded. This script reads that file and writes the row.
 *
 * Row written
 * ───────────
 *   DATE                   = snapshot date (default: today; --date to override)
 *   TOTAL_BANK_EUR         = dashboard's displayed total bank balance (all BANK category), EUR
 *   FORECAST_EUR           = dashboard's year-end (Dec) closing balance, EUR
 *   SRC_UPDATE_AT          = CURRENT_TIMESTAMP() at insert
 *   IS_APPROVED            = FALSE
 *   IS_APPROVED_UPDATED_AT = NULL (set later by the external Workato/n8n approval automation)
 *
 * Environment (from .env in repo root, or the process environment)
 * ────────────────────────────────────────────────────────────────
 *   SNOWFLAKE_ACCOUNT              e.g. FQXIBQO-LSPORTS_GCP
 *   SNOWFLAKE_USER                 read/write service user
 *   SNOWFLAKE_PRIVATE_KEY_PATH     path to the (unencrypted) PEM private key
 *   SNOWFLAKE_WAREHOUSE            e.g. finance_wh
 *   -- optional write-specific overrides (fall back to the above) --
 *   SNOWFLAKE_WRITE_USER
 *   SNOWFLAKE_WRITE_PRIVATE_KEY_PATH
 *   SNOWFLAKE_WRITE_ROLE           role that holds INSERT on RAW.LANDING_FINANCE
 *   -- optional data overrides / config --
 *   NET_CASH_SNAPSHOT_PATH         path to the persisted snapshot json (default: data/net-cash-forecast.json)
 *   NET_CASH_TOTAL_BANK_EUR        hard override for TOTAL_BANK_EUR
 *   NET_CASH_FORECAST_EUR          hard override for FORECAST_EUR
 *
 * Flags
 * ─────
 *   --dry-run           print the resolved row + INSERT SQL, do NOT write to Snowflake
 *   --force             insert even if a row already exists for the target DATE
 *   --date=YYYY-MM-DD   override the snapshot date (default: today, local server date)
 *   --create-table      run CREATE TABLE IF NOT EXISTS before inserting (needs CREATE priv)
 *
 * Exit codes: 0 = row written (or dry-run/skip), 1 = error.
 */

const fs = require('fs');
const path = require('path');
const snowflake = require('snowflake-sdk');

// Best-effort .env load (dotenv is a project dependency). Never logs secrets.
try { require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') }); } catch { /* env may already be exported */ }

const REPO_ROOT = path.resolve(__dirname, '..');
const TABLE = 'RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST';

function parseArgs(argv) {
  const a = { dryRun: false, force: false, createTable: false, date: null };
  for (const arg of argv.slice(2)) {
    if (arg === '--dry-run') a.dryRun = true;
    else if (arg === '--force') a.force = true;
    else if (arg === '--create-table') a.createTable = true;
    else if (arg.startsWith('--date=')) a.date = arg.slice('--date='.length);
  }
  return a;
}

function localToday() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

// Resolve the figures to write: persisted dashboard snapshot, with env overrides.
function resolveSnapshot(args) {
  const snapPath = process.env.NET_CASH_SNAPSHOT_PATH
    ? path.resolve(process.env.NET_CASH_SNAPSHOT_PATH)
    : path.resolve(REPO_ROOT, 'data', 'net-cash-forecast.json');

  let persisted = null;
  try { persisted = JSON.parse(fs.readFileSync(snapPath, 'utf-8')); } catch { /* no file yet */ }

  const envBank = process.env.NET_CASH_TOTAL_BANK_EUR;
  const envFcst = process.env.NET_CASH_FORECAST_EUR;

  const totalBankEur = envBank != null && envBank !== '' ? Number(envBank) : (persisted ? Number(persisted.totalBankEur) : NaN);
  const forecastEur  = envFcst != null && envFcst !== '' ? Number(envFcst) : (persisted ? Number(persisted.forecastEur)  : NaN);

  return {
    snapPath,
    persisted,
    date: args.date || localToday(),
    totalBankEur,
    forecastEur,
    persistedDate: persisted?.date || null,
    persistedUpdatedAt: persisted?.updatedAt || null,
  };
}

function connect() {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const username = process.env.SNOWFLAKE_WRITE_USER || process.env.SNOWFLAKE_USER;
  const warehouse = process.env.SNOWFLAKE_WAREHOUSE;
  const role = process.env.SNOWFLAKE_WRITE_ROLE || process.env.SNOWFLAKE_ROLE || undefined;
  const keyPath = process.env.SNOWFLAKE_WRITE_PRIVATE_KEY_PATH || process.env.SNOWFLAKE_PRIVATE_KEY_PATH;

  if (!account || !username || !keyPath) {
    throw new Error('Missing Snowflake config: need SNOWFLAKE_ACCOUNT, SNOWFLAKE_(WRITE_)USER, SNOWFLAKE_(WRITE_)PRIVATE_KEY_PATH');
  }
  const privateKey = fs.readFileSync(keyPath, 'utf-8').trim();

  return new Promise((resolve, reject) => {
    const conn = snowflake.createConnection({
      account, username, authenticator: 'SNOWFLAKE_JWT', privateKey, warehouse, role,
      database: 'RAW', schema: 'LANDING_FINANCE', application: 'BankDashboardNetCashJob',
    });
    conn.connect((err, c) => (err ? reject(err) : resolve(c)));
  });
}

function exec(conn, sqlText, binds = []) {
  return new Promise((resolve, reject) => {
    conn.execute({ sqlText, binds, complete: (err, _stmt, rows) => (err ? reject(err) : resolve(rows || [])) });
  });
}

const CREATE_DDL = `CREATE TABLE IF NOT EXISTS ${TABLE} (
  DATE TIMESTAMP_NTZ,
  TOTAL_BANK_EUR FLOAT,
  FORECAST_EUR FLOAT,
  SRC_UPDATE_AT TIMESTAMP_NTZ,
  IS_APPROVED BOOLEAN,
  IS_APPROVED_UPDATED_AT TIMESTAMP_NTZ
)`;

async function main() {
  const args = parseArgs(process.argv);
  const snap = resolveSnapshot(args);

  // Validate figures.
  if (!Number.isFinite(snap.totalBankEur) || !Number.isFinite(snap.forecastEur)) {
    console.error('[net-cash] No usable figures.');
    console.error(`[net-cash]   snapshot file: ${snap.snapPath} ${snap.persisted ? '(found)' : '(MISSING)'}`);
    console.error(`[net-cash]   totalBankEur=${snap.totalBankEur} forecastEur=${snap.forecastEur}`);
    console.error('[net-cash] Ensure the dashboard has been loaded (LSports, current year) so the snapshot is written,');
    console.error('[net-cash] or pass NET_CASH_TOTAL_BANK_EUR / NET_CASH_FORECAST_EUR env overrides.');
    process.exit(1);
  }

  const row = {
    DATE: `${snap.date} 00:00:00`,
    TOTAL_BANK_EUR: Math.round(snap.totalBankEur),
    FORECAST_EUR: Math.round(snap.forecastEur),
    SRC_UPDATE_AT: 'CURRENT_TIMESTAMP()',
    IS_APPROVED: false,
  };

  console.log('[net-cash] Resolved row:');
  console.log(`[net-cash]   DATE           = ${row.DATE}`);
  console.log(`[net-cash]   TOTAL_BANK_EUR = ${row.TOTAL_BANK_EUR.toLocaleString()}`);
  console.log(`[net-cash]   FORECAST_EUR   = ${row.FORECAST_EUR.toLocaleString()}`);
  if (snap.persistedDate && snap.persistedDate !== snap.date) {
    console.warn(`[net-cash]   NOTE: figures are as-of ${snap.persistedDate} (dashboard last loaded ${snap.persistedUpdatedAt || '?'}), inserted under DATE ${snap.date}.`);
  }

  const insertSql =
    `INSERT INTO ${TABLE} (DATE, TOTAL_BANK_EUR, FORECAST_EUR, SRC_UPDATE_AT, IS_APPROVED) ` +
    `SELECT TO_TIMESTAMP_NTZ(?), ?, ?, CURRENT_TIMESTAMP(), FALSE`;

  if (args.dryRun) {
    console.log('[net-cash] --dry-run: not writing. SQL would be:');
    console.log(`[net-cash]   ${insertSql}`);
    console.log(`[net-cash]   binds = ['${row.DATE}', ${row.TOTAL_BANK_EUR}, ${row.FORECAST_EUR}]`);
    process.exit(0);
  }

  let conn;
  try {
    conn = await connect();

    if (args.createTable) {
      await exec(conn, CREATE_DDL);
      console.log('[net-cash] Ensured table exists.');
    }

    if (!args.force) {
      const dupe = await exec(conn, `SELECT COUNT(*) AS CNT FROM ${TABLE} WHERE DATE::DATE = TO_DATE(?)`, [snap.date]);
      const cnt = Number(dupe?.[0]?.CNT || 0);
      if (cnt > 0) {
        console.log(`[net-cash] A row for ${snap.date} already exists (${cnt}). Skipping (use --force to insert anyway).`);
        process.exit(0);
      }
    }

    await exec(conn, insertSql, [row.DATE, row.TOTAL_BANK_EUR, row.FORECAST_EUR]);
    console.log(`[net-cash] ✓ Inserted 1 row into ${TABLE} for ${snap.date}.`);
    process.exit(0);
  } catch (e) {
    console.error(`[net-cash] ERROR: ${e && e.message ? e.message : e}`);
    process.exit(1);
  } finally {
    if (conn) { try { conn.destroy(() => {}); } catch { /* ignore */ } }
  }
}

main();
