#!/usr/bin/env node
/**
 * Daily Net Cash snapshot → Snowflake
 * ───────────────────────────────────────────────────────────────────────────
 * Inserts ONE append-only row into
 *   RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST
 *
 * Intended to run once per day at 23:00 Asia/Jerusalem via cron (see
 * docs/net-cash-snapshot-setup.md).
 *
 * Value sourcing (each with a fallback chain, so the job is robust even before
 * the finance-it-backend /api/net-cash-forecast route exists):
 *
 *   TOTAL_BANK_EUR  = env NET_CASH_TOTAL_BANK_EUR
 *                     → persisted dashboard snapshot (data/net-cash-forecast.json)
 *                     → live NetSuite fetchBankBalance().primary.currentBalance   [fresh]
 *   FORECAST_EUR    = env NET_CASH_FORECAST_EUR
 *                     → persisted dashboard snapshot
 *                     → last FORECAST_EUR already in Snowflake (carry-forward)
 *
 * The persisted snapshot is written by the dashboard (POST /api/net-cash-forecast)
 * and is the "as presented" source of truth; NetSuite / carry-forward are fallbacks
 * that keep the daily row flowing when the dashboard hasn't been loaded that day.
 * The accurate forecast (after-savings, incl. pipeline/churn/unpaid-carry) only
 * exists client-side, which is why there is no server-side recompute.
 *
 * Row written
 * ───────────
 *   DATE                   = snapshot date (default: today; --date to override)
 *   TOTAL_BANK_EUR         = total bank balance (all BANK category), EUR
 *   FORECAST_EUR           = year-end (Dec) closing balance, EUR
 *   SRC_UPDATE_AT          = CURRENT_TIMESTAMP() at insert
 *   IS_APPROVED            = FALSE
 *   IS_APPROVED_UPDATED_AT = NULL (set later by the external Workato/n8n approval automation)
 *
 * Environment (from .env in repo root, or the process environment)
 * ────────────────────────────────────────────────────────────────
 *   SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PRIVATE_KEY_PATH / SNOWFLAKE_WAREHOUSE
 *   optional write overrides: SNOWFLAKE_WRITE_USER / SNOWFLAKE_WRITE_PRIVATE_KEY_PATH / SNOWFLAKE_WRITE_ROLE
 *   NetSuite creds used by createNetSuiteClient (already configured for the app)
 *   NET_CASH_SNAPSHOT_PATH   (optional) path to the persisted snapshot json
 *   NET_CASH_SUBSIDIARY      (optional) NS subsidiary for the bank balance (default 3 = LSports)
 *   NET_CASH_TOTAL_BANK_EUR / NET_CASH_FORECAST_EUR  (optional manual overrides)
 *   NET_CASH_NO_NS=1         (optional) skip the live NetSuite bank fallback
 *
 * Flags
 * ─────
 *   --dry-run           resolve + print the row and SQL, do NOT write to Snowflake
 *   --force             insert even if a row already exists for the target DATE
 *   --date=YYYY-MM-DD   override the snapshot date (default: today, local server date)
 *   --create-table      run CREATE TABLE IF NOT EXISTS before inserting (needs CREATE priv)
 *
 * Exit codes: 0 = row written (or dry-run/skip), 1 = error.
 */

const fs = require('fs');
const path = require('path');
const snowflake = require('snowflake-sdk');

try { require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') }); } catch { /* env may already be exported */ }

const REPO_ROOT = path.resolve(__dirname, '..');
const TABLE = 'RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST';

function parseArgs(argv) {
  const a = { dryRun: false, force: false, createTable: false, describe: false, date: null };
  for (const arg of argv.slice(2)) {
    if (arg === '--dry-run') a.dryRun = true;
    else if (arg === '--force') a.force = true;
    else if (arg === '--create-table') a.createTable = true;
    else if (arg === '--describe') a.describe = true;
    else if (arg.startsWith('--date=')) a.date = arg.slice('--date='.length);
  }
  return a;
}

function localToday() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function numOrNull(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function readPersisted() {
  const p = process.env.NET_CASH_SNAPSHOT_PATH
    ? path.resolve(process.env.NET_CASH_SNAPSHOT_PATH)
    : path.resolve(REPO_ROOT, 'data', 'net-cash-forecast.json');
  try { return { path: p, data: JSON.parse(fs.readFileSync(p, 'utf-8')) }; }
  catch { return { path: p, data: null }; }
}

// Live NetSuite bank balance (EUR primary book) — same source the dashboard header shows.
async function fetchBankFromNs() {
  if (process.env.NET_CASH_NO_NS === '1') return null;
  try {
    const { createNetSuiteClient } = require(path.resolve(REPO_ROOT, 'netsuite-api.cjs'));
    const sub = parseInt(process.env.NET_CASH_SUBSIDIARY || '3', 10);
    const ns = createNetSuiteClient(process.env, sub);
    const bal = await ns.fetchBankBalance();
    const eur = bal && bal.primary ? Number(bal.primary.currentBalance) : Number(bal && bal.currentBalance);
    return Number.isFinite(eur) ? Math.round(eur) : null;
  } catch (e) {
    console.warn(`[net-cash] NetSuite bank fallback unavailable: ${e && e.message ? e.message : e}`);
    return null;
  }
}

function connect() {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const username = process.env.SNOWFLAKE_WRITE_USER || process.env.SNOWFLAKE_USER;
  const warehouse = process.env.SNOWFLAKE_WAREHOUSE;
  const role = process.env.SNOWFLAKE_WRITE_ROLE || process.env.SNOWFLAKE_ROLE || undefined;
  const keyPath = process.env.SNOWFLAKE_WRITE_PRIVATE_KEY_PATH || process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
  if (!account || !username || !keyPath) {
    throw new Error('Missing Snowflake config: SNOWFLAKE_ACCOUNT, SNOWFLAKE_(WRITE_)USER, SNOWFLAKE_(WRITE_)PRIVATE_KEY_PATH');
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
  const date = args.date || localToday();
  const { path: snapPath, data: persisted } = readPersisted();

  // ── Resolve TOTAL_BANK_EUR: env → persisted (as presented) → live NetSuite ──
  let totalBankEur = numOrNull(process.env.NET_CASH_TOTAL_BANK_EUR);
  let bankSrc = 'env';
  if (totalBankEur == null && persisted) { totalBankEur = numOrNull(persisted.totalBankEur); bankSrc = 'dashboard snapshot'; }
  if (totalBankEur == null) { totalBankEur = await fetchBankFromNs(); bankSrc = 'live NetSuite'; }

  // ── Resolve FORECAST_EUR: env → persisted (Snowflake carry-forward tried on a real run) ──
  let forecastEur = numOrNull(process.env.NET_CASH_FORECAST_EUR);
  let forecastSrc = 'env';
  if (forecastEur == null && persisted) { forecastEur = numOrNull(persisted.forecastEur); forecastSrc = 'dashboard snapshot'; }

  const insertSql =
    `INSERT INTO ${TABLE} (DATE, TOTAL_BANK_EUR, FORECAST_EUR, SRC_UPDATE_AT, IS_APPROVED) ` +
    `SELECT TO_TIMESTAMP_NTZ(?), ?, ?, CURRENT_TIMESTAMP(), FALSE`;

  const reportRow = () => {
    console.log('[net-cash] Resolved row:');
    console.log(`[net-cash]   DATE           = ${date} 00:00:00`);
    console.log(`[net-cash]   TOTAL_BANK_EUR = ${Number.isFinite(totalBankEur) ? Math.round(totalBankEur).toLocaleString() : 'MISSING'}  (src: ${bankSrc})`);
    console.log(`[net-cash]   FORECAST_EUR   = ${Number.isFinite(forecastEur) ? Math.round(forecastEur).toLocaleString() : 'MISSING'}  (src: ${forecastSrc})`);
    if (persisted?.date && persisted.date !== date) {
      console.warn(`[net-cash]   NOTE: snapshot figures are as-of ${persisted.date} (updated ${persisted.updatedAt || '?'}).`);
    }
  };

  // ── Dry-run: credential-free. Show what env/file/NS resolved; do NOT touch Snowflake. ──
  if (args.dryRun) {
    reportRow();
    if (!Number.isFinite(forecastEur)) {
      console.warn('[net-cash] FORECAST_EUR unresolved from env/snapshot — a real run would carry forward the last Snowflake row.');
    }
    console.log('[net-cash] --dry-run: not writing. SQL:');
    console.log(`[net-cash]   ${insertSql}`);
    console.log(`[net-cash]   binds = ['${date} 00:00:00', ${Number.isFinite(totalBankEur) ? Math.round(totalBankEur) : 'null'}, ${Number.isFinite(forecastEur) ? Math.round(forecastEur) : 'null'}]`);
    process.exit(0);
  }

  let conn;
  try {
    conn = await connect();
    if (args.createTable) { await exec(conn, CREATE_DDL); console.log('[net-cash] Ensured table exists.'); }

    // Discover the table's ACTUAL columns and adapt the INSERT to them. The table may
    // pre-exist with a schema that differs from the spec (e.g. no SRC_UPDATE_AT), so we
    // insert only into columns that exist.
    const colRows = await exec(conn,
      `SELECT COLUMN_NAME FROM RAW.INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = 'LANDING_FINANCE' AND TABLE_NAME = 'NET_CASH_ACTUAL_AND_FORECAST'`);
    const cols = new Set(colRows.map((r) => String(r.COLUMN_NAME || '').toUpperCase()));

    if (args.describe) {
      console.log(`[net-cash] Columns in ${TABLE}:`);
      console.log(`[net-cash]   ${cols.size ? [...cols].join(', ') : '(table not found / no columns)'}`);
      process.exit(0);
    }
    if (cols.size === 0) {
      console.error(`[net-cash] Table ${TABLE} not found. Run with --create-table (needs CREATE privilege).`);
      process.exit(1);
    }

    // Forecast carry-forward: if still unknown, reuse the most recent row's FORECAST_EUR.
    if (forecastEur == null && cols.has('FORECAST_EUR')) {
      try {
        const last = await exec(conn, `SELECT FORECAST_EUR FROM ${TABLE} ORDER BY DATE DESC LIMIT 1`);
        const v = numOrNull(last?.[0]?.FORECAST_EUR);
        if (v != null) { forecastEur = v; forecastSrc = 'carry-forward (last Snowflake row)'; }
      } catch { /* ignore */ }
    }

    if (!Number.isFinite(totalBankEur) || !Number.isFinite(forecastEur)) {
      console.error('[net-cash] No usable figures.');
      console.error(`[net-cash]   snapshot file: ${snapPath} ${persisted ? '(found)' : '(MISSING)'}`);
      console.error(`[net-cash]   TOTAL_BANK_EUR=${totalBankEur} (src: ${bankSrc})  FORECAST_EUR=${forecastEur} (src: ${forecastSrc})`);
      console.error('[net-cash]   For the first run, pass NET_CASH_FORECAST_EUR (and optionally NET_CASH_TOTAL_BANK_EUR),');
      console.error('[net-cash]   or load the LSports current-year dashboard once the backend route persists the snapshot.');
      process.exit(1);
    }

    totalBankEur = Math.round(totalBankEur);
    forecastEur = Math.round(forecastEur);
    reportRow();

    // Build the INSERT from the intersection of our candidate columns and the table's columns.
    const candidates = [
      { name: 'DATE', expr: 'TO_TIMESTAMP_NTZ(?)', bind: `${date} 00:00:00` },
      { name: 'TOTAL_BANK_EUR', expr: '?', bind: totalBankEur },
      { name: 'FORECAST_EUR', expr: '?', bind: forecastEur },
      { name: 'SRC_UPDATE_AT', expr: 'CURRENT_TIMESTAMP()' },
      { name: 'IS_APPROVED', expr: 'FALSE' },
    ];
    const missingCore = ['DATE', 'TOTAL_BANK_EUR', 'FORECAST_EUR'].filter((n) => !cols.has(n));
    if (missingCore.length) {
      console.error(`[net-cash] Table is missing core column(s): ${missingCore.join(', ')}.`);
      console.error(`[net-cash]   Actual columns: ${[...cols].join(', ')}`);
      process.exit(1);
    }
    const skipped = ['SRC_UPDATE_AT', 'IS_APPROVED'].filter((n) => !cols.has(n));
    if (skipped.length) console.warn(`[net-cash] Table lacks ${skipped.join(', ')} — inserting without them.`);

    const used = candidates.filter((c) => cols.has(c.name));
    const adaptedSql =
      `INSERT INTO ${TABLE} (${used.map((c) => c.name).join(', ')}) ` +
      `SELECT ${used.map((c) => c.expr).join(', ')}`;
    const binds = used.filter((c) => Object.prototype.hasOwnProperty.call(c, 'bind')).map((c) => c.bind);

    if (!args.force) {
      const dupe = await exec(conn, `SELECT COUNT(*) AS CNT FROM ${TABLE} WHERE DATE::DATE = TO_DATE(?)`, [date]);
      if (Number(dupe?.[0]?.CNT || 0) > 0) {
        console.log(`[net-cash] A row for ${date} already exists. Skipping (use --force to insert anyway).`);
        process.exit(0);
      }
    }

    await exec(conn, adaptedSql, binds);
    console.log(`[net-cash] ✓ Inserted 1 row into ${TABLE} for ${date} (columns: ${used.map((c) => c.name).join(', ')}).`);
    process.exit(0);
  } catch (e) {
    console.error(`[net-cash] ERROR: ${e && e.message ? e.message : e}`);
    process.exit(1);
  } finally {
    if (conn) { try { conn.destroy(() => {}); } catch { /* ignore */ } }
  }
}

main();
