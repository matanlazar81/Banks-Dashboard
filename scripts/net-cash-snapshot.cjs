#!/usr/bin/env node
/**
 * Daily Net Cash snapshot → Snowflake
 * ───────────────────────────────────────────────────────────────────────────
 * Inserts ONE append-only row into
 *   RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST
 *
 * Runs once per day at 23:00 Asia/Jerusalem via cron (see
 * docs/net-cash-snapshot-setup.md).
 *
 * Values
 * ──────
 *   TOTAL_BANK_EUR  = NetSuite bank balance (all BANK/CredCard accounts, EUR primary
 *                     book) AS OF THE END OF THE PREVIOUS MONTH. So every day in July
 *                     reports the Jun 30 balance; every day in August reports Jul 31; etc.
 *                     Sourced live from NetSuite (fetchBankAccountListAsOf), so it needs
 *                     no dashboard. Override with env NET_CASH_TOTAL_BANK_EUR.
 *   FORECAST_EUR    = the "Exit plan June26" year-end (Dec) closing balance, after savings,
 *                     persisted from the dashboard (POST /api/net-cash-forecast →
 *                     data/net-cash-forecast.json, gated to that scenario). Fallback chain:
 *                     env NET_CASH_FORECAST_EUR → persisted snapshot → carry-forward (last
 *                     row's FORECAST_EUR in Snowflake). Only the first run needs a seed.
 *
 * Row written
 * ───────────
 *   DATE           = the SYNC TIMESTAMP (Asia/Jerusalem), incl. the hour, e.g.
 *                    2026-07-01 23:00:07. (--date overrides the date part; time stays live.)
 *   TOTAL_BANK_EUR = prev-month-end bank balance, EUR
 *   FORECAST_EUR   = Exit-plan-June26 year-end closing, EUR
 *   IS_APPROVED    = FALSE (only if the column exists)
 *   SRC_UPDATED_AT = CURRENT_TIMESTAMP() at insert (the load time; only if the column exists).
 *   IS_APPROVED_UPDATED_AT = NOT written here — external approval automation.
 *
 * The INSERT adapts to the table's ACTUAL columns (discovered via INFORMATION_SCHEMA):
 * it always writes DATE / TOTAL_BANK_EUR / FORECAST_EUR (required) and adds IS_APPROVED
 * only if that column exists.
 *
 * Environment (from .env in repo root, or the process environment)
 * ────────────────────────────────────────────────────────────────
 *   SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PRIVATE_KEY_PATH / SNOWFLAKE_WAREHOUSE
 *   optional write overrides: SNOWFLAKE_WRITE_USER / SNOWFLAKE_WRITE_PRIVATE_KEY_PATH / SNOWFLAKE_WRITE_ROLE
 *   NetSuite creds used by createNetSuiteClient (already configured for the app)
 *   NET_CASH_SNAPSHOT_PATH   (optional) path to the persisted forecast json
 *   NET_CASH_SUBSIDIARY      (optional) NS subsidiary for the bank balance (default 3 = LSports)
 *   NET_CASH_TOTAL_BANK_EUR / NET_CASH_FORECAST_EUR  (optional manual overrides)
 *   NET_CASH_NO_NS=1         (optional) skip the live NetSuite bank fetch
 *
 * APPEND-ONLY: every run inserts a NEW row. Nothing is overwritten, deduped, or deleted —
 * each update is preserved as its own row (a full history), keyed by the DATE sync timestamp.
 *
 * Flags
 * ─────
 *   --dry-run           resolve + print the row and SQL, do NOT write to Snowflake (no creds needed)
 *   --date=YYYY-MM-DD   override the snapshot date (time portion stays the live sync time)
 *   --describe          print the table's actual columns and exit
 *   --show              print the last 10 rows and exit
 *   --create-table      run CREATE TABLE IF NOT EXISTS before inserting (needs CREATE privilege)
 *
 * Exit codes: 0 = row appended (or dry-run/describe/show), 1 = error.
 */

const fs = require('fs');
const path = require('path');
const snowflake = require('snowflake-sdk');

try { require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') }); } catch { /* env may already be exported */ }

const REPO_ROOT = path.resolve(__dirname, '..');
const TABLE = 'RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST';

function parseArgs(argv) {
  const a = { dryRun: false, createTable: false, describe: false, show: false, date: null };
  for (const arg of argv.slice(2)) {
    if (arg === '--dry-run') a.dryRun = true;
    else if (arg === '--create-table') a.createTable = true;
    else if (arg === '--describe') a.describe = true;
    else if (arg === '--show') a.show = true;                 // print recent rows and exit
    else if (arg.startsWith('--date=')) a.date = arg.slice('--date='.length);
  }
  return a;
}

function numOrNull(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// Current wall-clock split into date + time in Asia/Jerusalem (the sync's local time).
function israelNowParts() {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Jerusalem', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  });
  const p = Object.fromEntries(fmt.formatToParts(new Date()).map((x) => [x.type, x.value]));
  return { date: `${p.year}-${p.month}-${p.day}`, time: `${p.hour}:${p.minute}:${p.second}` };
}

// Last calendar day of the month BEFORE dateStr ('YYYY-MM-DD'). July date → '<yr>-06-30'.
function prevMonthEnd(dateStr) {
  const [y, m] = dateStr.split('-').map(Number); // m = 1..12
  const d = new Date(Date.UTC(y, m - 1, 1)); // 1st of the current month
  d.setUTCDate(0); // roll back to the last day of the previous month
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

function readPersistedForecast() {
  const p = process.env.NET_CASH_SNAPSHOT_PATH
    ? path.resolve(process.env.NET_CASH_SNAPSHOT_PATH)
    : path.resolve(REPO_ROOT, 'data', 'net-cash-forecast.json');
  try { return { path: p, data: JSON.parse(fs.readFileSync(p, 'utf-8')) }; }
  catch { return { path: p, data: null }; }
}

// Total EUR bank balance (all BANK/CredCard accounts, primary book) as of a given date.
async function fetchBankAsOf(asOfDate) {
  if (process.env.NET_CASH_NO_NS === '1') return null;
  try {
    const { createNetSuiteClient } = require(path.resolve(REPO_ROOT, 'netsuite-api.cjs'));
    const sub = parseInt(process.env.NET_CASH_SUBSIDIARY || '3', 10);
    const ns = createNetSuiteClient(process.env, sub);
    const accounts = await ns.fetchBankAccountListAsOf(asOfDate);
    const eur = (accounts || []).reduce((s, a) => s + (Number(a.primaryBalance) || 0), 0);
    return Number.isFinite(eur) ? Math.round(eur) : null;
  } catch (e) {
    console.warn(`[net-cash] NetSuite bank fetch failed: ${e && e.message ? e.message : e}`);
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

// Matches the live table (note SRC_UPDATED_AT — with a "D"). Only used by --create-table
// on a fresh environment; a no-op when the table already exists.
const CREATE_DDL = `CREATE TABLE IF NOT EXISTS ${TABLE} (
  DATE TIMESTAMP_NTZ,
  TOTAL_BANK_EUR FLOAT,
  FORECAST_EUR FLOAT,
  SRC_UPDATED_AT TIMESTAMP_NTZ,
  IS_APPROVED BOOLEAN,
  IS_APPROVED_UPDATED_AT TIMESTAMP_NTZ
)`;

async function main() {
  const args = parseArgs(process.argv);
  const nowParts = israelNowParts();
  const dateOnly = args.date || nowParts.date;      // calendar day (dedupe key)
  const syncTs = `${dateOnly} ${nowParts.time}`;    // DATE value = sync timestamp incl. hour
  const asOf = prevMonthEnd(dateOnly);              // bank balance as-of = previous month-end

  const { path: snapPath, data: persisted } = readPersistedForecast();

  // ── TOTAL_BANK_EUR: env override → live NetSuite balance as of previous month-end ──
  let totalBankEur = numOrNull(process.env.NET_CASH_TOTAL_BANK_EUR);
  let bankSrc = 'env';
  if (totalBankEur == null) { totalBankEur = await fetchBankAsOf(asOf); bankSrc = `NetSuite as of ${asOf}`; }

  // ── FORECAST_EUR: env → persisted snapshot (Snowflake carry-forward tried on a real run) ──
  let forecastEur = numOrNull(process.env.NET_CASH_FORECAST_EUR);
  let forecastSrc = 'env';
  if (forecastEur == null && persisted) { forecastEur = numOrNull(persisted.forecastEur); forecastSrc = `dashboard snapshot (${persisted.scenario || 'scenario?'})`; }

  const reportRow = () => {
    console.log('[net-cash] Resolved row:');
    console.log(`[net-cash]   DATE           = ${syncTs}  (Asia/Jerusalem)`);
    console.log(`[net-cash]   TOTAL_BANK_EUR = ${Number.isFinite(totalBankEur) ? Math.round(totalBankEur).toLocaleString() : 'MISSING'}  (src: ${bankSrc})`);
    console.log(`[net-cash]   FORECAST_EUR   = ${Number.isFinite(forecastEur) ? Math.round(forecastEur).toLocaleString() : 'MISSING'}  (src: ${forecastSrc})`);
  };

  // ── Dry-run: credential-free. Show what resolved; do NOT touch Snowflake. ──
  if (args.dryRun) {
    reportRow();
    if (!Number.isFinite(forecastEur)) {
      console.warn('[net-cash] FORECAST_EUR unresolved from env/snapshot — a real run would carry forward the last Snowflake row.');
    }
    console.log('[net-cash] --dry-run: not writing. Would insert:');
    console.log(`[net-cash]   DATE=TO_TIMESTAMP_NTZ('${syncTs}'), TOTAL_BANK_EUR=${Number.isFinite(totalBankEur) ? Math.round(totalBankEur) : 'null'}, FORECAST_EUR=${Number.isFinite(forecastEur) ? Math.round(forecastEur) : 'null'} (+ SRC_UPDATED_AT=CURRENT_TIMESTAMP() and IS_APPROVED=FALSE if those columns exist)`);
    process.exit(0);
  }

  let conn;
  try {
    conn = await connect();
    if (args.createTable) { await exec(conn, CREATE_DDL); console.log('[net-cash] Ensured table exists.'); }

    // Discover the table's ACTUAL columns and adapt the INSERT.
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

    if (args.show) {
      const rows = await exec(conn, `SELECT * FROM ${TABLE} ORDER BY DATE DESC LIMIT 10`);
      console.log(`[net-cash] Last ${rows.length} row(s) in ${TABLE} (newest first):`);
      for (const r of rows) console.log(`[net-cash]   ${JSON.stringify(r)}`);
      process.exit(0);
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
      console.error('[net-cash]   For the first run, pass NET_CASH_FORECAST_EUR; the bank balance auto-fetches from NetSuite.');
      process.exit(1);
    }

    totalBankEur = Math.round(totalBankEur);
    forecastEur = Math.round(forecastEur);
    reportRow();

    // Build the INSERT from the intersection of our candidate columns and the table's columns.
    // SRC_UPDATED_AT = load time (when this job wrote the row). IS_APPROVED_UPDATED_AT is left
    // to the external approval automation.
    const candidates = [
      { name: 'DATE', expr: 'TO_TIMESTAMP_NTZ(?)', bind: syncTs },
      { name: 'TOTAL_BANK_EUR', expr: '?', bind: totalBankEur },
      { name: 'FORECAST_EUR', expr: '?', bind: forecastEur },
      { name: 'SRC_UPDATED_AT', expr: 'CURRENT_TIMESTAMP()' },
      { name: 'IS_APPROVED', expr: 'FALSE' },
    ];
    const missingCore = ['DATE', 'TOTAL_BANK_EUR', 'FORECAST_EUR'].filter((n) => !cols.has(n));
    if (missingCore.length) {
      console.error(`[net-cash] Table is missing core column(s): ${missingCore.join(', ')}.`);
      console.error(`[net-cash]   Actual columns: ${[...cols].join(', ')}`);
      process.exit(1);
    }
    const used = candidates.filter((c) => cols.has(c.name));
    const adaptedSql =
      `INSERT INTO ${TABLE} (${used.map((c) => c.name).join(', ')}) ` +
      `SELECT ${used.map((c) => c.expr).join(', ')}`;
    const binds = used.filter((c) => Object.prototype.hasOwnProperty.call(c, 'bind')).map((c) => c.bind);

    // Append-only history: every run inserts a NEW row (each update is preserved, keyed by the
    // DATE sync timestamp). Rows are never overwritten or deleted by this job.
    await exec(conn, adaptedSql, binds);
    console.log(`[net-cash] ✓ Appended 1 row for ${syncTs} (columns: ${used.map((c) => c.name).join(', ')}).`);
    process.exit(0);
  } catch (e) {
    console.error(`[net-cash] ERROR: ${e && e.message ? e.message : e}`);
    process.exit(1);
  } finally {
    if (conn) { try { conn.destroy(() => {}); } catch { /* ignore */ } }
  }
}

main();
