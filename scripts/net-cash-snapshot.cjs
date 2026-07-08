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
 *                     REVAL-CLOSED GUARD: a month-end is only trusted once NetSuite carries
 *                     the POSTED month-end FX revaluation for that date. If it isn't posted
 *                     yet, the as-of date steps back one month-end (max 2 steps, with a
 *                     warning in the summary) so we never report a partially-posted month.
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
 *   TOTAL_BANK_EUR = prev-month-end bank balance, EUR (posted GL, reval-closed guard above)
 *   FORECAST_EUR   = Exit-plan-June26 year-end closing, EUR
 *   MODEL_CLOSING_EUR = the dashboard model's flow-forward closing of the last COMPLETED
 *                    month (from data/net-cash-forecast.json, written by the compute).
 *                    Ledger vs model view of the same month-end — the difference is
 *                    bookkeeping not yet posted. Only written if the column exists
 *                    (one-time DDL: ALTER TABLE ... ADD COLUMN MODEL_CLOSING_EUR FLOAT).
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
 *   NET_CASH_SYNC_ENABLED    (optional) 'false' stops the sync (no Snowflake write); default 'true'
 *   NET_CASH_EMAIL_TO        (optional) email recipient(s) for the per-run summary, comma-separated
 *   NET_CASH_SMTP_URL        (optional) SMTP connection URL (relay = no creds; or smtps://user:pw@host)
 *   NET_CASH_EMAIL_FROM      (optional) sender address (defaults to SMTP_USER, then NET_CASH_EMAIL_TO)
 *   NET_CASH_SMTP_HELO       (optional) EHLO name for the Workspace relay (defaults to sender domain)
 *   SLACK_BOT_TOKEN          (optional) post the summary to Slack (reuses the backend's bot token)
 *   NET_CASH_SLACK_CHANNEL   (optional) Slack channel name or ID (default 'cash_flow_sync')
 *
 * APPEND-ONLY: every run inserts a NEW row. Nothing is overwritten, deduped, or deleted —
 * each update is preserved as its own row (a full history), keyed by the DATE sync timestamp.
 *
 * NOTIFICATIONS / CONTROL:
 *   • After each run it sends a summary (the data pushed + the ACTIVE flag) to email and/or Slack,
 *     whichever is configured (email needs NET_CASH_EMAIL_TO + a transport; Slack needs
 *     SLACK_BOT_TOKEN). If neither is set it just logs the body.
 *   • NET_CASH_SYNC_ENABLED=false stops the write entirely (and sends a DISABLED notice), so you
 *     can pause/resume the sync any time by editing .env — no code change, no restart.
 *   • The pushed FORECAST_EUR is the dashboard's Revenue:Pipeline + Salary:Actual year-end close.
 *
 * Flags
 * ─────
 *   --dry-run           resolve + print the row, the SQL, and a preview of the notification
 *                       message (exactly as Slack would post it); do NOT write to Snowflake and
 *                       do NOT send anything (no creds needed)
 *   --refresh           recompute the forecast server-side (NO browser) BEFORE pushing, by running
 *                       scripts/net-cash-forecast-compute.cjs: it fetches fresh NetSuite + Snowflake
 *                       data, runs the shared forecast engine, and rewrites data/net-cash-forecast.json.
 *                       No login/Playwright needed; falls through to the last persisted forecast if the
 *                       compute can't run. One command = recompute + push + send.
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
  const a = { dryRun: false, createTable: false, describe: false, show: false, date: null, refresh: false };
  for (const arg of argv.slice(2)) {
    if (arg === '--dry-run') a.dryRun = true;
    else if (arg === '--create-table') a.createTable = true;
    else if (arg === '--describe') a.describe = true;
    else if (arg === '--show') a.show = true;                 // print recent rows and exit
    else if (arg === '--refresh') a.refresh = true;           // recompute forecast (headless) before pushing
    else if (arg.startsWith('--date=')) a.date = arg.slice('--date='.length);
  }
  return a;
}

// Recompute the forecast server-side by running scripts/net-cash-forecast-compute.cjs, which
// fetches fresh NetSuite + Snowflake data, runs the SHARED forecast engine (the same module the
// browser runs), and re-writes data/net-cash-forecast.json. This is how the nightly job
// "recomputes on the backend" with no browser and no login. Runs to completion BEFORE the push
// reads the file. Never throws — if it fails, we log and fall through to the last persisted
// forecast so the push still runs.
function runServerCompute() {
  return new Promise((resolve) => {
    const { spawn } = require('child_process');
    const script = path.resolve(__dirname, 'net-cash-forecast-compute.cjs');
    console.log('[net-cash] --refresh: recomputing forecast server-side (no browser)...');
    let child;
    try {
      child = spawn(process.execPath, [script], { stdio: 'inherit', env: process.env });
    } catch (e) {
      console.warn(`[net-cash] server compute failed to start: ${e && e.message ? e.message : e}`);
      return resolve(1);
    }
    child.on('error', (e) => { console.warn(`[net-cash] server compute error: ${e && e.message ? e.message : e}`); resolve(1); });
    child.on('exit', (code) => { console.log(`[net-cash] server compute finished (exit ${code}).`); resolve(code); });
  });
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

// ── Notifications (optional, env-driven) ───────────────────────────────────
// A run summary can go to email and/or Slack — both optional and independent; whichever is
// configured fires. NO credentials live in code — they sit in .env only.
//   Email:  NET_CASH_EMAIL_TO (required) + transport:
//             1. NET_CASH_SMTP_URL (smtp://host:port  or  smtps://user:pw@host:465), else
//             2. the backend's existing SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS.
//           NET_CASH_EMAIL_FROM optional sender; NET_CASH_SMTP_HELO optional relay EHLO name.
//   Slack:  SLACK_BOT_TOKEN (required) + NET_CASH_SLACK_CHANNEL (name or ID; default cash_flow_sync).
//           The bot must be a member of the channel.
function buildSummary({ enabled, wrote, dryRun, syncTs, totalBankEur, forecastEur, modelClosingEur, modelClosingMonth, bankAsOf, asOfNote, tableName, error, currentBankEur, currentBankReval, currentBankRevalOk, currentBankDate, dividendExcludedEur, currentBankDividendExcl }) {
  const fmtEur = (v) => (v != null && Number.isFinite(Number(v)) ? `EUR ${Math.round(Number(v)).toLocaleString('en-US')}` : 'MISSING');
  const statusLine = error ? 'FAILED'
    : !enabled ? 'DISABLED — no row written'
    : dryRun ? 'DRY RUN — no row written'
    : wrote ? 'row written' : 'no row written';
  const lines = [
    'Net-Cash daily sync',
    `Run (Asia/Jerusalem): ${syncTs}`,
    '',
    `Sync ACTIVE flag : ${enabled ? 'TRUE' : 'FALSE'}`,
    '   to stop  : set NET_CASH_SYNC_ENABLED=false in .env',
    '   to resume: set NET_CASH_SYNC_ENABLED=true  in .env',
    '',
    `Status           : ${statusLine}`,
    `Table            : ${tableName}`,
    '',
    `DATE             : ${syncTs}`,
    `TOTAL_BANK_EUR   : ${fmtEur(totalBankEur)}${bankAsOf ? ` (prev month-end, posted GL as of ${bankAsOf})` : ''}`,
    ...(currentBankEur != null ? [`CURRENT_BANK_EUR : ${fmtEur(currentBankEur)} (live as of ${currentBankDate}${currentBankRevalOk ? (currentBankReval ? `; open-month reval ${fmtEur(currentBankReval)} excluded until month-end` : '') : '; reval NOT neutralized — lookup failed'}${currentBankDividendExcl ? `; dividend ${fmtEur(currentBankDividendExcl)} excluded` : ''})`] : []),
    `FORECAST_EUR     : ${fmtEur(forecastEur)}`,
  ];
  if (modelClosingEur != null) lines.push(`MODEL_CLOSING_EUR: ${fmtEur(modelClosingEur)}${modelClosingMonth ? ` (model closing for ${modelClosingMonth})` : ''}`);
  if (asOfNote) lines.push(`⚠ Reval guard    : ${asOfNote}`);
  lines.push(
    '',
    'Revenue basis    : Pipeline',
    'Salary basis     : Actual',
  );
  if (error) lines.push('', `Error            : ${error}`);
  return { statusLine, subject: `[Net-Cash -> Snowflake] ${statusLine} — ${syncTs}`, text: lines.join('\n') };
}

async function sendEmail(payload) {
  const to = process.env.NET_CASH_EMAIL_TO;
  if (!to) return; // email not configured — skip (Slack may still fire)
  // Transport: explicit URL override, else reuse the app's existing SMTP_* settings.
  const smtpUrl = process.env.NET_CASH_SMTP_URL;
  let transport = null;
  let from = process.env.NET_CASH_EMAIL_FROM || to;
  if (smtpUrl) {
    // Parse the URL into an options object so we can also set the EHLO name below.
    try {
      const u = new URL(smtpUrl);
      const secure = u.protocol === 'smtps:';
      transport = {
        host: u.hostname,
        port: u.port ? parseInt(u.port, 10) : (secure ? 465 : 587),
        secure,
        auth: u.username
          ? { user: decodeURIComponent(u.username), pass: decodeURIComponent(u.password || '') }
          : undefined,
      };
    } catch { transport = smtpUrl; }
  } else if (process.env.SMTP_HOST) {
    const port = parseInt(process.env.SMTP_PORT || '587', 10);
    transport = {
      host: process.env.SMTP_HOST,
      port,
      secure: port === 465,
      auth: (process.env.SMTP_USER && process.env.SMTP_PASS)
        ? { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS } : undefined,
    };
    from = process.env.NET_CASH_EMAIL_FROM || process.env.SMTP_USER || to;
  }
  // EHLO/HELO name — the Workspace SMTP relay (no-auth path) requires the server to present
  // one of your domains here. Default to the sender's domain (e.g. lsports.eu); override with
  // NET_CASH_SMTP_HELO. Only applies when transport is an options object (not a raw URL string).
  if (transport && typeof transport === 'object') {
    const helo = process.env.NET_CASH_SMTP_HELO || (from && from.includes('@') ? from.split('@').pop() : null);
    if (helo) transport.name = helo;
  }
  const { subject, text } = buildSummary(payload);
  if (!transport) {
    console.log('[net-cash] email not sent (set NET_CASH_SMTP_URL or SMTP_HOST/PORT/USER/PASS). Would have sent:');
    console.log(text.split('\n').map((l) => `[net-cash]   ${l}`).join('\n'));
    return;
  }
  let nodemailer;
  try { nodemailer = require('nodemailer'); }
  catch { console.warn('[net-cash] nodemailer not installed (run: npm i nodemailer) — email skipped.'); return; }
  try {
    const transporter = nodemailer.createTransport(transport);
    await transporter.sendMail({ from, to, subject, text });
    console.log(`[net-cash] email sent to ${to}`);
  } catch (e) {
    console.warn(`[net-cash] email send failed: ${e && e.message ? e.message : e}`);
  }
}

// Post the run summary to Slack via chat.postMessage (Bot token). The bot must be a member
// of the target channel. Channel from NET_CASH_SLACK_CHANNEL (name or ID); default cash_flow_sync.
async function sendSlack(payload) {
  const token = process.env.SLACK_BOT_TOKEN;
  if (!token) return; // Slack not configured — skip
  const channel = process.env.NET_CASH_SLACK_CHANNEL || 'cash_flow_sync';
  const { statusLine, text } = buildSummary(payload);
  const body = {
    channel,
    text: `Net-Cash → Snowflake — ${statusLine}`, // fallback / notification text
    blocks: [
      { type: 'section', text: { type: 'mrkdwn', text: `*Net-Cash → Snowflake* — ${statusLine}` } },
      { type: 'section', text: { type: 'mrkdwn', text: '```\n' + text + '\n```' } },
    ],
    unfurl_links: false,
  };
  try {
    const res = await fetch('https://slack.com/api/chat.postMessage', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json; charset=utf-8' },
      body: JSON.stringify(body),
    });
    const data = await res.json().catch(() => ({}));
    if (data && data.ok) console.log(`[net-cash] Slack message posted to #${channel}`);
    else console.warn(`[net-cash] Slack post failed: ${(data && data.error) || 'unknown'} (channel: ${channel}). If channel_not_found/not_in_channel, invite the bot to the channel or set NET_CASH_SLACK_CHANNEL to its ID.`);
  } catch (e) {
    console.warn(`[net-cash] Slack post error: ${e && e.message ? e.message : e}`);
  }
}

// Fan the run summary out to whichever channels are configured (email + Slack); one failing
// never blocks the other, and never aborts the sync itself.
async function notify(payload) {
  await Promise.allSettled([sendEmail(payload), sendSlack(payload)]);
}

// Shared NetSuite client (created once per run; null when NET_CASH_NO_NS=1).
let _ns = null;
function getNs() {
  if (process.env.NET_CASH_NO_NS === '1') return null;
  if (!_ns) {
    const { createNetSuiteClient } = require(path.resolve(REPO_ROOT, 'netsuite-api.cjs'));
    const sub = parseInt(process.env.NET_CASH_SUBSIDIARY || '3', 10);
    _ns = createNetSuiteClient(process.env, sub);
  }
  return _ns;
}

// Total EUR bank balance (all BANK/CredCard accounts, primary book) as of a given date.
async function fetchBankAsOf(asOfDate) {
  const ns = getNs();
  if (!ns) return null;
  try {
    const accounts = await ns.fetchBankAccountListAsOf(asOfDate);
    const eur = (accounts || []).reduce((s, a) => s + (Number(a.primaryBalance) || 0), 0);
    return Number.isFinite(eur) ? Math.round(eur) : null;
  } catch (e) {
    console.warn(`[net-cash] NetSuite bank fetch failed: ${e && e.message ? e.message : e}`);
    return null;
  }
}

// Realized dividend distribution by month (magnitude EUR: |distribution| + |WHT|, both legs left the
// bank) for the given years. Used to EXCLUDE the dividend from the bank figures: the live bank reflects
// the dividend cash-out, so adding it back yields the operating balance (matching FORECAST_EUR /
// MODEL_CLOSING_EUR, which already exclude it). Best-effort: returns {} (with a warning) if the lookup
// fails or the method is absent, so the bank figure simply stays raw and the sync never breaks.
async function fetchDividendByMonth(years) {
  const ns = getNs();
  if (!ns || typeof ns.fetchDividendDistributions !== 'function') return {};
  const out = {};
  try {
    for (const y of years) {
      const d = await ns.fetchDividendDistributions(parseInt(y, 10));
      for (const [m, v] of Object.entries((d && d.byMonth) || {})) {
        out[m] = Math.round(Math.abs(v.distributionEUR || 0) + Math.abs(v.whtEUR || 0));
      }
    }
  } catch (e) {
    console.warn(`[net-cash] dividend lookup failed (${e && e.message ? e.message : e}) — bank figures left raw (dividend NOT excluded).`);
    return {};
  }
  return out;
}

// Live bank balance as of TODAY, with the OPEN month's FX reval neutralized.
// The raw as-of-today sum carries a one-sided reval swing — the prior month-end mark is
// reversed on the 1st, but this month's month-end mark isn't posted until the month closes —
// which distorts the live figure (e.g. a -€2.75M July phantom). Subtract the current-month
// reval (the same bank-classified figure the dashboard uses) so the number reflects real cash
// until the month closes and reval can be shown correctly. Best-effort: if the reval can't be
// determined, returns the raw balance and flags revalOk=false.
async function fetchCurrentBankExReval(dateOnly) {
  const ns = getNs();
  if (!ns) return { eur: null, revalEur: 0, revalOk: false };
  const raw = await fetchBankAsOf(dateOnly);
  if (raw == null) return { eur: null, revalEur: 0, revalOk: false };
  let revalEur = 0, revalOk = false;
  try {
    if (typeof ns.fetchBankClassifiedYearly === 'function') {
      const year = parseInt(String(dateOnly).slice(0, 4), 10);
      const mKey = String(dateOnly).slice(0, 7);
      const bc = await ns.fetchBankClassifiedYearly(year);
      revalEur = Math.round((bc && bc.byMonth && bc.byMonth[mKey] && bc.byMonth[mKey].reval && bc.byMonth[mKey].reval.eur) || 0);
      revalOk = true;
    }
  } catch (e) {
    console.warn(`[net-cash] current-month reval lookup failed (${e && e.message ? e.message : e}) — current balance shown WITH the open-month FX swing.`);
  }
  return { eur: Math.round(raw - revalEur), revalEur, revalOk };
}

// Reval-closed guard: a month-end balance is only "final" once NetSuite carries the POSTED
// month-end FX revaluation mark dated on that month-end. Until then the as-of sum is a
// partially-posted figure. Step back one month-end at a time (max 2 steps) to the last
// closed month; if none of the candidates is closed, keep the last one and flag it.
async function resolveClosedMonthEnd(asOf) {
  const ns = getNs();
  if (!ns) return { asOf, note: '' };
  let candidate = asOf;
  try {
    for (let step = 0; step < 3; step++) {
      if (await ns.hasPostedMonthEndReval(candidate)) {
        if (step > 0) console.log(`[net-cash] reval-closed guard: using ${candidate} (fell back from ${asOf}).`);
        return { asOf: candidate, note: step > 0 ? `month-end reval not posted for ${asOf} — fell back to ${candidate}` : '' };
      }
      if (step === 2) break;
      const prev = prevMonthEnd(candidate);
      console.warn(`[net-cash] month-end FX reval NOT posted for ${candidate} — stepping back to ${prev}.`);
      candidate = prev;
    }
    console.warn(`[net-cash] no closed month-end found within 2 steps of ${asOf} — using ${candidate} anyway.`);
    return { asOf: candidate, note: `no posted month-end reval found near ${asOf} — using ${candidate} UNVERIFIED` };
  } catch (e) {
    console.warn(`[net-cash] reval check failed (${e && e.message ? e.message : e}) — using ${asOf}.`);
    return { asOf, note: '' };
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
  MODEL_CLOSING_EUR FLOAT,
  SRC_UPDATED_AT TIMESTAMP_NTZ,
  IS_APPROVED BOOLEAN,
  IS_APPROVED_UPDATED_AT TIMESTAMP_NTZ
)`;

async function main() {
  const args = parseArgs(process.argv);
  const syncEnabled = String(process.env.NET_CASH_SYNC_ENABLED ?? 'true').trim().toLowerCase() !== 'false';
  const nowParts = israelNowParts();
  const dateOnly = args.date || nowParts.date;      // calendar day (dedupe key)
  const syncTs = `${dateOnly} ${nowParts.time}`;    // DATE value = sync timestamp incl. hour
  let asOf = prevMonthEnd(dateOnly);                // bank balance as-of = previous month-end
  let asOfNote = '';                                // set when the reval-closed guard falls back

  // --refresh: recompute the forecast server-side (no browser) BEFORE reading the file,
  // so the pushed FORECAST_EUR is fresh — no one's browser needed. Skipped for describe/show.
  if (args.refresh && !args.describe && !args.show) {
    await runServerCompute();
  }

  const { path: snapPath, data: persisted } = readPersistedForecast();

  // ── TOTAL_BANK_EUR: env override → live NetSuite balance as of the last CLOSED month-end ──
  let totalBankEur = numOrNull(process.env.NET_CASH_TOTAL_BANK_EUR);
  let bankSrc = 'env';
  if (totalBankEur == null) {
    ({ asOf, note: asOfNote } = await resolveClosedMonthEnd(asOf)); // reval-closed guard
    totalBankEur = await fetchBankAsOf(asOf);
    bankSrc = `NetSuite as of ${asOf}`;
  }

  // ── Exclude the dividend from the bank figures (operating basis) ──
  // The bank reflects the dividend cash-out; add the realized dividend back so TOTAL_BANK_EUR matches
  // FORECAST_EUR / MODEL_CLOSING_EUR (which already exclude it). Skipped when TOTAL_BANK_EUR came from
  // the NET_CASH_TOTAL_BANK_EUR env override (that value is taken as-is). Also applied to the alert's
  // CURRENT_BANK_EUR below. Fetched once (both years, for a Jan boundary); {} on failure → no add-back.
  const divYears = [...new Set([String(asOf).slice(0, 4), String(dateOnly).slice(0, 4)])];
  const divByMonth = await fetchDividendByMonth(divYears);
  const divThrough = (mk) => Object.entries(divByMonth).reduce((s, [m, v]) => s + (m <= mk ? v : 0), 0);
  let dividendExcludedEur = 0;
  if (bankSrc.startsWith('NetSuite') && Number.isFinite(totalBankEur)) {
    dividendExcludedEur = divThrough(String(asOf).slice(0, 7));
    if (dividendExcludedEur) {
      totalBankEur += dividendExcludedEur;
      console.log(`[net-cash] TOTAL_BANK_EUR: +€${dividendExcludedEur.toLocaleString()} (dividend excluded → operating basis).`);
    }
  }

  // ── FORECAST_EUR: env → persisted snapshot (Snowflake carry-forward tried on a real run) ──
  let forecastEur = numOrNull(process.env.NET_CASH_FORECAST_EUR);
  let forecastSrc = 'env';
  if (forecastEur == null && persisted) { forecastEur = numOrNull(persisted.forecastEur); forecastSrc = `dashboard snapshot (${persisted.scenario || 'scenario?'})`; }

  // ── MODEL_CLOSING_EUR: the model's flow-forward closing of the last completed month ──
  // (optional — written only when resolved AND the Snowflake column exists)
  let modelClosingEur = persisted ? numOrNull(persisted.modelClosingEur) : null;
  if (modelClosingEur != null) modelClosingEur = Math.round(modelClosingEur);
  const modelClosingMonth = (persisted && persisted.modelClosingMonth) || null;

  // ── CURRENT_BANK_EUR: live balance as of today, open-month FX reval neutralized (notification only) ──
  const currentBank = await fetchCurrentBankExReval(dateOnly);
  // Exclude the dividend here too (alert consistency): add the dividend realized through today.
  const currentBankDividendExcl = currentBank.eur != null ? divThrough(String(dateOnly).slice(0, 7)) : 0;
  if (currentBankDividendExcl) currentBank.eur += currentBankDividendExcl;

  const reportRow = () => {
    console.log('[net-cash] Resolved row:');
    console.log(`[net-cash]   DATE           = ${syncTs}  (Asia/Jerusalem)`);
    console.log(`[net-cash]   TOTAL_BANK_EUR = ${Number.isFinite(totalBankEur) ? Math.round(totalBankEur).toLocaleString() : 'MISSING'}  (src: ${bankSrc})`);
    console.log(`[net-cash]   CURRENT_BANK_EUR = ${currentBank.eur != null ? Math.round(currentBank.eur).toLocaleString() : '(n/a)'}  (live as of ${dateOnly}${currentBank.revalOk && currentBank.revalEur ? `, open-month reval ${Math.round(currentBank.revalEur).toLocaleString()} excluded` : ''})`);
    console.log(`[net-cash]   FORECAST_EUR   = ${Number.isFinite(forecastEur) ? Math.round(forecastEur).toLocaleString() : 'MISSING'}  (src: ${forecastSrc})`);
    console.log(`[net-cash]   MODEL_CLOSING_EUR = ${modelClosingEur != null ? modelClosingEur.toLocaleString() : '(not resolved — column skipped)'}${modelClosingMonth ? `  (model closing for ${modelClosingMonth})` : ''}`);
    if (asOfNote) console.warn(`[net-cash]   ⚠ ${asOfNote}`);
  };

  // ── Dry-run: credential-free. Show what resolved; do NOT touch Snowflake. ──
  if (args.dryRun) {
    reportRow();
    if (!Number.isFinite(forecastEur)) {
      console.warn('[net-cash] FORECAST_EUR unresolved from env/snapshot — a real run would carry forward the last Snowflake row.');
    }
    console.log('[net-cash] --dry-run: not writing. Would insert:');
    console.log(`[net-cash]   DATE=TO_TIMESTAMP_NTZ('${syncTs}'), TOTAL_BANK_EUR=${Number.isFinite(totalBankEur) ? Math.round(totalBankEur) : 'null'}, FORECAST_EUR=${Number.isFinite(forecastEur) ? Math.round(forecastEur) : 'null'}${modelClosingEur != null ? `, MODEL_CLOSING_EUR=${modelClosingEur}` : ''} (+ SRC_UPDATED_AT=CURRENT_TIMESTAMP() and IS_APPROVED=FALSE if those columns exist)`);
    // Preview the notification body EXACTLY as a real successful run would post it to Slack.
    // Nothing is sent — this only prints. Rendered with enabled/wrote=true so it matches the
    // live "row written" message (not a DRY RUN status), using the values resolved above.
    const previewPayload = { enabled: true, wrote: true, dryRun: false, syncTs, totalBankEur, forecastEur, modelClosingEur, modelClosingMonth, bankAsOf: asOf, asOfNote, currentBankEur: currentBank.eur, currentBankReval: currentBank.revalEur, currentBankRevalOk: currentBank.revalOk, currentBankDate: dateOnly, dividendExcludedEur, currentBankDividendExcl, tableName: TABLE };
    const { statusLine: previewStatus, text: previewText } = buildSummary(previewPayload);
    console.log('');
    console.log('[net-cash] --dry-run: notification message preview (NOT sent):');
    console.log('┌─ Slack (#' + (process.env.NET_CASH_SLACK_CHANNEL || 'cash_flow_sync') + ') ' + '─'.repeat(30));
    console.log('│ *Net-Cash → Snowflake* — ' + previewStatus);
    console.log('│ ```');
    for (const line of previewText.split('\n')) console.log('│ ' + line);
    console.log('│ ```');
    console.log('└' + '─'.repeat(48));
    process.exit(0);
  }

  // ── Enable flag: pause/resume the whole sync via NET_CASH_SYNC_ENABLED ──
  if (!syncEnabled) {
    reportRow();
    console.log('[net-cash] NET_CASH_SYNC_ENABLED=false → sync DISABLED, not writing to Snowflake.');
    await notify({ enabled: false, wrote: false, dryRun: false, syncTs, totalBankEur, forecastEur, modelClosingEur, modelClosingMonth, bankAsOf: asOf, asOfNote, currentBankEur: currentBank.eur, currentBankReval: currentBank.revalEur, currentBankRevalOk: currentBank.revalOk, currentBankDate: dateOnly, dividendExcludedEur, currentBankDividendExcl, tableName: TABLE });
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
      ...(modelClosingEur != null ? [{ name: 'MODEL_CLOSING_EUR', expr: '?', bind: modelClosingEur }] : []),
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
    await notify({ enabled: true, wrote: true, dryRun: false, syncTs, totalBankEur, forecastEur, modelClosingEur, modelClosingMonth, bankAsOf: asOf, asOfNote, currentBankEur: currentBank.eur, currentBankReval: currentBank.revalEur, currentBankRevalOk: currentBank.revalOk, currentBankDate: dateOnly, dividendExcludedEur, currentBankDividendExcl, tableName: TABLE });
    process.exit(0);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.error(`[net-cash] ERROR: ${msg}`);
    try { await notify({ enabled: syncEnabled, wrote: false, dryRun: false, syncTs, totalBankEur, forecastEur, modelClosingEur, modelClosingMonth, bankAsOf: asOf, asOfNote, currentBankEur: currentBank.eur, currentBankReval: currentBank.revalEur, currentBankRevalOk: currentBank.revalOk, currentBankDate: dateOnly, dividendExcludedEur, currentBankDividendExcl, tableName: TABLE, error: msg }); } catch { /* ignore */ }
    process.exit(1);
  } finally {
    if (conn) { try { conn.destroy(() => {}); } catch { /* ignore */ } }
  }
}

main();
