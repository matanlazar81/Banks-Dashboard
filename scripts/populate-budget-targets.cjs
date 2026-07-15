// Populate FCT_BUDGET_TARGET_BY_DEPT_ACCT in data/banks-dashboard.db from Snowflake FCT_BUDGET.
//
// CLI usage:
//   node scripts/populate-budget-targets.cjs                # default: subsidiary 3, years 2026 2027
//   node scripts/populate-budget-targets.cjs 3 2026 2027    # explicit
//
// Programmatic usage (e.g. from /api/sync-budget-targets):
//   const { populateBudgetTargets } = require('./scripts/populate-budget-targets.cjs');
//   const result = await populateBudgetTargets({ subsidiary: 3, years: [2026], env: process.env });

const path = require('path');
const { createSnowflakeClient, createSnowflakeWriteClient } = require('../snowflake-api.cjs');
const { getDb, DB_PATH } = require('../db.cjs');

// BI-3228: Snowflake reads scoped to CONSUMER_HUB__FINANCE passthrough views (__FINANCE).
const CH = 'DL_PRODUCTION.CONSUMER_HUB__FINANCE';
const T_FCT_BUDGET = `${CH}.FCT_BUDGET__FINANCE`;
const T_DIM_GL_ACCOUNT = `${CH}.DIM_GL_ACCOUNT__FINANCE`;
const T_DIM_DEPARTMENT = `${CH}.DIM_DEPARTMENT__FINANCE`;

function runSelect(conn, sql) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText: sql,
      complete: (err, _stmt, rows) => (err ? reject(err) : resolve(rows || [])),
    });
  });
}

async function populateBudgetTargets({ subsidiary, years, env, log = console.log }) {
  if (!Number.isFinite(subsidiary)) throw new Error('subsidiary required');
  if (!Array.isArray(years) || years.length === 0) throw new Error('years required (non-empty array)');
  const fiscalYears = years.map((y) => Number(y)).filter((y) => Number.isFinite(y));
  if (fiscalYears.length === 0) throw new Error('years must contain at least one valid year');

  log(`[populate] subsidiary=${subsidiary} fiscal_years=${fiscalYears.join(',')}`);
  log(`[populate] target db=${DB_PATH}`);

  const sf = createSnowflakeClient(env);
  if (!sf) throw new Error('Snowflake client failed to initialize (check .env)');
  const conn = await sf.getConnection();

  // BI-3228: passthrough views are column-parity with source by contract — no runtime schema
  // introspection. The sports location dimension is intentionally dropped: dbt has no financial
  // location model (the old existence-probe silently skipped the join in prod), so LOCATION is a
  // constant. CURRENCY_CODE is a source column on FCT_BUDGET, mirrored in the __FINANCE view.

  const yearStart = Math.min(...fiscalYears);
  const yearEnd = Math.max(...fiscalYears);

  // Fetch at monthly grain so we can store both annual total and per-month breakdown.
  const sql = `
    SELECT
      EXTRACT(YEAR  FROM b.BUDGET_MONTH_DATE)                    AS FISCAL_YEAR,
      EXTRACT(MONTH FROM b.BUDGET_MONTH_DATE)                    AS MONTH_NUM,
      COALESCE(d.DEPARTMENT_NAME, 'Unassigned')                  AS DEPARTMENT,
      'Unassigned'                                               AS LOCATION,
      COALESCE(b.CURRENCY_CODE, 'ILS')                           AS CURRENCY,
      g.GL_ACCOUNT_NUMBER                                        AS ACCOUNT_NUMBER,
      g.GL_ACCOUNT_NAME                                          AS ACCOUNT_NAME,
      g.GL_ACCOUNT_ID                                            AS NETSUITE_INTERNAL_NUMBER,
      g.PARENT_GL_ACCOUNT_NAME                                   AS CATEGORY,
      ROUND(SUM(b.AMOUNT_ILS_CC), 2)                             AS MONTH_AMOUNT_ILS,
      b.SUBSIDIARY_ID                                            AS SUBSIDIARY_ID
    FROM ${T_FCT_BUDGET} b
    JOIN      ${T_DIM_GL_ACCOUNT}  g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
    LEFT JOIN ${T_DIM_DEPARTMENT}  d ON b.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE b.SUBSIDIARY_ID = ${subsidiary}
      AND b.BUDGET_MONTH_DATE >= '${yearStart}-01-01'
      AND b.BUDGET_MONTH_DATE <= '${yearEnd}-12-31'
      AND EXTRACT(YEAR FROM b.BUDGET_MONTH_DATE) IN (${fiscalYears.join(',')})
    GROUP BY
      EXTRACT(YEAR  FROM b.BUDGET_MONTH_DATE),
      EXTRACT(MONTH FROM b.BUDGET_MONTH_DATE),
      d.DEPARTMENT_NAME,
      b.CURRENCY_CODE,
      g.GL_ACCOUNT_NUMBER, g.GL_ACCOUNT_NAME, g.GL_ACCOUNT_ID, g.PARENT_GL_ACCOUNT_NAME,
      b.SUBSIDIARY_ID
  `;

  const t0 = Date.now();
  const monthRows = await runSelect(conn, sql);
  const elapsedMs = Date.now() - t0;
  log(`[populate] fetched ${monthRows.length} monthly rows in ${(elapsedMs / 1000).toFixed(1)}s`);

  // Group monthly rows into one entry per (year, sub, dept, loc, acct, currency).
  const groups = new Map();
  for (const r of monthRows) {
    const key = `${r.FISCAL_YEAR}|${r.SUBSIDIARY_ID}|${r.DEPARTMENT}|${r.LOCATION}|${r.ACCOUNT_NUMBER}|${r.CURRENCY}`;
    let g = groups.get(key);
    if (!g) {
      g = {
        FISCAL_YEAR: Number(r.FISCAL_YEAR),
        SUBSIDIARY_ID: Number(r.SUBSIDIARY_ID),
        DEPARTMENT: r.DEPARTMENT || 'Unassigned',
        LOCATION: r.LOCATION || 'Unassigned',
        CURRENCY: r.CURRENCY || 'ILS',
        ACCOUNT_NUMBER: String(r.ACCOUNT_NUMBER || ''),
        ACCOUNT_NAME: r.ACCOUNT_NAME || null,
        NETSUITE_INTERNAL_NUMBER: r.NETSUITE_INTERNAL_NUMBER != null ? Number(r.NETSUITE_INTERNAL_NUMBER) : null,
        CATEGORY: r.CATEGORY || null,
        annual: 0,
        monthly: {},
      };
      groups.set(key, g);
    }
    const amt = r.MONTH_AMOUNT_ILS != null ? Number(r.MONTH_AMOUNT_ILS) : 0;
    const mkey = String(Number(r.MONTH_NUM)).padStart(2, '0');
    g.monthly[mkey] = (g.monthly[mkey] || 0) + amt;
    g.annual += amt;
  }
  const rows = [...groups.values()].filter((g) => Math.abs(g.annual) > 0);

  const db = getDb();

  // Non-destructive sync:
  // - Upsert rows from Snowflake, touching ONLY source-owned columns. User-override
  //   columns (USER_OVERRIDE_*, USER_EDITED_*) are preserved.
  // - Delete rows in scope (subsidiary, year) that did not appear in this sync AND
  //   carry no user override. User-overridden rows are kept even if the underlying
  //   GL account is removed from Snowflake.
  const syncStart = new Date().toISOString().slice(0, 19).replace('T', ' ');

  const upsert = db.prepare(`
    INSERT INTO FCT_BUDGET_TARGET_BY_DEPT_ACCT (
      FISCAL_YEAR, DEPARTMENT, LOCATION, CURRENCY,
      ACCOUNT_NUMBER, ACCOUNT_NAME, NETSUITE_INTERNAL_NUMBER, CATEGORY,
      SOURCE_AMOUNT_ILS, MONTHLY_SOURCE_ILS, SUBSIDIARY_ID, SOURCE_SYNCED_AT
    ) VALUES (
      @FISCAL_YEAR, @DEPARTMENT, @LOCATION, @CURRENCY,
      @ACCOUNT_NUMBER, @ACCOUNT_NAME, @NETSUITE_INTERNAL_NUMBER, @CATEGORY,
      @SOURCE_AMOUNT_ILS, @MONTHLY_SOURCE_ILS, @SUBSIDIARY_ID, @SOURCE_SYNCED_AT
    )
    ON CONFLICT (FISCAL_YEAR, SUBSIDIARY_ID, DEPARTMENT, LOCATION, ACCOUNT_NUMBER, CURRENCY)
    DO UPDATE SET
      ACCOUNT_NAME             = excluded.ACCOUNT_NAME,
      NETSUITE_INTERNAL_NUMBER = excluded.NETSUITE_INTERNAL_NUMBER,
      CATEGORY                 = excluded.CATEGORY,
      SOURCE_AMOUNT_ILS        = excluded.SOURCE_AMOUNT_ILS,
      MONTHLY_SOURCE_ILS       = excluded.MONTHLY_SOURCE_ILS,
      SOURCE_SYNCED_AT         = excluded.SOURCE_SYNCED_AT
  `);

  const cleanup = db.prepare(`
    DELETE FROM FCT_BUDGET_TARGET_BY_DEPT_ACCT
    WHERE SUBSIDIARY_ID = ? AND FISCAL_YEAR = ?
      AND SOURCE_SYNCED_AT < ?
      AND USER_OVERRIDE_AMOUNT_ILS IS NULL
      AND USER_OVERRIDE_PCT IS NULL
  `);

  let deletedOrphans = 0;
  const txn = db.transaction(() => {
    for (const r of rows) {
      upsert.run({
        FISCAL_YEAR: r.FISCAL_YEAR,
        DEPARTMENT: r.DEPARTMENT,
        LOCATION: r.LOCATION,
        CURRENCY: r.CURRENCY,
        ACCOUNT_NUMBER: r.ACCOUNT_NUMBER,
        ACCOUNT_NAME: r.ACCOUNT_NAME,
        NETSUITE_INTERNAL_NUMBER: r.NETSUITE_INTERNAL_NUMBER,
        CATEGORY: r.CATEGORY,
        SOURCE_AMOUNT_ILS: Math.round(r.annual * 100) / 100,
        MONTHLY_SOURCE_ILS: JSON.stringify(r.monthly),
        SUBSIDIARY_ID: r.SUBSIDIARY_ID,
        SOURCE_SYNCED_AT: syncStart,
      });
    }
    for (const yr of fiscalYears) {
      deletedOrphans += cleanup.run(subsidiary, yr, syncStart).changes;
    }
  });
  txn();

  const summary = db
    .prepare(`
      SELECT
        FISCAL_YEAR,
        COUNT(*) AS ROW_COUNT,
        ROUND(SUM(ANNUAL_BUDGET_TARGET_AMOUNT), 2) AS TOTAL_ILS,
        SUM(CASE WHEN USER_OVERRIDE_AMOUNT_ILS IS NOT NULL OR USER_OVERRIDE_PCT IS NOT NULL THEN 1 ELSE 0 END) AS OVERRIDE_COUNT
      FROM FCT_BUDGET_TARGET_BY_DEPT_ACCT
      WHERE SUBSIDIARY_ID = ? AND FISCAL_YEAR IN (${fiscalYears.join(',')})
      GROUP BY FISCAL_YEAR ORDER BY FISCAL_YEAR
    `)
    .all(subsidiary);

  log('[populate] summary by year:');
  for (const s of summary) log(`  ${s.FISCAL_YEAR}: ${s.ROW_COUNT} rows, total ILS ${s.TOTAL_ILS}, preserved ${s.OVERRIDE_COUNT} user override(s)`);
  if (deletedOrphans > 0) log(`[populate] cleaned up ${deletedOrphans} stale row(s) without user overrides`);

  // ── Write-back: land the full Budget Targets snapshot into RAW.LANDING_FINANCE ──
  // Non-fatal: a landing failure must never break the (already-committed) local
  // sync. Off unless SNOWFLAKE_LANDING_WRITE is enabled. The whole table is pushed
  // (not just this sync's scope) so OVERWRITE doesn't drop other years/subsidiaries.
  let landingWrite = null;
  try {
    const writer = createSnowflakeWriteClient(env);
    if (writer) {
      const allRows = db.prepare(`
        SELECT FISCAL_YEAR, SUBSIDIARY_ID, DEPARTMENT, LOCATION, CURRENCY,
               ACCOUNT_NUMBER, ACCOUNT_NAME, NETSUITE_INTERNAL_NUMBER, CATEGORY,
               SOURCE_AMOUNT_ILS, USER_OVERRIDE_AMOUNT_ILS, USER_OVERRIDE_PCT,
               ANNUAL_BUDGET_TARGET_AMOUNT, MONTHLY_SOURCE_ILS, USER_EDITED_BY,
               USER_EDITED_AT, SOURCE_SYNCED_AT
        FROM FCT_BUDGET_TARGET_BY_DEPT_ACCT
      `).all();
      const t1 = Date.now();
      const res = await writer.writeBudgetTargetsLanding(allRows);
      landingWrite = { ok: true, ...res, elapsedMs: Date.now() - t1 };
      log(`[populate] landed ${res.rowCount} rows → ${res.table} in ${((Date.now() - t1) / 1000).toFixed(1)}s`);
    } else {
      log('[populate] landing write-back disabled (set SNOWFLAKE_LANDING_WRITE=1 to enable)');
    }
  } catch (e) {
    landingWrite = { ok: false, error: e && e.message ? e.message : String(e) };
    log(`[populate] landing write-back FAILED (local sync unaffected): ${landingWrite.error}`);
  }

  return {
    subsidiary,
    fiscalYears,
    rowCount: rows.length,
    elapsedMs,
    deletedOrphans,
    landingWrite,
    summary: summary.map((s) => ({
      fiscalYear: s.FISCAL_YEAR,
      rowCount: s.ROW_COUNT,
      totalIls: s.TOTAL_ILS,
      preservedOverrides: s.OVERRIDE_COUNT,
    })),
    dbPath: DB_PATH,
  };
}

async function cli() {
  const dotenv = require('dotenv');
  dotenv.config({ path: path.join(__dirname, '..', '.env') });
  const subsidiary = parseInt(process.argv[2] || '3', 10);
  const yearArgs = process.argv.slice(3).map((y) => parseInt(y, 10)).filter(Boolean);
  const years = yearArgs.length ? yearArgs : [2026, 2027];
  try {
    await populateBudgetTargets({ subsidiary, years, env: process.env });
    process.exit(0);
  } catch (e) {
    console.error('[populate] failed:', e.message || e);
    process.exit(1);
  }
}

if (require.main === module) cli();

module.exports = { populateBudgetTargets };
