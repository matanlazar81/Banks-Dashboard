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
const { createSnowflakeClient } = require('../snowflake-api.cjs');
const { getDb, DB_PATH } = require('../db.cjs');

function runSelect(conn, sql) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText: sql,
      complete: (err, _stmt, rows) => (err ? reject(err) : resolve(rows || [])),
    });
  });
}

async function discoverColumns(conn, schema, table) {
  const rows = await runSelect(conn, `
    SELECT COLUMN_NAME
    FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${table}'
  `);
  return new Set(rows.map((r) => r.COLUMN_NAME));
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

  const budgetCols = await discoverColumns(conn, 'FINANCE', 'FCT_BUDGET');
  const hasLocationId = budgetCols.has('LOCATION_ID');
  const hasCurrencyCode = budgetCols.has('CURRENCY_CODE');
  const dimLocationCols = hasLocationId ? await discoverColumns(conn, 'FINANCE', 'DIM_LOCATION') : new Set();
  const hasDimLocation = dimLocationCols.has('LOCATION_NAME');

  log(`[populate] FCT_BUDGET.LOCATION_ID  : ${hasLocationId ? 'yes' : 'no'}`);
  log(`[populate] FCT_BUDGET.CURRENCY_CODE: ${hasCurrencyCode ? 'yes' : 'no'}`);
  log(`[populate] DIM_LOCATION usable     : ${hasDimLocation ? 'yes' : 'no'}`);

  const yearStart = Math.min(...fiscalYears);
  const yearEnd = Math.max(...fiscalYears);

  const sql = `
    SELECT
      EXTRACT(YEAR FROM b.BUDGET_MONTH_DATE)                     AS FISCAL_YEAR,
      COALESCE(d.DEPARTMENT_NAME, 'Unassigned')                  AS DEPARTMENT,
      ${hasLocationId && hasDimLocation
        ? `COALESCE(l.LOCATION_NAME, 'Unassigned')`
        : `'Unassigned'`}                                        AS LOCATION,
      ${hasCurrencyCode ? `COALESCE(b.CURRENCY_CODE, 'ILS')` : `'ILS'`}
                                                                 AS CURRENCY,
      g.GL_ACCOUNT_NUMBER                                        AS ACCOUNT_NUMBER,
      g.GL_ACCOUNT_NAME                                          AS ACCOUNT_NAME,
      g.GL_ACCOUNT_ID                                            AS NETSUITE_INTERNAL_NUMBER,
      ROUND(SUM(b.AMOUNT_ILS_CC), 2)                             AS ANNUAL_BUDGET_TARGET_AMOUNT,
      b.SUBSIDIARY_ID                                            AS SUBSIDIARY_ID
    FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
    JOIN      DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT  g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
    LEFT JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT  d ON b.DEPARTMENT_ID = d.DEPARTMENT_ID
    ${hasLocationId && hasDimLocation
      ? `LEFT JOIN DL_PRODUCTION.FINANCE.DIM_LOCATION    l ON b.LOCATION_ID   = l.LOCATION_ID`
      : ''}
    WHERE b.SUBSIDIARY_ID = ${subsidiary}
      AND b.BUDGET_MONTH_DATE >= '${yearStart}-01-01'
      AND b.BUDGET_MONTH_DATE <= '${yearEnd}-12-31'
      AND EXTRACT(YEAR FROM b.BUDGET_MONTH_DATE) IN (${fiscalYears.join(',')})
    GROUP BY
      EXTRACT(YEAR FROM b.BUDGET_MONTH_DATE),
      d.DEPARTMENT_NAME,
      ${hasLocationId && hasDimLocation ? 'l.LOCATION_NAME,' : ''}
      ${hasCurrencyCode ? 'b.CURRENCY_CODE,' : ''}
      g.GL_ACCOUNT_NUMBER, g.GL_ACCOUNT_NAME, g.GL_ACCOUNT_ID,
      b.SUBSIDIARY_ID
    HAVING ABS(SUM(b.AMOUNT_ILS_CC)) > 0
    ORDER BY FISCAL_YEAR, DEPARTMENT, ACCOUNT_NUMBER
  `;

  const t0 = Date.now();
  const rows = await runSelect(conn, sql);
  const elapsedMs = Date.now() - t0;
  log(`[populate] fetched ${rows.length} rows in ${(elapsedMs / 1000).toFixed(1)}s`);

  const db = getDb();

  // Wipe and re-insert ONLY the (subsidiary, year) slices being refreshed so other
  // years/subs that aren't in scope are left untouched.
  const deleteScope = db.prepare(`
    DELETE FROM FCT_BUDGET_TARGET_BY_DEPT_ACCT
    WHERE SUBSIDIARY_ID = ? AND FISCAL_YEAR = ?
  `);
  const insert = db.prepare(`
    INSERT INTO FCT_BUDGET_TARGET_BY_DEPT_ACCT (
      FISCAL_YEAR, DEPARTMENT, LOCATION, CURRENCY,
      ACCOUNT_NUMBER, ACCOUNT_NAME, NETSUITE_INTERNAL_NUMBER,
      ANNUAL_BUDGET_TARGET_AMOUNT, SUBSIDIARY_ID
    ) VALUES (
      @FISCAL_YEAR, @DEPARTMENT, @LOCATION, @CURRENCY,
      @ACCOUNT_NUMBER, @ACCOUNT_NAME, @NETSUITE_INTERNAL_NUMBER,
      @ANNUAL_BUDGET_TARGET_AMOUNT, @SUBSIDIARY_ID
    )
    ON CONFLICT (FISCAL_YEAR, SUBSIDIARY_ID, DEPARTMENT, LOCATION, ACCOUNT_NUMBER, CURRENCY)
    DO UPDATE SET
      ACCOUNT_NAME                = excluded.ACCOUNT_NAME,
      NETSUITE_INTERNAL_NUMBER    = excluded.NETSUITE_INTERNAL_NUMBER,
      ANNUAL_BUDGET_TARGET_AMOUNT = excluded.ANNUAL_BUDGET_TARGET_AMOUNT,
      LOADED_AT                   = datetime('now')
  `);

  const txn = db.transaction(() => {
    for (const yr of fiscalYears) deleteScope.run(subsidiary, yr);
    for (const r of rows) {
      insert.run({
        FISCAL_YEAR: Number(r.FISCAL_YEAR),
        DEPARTMENT: r.DEPARTMENT || 'Unassigned',
        LOCATION: r.LOCATION || 'Unassigned',
        CURRENCY: r.CURRENCY || 'ILS',
        ACCOUNT_NUMBER: String(r.ACCOUNT_NUMBER || ''),
        ACCOUNT_NAME: r.ACCOUNT_NAME || null,
        NETSUITE_INTERNAL_NUMBER: r.NETSUITE_INTERNAL_NUMBER != null ? Number(r.NETSUITE_INTERNAL_NUMBER) : null,
        ANNUAL_BUDGET_TARGET_AMOUNT: r.ANNUAL_BUDGET_TARGET_AMOUNT != null ? Number(r.ANNUAL_BUDGET_TARGET_AMOUNT) : 0,
        SUBSIDIARY_ID: Number(r.SUBSIDIARY_ID),
      });
    }
  });
  txn();

  const summary = db
    .prepare(`
      SELECT FISCAL_YEAR, COUNT(*) AS ROW_COUNT, ROUND(SUM(ANNUAL_BUDGET_TARGET_AMOUNT), 2) AS TOTAL_ILS
      FROM FCT_BUDGET_TARGET_BY_DEPT_ACCT
      WHERE SUBSIDIARY_ID = ? AND FISCAL_YEAR IN (${fiscalYears.join(',')})
      GROUP BY FISCAL_YEAR ORDER BY FISCAL_YEAR
    `)
    .all(subsidiary);

  log('[populate] summary by year:');
  for (const s of summary) log(`  ${s.FISCAL_YEAR}: ${s.ROW_COUNT} rows, total ILS ${s.TOTAL_ILS}`);

  return {
    subsidiary,
    fiscalYears,
    rowCount: rows.length,
    elapsedMs,
    summary: summary.map((s) => ({
      fiscalYear: s.FISCAL_YEAR,
      rowCount: s.ROW_COUNT,
      totalIls: s.TOTAL_ILS,
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
