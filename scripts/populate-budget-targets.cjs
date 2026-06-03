// Populate FCT_BUDGET_TARGET_BY_DEPT_ACCT in data/banks-dashboard.db from Snowflake FCT_BUDGET.
//
// Usage:
//   node scripts/populate-budget-targets.cjs                # default: subsidiary 3, years 2026 2027
//   node scripts/populate-budget-targets.cjs 3 2026 2027    # explicit
//
// Reads .env for Snowflake credentials. Snowflake access is read-only.
// SQLite writes are local to the dashboard server (data/banks-dashboard.db).

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

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

async function discoverBudgetColumns(conn) {
  const rows = await runSelect(conn, `
    SELECT COLUMN_NAME
    FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'FCT_BUDGET'
  `);
  return new Set(rows.map((r) => r.COLUMN_NAME));
}

async function discoverDimLocation(conn) {
  const rows = await runSelect(conn, `
    SELECT COLUMN_NAME
    FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'DIM_LOCATION'
  `);
  return new Set(rows.map((r) => r.COLUMN_NAME));
}

async function main() {
  const subsidiary = parseInt(process.argv[2] || '3', 10);
  const years = process.argv.slice(3).map((y) => parseInt(y, 10)).filter(Boolean);
  const fiscalYears = years.length ? years : [2026, 2027];

  console.log(`[populate] subsidiary=${subsidiary} fiscal_years=${fiscalYears.join(',')}`);
  console.log(`[populate] target db=${DB_PATH}`);

  const sf = createSnowflakeClient(process.env);
  if (!sf) {
    console.error('Snowflake client failed to initialize. Check .env.');
    process.exit(1);
  }
  const conn = await sf.getConnection();

  const budgetCols = await discoverBudgetColumns(conn);
  const hasLocationId = budgetCols.has('LOCATION_ID');
  const hasCurrencyCode = budgetCols.has('CURRENCY_CODE');
  const dimLocationCols = hasLocationId ? await discoverDimLocation(conn) : new Set();
  const hasDimLocation = dimLocationCols.has('LOCATION_NAME');

  console.log(`[populate] FCT_BUDGET.LOCATION_ID  : ${hasLocationId ? 'yes' : 'no'}`);
  console.log(`[populate] FCT_BUDGET.CURRENCY_CODE: ${hasCurrencyCode ? 'yes' : 'no'}`);
  console.log(`[populate] DIM_LOCATION usable     : ${hasDimLocation ? 'yes' : 'no'}`);

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
  console.log(`[populate] fetched ${rows.length} rows in ${((Date.now() - t0) / 1000).toFixed(1)}s`);

  const db = getDb();
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

  const txn = db.transaction((batch) => {
    for (const r of batch) {
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
  txn(rows);

  const summary = db
    .prepare(`
      SELECT FISCAL_YEAR, COUNT(*) AS ROW_COUNT, ROUND(SUM(ANNUAL_BUDGET_TARGET_AMOUNT), 2) AS TOTAL_ILS
      FROM FCT_BUDGET_TARGET_BY_DEPT_ACCT
      WHERE SUBSIDIARY_ID = ?
      GROUP BY FISCAL_YEAR ORDER BY FISCAL_YEAR
    `)
    .all(subsidiary);
  console.log('[populate] summary by year:');
  for (const s of summary) console.log(`  ${s.FISCAL_YEAR}: ${s.ROW_COUNT} rows, total ILS ${s.TOTAL_ILS}`);

  process.exit(0);
}

main().catch((e) => {
  console.error('[populate] failed:', e);
  process.exit(1);
});
