// Inspect Snowflake source columns used by sql/FCT_BUDGET_TARGET_BY_DEPT_ACCT.sql.
// Usage:  node scripts/inspect-budget-schema.cjs
// Reads .env for Snowflake credentials. Read-only — no writes are issued.

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { createSnowflakeClient } = require('../snowflake-api.cjs');

const TABLES = [
  { db: 'DL_PRODUCTION', schema: 'FINANCE', name: 'FCT_BUDGET' },
  { db: 'DL_PRODUCTION', schema: 'FINANCE', name: 'DIM_DEPARTMENT' },
  { db: 'DL_PRODUCTION', schema: 'FINANCE', name: 'DIM_GL_ACCOUNT' },
  { db: 'DL_PRODUCTION', schema: 'FINANCE', name: 'DIM_LOCATION' },
];

const REQUIRED = {
  FCT_BUDGET: ['SUBSIDIARY_ID', 'DEPARTMENT_ID', 'GL_ACCOUNT_ID', 'BUDGET_MONTH_DATE', 'AMOUNT_ILS_CC'],
  ASSUMED:    { FCT_BUDGET: ['LOCATION_ID', 'CURRENCY_CODE'] },
  DIM_DEPARTMENT: ['DEPARTMENT_ID', 'DEPARTMENT_NAME'],
  DIM_GL_ACCOUNT: ['GL_ACCOUNT_ID', 'GL_ACCOUNT_NUMBER', 'GL_ACCOUNT_NAME'],
  DIM_LOCATION:   ['LOCATION_ID', 'LOCATION_NAME'],
};

function runSelect(conn, sql) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText: sql,
      complete: (err, _stmt, rows) => (err ? reject(err) : resolve(rows || [])),
    });
  });
}

(async () => {
  const sf = createSnowflakeClient(process.env);
  if (!sf) {
    console.error('Snowflake client could not be created. Check .env (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH).');
    process.exit(1);
  }
  const conn = await sf.getConnection();

  for (const t of TABLES) {
    const fq = `${t.db}.${t.schema}.${t.name}`;
    console.log(`\n── ${fq} ──`);
    try {
      const cols = await runSelect(conn, `
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM ${t.db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '${t.schema}' AND TABLE_NAME = '${t.name}'
        ORDER BY ORDINAL_POSITION
      `);
      if (cols.length === 0) {
        console.log('  (table not found or no SELECT grant)');
        continue;
      }
      const names = new Set(cols.map((c) => c.COLUMN_NAME));
      for (const c of cols) {
        console.log(`  ${c.COLUMN_NAME.padEnd(30)} ${c.DATA_TYPE}${c.IS_NULLABLE === 'NO' ? ' NOT NULL' : ''}`);
      }
      const reqList = REQUIRED[t.name] || [];
      const missing = reqList.filter((n) => !names.has(n));
      if (missing.length) console.log(`  MISSING required: ${missing.join(', ')}`);

      if (t.name === 'FCT_BUDGET') {
        const assumed = REQUIRED.ASSUMED.FCT_BUDGET;
        const absent = assumed.filter((n) => !names.has(n));
        if (absent.length) {
          console.log(`  ASSUMED but NOT PRESENT (adjust SQL): ${absent.join(', ')}`);
        } else {
          console.log(`  Assumed columns present: ${assumed.join(', ')}`);
        }
      }
    } catch (e) {
      console.log(`  query failed: ${e.message}`);
    }
  }

  console.log('\nDone. Review the output before running sql/FCT_BUDGET_TARGET_BY_DEPT_ACCT.sql.');
  process.exit(0);
})();
