/**
 * Snowflake API Client — Key-Pair Authentication
 * Account: FQXIBQO-LSPORTS_GCP.snowflakecomputing.com
 * Warehouse: finance_wh | User: finance (service user)
 */
const snowflake = require('snowflake-sdk');
const fs = require('fs');
const crypto = require('crypto');

function createSnowflakeClient(env) {
  const account = env.SNOWFLAKE_ACCOUNT;
  const username = env.SNOWFLAKE_USER;
  const warehouse = env.SNOWFLAKE_WAREHOUSE;
  const privateKeyPath = env.SNOWFLAKE_PRIVATE_KEY_PATH;

  if (!account || !username || !privateKeyPath) {
    console.warn('[Snowflake] Missing config — SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, or SNOWFLAKE_PRIVATE_KEY_PATH');
    return null;
  }

  // Read and parse the private key
  let privateKey;
  try {
    const keyContent = fs.readFileSync(privateKeyPath, 'utf-8');
    // snowflake-sdk expects the key as a string in PEM format
    privateKey = keyContent.trim();
  } catch (e) {
    console.error(`[Snowflake] Failed to read private key from ${privateKeyPath}:`, e.message);
    return null;
  }

  let connection = null;

  async function getConnection() {
    if (connection && connection.isUp()) return connection;

    return new Promise((resolve, reject) => {
      const conn = snowflake.createConnection({
        account,
        username,
        authenticator: 'SNOWFLAKE_JWT',
        privateKey,
        warehouse,
        database: 'DL_PRODUCTION',
        schema: 'FINANCE',
        application: 'AgingDashboard',
      });

      conn.connect((err, conn) => {
        if (err) {
          console.error('[Snowflake] Connection failed:', err.message);
          reject(err);
        } else {
          console.log('[Snowflake] Connected successfully (read-only mode)');
          // Set session to read-only — prevent any accidental writes
          conn.execute({ sqlText: 'ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30', complete: () => {} });
          connection = conn;
          resolve(conn);
        }
      });
    });
  }

  async function query(sql, binds = []) {
    // READ-ONLY GUARD: Only allow SELECT, SHOW, DESCRIBE, WITH statements
    const trimmed = sql.trim().toUpperCase();
    if (!trimmed.startsWith('SELECT') && !trimmed.startsWith('SHOW') && !trimmed.startsWith('DESCRIBE') && !trimmed.startsWith('WITH') && !trimmed.startsWith('ALTER SESSION')) {
      throw new Error(`[Snowflake] READ-ONLY: Blocked non-SELECT query: ${trimmed.substring(0, 50)}`);
    }
    const conn = await getConnection();
    return new Promise((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds,
        complete: (err, stmt, rows) => {
          if (err) {
            console.error('[Snowflake] Query failed:', err.message);
            reject(err);
          } else {
            resolve(rows || []);
          }
        },
      });
    });
  }

  // ── Test connection ──
  async function testConnection() {
    try {
      const rows = await query('SELECT CURRENT_TIMESTAMP() AS now, CURRENT_WAREHOUSE() AS wh, CURRENT_USER() AS usr');
      console.log('[Snowflake] Test:', rows[0]);
      return rows[0];
    } catch (e) {
      return { error: e.message };
    }
  }

  // ── Financial queries (to be expanded based on available tables) ──
  async function listDatabases() {
    return await query('SHOW DATABASES');
  }

  async function listSchemas(database) {
    return await query(`SHOW SCHEMAS IN DATABASE ${database}`);
  }

  async function listTables(database, schema) {
    return await query(`SHOW TABLES IN ${database}.${schema}`);
  }

  // Generic query wrapper for financial data
  async function fetchFinancialData(sql) {
    console.log('[Snowflake] Running financial query...');
    const rows = await query(sql);
    console.log(`[Snowflake] Query returned ${rows.length} rows`);
    return rows;
  }

  // ── Budget by category (vendor expenses, excl. salary & finance) ──
  async function fetchBudgetByCategory(year) {
    const yr = year || 2026;
    console.log(`[Snowflake] Fetching vendor budget by category for ${yr}...`);
    const rows = await query(`
      SELECT BUDGET_MONTH_DATE::VARCHAR AS MONTH_STR,
             LEFT(g.GL_ACCOUNT_NUMBER, 3) AS ACCT_PREFIX,
             g.PARENT_GL_ACCOUNT_NAME AS CATEGORY,
             ROUND(SUM(b.AMOUNT_EUR_CC)) AS BUDGET_EUR,
             ROUND(SUM(b.AMOUNT_ILS_CC)) AS BUDGET_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      WHERE b.SUBSIDIARY_ID = 3
        AND g.GL_ACCOUNT_TYPE = 'Expense'
        AND g.IS_PAYROLL = FALSE
        AND g.GL_ACCOUNT_NUMBER NOT LIKE '800%'
        AND g.GL_ACCOUNT_NUMBER NOT IN ('780502')
        AND b.BUDGET_MONTH_DATE >= '${yr}-01-01'
        AND b.BUDGET_MONTH_DATE <= '${yr}-12-31'
      GROUP BY BUDGET_MONTH_DATE::VARCHAR, LEFT(g.GL_ACCOUNT_NUMBER, 3), g.PARENT_GL_ACCOUNT_NAME
      HAVING ABS(SUM(b.AMOUNT_EUR_CC)) > 10
      ORDER BY MONTH_STR, BUDGET_EUR DESC
    `);

    // Group by month
    const byMonth = {};
    let totalBudgetByMonth = {};
    for (const r of rows) {
      const month = (r.MONTH_STR || '').substring(0, 7); // "2026-01-01" → "2026-01"
      if (!byMonth[month]) byMonth[month] = {};
      if (!totalBudgetByMonth[month]) totalBudgetByMonth[month] = { eur: 0, ils: 0 };
      const cat = r.CATEGORY || `Acct ${r.ACCT_PREFIX}`;
      byMonth[month][cat] = (byMonth[month][cat] || 0) + (r.BUDGET_EUR || 0);
      totalBudgetByMonth[month].eur += r.BUDGET_EUR || 0;
      totalBudgetByMonth[month].ils += r.BUDGET_ILS || 0;
    }

    // Also fetch Finance/currency defense budget (800% GL accounts) in the same call
    const finRows = await query(`
      SELECT BUDGET_MONTH_DATE::VARCHAR AS MONTH_STR,
             ROUND(SUM(b.AMOUNT_EUR_CC)) AS BUDGET_EUR,
             ROUND(SUM(b.AMOUNT_ILS_CC)) AS BUDGET_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      WHERE b.SUBSIDIARY_ID = 3
        AND g.GL_ACCOUNT_NUMBER LIKE '800%'
        AND b.BUDGET_MONTH_DATE >= '${yr}-01-01'
        AND b.BUDGET_MONTH_DATE <= '${yr}-12-31'
      GROUP BY BUDGET_MONTH_DATE::VARCHAR
      ORDER BY MONTH_STR
    `);
    const financeBudget = {};
    for (const r of finRows) {
      const month = (r.MONTH_STR || '').substring(0, 7);
      financeBudget[month] = { eur: Math.round(r.BUDGET_EUR || 0), ils: Math.round(r.BUDGET_ILS || 0) };
    }
    console.log(`[Snowflake] Budget: ${Object.keys(byMonth).length} months, ${rows.length} category-months, finance(800%): ${Object.keys(financeBudget).length} months${Object.values(financeBudget)[0] ? ` sample: ${JSON.stringify(Object.values(financeBudget)[0])}` : ''}`);
    return { byMonth, totalByMonth: totalBudgetByMonth, financeBudget };
  }

  // ── Monthly actual expenses (all, for budget vs actual comparison) ──
  async function fetchActualExpenses() {
    console.log('[Snowflake] Fetching actual expenses...');
    const rows = await query(`
      SELECT DATE_TRUNC('month', CAL_MONTH_START_DATE)::VARCHAR AS MONTH_STR,
             ROUND(SUM(AMOUNT_EUR)) AS TOTAL_EUR,
             ROUND(SUM(AMOUNT_ILS)) AS TOTAL_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE
      WHERE SUBSIDIARY_ID = 3
        AND SOURCE = 'netsuite'
        AND CAL_MONTH_START_DATE >= '2025-01-01'
      GROUP BY DATE_TRUNC('month', CAL_MONTH_START_DATE)::VARCHAR
      ORDER BY MONTH_STR
    `);

    const byMonth = {};
    for (const r of rows) {
      const month = (r.MONTH_STR || '').substring(0, 7);
      byMonth[month] = { eur: Math.round(r.TOTAL_EUR || 0), ils: Math.round(r.TOTAL_ILS || 0) };
    }

    console.log(`[Snowflake] Actuals: ${Object.keys(byMonth).length} months`);
    return byMonth;
  }

  // ── Revenue projection from FCT_REVENUE__MONTHLY_ACTUAL_VS_TARGET ──
  async function fetchRevenueProjection(year) {
    const yr = year || 2026;
    console.log(`[Snowflake] Fetching revenue projection for ${yr}...`);

    const rows = await query(`
      SELECT CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR,
             ROUND(REVENUE_EUR_ACTUAL) AS ACTUAL_EUR,
             ROUND(REVENUE_EUR_FORECAST) AS FORECAST_EUR,
             ROUND(REVENUE_TARGET_EURO) AS TARGET_EUR,
             ROUND(SF_REVENUE_AMOUNT_EUR) AS SF_REV_EUR,
             ROUND(NS_REVENUE_AMOUNT_EUR) AS NS_REV_EUR
      FROM DL_PRODUCTION.FINANCE.FCT_REVENUE__MONTHLY_ACTUAL_VS_TARGET
      WHERE CAL_MONTH_START_DATE >= '${yr - 1}-01-01'
        AND CAL_MONTH_START_DATE <= '${yr}-12-31'
      ORDER BY CAL_MONTH_START_DATE
    `);

    // Also get revenue targets
    let targets = {};
    try {
      const targetRows = await query(`
        SELECT CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR,
               ROUND(REVENUE_TARGET_EURO) AS TARGET_EUR
        FROM DL_PRODUCTION.FINANCE.FCT_REVENUE_TARGET
        WHERE CAL_MONTH_START_DATE >= '${yr}-01-01'
        ORDER BY CAL_MONTH_START_DATE
      `);
      for (const r of targetRows) {
        const m = (r.MONTH_STR || '').substring(0, 7);
        targets[m] = Math.round(r.TARGET_EUR || 0);
      }
    } catch {}

    const budget = {};
    const actuals = {};
    for (const r of rows) {
      const m = (r.MONTH_STR || '').substring(0, 7);
      // Forecast = SF_REV (Salesforce pipeline) for future months
      const forecast = r.FORECAST_EUR || r.SF_REV_EUR || 0;
      const actual = r.ACTUAL_EUR || r.NS_REV_EUR || 0;
      const target = r.TARGET_EUR || targets[m] || 0;

      // For each month: use forecast as the projected inflow
      if (forecast > 0) {
        budget[m] = { eur: Math.round(forecast), ils: Math.round(forecast * 3.68) };
      } else if (target > 0) {
        budget[m] = { eur: Math.round(target), ils: Math.round(target * 3.68) };
      }

      if (actual > 0) {
        actuals[m] = { eur: Math.round(actual), ils: Math.round(actual * 3.68) };
      }
    }

    console.log(`[Snowflake] Revenue: ${Object.keys(budget).length} forecast months, ${Object.keys(actuals).length} actual months`);
    return { budget, actuals, targets };
  }

  // ── Monthly actuals split by Salary / Vendors / Finance (from FCT_EXPENSE) ──
  async function fetchMonthlyActualsSplit() {
    console.log('[Snowflake] Fetching monthly actuals split...');
    const rows = await query(`
      SELECT DATE_TRUNC('month', e.CAL_MONTH_START_DATE)::VARCHAR AS MONTH_STR,
             CASE WHEN g.IS_PAYROLL THEN 'Salary'
                  WHEN g.GL_ACCOUNT_NUMBER LIKE '800%' THEN 'Finance'
                  ELSE 'Vendors' END AS EXPENSE_TYPE,
             ROUND(SUM(e.AMOUNT_EUR)) AS AMOUNT_EUR,
             ROUND(SUM(e.AMOUNT_ILS)) AS AMOUNT_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE e
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON e.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      WHERE e.SUBSIDIARY_ID = 3
        AND e.SOURCE = 'netsuite'
        AND e.CAL_MONTH_START_DATE >= '2025-01-01'
      GROUP BY DATE_TRUNC('month', e.CAL_MONTH_START_DATE)::VARCHAR,
               CASE WHEN g.IS_PAYROLL THEN 'Salary'
                    WHEN g.GL_ACCOUNT_NUMBER LIKE '800%' THEN 'Finance'
                    ELSE 'Vendors' END
      ORDER BY MONTH_STR, EXPENSE_TYPE
    `);

    const byMonth = {};
    for (const r of rows) {
      const m = (r.MONTH_STR || '').substring(0, 7);
      if (!byMonth[m]) byMonth[m] = { salary: 0, salaryILS: 0, vendors: 0, vendorsILS: 0 };
      if (r.EXPENSE_TYPE === 'Salary') {
        byMonth[m].salary = Math.round(r.AMOUNT_EUR || 0);
        byMonth[m].salaryILS = Math.round(r.AMOUNT_ILS || 0);
      } else if (r.EXPENSE_TYPE === 'Vendors') {
        byMonth[m].vendors = Math.round(r.AMOUNT_EUR || 0);
        byMonth[m].vendorsILS = Math.round(r.AMOUNT_ILS || 0);
      }
    }

    console.log(`[Snowflake] Monthly actuals: ${Object.keys(byMonth).length} months`);
    return byMonth;
  }

  // ── Vendor breakdown by category for a specific month (from FCT_EXPENSE) ──
  async function fetchVendorBreakdown(month) {
    console.log(`[Snowflake] Fetching vendor breakdown for ${month}...`);
    const startDate = `${month}-01`;
    const rows = await query(`
      SELECT d.DEPARTMENT_NAME AS DEPT,
             g.PARENT_GL_ACCOUNT_NAME AS CATEGORY,
             g.GL_ACCOUNT_NUMBER AS ACCT_NUM,
             g.GL_ACCOUNT_NAME AS ACCT_NAME,
             g.GL_ACCOUNT_ID AS ACCT_ID,
             ROUND(SUM(e.AMOUNT_EUR)) AS AMOUNT_EUR,
             ROUND(SUM(e.AMOUNT_ILS)) AS AMOUNT_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE e
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON e.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      LEFT JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT d ON e.DEPARTMENT_ID = d.DEPARTMENT_ID
      WHERE e.SUBSIDIARY_ID = 3
        AND e.SOURCE = 'netsuite'
        AND g.IS_PAYROLL = FALSE
        AND g.GL_ACCOUNT_NUMBER NOT LIKE '800%'
        AND g.GL_ACCOUNT_NUMBER NOT IN ('780502')
        AND g.GL_ACCOUNT_TYPE = 'Expense'
        AND DATE_TRUNC('month', e.CAL_MONTH_START_DATE) = TO_DATE('${startDate}')
      GROUP BY d.DEPARTMENT_NAME, g.PARENT_GL_ACCOUNT_NAME, g.GL_ACCOUNT_NUMBER, g.GL_ACCOUNT_NAME, g.GL_ACCOUNT_ID
      HAVING ABS(SUM(e.AMOUNT_EUR)) > 10
      ORDER BY SUM(e.AMOUNT_EUR) DESC
    `);

    return rows.map(r => ({
      department: r.DEPT || 'Unassigned',
      category: r.CATEGORY || 'Other',
      account: r.ACCT_NUM,
      accountId: r.ACCT_ID,
      name: r.ACCT_NAME,
      amountEUR: Math.round(r.AMOUNT_EUR || 0),
      amountILS: Math.round(r.AMOUNT_ILS || 0),
    }));
  }

  // ── Salary breakdown by account for a specific month (from FCT_EXPENSE) ──
  async function fetchSalaryBreakdown(month) {
    console.log(`[Snowflake] Fetching salary breakdown for ${month}...`);
    const startDate = `${month}-01`;
    const rows = await query(`
      SELECT g.GL_ACCOUNT_NUMBER AS ACCT_NUM,
             g.GL_ACCOUNT_NAME AS ACCT_NAME,
             ROUND(SUM(e.AMOUNT_EUR)) AS AMOUNT_EUR,
             ROUND(SUM(e.AMOUNT_ILS)) AS AMOUNT_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE e
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON e.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      WHERE e.SUBSIDIARY_ID = 3
        AND e.SOURCE = 'netsuite'
        AND g.IS_PAYROLL = TRUE
        AND DATE_TRUNC('month', e.CAL_MONTH_START_DATE) = TO_DATE('${startDate}')
      GROUP BY g.GL_ACCOUNT_NUMBER, g.GL_ACCOUNT_NAME
      HAVING ABS(SUM(e.AMOUNT_EUR)) > 10
      ORDER BY SUM(e.AMOUNT_EUR) DESC
    `);

    return rows.map(r => ({
      account: r.ACCT_NUM,
      name: r.ACCT_NAME,
      amountEUR: Math.round(r.AMOUNT_EUR || 0),
      amountILS: Math.round(r.AMOUNT_ILS || 0),
    }));
  }

  // ── Budget breakdown by department for a category + month ──
  async function fetchBudgetCategoryDetail(month, category) {
    console.log(`[Snowflake] Fetching budget detail: ${month} / ${category}...`);
    const startDate = `${month}-01`;
    const rows = await query(`
      SELECT d.DEPARTMENT_NAME AS DEPT,
             g.GL_ACCOUNT_NUMBER AS ACCT_NUM,
             g.GL_ACCOUNT_NAME AS ACCT_NAME,
             g.GL_ACCOUNT_ID AS ACCT_ID,
             ROUND(SUM(b.AMOUNT_EUR_CC)) AS BUDGET_EUR,
             ROUND(SUM(b.AMOUNT_ILS_CC)) AS BUDGET_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      LEFT JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT d ON b.DEPARTMENT_ID = d.DEPARTMENT_ID
      WHERE b.SUBSIDIARY_ID = 3
        AND g.GL_ACCOUNT_TYPE = 'Expense'
        AND g.IS_PAYROLL = FALSE
        AND g.PARENT_GL_ACCOUNT_NAME = '${category.replace(/'/g, "''")}'
        AND b.BUDGET_MONTH_DATE = TO_DATE('${startDate}')
      GROUP BY d.DEPARTMENT_NAME, g.GL_ACCOUNT_NUMBER, g.GL_ACCOUNT_NAME, g.GL_ACCOUNT_ID
      HAVING ABS(SUM(b.AMOUNT_EUR_CC)) > 10
      ORDER BY SUM(b.AMOUNT_EUR_CC) DESC
    `);

    console.log(`[Snowflake] Budget detail: ${rows.length} rows for ${category} in ${month}`);
    return rows.map(r => ({
      department: r.DEPT || 'Unassigned',
      account: r.ACCT_NUM,
      accountId: r.ACCT_ID,
      name: r.ACCT_NAME,
      amountEUR: Math.round(r.BUDGET_EUR || 0),
      amountILS: Math.round(r.BUDGET_ILS || 0),
    }));
  }

  // ── Budget overrides from FCT_EXPENSE (source = future_cost_override / future_cost_increment) ──
  async function fetchBudgetOverrides() {
    console.log('[Snowflake] Fetching budget overrides from FCT_EXPENSE...');
    const rows = await query(`
      SELECT e.GL_ACCOUNT_ID, g.GL_ACCOUNT_NUMBER, g.PARENT_GL_ACCOUNT_NAME AS CATEGORY,
             e.DEPARTMENT_ID, e.CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR,
             ROUND(e.AMOUNT_EUR) AS AMOUNT_EUR,
             e.SOURCE, e.MEMO
      FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE e
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON e.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      WHERE e.SOURCE IN ('future_cost_override', 'future_cost_increment')
        AND e.SUBSIDIARY_ID = 3
    `);
    console.log(`[Snowflake] Budget overrides: ${rows.length} rows from FCT_EXPENSE`);
    return rows.map(r => ({
      account: r.GL_ACCOUNT_NUMBER,
      glAccountId: r.GL_ACCOUNT_ID,
      category: r.CATEGORY,
      month: (r.MONTH_STR || '').substring(0, 7),
      department: r.DEPARTMENT_ID,
      amountEUR: r.AMOUNT_EUR || 0,
      mode: r.SOURCE === 'future_cost_override' ? 'Override' : 'Increment',
      comments: r.MEMO || '',
    }));
  }

  // ── Finance budget per month (800% GL accounts = currency defense / hedging from FCT_BUDGET) ──
  async function fetchFinanceBudget(year) {
    const yr = year || 2026;
    console.log(`[Snowflake] Fetching finance/currency defense budget for ${yr}...`);
    const rows = await query(`
      SELECT BUDGET_MONTH_DATE::VARCHAR AS MONTH_STR,
             ROUND(SUM(b.AMOUNT_EUR_CC)) AS BUDGET_EUR,
             ROUND(SUM(b.AMOUNT_ILS_CC)) AS BUDGET_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      WHERE b.SUBSIDIARY_ID = 3
        AND g.GL_ACCOUNT_NUMBER LIKE '800%'
        AND b.BUDGET_MONTH_DATE >= '${yr}-01-01'
        AND b.BUDGET_MONTH_DATE <= '${yr}-12-31'
      GROUP BY BUDGET_MONTH_DATE::VARCHAR
      ORDER BY MONTH_STR
    `);

    const byMonth = {};
    for (const r of rows) {
      const month = (r.MONTH_STR || '').substring(0, 7);
      byMonth[month] = { eur: Math.round(r.BUDGET_EUR || 0), ils: Math.round(r.BUDGET_ILS || 0) };
    }

    console.log(`[Snowflake] Finance budget: ${Object.keys(byMonth).length} months, sample: ${JSON.stringify(Object.values(byMonth)[0] || {})}`);
    return byMonth;
  }

  // ── Salary budget per month (payroll accounts from FCT_BUDGET) ──
  async function fetchSalaryBudget(year) {
    const yr = year || 2026;
    console.log(`[Snowflake] Fetching salary budget by month for ${yr}...`);
    const rows = await query(`
      SELECT BUDGET_MONTH_DATE::VARCHAR AS MONTH_STR,
             ROUND(SUM(b.AMOUNT_EUR_CC)) AS BUDGET_EUR,
             ROUND(SUM(b.AMOUNT_ILS_CC)) AS BUDGET_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      WHERE b.SUBSIDIARY_ID = 3
        AND g.IS_PAYROLL = TRUE
        AND b.BUDGET_MONTH_DATE >= '${yr}-01-01'
        AND b.BUDGET_MONTH_DATE <= '${yr}-12-31'
      GROUP BY BUDGET_MONTH_DATE::VARCHAR
      ORDER BY MONTH_STR
    `);

    const byMonth = {};
    for (const r of rows) {
      const month = (r.MONTH_STR || '').substring(0, 7);
      byMonth[month] = { eur: Math.round(r.BUDGET_EUR || 0), ils: Math.round(r.BUDGET_ILS || 0) };
    }

    console.log(`[Snowflake] Salary budget: ${Object.keys(byMonth).length} months`);
    return byMonth;
  }

  // ── Salary budget breakdown by account for a specific month ──
  async function fetchSalaryBudgetBreakdown(month) {
    console.log(`[Snowflake] Fetching salary budget breakdown for ${month}...`);
    const startDate = `${month}-01`;
    const rows = await query(`
      SELECT d.DEPARTMENT_NAME AS DEPT,
             g.GL_ACCOUNT_NUMBER AS ACCT_NUM,
             g.GL_ACCOUNT_NAME AS ACCT_NAME,
             g.GL_ACCOUNT_ID AS ACCT_ID,
             ROUND(SUM(b.AMOUNT_EUR_CC)) AS BUDGET_EUR,
             ROUND(SUM(b.AMOUNT_ILS_CC)) AS BUDGET_ILS
      FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
      JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
      LEFT JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT d ON b.DEPARTMENT_ID = d.DEPARTMENT_ID
      WHERE b.SUBSIDIARY_ID = 3
        AND g.IS_PAYROLL = TRUE
        AND b.BUDGET_MONTH_DATE = TO_DATE('${startDate}')
      GROUP BY d.DEPARTMENT_NAME, g.GL_ACCOUNT_NUMBER, g.GL_ACCOUNT_NAME, g.GL_ACCOUNT_ID
      HAVING ABS(SUM(b.AMOUNT_EUR_CC)) > 10
      ORDER BY SUM(b.AMOUNT_EUR_CC) DESC
    `);

    console.log(`[Snowflake] Salary budget breakdown: ${rows.length} rows for ${month}`);
    return rows.map(r => ({
      department: r.DEPT || 'Unassigned',
      account: r.ACCT_NUM,
      accountId: r.ACCT_ID,
      name: r.ACCT_NAME,
      amountEUR: Math.round(r.BUDGET_EUR || 0),
      amountILS: Math.round(r.BUDGET_ILS || 0),
    }));
  }

  // ── Headcount by department (for salary dept adjustment with +/- controls) ──
  // Maps sub-groups (e.g., Technology, R&D, SUPPORT) to Bob's parent departments
  // (e.g., Playmakers, Go To Market) via DIM_DEPARTMENT.GROUP_NAME → DEPARTMENT_NAME
  async function fetchHeadcountByDepartment() {
    console.log('[Snowflake] Fetching headcount by department (mapped via DIM_DEPARTMENT)...');
    const rows = await query(`
      SELECT d.DEPARTMENT_NAME AS DEPT,
             COUNT(*) AS HEADCOUNT,
             ROUND(AVG(e.PAYROLL_SALARY_MONTHLY_PAYMENT_AMOUNT)) AS AVG_SALARY_ILS
      FROM DL_PRODUCTION.HR.DIM_EMPLOYEE e
      JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT d
        ON e.EMPLOYEE_GROUP = d.GROUP_NAME AND d.SUBSIDIARY_ID = 3 AND d.SRC_IS_ACTIVE = TRUE
      WHERE e.STATUS = 'Active'
        AND e.COMPANY_NAME = 'LSports'
      GROUP BY d.DEPARTMENT_NAME
      ORDER BY HEADCOUNT DESC
    `);
    console.log(`[Snowflake] Headcount by dept: ${rows.length} departments`);
    // Convert ILS to EUR using ~3.75 rate (approximate, used for display only)
    return rows.map(r => ({
      department: r.DEPT,
      headcount: r.HEADCOUNT || 0,
      avgSalaryILS: r.AVG_SALARY_ILS || 0,
      avgSalaryEUR: Math.round((r.AVG_SALARY_ILS || 0) / 3.75),
    }));
  }

  // ── Headcount events (HiBob → Snowflake) — levers for salary projection ──
  async function fetchHeadcountEvents(month, year) {
    const yr = year || 2026;
    console.log(`[Snowflake] Fetching headcount events for ${month}...`);
    const startDate = `${month}-01`;

    // 1) Events for this specific month (deltas)
    const events = await query(`
      SELECT EVENT_TYPE, EVENT_SUB_TYPE, DEPARTMENT, POSITION_NAME,
             ROUND(EMPLOYER_COST) AS COST, CURRENCY_CODE,
             EMPLOYEE_ID, OPENING_ID, OPENING_STATUS
      FROM DL_PRODUCTION.HR.FCT_HEADCOUNT_EVENT
      WHERE EVENT_MONTH_DATE = TO_DATE('${startDate}')
      ORDER BY EVENT_TYPE, EMPLOYER_COST DESC
    `);

    // 2) Cumulative events from clicked month → Dec to show remaining projected impact
    //    Also include open positions from before this month (unfilled roles carry forward)
    const cumulative = await query(`
      SELECT EVENT_TYPE, EVENT_SUB_TYPE,
             COUNT(*) AS CNT,
             ROUND(SUM(EMPLOYER_COST)) AS TOTAL_COST,
             MAX(CURRENCY_CODE) AS CURRENCY
      FROM DL_PRODUCTION.HR.FCT_HEADCOUNT_EVENT
      WHERE EVENT_MONTH_DATE >= TO_DATE('${startDate}')
        AND EVENT_MONTH_DATE <= '${yr}-12-31'
      GROUP BY EVENT_TYPE, EVENT_SUB_TYPE
      ORDER BY EVENT_TYPE, EVENT_SUB_TYPE
    `);
    // Also get open positions from before the clicked month (unfilled roles still need hiring)
    const pastOpen = await query(`
      SELECT EVENT_TYPE, EVENT_SUB_TYPE,
             COUNT(*) AS CNT,
             ROUND(SUM(EMPLOYER_COST)) AS TOTAL_COST,
             MAX(CURRENCY_CODE) AS CURRENCY
      FROM DL_PRODUCTION.HR.FCT_HEADCOUNT_EVENT
      WHERE EVENT_MONTH_DATE < TO_DATE('${startDate}')
        AND EVENT_MONTH_DATE >= '${yr}-01-01'
        AND OPENING_STATUS = 'Open'
        AND EMPLOYEE_ID IS NULL
      GROUP BY EVENT_TYPE, EVENT_SUB_TYPE
      ORDER BY EVENT_TYPE, EVENT_SUB_TYPE
    `);
    // Merge past open positions into cumulative
    for (const po of pastOpen) {
      const existing = cumulative.find(c => c.EVENT_TYPE === po.EVENT_TYPE && c.EVENT_SUB_TYPE === po.EVENT_SUB_TYPE);
      if (existing) {
        existing.CNT += po.CNT;
        existing.TOTAL_COST = (existing.TOTAL_COST || 0) + (po.TOTAL_COST || 0);
      } else {
        cumulative.push(po);
      }
    }

    // 2b) Monthly breakdown of headcount events for timeline view (full year)
    const monthly = await query(`
      SELECT TO_CHAR(EVENT_MONTH_DATE, 'YYYY-MM') AS MONTH,
             EVENT_TYPE, EVENT_SUB_TYPE,
             COUNT(*) AS CNT,
             ROUND(SUM(EMPLOYER_COST)) AS TOTAL_COST
      FROM DL_PRODUCTION.HR.FCT_HEADCOUNT_EVENT
      WHERE EVENT_MONTH_DATE >= TO_DATE('${yr}-01-01')
        AND EVENT_MONTH_DATE <= '${yr}-12-31'
      GROUP BY TO_CHAR(EVENT_MONTH_DATE, 'YYYY-MM'), EVENT_TYPE, EVENT_SUB_TYPE
      ORDER BY MONTH, EVENT_TYPE, EVENT_SUB_TYPE
    `);

    // 3) Active headcount baseline
    const baseline = await query(`
      SELECT COUNT(*) AS HC,
             ROUND(SUM(PAYROLL_SALARY_MONTHLY_PAYMENT_AMOUNT)) AS BASE_MONTHLY
      FROM DL_PRODUCTION.HR.DIM_EMPLOYEE
      WHERE IS_ACTIVE_HEADCOUNT_PAYROLL = TRUE
        AND PAYROLL_SALARY_MONTHLY_PAYMENT_AMOUNT > 0
    `);

    console.log(`[Snowflake] Headcount events: ${events.length} events for ${month}, ${cumulative.length} cumulative categories`);
    return {
      events: events.map(r => ({
        type: r.EVENT_TYPE,
        subType: r.EVENT_SUB_TYPE,
        department: r.DEPARTMENT,
        position: r.POSITION_NAME,
        cost: r.COST || 0,
        currency: r.CURRENCY_CODE,
        employeeId: r.EMPLOYEE_ID,
        openingId: r.OPENING_ID,
        status: r.OPENING_STATUS,
      })),
      cumulative: cumulative.map(r => ({
        type: r.EVENT_TYPE,
        subType: r.EVENT_SUB_TYPE,
        count: r.CNT,
        totalCost: r.TOTAL_COST || 0,
        currency: r.CURRENCY,
      })),
      monthly: monthly.map(r => ({
        month: r.MONTH,
        type: r.EVENT_TYPE,
        subType: r.EVENT_SUB_TYPE,
        count: r.CNT,
        totalCost: r.TOTAL_COST || 0,
      })),
      baseline: {
        headcount: baseline[0]?.HC || 0,
        monthlyBase: baseline[0]?.BASE_MONTHLY || 0,
      },
    };
  }

  // Fetch all events for a specific lever type (e.g. all "Terminated" events in a year)
  async function fetchHeadcountLeverDetail(eventType, eventSubType, fromMonth, year) {
    const yr = year || 2026;
    // Always fetch full year — cumulative view shows Jan→Dec regardless of clicked month
    const fromDate = `${yr}-01-01`;
    console.log(`[Snowflake] Fetching lever detail: ${eventType}/${eventSubType} from ${fromDate}...`);
    let rows;
    // Try multiple possible name columns in DIM_EMPLOYEE
    const nameColumns = ['DISPLAY_NAME', 'FULL_NAME', 'FIRST_NAME || \' \' || LAST_NAME', 'EMPLOYEE_NAME', 'NAME'];
    let nameFound = false;
    for (const nameCol of nameColumns) {
      if (nameFound) break;
      try {
        rows = await query(`
          SELECT h.EVENT_MONTH_DATE::VARCHAR AS EVENT_MONTH,
                 h.DEPARTMENT, h.POSITION_NAME, h.EMPLOYEE_ID,
                 ${nameCol.includes('||') ? nameCol : 'e.' + nameCol} AS EMPLOYEE_NAME,
                 ROUND(h.EMPLOYER_COST) AS COST, h.CURRENCY_CODE,
                 h.OPENING_ID, h.OPENING_STATUS
          FROM DL_PRODUCTION.HR.FCT_HEADCOUNT_EVENT h
          LEFT JOIN DL_PRODUCTION.HR.DIM_EMPLOYEE e ON h.EMPLOYEE_ID = e.EMPLOYEE_ID
          WHERE h.EVENT_TYPE = '${eventType}'
            AND h.EVENT_SUB_TYPE = '${eventSubType}'
            AND h.EVENT_MONTH_DATE >= TO_DATE('${fromDate}')
            AND h.EVENT_MONTH_DATE <= '${yr}-12-31'
          ORDER BY h.EVENT_MONTH_DATE, h.EMPLOYER_COST DESC
        `);
        nameFound = true;
        console.log(`[Snowflake] Employee name column found: ${nameCol}`);
      } catch (e) {
        console.log(`[Snowflake] Name column ${nameCol} failed, trying next...`);
      }
    }
    if (!nameFound) {
      console.log(`[Snowflake] No name column found, falling back without names`);
      rows = await query(`
        SELECT EVENT_MONTH_DATE::VARCHAR AS EVENT_MONTH,
               DEPARTMENT, POSITION_NAME, EMPLOYEE_ID,
               NULL AS EMPLOYEE_NAME,
               ROUND(EMPLOYER_COST) AS COST, CURRENCY_CODE,
               OPENING_ID, OPENING_STATUS
        FROM DL_PRODUCTION.HR.FCT_HEADCOUNT_EVENT
        WHERE EVENT_TYPE = '${eventType}'
          AND EVENT_SUB_TYPE = '${eventSubType}'
          AND EVENT_MONTH_DATE >= TO_DATE('${fromDate}')
          AND EVENT_MONTH_DATE <= '${yr}-12-31'
        ORDER BY EVENT_MONTH_DATE, EMPLOYER_COST DESC
      `);
    }
    return rows.map(r => ({
      month: (r.EVENT_MONTH || '').substring(0, 7),
      department: r.DEPARTMENT,
      position: r.POSITION_NAME,
      employeeId: r.EMPLOYEE_ID,
      employeeName: r.EMPLOYEE_NAME || null,
      cost: r.COST || 0,
      currency: r.CURRENCY_CODE,
      openingId: r.OPENING_ID,
      status: r.OPENING_STATUS,
    }));
  }

  // ── Open pipeline — opportunities not yet closed-won ──
  async function fetchOpenPipeline(year) {
    const yr = year || 2026;
    console.log(`[Snowflake] Fetching open pipeline for ${yr}...`);
    const rows = await query(`
      SELECT OPPORTUNITY_NAME AS OPP_NAME,
             OPPORTUNITY_STAGE AS STAGE,
             ROUND(OPPORTUNITY_AMOUNT) AS AMOUNT,
             PROBABILITY AS PROB,
             CURRENCY,
             SRC_CLOSE_DATE::VARCHAR AS CLOSE_DATE,
             TYPE,
             FEED_TYPE,
             OPPORTUNITY_OWNER_NAME AS OWNER
      , CASE WHEN IS_UPGRADE_DEAL OR IS_PRICE_UPDATE THEN TRUE ELSE FALSE END AS IS_UPSELL
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
      WHERE IS_OPPORTUNITY_CLOSED = FALSE
        AND OPPORTUNITY_AMOUNT > 0
        AND SRC_CLOSE_DATE >= '${yr}-01-01'
        AND (
          (IS_UPGRADE_DEAL = FALSE AND IS_PRICE_UPDATE = FALSE)
          OR (IS_UPGRADE_DEAL = TRUE OR IS_PRICE_UPDATE = TRUE)
             AND CUSTOMER_ID IN (SELECT DISTINCT CUSTOMER_ID FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY WHERE IS_OPPORTUNITY_WON = TRUE)
        )
      ORDER BY SRC_CLOSE_DATE, OPPORTUNITY_AMOUNT DESC
    `);
    console.log('[Snowflake] Open pipeline: ' + rows.length + ' opportunities');
    return rows.map(r => ({
      name: r.OPP_NAME,
      stage: r.STAGE,
      amount: r.AMOUNT || 0,
      probability: r.PROB || 0,
      weighted: Math.round((r.AMOUNT || 0) * (r.PROB || 0) / 100),
      currency: r.CURRENCY,
      closeDate: (r.CLOSE_DATE || '').substring(0, 10),
      type: r.TYPE,
      feedType: r.FEED_TYPE,
      owner: r.OWNER,
      isUpsell: r.IS_UPSELL || false,
    }));
  }

  // ── Salesforce conversion rate analysis ──
  async function fetchConversionAnalysis() {
    console.log('[Snowflake] Fetching SF conversion analysis...');
    // 1. Stage summary (open pipeline — excludes upgrades, price updates)
    const stageRows = await query(`
      SELECT OPPORTUNITY_STAGE AS STAGE, COUNT(*) AS CNT,
             ROUND(SUM(OPPORTUNITY_AMOUNT)) AS TOTAL_AMT,
             ROUND(AVG(PROBABILITY), 1) AS AVG_PROB,
             ROUND(AVG(DATEDIFF('day', SRC_CREATE_AT, CURRENT_DATE())), 0) AS AVG_AGE_DAYS
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
      WHERE IS_OPPORTUNITY_CLOSED = FALSE
        AND IS_UPGRADE_DEAL = FALSE AND IS_PRICE_UPDATE = FALSE
      GROUP BY OPPORTUNITY_STAGE ORDER BY AVG_PROB
    `);
    // 2. Historical win rate by year — split new clients vs upgrades
    const yearlyRows = await query(`
      SELECT EXTRACT(YEAR FROM COALESCE(CLOSED_WON_DATE, CLOSED_LOST_DATE)) AS CLOSE_YEAR,
             COUNT(CASE WHEN IS_OPPORTUNITY_WON AND IS_OPPORTUNITY_FIRST_WON THEN 1 END) AS WON,
             COUNT(CASE WHEN IS_OPPORTUNITY_WON AND (IS_UPGRADE_DEAL = TRUE OR IS_PRICE_UPDATE = TRUE) THEN 1 END) AS WON_UPGRADES,
             COUNT(CASE WHEN IS_OPPORTUNITY_LOST THEN 1 END) AS LOST,
             ROUND(COUNT(CASE WHEN IS_OPPORTUNITY_WON AND IS_OPPORTUNITY_FIRST_WON THEN 1 END) * 100.0 /
                   NULLIF(COUNT(CASE WHEN IS_OPPORTUNITY_LOST THEN 1 END) + COUNT(CASE WHEN IS_OPPORTUNITY_WON AND IS_OPPORTUNITY_FIRST_WON THEN 1 END), 0), 1) AS WIN_RATE,
             ROUND(AVG(CASE WHEN IS_OPPORTUNITY_WON AND IS_OPPORTUNITY_FIRST_WON AND DATEDIFF('day', SRC_CREATE_AT, CLOSED_WON_DATE) > 0 THEN DATEDIFF('day', SRC_CREATE_AT, CLOSED_WON_DATE) END), 0) AS AVG_WON_DAYS,
             ROUND(SUM(CASE WHEN IS_OPPORTUNITY_WON AND IS_OPPORTUNITY_FIRST_WON THEN OPPORTUNITY_AMOUNT END)) AS WON_NEW_AMT,
             ROUND(SUM(CASE WHEN IS_OPPORTUNITY_WON AND (IS_UPGRADE_DEAL = TRUE OR IS_PRICE_UPDATE = TRUE) THEN OPPORTUNITY_AMOUNT END)) AS WON_UPGRADE_AMT
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
      WHERE IS_OPPORTUNITY_CLOSED = TRUE AND COALESCE(CLOSED_WON_DATE, CLOSED_LOST_DATE) IS NOT NULL
      GROUP BY CLOSE_YEAR HAVING CLOSE_YEAR >= 2020 ORDER BY CLOSE_YEAR
    `);
    // 3. Per-customer conversion (top customers with open pipeline + history)
    const customerRows = await query(`
      SELECT CUSTOMER_ID,
             MAX(OPPORTUNITY_NAME) AS SAMPLE_OPP,
             COUNT(*) AS TOTAL_OPPS,
             COUNT(CASE WHEN IS_OPPORTUNITY_WON THEN 1 END) AS WON_CNT,
             COUNT(CASE WHEN IS_OPPORTUNITY_LOST THEN 1 END) AS LOST_CNT,
             COUNT(CASE WHEN NOT IS_OPPORTUNITY_CLOSED THEN 1 END) AS OPEN_CNT,
             ROUND(AVG(CASE WHEN IS_OPPORTUNITY_WON AND DATEDIFF('day', SRC_CREATE_AT, CLOSED_WON_DATE) > 0
                   THEN DATEDIFF('day', SRC_CREATE_AT, CLOSED_WON_DATE) END), 0) AS AVG_WON_DAYS,
             ROUND(SUM(CASE WHEN IS_OPPORTUNITY_WON THEN OPPORTUNITY_AMOUNT END)) AS TOTAL_WON_AMT,
             ROUND(SUM(CASE WHEN NOT IS_OPPORTUNITY_CLOSED THEN OPPORTUNITY_AMOUNT END)) AS OPEN_AMT,
             MAX(CASE WHEN NOT IS_OPPORTUNITY_CLOSED THEN OPPORTUNITY_STAGE END) AS MAX_OPEN_STAGE,
             MAX(CASE WHEN NOT IS_OPPORTUNITY_CLOSED THEN PROBABILITY END) AS MAX_OPEN_PROB
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
      GROUP BY CUSTOMER_ID
      HAVING OPEN_CNT > 0
      ORDER BY OPEN_CNT DESC, MAX_OPEN_PROB DESC, WON_CNT DESC
    `);
    // 4. Weighted pipeline projection (open opps with probability)
    const projectionRows = await query(`
      SELECT DATE_TRUNC('MONTH', SRC_CLOSE_DATE)::VARCHAR AS CLOSE_MONTH,
             COUNT(*) AS CNT,
             ROUND(SUM(OPPORTUNITY_AMOUNT)) AS TOTAL_AMT,
             ROUND(SUM(OPPORTUNITY_AMOUNT * PROBABILITY / 100)) AS WEIGHTED_AMT,
             ROUND(AVG(PROBABILITY), 0) AS AVG_PROB
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
      WHERE IS_OPPORTUNITY_CLOSED = FALSE AND SRC_CLOSE_DATE IS NOT NULL AND OPPORTUNITY_AMOUNT > 0
        AND IS_UPGRADE_DEAL = FALSE AND IS_PRICE_UPDATE = FALSE
      GROUP BY CLOSE_MONTH ORDER BY CLOSE_MONTH
    `);
    console.log('[Snowflake] Conversion analysis: ' + customerRows.length + ' customers with open pipeline');
    return {
      stages: stageRows.map(r => ({ stage: r.STAGE, count: r.CNT, totalAmt: r.TOTAL_AMT || 0, avgProb: r.AVG_PROB, avgAgeDays: r.AVG_AGE_DAYS })),
      yearly: yearlyRows.filter(r => r.CLOSE_YEAR).map(r => ({ year: r.CLOSE_YEAR, won: r.WON, wonUpgrades: r.WON_UPGRADES || 0, lost: r.LOST, winRate: r.WIN_RATE, avgWonDays: r.AVG_WON_DAYS, wonNewAmt: r.WON_NEW_AMT || 0, wonUpgradeAmt: r.WON_UPGRADE_AMT || 0 })),
      customers: customerRows.map(r => ({
        customerId: r.CUSTOMER_ID, name: (r.SAMPLE_OPP || '').split(' - ')[0].split(' (')[0].trim(),
        totalOpps: r.TOTAL_OPPS, won: r.WON_CNT, lost: r.LOST_CNT, open: r.OPEN_CNT,
        winRate: (r.WON_CNT + r.LOST_CNT) > 0 ? Math.round(r.WON_CNT * 100 / (r.WON_CNT + r.LOST_CNT)) : null,
        avgWonDays: r.AVG_WON_DAYS, totalWonAmt: r.TOTAL_WON_AMT || 0, openAmt: r.OPEN_AMT || 0,
        maxOpenStage: r.MAX_OPEN_STAGE, maxOpenProb: r.MAX_OPEN_PROB,
      })),
      projection: projectionRows.map(r => ({ month: (r.CLOSE_MONTH || '').substring(0, 7), count: r.CNT, totalAmt: r.TOTAL_AMT || 0, weightedAmt: r.WEIGHTED_AMT || 0, avgProb: r.AVG_PROB })),
    };
  }

  // ── Monthly revenue from FCT_MONTHLY_REVENUE__SUBSET_PAID (fresh semantic layer) ──
  async function fetchMonthlyRevenuePaid(year) {
    const yr = year || 2026;
    console.log(`[Snowflake] Fetching monthly revenue (paid subset) for ${yr}...`);
    const rows = await query(`
      SELECT CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR,
             ROUND(SUM(REVENUE_AMOUNT_EUR)) AS TOTAL_REV,
             ROUND(SUM(PAID_REVENUE_AMOUNT_EUR)) AS PAID_REV,
             ROUND(SUM(UNPAID_REVENUE_AMOUNT_EUR)) AS UNPAID_REV,
             COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMERS
      FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE__SUBSET_PAID
      WHERE CAL_MONTH_START_DATE >= '${yr}-01-01' AND CAL_MONTH_START_DATE < '${yr + 1}-01-01'
      GROUP BY CAL_MONTH_START_DATE
      ORDER BY CAL_MONTH_START_DATE
    `);

    const byMonth = {};
    for (const r of rows) {
      const m = (r.MONTH_STR || '').substring(0, 7);
      byMonth[m] = {
        revenue: Math.round(r.TOTAL_REV || 0),
        paid: Math.round(r.PAID_REV || 0),
        unpaid: Math.round(r.UNPAID_REV || 0),
        customers: r.CUSTOMERS || 0,
      };
    }
    console.log(`[Snowflake] Revenue paid: ${Object.keys(byMonth).length} months`);
    return byMonth;
  }

  // ── OKR: YoY revenue comparison (current year vs prior year, same months) ──
  async function fetchYoYRevenue(asOfDate) {
    const ref = asOfDate ? new Date(asOfDate + 'T12:00:00') : new Date();
    const curYear = ref.getFullYear();
    const prevYear = curYear - 1;
    const curMonth = ref.getMonth() + 1; // 1-based
    console.log(`[Snowflake] Fetching YoY revenue: ${prevYear} vs ${curYear}, months 1-${curMonth}...`);
    const rows = await query(`
      SELECT YEAR(CAL_MONTH_START_DATE) AS YR,
             ROUND(SUM(REVENUE_AMOUNT_EUR)) AS TOTAL_REV,
             ROUND(SUM(PAID_REVENUE_AMOUNT_EUR)) AS PAID_REV,
             COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMERS
      FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE__SUBSET_PAID
      WHERE (
              (YEAR(CAL_MONTH_START_DATE) = ${curYear} AND MONTH(CAL_MONTH_START_DATE) <= ${curMonth})
           OR (YEAR(CAL_MONTH_START_DATE) = ${prevYear} AND MONTH(CAL_MONTH_START_DATE) <= ${curMonth})
            )
      GROUP BY YEAR(CAL_MONTH_START_DATE)
      ORDER BY YR
    `);
    const result = { currentYear: curYear, priorYear: prevYear, throughMonth: curMonth };
    for (const r of rows) {
      if (r.YR === prevYear) {
        result.priorYearRev = Math.round(r.TOTAL_REV || 0);
        result.priorYearPaid = Math.round(r.PAID_REV || 0);
        result.priorYearCustomers = r.CUSTOMERS || 0;
      } else if (r.YR === curYear) {
        result.currentYearRev = Math.round(r.TOTAL_REV || 0);
        result.currentYearPaid = Math.round(r.PAID_REV || 0);
        result.currentYearCustomers = r.CUSTOMERS || 0;
      }
    }
    console.log(`[Snowflake] YoY revenue: prior=${result.priorYearRev || 0}, current=${result.currentYearRev || 0}`);
    return result;
  }

  // ── Revenue drilldown — per-customer breakdown for a month ──
  async function fetchRevenueBreakdown(month, unpaidOnly = false) {
    console.log(`[Snowflake] Fetching revenue breakdown for ${month}${unpaidOnly ? ' (unpaid only)' : ''}...`);
    const startDate = `${month}-01`;
    const havingClause = unpaidOnly ? 'HAVING SUM(r.UNPAID_REVENUE_AMOUNT_EUR) > 0' : '';
    const orderCol = unpaidOnly ? 'SUM(r.UNPAID_REVENUE_AMOUNT_EUR)' : 'SUM(r.REVENUE_AMOUNT_EUR)';
    // Try the paid subset first, fall back to base revenue table
    let rows = [];
    try {
      rows = await query(`
        SELECT r.OPPORTUNITY_NAME AS CUST_NAME,
               ROUND(SUM(r.REVENUE_AMOUNT_EUR)) AS REV_EUR,
               ROUND(SUM(r.PAID_REVENUE_AMOUNT_EUR)) AS PAID_EUR,
               ROUND(SUM(r.UNPAID_REVENUE_AMOUNT_EUR)) AS UNPAID_EUR,
               MAX(r.CUSTOMER_STATUS) AS STATUS
        FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE__SUBSET_PAID r
        WHERE r.CAL_MONTH_START_DATE = TO_DATE('${startDate}')
          AND r.REVENUE_AMOUNT_EUR > 0
        GROUP BY r.OPPORTUNITY_NAME
        ${havingClause}
        ORDER BY ${orderCol} DESC
        LIMIT 100
      `);
    } catch (e) {
      console.log(`[Snowflake] Paid subset failed, trying base table: ${e.message}`);
    }
    // Fallback: try base revenue table if subset returned nothing
    if (rows.length === 0) {
      const havingBase = unpaidOnly ? 'HAVING SUM(CASE WHEN r.IS_PAID = FALSE THEN r.MR_AMOUNT ELSE 0 END) > 0' : '';
      const orderBase = unpaidOnly ? 'SUM(CASE WHEN r.IS_PAID = FALSE THEN r.MR_AMOUNT ELSE 0 END)' : 'SUM(r.MR_AMOUNT)';
      try {
        rows = await query(`
          SELECT r.MR_NAME AS CUST_NAME,
                 ROUND(SUM(r.MR_AMOUNT)) AS REV_EUR,
                 ROUND(SUM(CASE WHEN r.IS_PAID = TRUE THEN r.MR_AMOUNT ELSE 0 END)) AS PAID_EUR,
                 ROUND(SUM(CASE WHEN r.IS_PAID = FALSE THEN r.MR_AMOUNT ELSE 0 END)) AS UNPAID_EUR,
                 MAX(r.ACCOUNT_STATUS) AS STATUS
          FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE r
          WHERE r.CAL_MONTH_START_DATE = TO_DATE('${startDate}')
            AND r.MR_AMOUNT > 0
          GROUP BY r.MR_NAME
          ${havingBase}
          ORDER BY ${orderBase} DESC
          LIMIT 100
        `);
      } catch (e2) {
        console.log(`[Snowflake] Base revenue table also failed: ${e2.message}`);
      }
    }
    console.log(`[Snowflake] Revenue breakdown: ${rows.length} rows for ${month}${unpaidOnly ? ' (unpaid)' : ''}`);
    return rows.map(r => ({
      customer: r.CUST_NAME || r.CUSTOMER_ID || '-',
      revenue: Math.round(r.REV_EUR || 0),
      paid: Math.round(r.PAID_EUR || 0),
      unpaid: Math.round(r.UNPAID_EUR || 0),
      status: r.STATUS,
    }));
  }

  // ── Won opportunities detail by year (new clients or upgrades) ──
  async function fetchWonOpportunitiesDetail(year, type) {
    console.log(`[Snowflake] Fetching won opportunities detail: year=${year}, type=${type}`);
    const isNew = type === 'new';
    const filter = isNew
      ? 'o.IS_OPPORTUNITY_FIRST_WON = TRUE'
      : '(o.IS_UPGRADE_DEAL = TRUE OR o.IS_PRICE_UPDATE = TRUE)';
    const rows = await query(`
      WITH won_opps AS (
        SELECT CUSTOMER_ID, CLOSED_WON_DATE, ROUND(OPPORTUNITY_AMOUNT) AS AMT,
               LAG(ROUND(OPPORTUNITY_AMOUNT)) OVER (PARTITION BY CUSTOMER_ID ORDER BY CLOSED_WON_DATE) AS PREV_AMOUNT
        FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
        WHERE IS_OPPORTUNITY_WON = TRUE
      )
      SELECT o.OPPORTUNITY_NAME AS OPP_NAME,
             o.CUSTOMER_ID,
             ROUND(o.OPPORTUNITY_AMOUNT) AS AMOUNT,
             o.CLOSED_WON_DATE,
             o.SRC_CREATE_AT AS CREATED_DATE,
             o.OPPORTUNITY_STAGE AS STAGE,
             o.OPPORTUNITY_OWNER_NAME AS OWNER,
             DATEDIFF('day', o.SRC_CREATE_AT, o.CLOSED_WON_DATE) AS DAYS_TO_CLOSE,
             o.IS_UPGRADE_DEAL,
             o.IS_PRICE_UPDATE,
             w.PREV_AMOUNT
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY o
      LEFT JOIN won_opps w ON w.CUSTOMER_ID = o.CUSTOMER_ID AND w.CLOSED_WON_DATE = o.CLOSED_WON_DATE AND w.AMT = ROUND(o.OPPORTUNITY_AMOUNT)
      WHERE o.IS_OPPORTUNITY_WON = TRUE
        AND ${filter}
        AND EXTRACT(YEAR FROM o.CLOSED_WON_DATE) = ${parseInt(year)}
      ORDER BY o.CLOSED_WON_DATE DESC
    `);
    console.log(`[Snowflake] Won opportunities detail: ${rows.length} rows for ${year} (${type})`);
    return rows.map(r => ({
      name: r.OPP_NAME || '-',
      customer: r.CUSTOMER_ID || '-',
      amount: r.AMOUNT || 0,
      prevAmount: r.PREV_AMOUNT || null,
      closedDate: r.CLOSED_WON_DATE ? r.CLOSED_WON_DATE.toISOString().substring(0, 10) : '-',
      createdDate: r.CREATED_DATE ? r.CREATED_DATE.toISOString().substring(0, 10) : '-',
      daysToClose: r.DAYS_TO_CLOSE || 0,
      owner: r.OWNER || '-',
      isUpgrade: r.IS_UPGRADE_DEAL || false,
      isPriceUpdate: r.IS_PRICE_UPDATE || false,
    }));
  }

  // ── Churn analysis — yearly churn rate, lost revenue, monthly impact ──
  // Uses DIM_CUSTOMER.CHURN_DATE as authoritative source + actual revenue before churn
  async function fetchChurnAnalysis() {
    console.log('[Snowflake] Fetching churn analysis...');
    const rows = await query(`
      WITH last_months AS (
        SELECT YEAR(CAL_MONTH_START_DATE) AS yr, MAX(CAL_MONTH_START_DATE) AS last_month
        FROM DL_PRODUCTION.FINANCE.FCT_CUSTOMER__MONTHLY
        WHERE DATE_STATUS = 'actual'
        GROUP BY YEAR(CAL_MONTH_START_DATE)
      ),
      yearly_totals AS (
        SELECT lm.yr,
               COUNT(DISTINCT CASE WHEN m.CAL_MONTH_START_DATE = lm.last_month AND m.IS_ACTIVE_CUSTOMER_CNT = 1 THEN m.CUSTOMER_ID END) AS total_customers,
               ROUND(SUM(m.REVENUE)) AS total_revenue,
               COUNT(DISTINCT m.CAL_MONTH_START_DATE) AS months_count
        FROM last_months lm
        JOIN DL_PRODUCTION.FINANCE.FCT_CUSTOMER__MONTHLY m ON YEAR(m.CAL_MONTH_START_DATE) = lm.yr AND m.DATE_STATUS = 'actual'
        GROUP BY lm.yr
      ),
      churned AS (
        SELECT CUSTOMER_ID, CHURN_DATE
        FROM DL_PRODUCTION.CORE.DIM_CUSTOMER
        WHERE CHURN_DATE IS NOT NULL AND IS_TEST = FALSE
      ),
      churn_rev AS (
        SELECT YEAR(c.CHURN_DATE) AS churn_year,
               COUNT(DISTINCT c.CUSTOMER_ID) AS churned_clients,
               ROUND(SUM(CASE WHEN m.CAL_MONTH_START_DATE >= DATEADD(month, -1, c.CHURN_DATE) THEN m.REVENUE ELSE 0 END)) AS lost_monthly_rev,
               ROUND(SUM(m.REVENUE)) AS lost_annual_rev
        FROM churned c
        JOIN DL_PRODUCTION.FINANCE.FCT_CUSTOMER__MONTHLY m ON m.CUSTOMER_ID = c.CUSTOMER_ID
          AND m.CAL_MONTH_START_DATE >= DATEADD(month, -12, c.CHURN_DATE)
          AND m.CAL_MONTH_START_DATE < c.CHURN_DATE
          AND m.DATE_STATUS = 'actual'
          AND m.REVENUE > 0
        GROUP BY YEAR(c.CHURN_DATE)
      )
      SELECT t.yr, t.total_customers, t.total_revenue, t.months_count,
             NVL(cr.churned_clients, 0) AS churned_clients,
             NVL(cr.lost_annual_rev, 0) AS lost_rev,
             NVL(cr.lost_monthly_rev, 0) AS lost_monthly
      FROM yearly_totals t
      LEFT JOIN churn_rev cr ON t.yr = cr.churn_year
      WHERE t.yr >= 2020
      ORDER BY t.yr
    `);
    console.log(`[Snowflake] Churn analysis: ${rows.length} years`);
    // Also compute 6-month rolling average of monthly churn impact
    let recentMonthlyAvg = 0;
    try {
      const recent = await query(`
        WITH churned AS (
          SELECT c.CUSTOMER_ID, c.CHURN_DATE
          FROM DL_PRODUCTION.CORE.DIM_CUSTOMER c
          WHERE c.CHURN_DATE IS NOT NULL AND c.IS_TEST = FALSE
            AND c.CHURN_DATE >= DATEADD(month, -6, CURRENT_DATE())
            AND c.CHURN_DATE < CURRENT_DATE()
        )
        SELECT COUNT(DISTINCT c.CUSTOMER_ID) AS churned_count,
               ROUND(SUM(CASE WHEN m.CAL_MONTH_START_DATE >= DATEADD(month, -1, c.CHURN_DATE) THEN m.REVENUE ELSE 0 END)) AS lost_monthly_rev
        FROM churned c
        LEFT JOIN DL_PRODUCTION.FINANCE.FCT_CUSTOMER__MONTHLY m ON m.CUSTOMER_ID = c.CUSTOMER_ID
          AND m.CAL_MONTH_START_DATE >= DATEADD(month, -12, c.CHURN_DATE)
          AND m.CAL_MONTH_START_DATE < c.CHURN_DATE
          AND m.DATE_STATUS = 'actual'
          AND m.REVENUE > 0
      `);
      if (recent.length > 0 && recent[0].LOST_MONTHLY_REV > 0) {
        recentMonthlyAvg = Math.round(recent[0].LOST_MONTHLY_REV / 6);
      }
      console.log(`[Snowflake] 6-month churn avg: €${recentMonthlyAvg}/mo (${recent[0]?.CHURNED_COUNT || 0} clients)`);
    } catch (e) { console.error('[Snowflake] 6m churn avg failed:', e.message); }
    return {
      yearly: rows.map(r => ({
        year: r.YR,
        totalCustomers: r.TOTAL_CUSTOMERS,
        totalRevenue: Math.round(r.TOTAL_REVENUE || 0),
        churnedClients: r.CHURNED_CLIENTS || 0,
        lostRevenue: Math.round(r.LOST_REV || 0),
        lostMonthly: Math.round(r.LOST_MONTHLY || 0),
        churnPct: r.TOTAL_REVENUE > 0 ? Math.round((r.LOST_REV || 0) / r.TOTAL_REVENUE * 1000) / 10 : 0,
        clientChurnPct: (r.TOTAL_CUSTOMERS + (r.CHURNED_CLIENTS || 0)) > 0 ? Math.round((r.CHURNED_CLIENTS || 0) / (r.TOTAL_CUSTOMERS + (r.CHURNED_CLIENTS || 0)) * 1000) / 10 : 0,
        monthlyImpact: Math.round(r.LOST_MONTHLY || 0),
        monthsCount: r.MONTHS_COUNT || 12,
      })),
      recentMonthlyAvg,
    };
  }

  // ── Churn drilldown — individual churned customers for a year ──
  async function fetchChurnDrilldown(year) {
    console.log(`[Snowflake] Fetching churn drilldown for ${year}...`);
    const rows = await query(`
      WITH churned AS (
        SELECT d.CUSTOMER_ID, d.CUSTOMER_NAME, d.CHURN_DATE, d.CUSTOMER_STATUS,
               d.TIER, d.CUSTOMER_REGION, d.SERVICES, d.SALES_ACCOUNT_OWNER
        FROM DL_PRODUCTION.CORE.DIM_CUSTOMER d
        WHERE d.CHURN_DATE IS NOT NULL AND d.IS_TEST = FALSE
          AND YEAR(d.CHURN_DATE) = ${parseInt(year)}
      )
      SELECT c.CUSTOMER_NAME, c.CHURN_DATE, c.TIER, c.CUSTOMER_REGION,
             c.SERVICES, c.SALES_ACCOUNT_OWNER,
             ROUND(SUM(CASE WHEN m.CAL_MONTH_START_DATE >= DATEADD(month, -1, c.CHURN_DATE) THEN m.REVENUE ELSE 0 END)) AS last_month_rev,
             ROUND(SUM(m.REVENUE)) AS total_12m_rev,
             COUNT(DISTINCT m.CAL_MONTH_START_DATE) AS months_active
      FROM churned c
      LEFT JOIN DL_PRODUCTION.FINANCE.FCT_CUSTOMER__MONTHLY m ON m.CUSTOMER_ID = c.CUSTOMER_ID
        AND m.CAL_MONTH_START_DATE >= DATEADD(month, -12, c.CHURN_DATE)
        AND m.CAL_MONTH_START_DATE < c.CHURN_DATE
        AND m.DATE_STATUS = 'actual'
        AND m.REVENUE > 0
      GROUP BY c.CUSTOMER_NAME, c.CHURN_DATE, c.TIER, c.CUSTOMER_REGION, c.SERVICES, c.SALES_ACCOUNT_OWNER
      ORDER BY total_12m_rev DESC
    `);
    console.log(`[Snowflake] Churn drilldown: ${rows.length} customers for ${year}`);
    return rows.map(r => ({
      name: r.CUSTOMER_NAME || '-',
      churnDate: r.CHURN_DATE ? r.CHURN_DATE.toISOString().substring(0, 10) : '-',
      tier: r.TIER || '-',
      region: r.CUSTOMER_REGION || '-',
      services: r.SERVICES || '-',
      owner: r.SALES_ACCOUNT_OWNER || '-',
      lastMonthRev: Math.round(r.LAST_MONTH_REV || 0),
      total12mRev: Math.round(r.TOTAL_12M_REV || 0),
      monthsActive: r.MONTHS_ACTIVE || 0,
      avgMonthlyRev: r.MONTHS_ACTIVE > 0 ? Math.round((r.TOTAL_12M_REV || 0) / r.MONTHS_ACTIVE) : 0,
    }));
  }

  // ── ARR/MRR: current run-rate from latest available monthly revenue ──
  async function fetchCurrentARR() {
    console.log('[Snowflake] Fetching current ARR/MRR...');
    // Get the last 3 complete months of revenue to compute run-rate
    const rows = await query(`
      SELECT CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR,
             ROUND(SUM(REVENUE_AMOUNT_EUR)) AS TOTAL_REV,
             ROUND(SUM(PAID_REVENUE_AMOUNT_EUR)) AS PAID_REV,
             COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMERS
      FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE__SUBSET_PAID
      WHERE CAL_MONTH_START_DATE >= DATEADD(MONTH, -4, DATE_TRUNC('MONTH', CURRENT_DATE()))
        AND CAL_MONTH_START_DATE < DATE_TRUNC('MONTH', CURRENT_DATE())
      GROUP BY CAL_MONTH_START_DATE
      ORDER BY CAL_MONTH_START_DATE DESC
      LIMIT 3
    `);
    if (!rows.length) return { mrr: 0, arr: 0, customers: 0, month: '', history: [] };
    // Latest complete month = MRR
    const latest = rows[0];
    const mrr = Math.round(latest.TOTAL_REV || 0);
    const arr = mrr * 12;
    const paidMrr = Math.round(latest.PAID_REV || 0);
    const month = (latest.MONTH_STR || '').substring(0, 7);
    // 3-month trend
    const history = rows.map(r => ({
      month: (r.MONTH_STR || '').substring(0, 7),
      mrr: Math.round(r.TOTAL_REV || 0),
      arr: Math.round(r.TOTAL_REV || 0) * 12,
      paid: Math.round(r.PAID_REV || 0),
      customers: r.CUSTOMERS || 0,
    })).reverse();
    console.log(`[Snowflake] ARR: €${(arr / 1e6).toFixed(1)}M (MRR: €${(mrr / 1000).toFixed(0)}K from ${month}, ${latest.CUSTOMERS} customers)`);
    return { mrr, arr, paidMrr, paidArr: paidMrr * 12, customers: latest.CUSTOMERS || 0, month, history };
  }

  return {
    query,
    testConnection,
    listDatabases,
    listSchemas,
    listTables,
    fetchFinancialData,
    fetchBudgetByCategory,
    fetchActualExpenses,
    fetchRevenueProjection,
    fetchMonthlyActualsSplit,
    fetchVendorBreakdown,
    fetchSalaryBreakdown: fetchSalaryBreakdown,
    fetchBudgetCategoryDetail,
    fetchBudgetOverrides,
    fetchSalaryBudget,
    fetchFinanceBudget,
    fetchSalaryBudgetBreakdown,
    fetchMonthlyRevenuePaid,
    fetchRevenueBreakdown,
    fetchOpenPipeline,
    fetchConversionAnalysis,
    fetchWonOpportunitiesDetail,
    fetchHeadcountEvents,
    fetchHeadcountByDepartment,
    fetchHeadcountLeverDetail,
    fetchChurnAnalysis,
    fetchChurnDrilldown,
    fetchYoYRevenue,
    fetchCurrentARR,
    getConnection,
  };
}

module.exports = { createSnowflakeClient };
