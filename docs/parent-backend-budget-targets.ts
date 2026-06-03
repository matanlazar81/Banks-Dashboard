// ============================================================================
// Paste this whole block into finance-it/backend/src/routes/bankDashboardApi.ts
// just BEFORE the final `export default router;` line.
//
// It adds five endpoints backed by PostgreSQL (the parent backend's `pool`):
//   GET  /api/whoami                   — current user + canSync flag
//   POST /api/sync-budget-targets      — Snowflake → Postgres refresh (gated)
//   GET  /api/budget-targets           — list rows for a (year, subsidiary)
//   PUT  /api/budget-targets           — set USER_OVERRIDE_AMOUNT_ILS / _PCT
//   GET  /api/budget-target-edits      — audit log polling for notifications
//
// All endpoints reuse the existing `bankRole` middleware and `snowflakeService.getClient()`.
// Tables are created on module load via `ensureBudgetTablesExist()` (idempotent).
//
// After pasting:
//   1. npm run build      (tsc)
//   2. restart the backend (pm2 / systemctl, however you run it)
//   3. curl -isk 'https://finance-it.lsports.eu/business-tools/bank-dashboard/api/whoami'
//      → JSON like {"email":"matan.l@lsports.eu","canSync":true}
// ============================================================================

// ── helpers ────────────────────────────────────────────────────────────────

// SYNC_ALLOWLIST env var controls who can trigger Snowflake → Postgres refresh.
// Default: matan.l@lsports.eu. Comma-separated for multiple admins.
function getSyncAllowlist(): string[] {
  return (process.env.SYNC_ALLOWLIST || 'matan.l@lsports.eu')
    .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
}

// Adjust this if your auth middleware puts the email under a different key.
function getCallerEmail(req: any): string {
  return (req.user?.email || req.user?.preferred_username || req.user?.upn || '')
    .toString().trim().toLowerCase();
}

function canUserSync(email: string): boolean {
  if (!email) return false;
  return getSyncAllowlist().includes(email);
}

// Create the tables on first use (idempotent). Kicks off at module load so the
// first request doesn't pay the migration latency.
let budgetTablesReady: Promise<void> | null = null;
function ensureBudgetTablesExist(): Promise<void> {
  if (!budgetTablesReady) {
    budgetTablesReady = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS budget_target_by_dept_acct (
          fiscal_year                  INTEGER NOT NULL,
          subsidiary_id                INTEGER NOT NULL,
          department                   TEXT    NOT NULL DEFAULT 'Unassigned',
          location                     TEXT    NOT NULL DEFAULT 'Unassigned',
          account_number               TEXT    NOT NULL,
          currency                     TEXT    NOT NULL DEFAULT 'ILS',
          account_name                 TEXT,
          netsuite_internal_number     BIGINT,
          source_amount_ils            NUMERIC(18, 2),
          user_override_amount_ils     NUMERIC(18, 2),
          user_override_pct            NUMERIC(8, 4),
          user_edited_by               TEXT,
          user_edited_at               TIMESTAMPTZ,
          source_synced_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          loaded_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          annual_budget_target_amount  NUMERIC(18, 2) GENERATED ALWAYS AS (
            COALESCE(
              user_override_amount_ils,
              source_amount_ils * (1 + COALESCE(user_override_pct, 0) / 100.0),
              source_amount_ils
            )
          ) STORED,
          PRIMARY KEY (fiscal_year, subsidiary_id, department, location, account_number, currency)
        );
      `);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_budget_target_year_sub ON budget_target_by_dept_acct (fiscal_year, subsidiary_id);`);
      // Idempotent migration: add monthly breakdown column if the table predates it.
      await pool.query(`ALTER TABLE budget_target_by_dept_acct ADD COLUMN IF NOT EXISTS monthly_source_ils JSONB;`);
      await pool.query(`
        CREATE TABLE IF NOT EXISTS budget_target_edit_log (
          id              BIGSERIAL PRIMARY KEY,
          edited_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          edited_by       TEXT NOT NULL,
          fiscal_year     INTEGER NOT NULL,
          subsidiary_id   INTEGER NOT NULL,
          department      TEXT,
          location        TEXT,
          account_number  TEXT,
          currency        TEXT,
          field_name      TEXT NOT NULL,
          old_value       TEXT,
          new_value       TEXT
        );
      `);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_edit_log_at ON budget_target_edit_log (edited_at);`);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_edit_log_scope ON budget_target_edit_log (fiscal_year, subsidiary_id, edited_at);`);
    })();
  }
  return budgetTablesReady;
}
ensureBudgetTablesExist().catch(e => logger.error('ensureBudgetTablesExist failed', e));

// Pull Snowflake FCT_BUDGET → Postgres budget_target_by_dept_acct.
// Layer 1: raw FCT_BUDGET monthly values.
// Layer 2: FCT_EXPENSE overrides (future_cost_override = replace,
//          future_cost_increment = add).
// Layer 3: caller-supplied scenario adjustments — vendorCatAdj (non-payroll)
//          and salaryDeptAdj (payroll), each shaped as { "YYYY-MM": { key: pct } }.
// Idempotent: preserves user_override_* columns; deletes only rows that didn't
// appear in this sync AND have no user override.
async function populateBudgetTargets(opts: {
  subsidiary: number;
  years: number[];
  scenarioAdj?: {
    vendorCatAdj?: Record<string, Record<string, number>>;
    salaryDeptAdj?: Record<string, Record<string, number>>;
  };
}) {
  const { subsidiary, years } = opts;
  const scenarioAdj = opts.scenarioAdj || {};
  const vendorCatAdj = scenarioAdj.vendorCatAdj || {};
  const salaryDeptAdj = scenarioAdj.salaryDeptAdj || {};
  const sf: any = snowflakeService.getClient();
  if (!sf) throw new Error('Snowflake client not configured');

  const yearStart = Math.min(...years);
  const yearEnd = Math.max(...years);

  const budgetColsRows: any[] = await sf.query(`
    SELECT COLUMN_NAME FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'FCT_BUDGET'
  `);
  const budgetCols = new Set<string>(budgetColsRows.map(r => r.COLUMN_NAME));
  const hasLocationId = budgetCols.has('LOCATION_ID');
  const hasCurrencyCode = budgetCols.has('CURRENCY_CODE');
  let hasDimLocation = false;
  if (hasLocationId) {
    const dimRows: any[] = await sf.query(`
      SELECT COLUMN_NAME FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'DIM_LOCATION'
    `);
    hasDimLocation = new Set(dimRows.map(r => r.COLUMN_NAME)).has('LOCATION_NAME');
  }

  // Fetch at monthly grain. Includes CATEGORY (PARENT_GL_ACCOUNT_NAME),
  // IS_PAYROLL and DEPARTMENT_ID so we can apply overrides and scenario adjustments.
  const sfSql = `
    SELECT
      EXTRACT(YEAR  FROM b.BUDGET_MONTH_DATE) AS FISCAL_YEAR,
      EXTRACT(MONTH FROM b.BUDGET_MONTH_DATE) AS MONTH_NUM,
      b.DEPARTMENT_ID    AS DEPARTMENT_ID,
      COALESCE(d.DEPARTMENT_NAME, 'Unassigned') AS DEPARTMENT,
      ${hasLocationId && hasDimLocation ? `COALESCE(l.LOCATION_NAME, 'Unassigned')` : `'Unassigned'`} AS LOCATION,
      ${hasCurrencyCode ? `COALESCE(b.CURRENCY_CODE, 'ILS')` : `'ILS'`} AS CURRENCY,
      g.GL_ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
      g.GL_ACCOUNT_NAME   AS ACCOUNT_NAME,
      g.GL_ACCOUNT_ID     AS NETSUITE_INTERNAL_NUMBER,
      g.PARENT_GL_ACCOUNT_NAME AS CATEGORY,
      g.IS_PAYROLL        AS IS_PAYROLL,
      ROUND(SUM(b.AMOUNT_ILS_CC), 2) AS MONTH_AMOUNT_ILS,
      b.SUBSIDIARY_ID     AS SUBSIDIARY_ID
    FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
    JOIN      DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT  g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
    LEFT JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT  d ON b.DEPARTMENT_ID = d.DEPARTMENT_ID
    ${hasLocationId && hasDimLocation ? `LEFT JOIN DL_PRODUCTION.FINANCE.DIM_LOCATION l ON b.LOCATION_ID = l.LOCATION_ID` : ''}
    WHERE b.SUBSIDIARY_ID = ${subsidiary}
      AND b.BUDGET_MONTH_DATE >= '${yearStart}-01-01'
      AND b.BUDGET_MONTH_DATE <= '${yearEnd}-12-31'
      AND EXTRACT(YEAR FROM b.BUDGET_MONTH_DATE) IN (${years.join(',')})
    GROUP BY
      EXTRACT(YEAR  FROM b.BUDGET_MONTH_DATE),
      EXTRACT(MONTH FROM b.BUDGET_MONTH_DATE),
      b.DEPARTMENT_ID, d.DEPARTMENT_NAME,
      ${hasLocationId && hasDimLocation ? 'l.LOCATION_NAME,' : ''}
      ${hasCurrencyCode ? 'b.CURRENCY_CODE,' : ''}
      g.GL_ACCOUNT_NUMBER, g.GL_ACCOUNT_NAME, g.GL_ACCOUNT_ID,
      g.PARENT_GL_ACCOUNT_NAME, g.IS_PAYROLL,
      b.SUBSIDIARY_ID
  `;

  const t0 = Date.now();
  const monthRows: any[] = await sf.query(sfSql);

  // Layer 2: FCT_EXPENSE overrides. Match on (year-month, department_id, account_number).
  // Defensive: any failure here (missing columns, query errors) skips overrides
  // so Sync still produces Layer 1 + Layer 3 figures rather than blowing up.
  const overrideIndex = new Map<string, { amount: number; mode: 'Override' | 'Increment' }>();
  try {
    const expenseColsRows: any[] = await sf.query(`
      SELECT COLUMN_NAME FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'FCT_EXPENSE'
    `);
    const expenseCols = new Set<string>(expenseColsRows.map(r => r.COLUMN_NAME));
    // Pick the best available amount column. FCT_EXPENSE column naming differs across
    // environments — try the *_CC currency-converted ones first, then the plain names.
    let amountExpr: string | null = null;
    if (expenseCols.has('AMOUNT_ILS_CC'))      amountExpr = 'ROUND(e.AMOUNT_ILS_CC, 2)';
    else if (expenseCols.has('AMOUNT_ILS'))    amountExpr = 'ROUND(e.AMOUNT_ILS, 2)';
    else if (expenseCols.has('AMOUNT_EUR_CC')) amountExpr = 'ROUND(e.AMOUNT_EUR_CC * 3.68, 2)';
    else if (expenseCols.has('AMOUNT_EUR'))    amountExpr = 'ROUND(e.AMOUNT_EUR * 3.68, 2)';

    if (amountExpr) {
      const overrideRows: any[] = await sf.query(`
        SELECT
          e.DEPARTMENT_ID,
          g.GL_ACCOUNT_NUMBER,
          e.CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR,
          ${amountExpr} AS AMOUNT_ILS,
          e.SOURCE
        FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE e
        JOIN DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT g ON e.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
        WHERE e.SOURCE IN ('future_cost_override', 'future_cost_increment')
          AND e.SUBSIDIARY_ID = ${subsidiary}
          AND EXTRACT(YEAR FROM e.CAL_MONTH_START_DATE) IN (${years.join(',')})
      `);
      // Index overrides by composite key: "YYYY-MM|dept_id|account_number".
      for (const ov of overrideRows) {
        const mKey = (ov.MONTH_STR || '').substring(0, 7); // "2026-07"
        if (!mKey) continue;
        const key = `${mKey}|${ov.DEPARTMENT_ID}|${ov.GL_ACCOUNT_NUMBER}`;
        overrideIndex.set(key, {
          amount: Number(ov.AMOUNT_ILS) || 0,
          mode: ov.SOURCE === 'future_cost_override' ? 'Override' : 'Increment',
        });
      }
    }
  } catch (overrideErr: any) {
    // Don't block Sync if Layer 2 fails — log and continue with Layer 1 (+ Layer 3) only.
    logger.warn?.(`Layer 2 (FCT_EXPENSE overrides) skipped: ${overrideErr?.message || overrideErr}`);
  }

  const elapsedMs = Date.now() - t0;

  const MONTH_KEYS_LIST = ['01','02','03','04','05','06','07','08','09','10','11','12'] as const;

  // Group monthly rows into one entry per (year, subsidiary, dept, location, account, currency).
  type Aggregated = {
    FISCAL_YEAR: number; SUBSIDIARY_ID: number; DEPARTMENT: string; LOCATION: string;
    CURRENCY: string; ACCOUNT_NUMBER: string; ACCOUNT_NAME: string | null;
    NETSUITE_INTERNAL_NUMBER: number | null;
    CATEGORY: string | null; IS_PAYROLL: boolean;
    annual: number; monthly: Record<string, number>;
  };
  const groups = new Map<string, Aggregated>();
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
        IS_PAYROLL: r.IS_PAYROLL === true || r.IS_PAYROLL === 'true' || r.IS_PAYROLL === 1,
        annual: 0,
        monthly: {},
      };
      groups.set(key, g);
    }
    let amt = r.MONTH_AMOUNT_ILS != null ? Number(r.MONTH_AMOUNT_ILS) : 0;
    const mNum = Number(r.MONTH_NUM);
    const mkey = String(mNum).padStart(2, '0'); // "01" .. "12"
    const ymKey = `${r.FISCAL_YEAR}-${mkey}`;

    // Layer 2: FCT_EXPENSE override at (ym, dept_id, account).
    const ovKey = `${ymKey}|${r.DEPARTMENT_ID}|${r.ACCOUNT_NUMBER}`;
    const ov = overrideIndex.get(ovKey);
    if (ov) {
      amt = ov.mode === 'Override' ? ov.amount : amt + ov.amount;
    }

    // Layer 3: scenario adjustment. Percent of the post-override monthly amount.
    //  - payroll rows  → salaryDeptAdj[ym][department_name]
    //  - non-payroll  → vendorCatAdj[ym][category]
    let scenarioPct = 0;
    if (g.IS_PAYROLL) {
      scenarioPct = salaryDeptAdj[ymKey]?.[g.DEPARTMENT] || 0;
    } else if (g.CATEGORY) {
      scenarioPct = vendorCatAdj[ymKey]?.[g.CATEGORY] || 0;
    }
    if (scenarioPct) amt = amt * (1 + scenarioPct / 100);

    g.monthly[mkey] = (g.monthly[mkey] || 0) + amt;
    g.annual += amt;
  }

  // Drop rows whose annual is effectively zero (matches the prior HAVING > 0 filter).
  let rowsToWrite = [...groups.values()].filter(g => Math.abs(g.annual) > 0);
  let fallbackUsedFromYear: number | null = null;

  // Fallback for years not yet in Snowflake (e.g. FY2027 before FP&A loads it):
  // if a requested year produced no rows, try copying year-1's Postgres rows.
  // Scenario adjustments (Layer 3) are then applied with the TARGET year's keys.
  for (const targetYear of years) {
    if (rowsToWrite.some(r => r.FISCAL_YEAR === targetYear)) continue; // got data from Snowflake
    const sourceYear = targetYear - 1;
    const prev = await pool.query(
      `SELECT department, location, currency, account_number, account_name,
              netsuite_internal_number, source_amount_ils, monthly_source_ils, subsidiary_id
       FROM budget_target_by_dept_acct
       WHERE fiscal_year = $1 AND subsidiary_id = $2`,
      [sourceYear, subsidiary]
    );
    if (prev.rows.length === 0) continue;
    fallbackUsedFromYear = sourceYear;

    // Re-query account category / payroll flag so Layer 3 can apply.
    const accountIds = [...new Set(prev.rows.map((r: any) => r.netsuite_internal_number).filter((x: any) => x != null))];
    const accountMeta = new Map<number, { category: string | null; isPayroll: boolean }>();
    if (accountIds.length > 0) {
      try {
        const metaRows: any[] = await sf.query(`
          SELECT GL_ACCOUNT_ID, PARENT_GL_ACCOUNT_NAME AS CATEGORY, IS_PAYROLL
          FROM DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT
          WHERE GL_ACCOUNT_ID IN (${accountIds.join(',')})
        `);
        for (const m of metaRows) {
          accountMeta.set(Number(m.GL_ACCOUNT_ID), {
            category: m.CATEGORY || null,
            isPayroll: m.IS_PAYROLL === true || m.IS_PAYROLL === 'true' || m.IS_PAYROLL === 1,
          });
        }
      } catch (e: any) {
        logger.warn?.(`Account metadata lookup failed in fallback: ${e?.message || e}`);
      }
    }

    for (const r of prev.rows) {
      const meta = accountMeta.get(Number(r.netsuite_internal_number)) || { category: null, isPayroll: false };
      const monthlyRaw: Record<string, any> = typeof r.monthly_source_ils === 'string'
        ? (() => { try { return JSON.parse(r.monthly_source_ils); } catch { return {}; } })()
        : (r.monthly_source_ils || {});
      const adjustedMonthly: Record<string, number> = {};
      let annual = 0;
      for (const mkey of MONTH_KEYS_LIST) {
        let amt = Number(monthlyRaw[mkey]) || 0;
        const ymKey = `${targetYear}-${mkey}`;
        let scenarioPct = 0;
        if (meta.isPayroll) scenarioPct = salaryDeptAdj[ymKey]?.[r.department] || 0;
        else if (meta.category) scenarioPct = vendorCatAdj[ymKey]?.[meta.category] || 0;
        if (scenarioPct) amt = amt * (1 + scenarioPct / 100);
        if (amt !== 0) adjustedMonthly[mkey] = amt;
        annual += amt;
      }
      if (Math.abs(annual) === 0) continue;
      rowsToWrite.push({
        FISCAL_YEAR: targetYear,
        SUBSIDIARY_ID: Number(r.subsidiary_id),
        DEPARTMENT: r.department,
        LOCATION: r.location,
        CURRENCY: r.currency,
        ACCOUNT_NUMBER: r.account_number,
        ACCOUNT_NAME: r.account_name,
        NETSUITE_INTERNAL_NUMBER: r.netsuite_internal_number != null ? Number(r.netsuite_internal_number) : null,
        CATEGORY: meta.category,
        IS_PAYROLL: meta.isPayroll,
        annual,
        monthly: adjustedMonthly,
      });
    }
  }

  const syncStart = new Date();
  let deletedOrphans = 0;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const g of rowsToWrite) {
      await client.query(
        `INSERT INTO budget_target_by_dept_acct
         (fiscal_year, department, location, currency, account_number, account_name,
          netsuite_internal_number, source_amount_ils, monthly_source_ils, subsidiary_id, source_synced_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
         ON CONFLICT (fiscal_year, subsidiary_id, department, location, account_number, currency)
         DO UPDATE SET
           account_name = EXCLUDED.account_name,
           netsuite_internal_number = EXCLUDED.netsuite_internal_number,
           source_amount_ils = EXCLUDED.source_amount_ils,
           monthly_source_ils = EXCLUDED.monthly_source_ils,
           source_synced_at = EXCLUDED.source_synced_at`,
        [
          g.FISCAL_YEAR,
          g.DEPARTMENT,
          g.LOCATION,
          g.CURRENCY,
          g.ACCOUNT_NUMBER,
          g.ACCOUNT_NAME,
          g.NETSUITE_INTERNAL_NUMBER,
          Math.round(g.annual * 100) / 100,
          JSON.stringify(g.monthly),
          g.SUBSIDIARY_ID,
          syncStart,
        ]
      );
    }
    for (const yr of years) {
      const del = await client.query(
        `DELETE FROM budget_target_by_dept_acct
         WHERE subsidiary_id=$1 AND fiscal_year=$2
           AND source_synced_at < $3
           AND user_override_amount_ils IS NULL
           AND user_override_pct IS NULL`,
        [subsidiary, yr, syncStart]
      );
      deletedOrphans += del.rowCount || 0;
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }

  const summary = await pool.query(
    `SELECT fiscal_year,
            COUNT(*)::int AS row_count,
            ROUND(SUM(annual_budget_target_amount), 2) AS total_ils,
            SUM(CASE WHEN user_override_amount_ils IS NOT NULL OR user_override_pct IS NOT NULL THEN 1 ELSE 0 END)::int AS override_count
     FROM budget_target_by_dept_acct
     WHERE subsidiary_id = $1 AND fiscal_year = ANY($2::int[])
     GROUP BY fiscal_year ORDER BY fiscal_year`,
    [subsidiary, years]
  );

  return {
    subsidiary,
    fiscalYears: years,
    rowCount: rowsToWrite.length,
    elapsedMs,
    deletedOrphans,
    fallbackFromYear: fallbackUsedFromYear,
    summary: summary.rows.map((s: any) => ({
      fiscalYear: Number(s.fiscal_year),
      rowCount: Number(s.row_count),
      totalIls: Number(s.total_ils),
      preservedOverrides: Number(s.override_count),
    })),
  };
}

// ── routes ─────────────────────────────────────────────────────────────────

router.get('/whoami', bankRole, (req: any, res: Response) => {
  const email = getCallerEmail(req);
  res.json({ email, canSync: canUserSync(email) });
});

router.post('/sync-budget-targets', bankRole, async (req: any, res: Response) => {
  const email = getCallerEmail(req);
  if (!canUserSync(email)) {
    return res.status(403).json({ ok: false, error: 'Not authorized to sync. Contact the dashboard owner.' });
  }
  try {
    const subsidiary = parseInt((req.query.subsidiary as string) || '3', 10);
    const yearsParam = ((req.query.year as string) || (req.query.years as string) || '');
    const years = yearsParam.split(',').map(y => parseInt(y.trim(), 10)).filter(Number.isFinite);
    if (years.length === 0) {
      return res.status(400).json({ ok: false, error: 'year query param required, e.g. ?year=2026' });
    }
    // Optional Layer 3 scenario adjustments from the caller (frontend extracts
    // these from the active scenario's vendorCatAdj / salaryDeptAdj).
    const body = (req.body || {}) as any;
    const scenarioAdj = {
      vendorCatAdj: body.vendorCatAdj && typeof body.vendorCatAdj === 'object' ? body.vendorCatAdj : undefined,
      salaryDeptAdj: body.salaryDeptAdj && typeof body.salaryDeptAdj === 'object' ? body.salaryDeptAdj : undefined,
    };
    await ensureBudgetTablesExist();
    const result = await populateBudgetTargets({ subsidiary, years, scenarioAdj });
    res.json({ ok: true, triggeredBy: email, ...result });
  } catch (e: any) {
    logger.error('sync-budget-targets failed', e);
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

router.get('/budget-targets', bankRole, async (req: any, res: Response) => {
  try {
    await ensureBudgetTablesExist();
    const year = parseInt((req.query.year as string) || '0', 10);
    const subsidiary = parseInt((req.query.subsidiary as string) || '3', 10);
    if (!year) return res.status(400).json({ ok: false, error: 'year required' });
    const result = await pool.query(
      `SELECT fiscal_year AS "FISCAL_YEAR", department AS "DEPARTMENT", location AS "LOCATION",
              currency AS "CURRENCY", account_number AS "ACCOUNT_NUMBER", account_name AS "ACCOUNT_NAME",
              netsuite_internal_number AS "NETSUITE_INTERNAL_NUMBER",
              source_amount_ils AS "SOURCE_AMOUNT_ILS",
              monthly_source_ils AS "MONTHLY_SOURCE_ILS",
              user_override_amount_ils AS "USER_OVERRIDE_AMOUNT_ILS",
              user_override_pct AS "USER_OVERRIDE_PCT",
              annual_budget_target_amount AS "ANNUAL_BUDGET_TARGET_AMOUNT",
              subsidiary_id AS "SUBSIDIARY_ID",
              user_edited_by AS "USER_EDITED_BY",
              user_edited_at AS "USER_EDITED_AT",
              source_synced_at AS "SOURCE_SYNCED_AT"
       FROM budget_target_by_dept_acct
       WHERE fiscal_year = $1 AND subsidiary_id = $2
       ORDER BY department, account_number`,
      [year, subsidiary]
    );
    const rows = result.rows.map((r: any) => ({
      ...r,
      USER_EDITED_AT: r.USER_EDITED_AT instanceof Date ? r.USER_EDITED_AT.toISOString() : r.USER_EDITED_AT,
      SOURCE_SYNCED_AT: r.SOURCE_SYNCED_AT instanceof Date ? r.SOURCE_SYNCED_AT.toISOString() : r.SOURCE_SYNCED_AT,
      SOURCE_AMOUNT_ILS: r.SOURCE_AMOUNT_ILS != null ? Number(r.SOURCE_AMOUNT_ILS) : null,
      MONTHLY_SOURCE_ILS: r.MONTHLY_SOURCE_ILS || null, // JSONB returns parsed object from node-postgres
      USER_OVERRIDE_AMOUNT_ILS: r.USER_OVERRIDE_AMOUNT_ILS != null ? Number(r.USER_OVERRIDE_AMOUNT_ILS) : null,
      USER_OVERRIDE_PCT: r.USER_OVERRIDE_PCT != null ? Number(r.USER_OVERRIDE_PCT) : null,
      ANNUAL_BUDGET_TARGET_AMOUNT: r.ANNUAL_BUDGET_TARGET_AMOUNT != null ? Number(r.ANNUAL_BUDGET_TARGET_AMOUNT) : null,
    }));
    res.json({ ok: true, rows });
  } catch (e: any) {
    logger.error('GET /budget-targets failed', e);
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

router.put('/budget-targets', bankRole, async (req: any, res: Response) => {
  try {
    await ensureBudgetTablesExist();
    const email = getCallerEmail(req);
    if (!email) return res.status(401).json({ ok: false, error: 'Not authenticated' });
    const { fiscalYear, subsidiaryId, department, location, accountNumber, currency } = req.body || {};
    if (!fiscalYear || !subsidiaryId || !accountNumber) {
      return res.status(400).json({ ok: false, error: 'fiscalYear, subsidiaryId, accountNumber required' });
    }
    const cur = await pool.query(
      `SELECT user_override_amount_ils AS "USER_OVERRIDE_AMOUNT_ILS",
              user_override_pct AS "USER_OVERRIDE_PCT"
       FROM budget_target_by_dept_acct
       WHERE fiscal_year=$1 AND subsidiary_id=$2 AND department=$3 AND location=$4 AND account_number=$5 AND currency=$6`,
      [fiscalYear, subsidiaryId, department, location, accountNumber, currency]
    );
    if (cur.rows.length === 0) return res.status(404).json({ ok: false, error: 'Row not found' });
    const current = cur.rows[0];
    const columnFor: Record<string, string> = {
      USER_OVERRIDE_AMOUNT_ILS: 'user_override_amount_ils',
      USER_OVERRIDE_PCT: 'user_override_pct',
    };
    const updates: Array<{ field: string; column: string; oldVal: any; newVal: any }> = [];
    for (const field of ['USER_OVERRIDE_AMOUNT_ILS', 'USER_OVERRIDE_PCT']) {
      if (field in req.body) {
        const raw = req.body[field];
        const nv = raw === null || raw === '' || raw === undefined ? null : Number(raw);
        const cv = current[field] != null ? Number(current[field]) : null;
        if (nv !== cv) updates.push({ field, column: columnFor[field], oldVal: cv, newVal: nv });
      }
    }
    if (updates.length === 0) return res.json({ ok: true, changes: 0 });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const u of updates) {
        await client.query(
          `UPDATE budget_target_by_dept_acct
           SET ${u.column}=$1, user_edited_by=$2, user_edited_at=NOW()
           WHERE fiscal_year=$3 AND subsidiary_id=$4 AND department=$5 AND location=$6 AND account_number=$7 AND currency=$8`,
          [u.newVal, email, fiscalYear, subsidiaryId, department, location, accountNumber, currency]
        );
        await client.query(
          `INSERT INTO budget_target_edit_log
           (edited_by, fiscal_year, subsidiary_id, department, location, account_number, currency, field_name, old_value, new_value)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [email, fiscalYear, subsidiaryId, department, location, accountNumber, currency,
           u.field,
           u.oldVal == null ? null : String(u.oldVal),
           u.newVal == null ? null : String(u.newVal)]
        );
      }
      await client.query('COMMIT');
      res.json({ ok: true, changes: updates.length, editedBy: email, editedAt: new Date().toISOString() });
    } catch (e: any) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (e: any) {
    logger.error('PUT /budget-targets failed', e);
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

router.get('/budget-target-edits', bankRole, async (req: any, res: Response) => {
  try {
    await ensureBudgetTablesExist();
    const sinceRaw = (req.query.since as string) || new Date(Date.now() - 24*3600*1000).toISOString();
    const since = new Date(sinceRaw);
    const subsidiary = parseInt((req.query.subsidiary as string) || '0', 10);
    const year = parseInt((req.query.year as string) || '0', 10);
    const conds = ['edited_at > $1'];
    const args: any[] = [since];
    if (subsidiary) { conds.push(`subsidiary_id = $${args.length + 1}`); args.push(subsidiary); }
    if (year) { conds.push(`fiscal_year = $${args.length + 1}`); args.push(year); }
    const result = await pool.query(
      `SELECT id AS "ID", edited_at AS "EDITED_AT", edited_by AS "EDITED_BY",
              fiscal_year AS "FISCAL_YEAR", subsidiary_id AS "SUBSIDIARY_ID",
              department AS "DEPARTMENT", location AS "LOCATION",
              account_number AS "ACCOUNT_NUMBER", currency AS "CURRENCY",
              field_name AS "FIELD_NAME", old_value AS "OLD_VALUE", new_value AS "NEW_VALUE"
       FROM budget_target_edit_log
       WHERE ${conds.join(' AND ')}
       ORDER BY edited_at DESC
       LIMIT 200`,
      args
    );
    res.json({
      ok: true,
      edits: result.rows.map((r: any) => ({
        ...r,
        EDITED_AT: r.EDITED_AT instanceof Date ? r.EDITED_AT.toISOString() : r.EDITED_AT,
      })),
      viewerEmail: getCallerEmail(req),
      now: new Date().toISOString(),
    });
  } catch (e: any) {
    logger.error('GET /budget-target-edits failed', e);
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
