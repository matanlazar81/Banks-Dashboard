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
      // Layer 1 raw values (pre-Layer 2 + 3) — used by the frontend to highlight cells
      // that were modified by an FCT_EXPENSE override or scenario adjustment.
      await pool.query(`ALTER TABLE budget_target_by_dept_acct ADD COLUMN IF NOT EXISTS monthly_raw_ils JSONB;`);
      // PR-H: native EUR per month (Snowflake AMOUNT_EUR / AMOUNT_EUR_CC), so the
      // EUR export equals the dashboard/modal EUR exactly rather than being derived
      // from ILS / 3.68 (the dashboard's EUR uses Snowflake's own rate per row).
      await pool.query(`ALTER TABLE budget_target_by_dept_acct ADD COLUMN IF NOT EXISTS monthly_source_eur JSONB;`);
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
// Layer 4 (manual %): salaryAdjPctByMonth (Record<monthIdx, pct>) applied to ALL
//          payroll rows for the matching month.
// Layer 5 (HC levers): headcountAdj + deptHeadcount converted to a % per (dept,
//          month) using cumulative within-year HC delta / current headcount.
// Idempotent: preserves user_override_* columns; deletes only rows that didn't
// appear in this sync AND have no user override.
// PR-F: dashboard-driven totals. The frontend POSTs the exact monthly bucket
// figures it displays (after savings, after scenario adjustments, after bank-
// classified overlay for closed months). For each (year-month), the backend
// scales every GL account so the sum of payroll accounts equals dashboardTotals
// .salary, the sum of non-payroll accounts equals dashboardTotals.vendors, and
// an "OTHER" synthetic row carries dashboardTotals.other. Net effect: Targets
// per-bucket totals = dashboard totals by construction.
type DashboardBucketTotals = {
  salary:  { eur: number; ils: number };
  vendors: { eur: number; ils: number };
  other:   { eur: number; ils: number };
};

async function populateBudgetTargets(opts: {
  subsidiary: number;
  years: number[];
  scenarioAdj?: {
    vendorCatAdj?: Record<string, Record<string, number>>;
    salaryDeptAdj?: Record<string, Record<string, number>>;
    salaryAdjPctByMonth?: Record<string, number>;          // { "0".."11": pct } — 0 = January
    headcountAdj?: Record<string, Record<string, number>>; // { "YYYY-MM": { dept: hcDelta } }
    deptHeadcount?: Record<string, { count?: number }>;    // { dept: { count } } — current headcount
  };
  dashboardTotals?: Record<string, DashboardBucketTotals>; // { "YYYY-MM": { salary, vendors, other } }
}) {
  const { subsidiary, years } = opts;
  const scenarioAdj = opts.scenarioAdj || {};
  const vendorCatAdj = scenarioAdj.vendorCatAdj || {};
  const salaryDeptAdj = scenarioAdj.salaryDeptAdj || {};
  const salaryAdjPctByMonth = scenarioAdj.salaryAdjPctByMonth || {};
  const headcountAdj = scenarioAdj.headcountAdj || {};
  const deptHeadcount = scenarioAdj.deptHeadcount || {};

  // Pre-build cumulative HC delta per (year, month, dept). For payroll rows we then
  // convert this to a % using deptHeadcount and apply it on top of Layer 3.
  // Cumulative within-year semantics matches App.tsx (line ~1226-1228).
  const cumulativeHc: Record<string, Record<string, number>> = {}; // { "YYYY-MM": { dept: delta } }
  const hcYmKeys = Object.keys(headcountAdj).sort();
  for (const ym of hcYmKeys) {
    const year = ym.slice(0, 4);
    const cum: Record<string, number> = {};
    // Walk all entries up through ym within the same year
    for (const earlier of hcYmKeys) {
      if (earlier > ym) break;
      if (earlier.slice(0, 4) !== year) continue;
      for (const [dept, delta] of Object.entries(headcountAdj[earlier] || {})) {
        cum[dept] = (cum[dept] || 0) + (Number(delta) || 0);
      }
    }
    cumulativeHc[ym] = cum;
  }
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
      ${budgetCols.has('AMOUNT_EUR_CC') ? 'ROUND(SUM(b.AMOUNT_EUR_CC), 2)' : (budgetCols.has('AMOUNT_EUR') ? 'ROUND(SUM(b.AMOUNT_EUR), 2)' : 'ROUND(SUM(b.AMOUNT_ILS_CC) / 3.68, 2)')} AS MONTH_AMOUNT_EUR,
      b.SUBSIDIARY_ID     AS SUBSIDIARY_ID
    FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
    JOIN      DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT  g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
    LEFT JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT  d ON b.DEPARTMENT_ID = d.DEPARTMENT_ID
    ${hasLocationId && hasDimLocation ? `LEFT JOIN DL_PRODUCTION.FINANCE.DIM_LOCATION l ON b.LOCATION_ID = l.LOCATION_ID` : ''}
    WHERE b.SUBSIDIARY_ID = ${subsidiary}
      AND b.BUDGET_MONTH_DATE >= '${yearStart}-01-01'
      AND b.BUDGET_MONTH_DATE <= '${yearEnd}-12-31'
      AND EXTRACT(YEAR FROM b.BUDGET_MONTH_DATE) IN (${years.join(',')})
      -- Align with the dashboard's outflow definition (see snowflake-api.cjs
      -- fetchBudgetByCategory): only Expense GL accounts, exclude 800xxx
      -- (currency defense / FX hedging) except 800029 (Unrealized Gain/Loss)
      -- which IS reported as the reval line in Targets, and the legacy
      -- account 780502 the dashboard hides.
      AND g.GL_ACCOUNT_TYPE = 'Expense'
      AND (g.GL_ACCOUNT_NUMBER NOT LIKE '800%' OR g.GL_ACCOUNT_NUMBER = '800029')
      AND g.GL_ACCOUNT_NUMBER NOT IN ('780502')
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

  // "Past month" = a (year, month) the dashboard treats as closed (uses FCT_EXPENSE
  // actuals over FCT_BUDGET plan). Computed against server `now` in UTC.
  const _now = new Date();
  const _currentYearInt = _now.getUTCFullYear();
  const _currentMonthInt = _now.getUTCMonth() + 1; // 1-12
  const isPastMonth = (yr: number, mNum: number): boolean => {
    if (yr < _currentYearInt) return true;
    if (yr === _currentYearInt && mNum < _currentMonthInt) return true;
    return false;
  };

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
    // PR-I: use the PLAIN AMOUNT_ILS / AMOUNT_EUR columns for FCT_EXPENSE, matching
    // the dashboard (snowflake-api.cjs). The _CC (constant-currency) variants hold a
    // different value on FCT_EXPENSE and undercount actuals by the FX ratio.
    let amountExpr: string | null = null;
    if (expenseCols.has('AMOUNT_ILS'))         amountExpr = 'ROUND(e.AMOUNT_ILS, 2)';
    else if (expenseCols.has('AMOUNT_ILS_CC')) amountExpr = 'ROUND(e.AMOUNT_ILS_CC, 2)';
    else if (expenseCols.has('AMOUNT_EUR'))    amountExpr = 'ROUND(e.AMOUNT_EUR * 3.68, 2)';
    else if (expenseCols.has('AMOUNT_EUR_CC')) amountExpr = 'ROUND(e.AMOUNT_EUR_CC * 3.68, 2)';

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
    annual: number;
    monthly: Record<string, number>;     // post-Layer-2+3 final values (ILS)
    monthlyRaw: Record<string, number>;  // pre-Layer-2+3 raw values (for highlight delta)
    monthlyEur: Record<string, number>;  // PR-H: native EUR per month (final, scaled like ILS)
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
        monthlyRaw: {},
        monthlyEur: {},
      };
      groups.set(key, g);
    }
    const rawAmt = r.MONTH_AMOUNT_ILS != null ? Number(r.MONTH_AMOUNT_ILS) : 0;
    const rawAmtEur = r.MONTH_AMOUNT_EUR != null ? Number(r.MONTH_AMOUNT_EUR) : 0;
    let amt = rawAmt;
    const mNum = Number(r.MONTH_NUM);
    const mkey = String(mNum).padStart(2, '0'); // "01" .. "12"
    const ymKey = `${r.FISCAL_YEAR}-${mkey}`;
    const isPast = isPastMonth(Number(r.FISCAL_YEAR), mNum);

    // For PAST months we don't apply Layers 2-5 — actuals from FCT_EXPENSE will
    // overwrite g.monthly[mkey] in the next pass.
    if (!isPast) {
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

      // Layer 4: manual % per month (salaryAdjPctByMonth) — applies to ALL payroll
      // rows for the matching month index. Frontend stores this per the active year.
      if (g.IS_PAYROLL) {
        const monthIdxKey = String(mNum - 1); // "0" for January
        const manualPct = Number(salaryAdjPctByMonth[monthIdxKey]) || 0;
        if (manualPct) amt = amt * (1 + manualPct / 100);
    }

    // Layer 5: HC levers. Cumulative within-year HC delta divided by dept's
      // current headcount = % impact on the dept's payroll for that month.
      if (g.IS_PAYROLL) {
        const cumDelta = cumulativeHc[ymKey]?.[g.DEPARTMENT] || 0;
        const headcount = Number(deptHeadcount[g.DEPARTMENT]?.count) || 0;
        if (cumDelta !== 0 && headcount > 0) {
          const hcPct = (cumDelta / headcount) * 100;
          amt = amt * (1 + hcPct / 100);
        }
      }
    } // end !isPast — past months use raw budget value as placeholder until actuals overwrite

    // EUR mirrors the same proportional scenario adjustment applied to ILS,
    // preserving Snowflake's native per-row EUR/ILS rate.
    const amtEur = rawAmt !== 0 ? rawAmtEur * (amt / rawAmt) : rawAmtEur;

    g.monthly[mkey] = (g.monthly[mkey] || 0) + amt;
    g.monthlyRaw[mkey] = (g.monthlyRaw[mkey] || 0) + rawAmt;
    g.monthlyEur[mkey] = (g.monthlyEur[mkey] || 0) + amtEur;
    g.annual += amt;
  }

  // Pass 2: For PAST months, replace placeholder budget values with FCT_EXPENSE
  // actuals (matches the dashboard's "actuals over budget for closed months" rule).
  async function applyActualsForPastMonths(): Promise<string | null> {
    // Build the list of (year, month) tuples that are in the past for any requested year.
    const pastMonthDates: string[] = [];
    for (const yr of years) {
      for (let m = 1; m <= 12; m++) {
        if (isPastMonth(yr, m)) pastMonthDates.push(`${yr}-${String(m).padStart(2, '0')}-01`);
      }
    }
    if (pastMonthDates.length === 0) return null; // nothing to do (e.g. 2027 sync from Jun 2026)

    // Reuse the FCT_EXPENSE amount column detection from Layer 2 (handled in a try/catch
    // there). Replicate the lightweight discovery here so we still proceed if FCT_EXPENSE
    // is offline or shaped differently than expected.
    try {
      const expenseColsRows: any[] = await sf.query(`
        SELECT COLUMN_NAME FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'FCT_EXPENSE'
      `);
      const expenseCols = new Set<string>(expenseColsRows.map(r => r.COLUMN_NAME));
      // PR-I: match the dashboard's FCT_EXPENSE source exactly. snowflake-api.cjs
      // (fetchVendorBreakdown / fetchExpense*) reads the PLAIN AMOUNT_ILS / AMOUNT_EUR
      // columns for actuals. The "_CC" (constant-currency) variants on FCT_EXPENSE
      // hold a different value — using them undercounted past-month actuals by the
      // FX ratio (~3.69x). So prefer the non-CC columns here. (FCT_BUDGET is the
      // opposite: its _CC columns are the real values, kept in the Layer-1 query.)
      let amountExpr: string | null = null;
      if (expenseCols.has('AMOUNT_ILS'))         amountExpr = 'ROUND(SUM(e.AMOUNT_ILS), 2)';
      else if (expenseCols.has('AMOUNT_ILS_CC')) amountExpr = 'ROUND(SUM(e.AMOUNT_ILS_CC), 2)';
      else if (expenseCols.has('AMOUNT_EUR'))    amountExpr = 'ROUND(SUM(e.AMOUNT_EUR) * 3.68, 2)';
      else if (expenseCols.has('AMOUNT_EUR_CC')) amountExpr = 'ROUND(SUM(e.AMOUNT_EUR_CC) * 3.68, 2)';
      if (!amountExpr) {
        logger.warn?.('PR-B: FCT_EXPENSE has no usable amount column; past-month actuals skipped.');
        return 'FCT_EXPENSE has no usable amount column';
      }
      // PR-H/PR-I: native EUR expression. Prefer plain AMOUNT_EUR (what the
      // dashboard uses for FCT_EXPENSE actuals); the _CC variant is constant-
      // currency and undercounts. Fall back to ILS/3.68 only if no EUR column.
      let amountExprEur: string;
      if (expenseCols.has('AMOUNT_EUR'))         amountExprEur = 'ROUND(SUM(e.AMOUNT_EUR), 2)';
      else if (expenseCols.has('AMOUNT_EUR_CC')) amountExprEur = 'ROUND(SUM(e.AMOUNT_EUR_CC), 2)';
      else                                       amountExprEur = `ROUND((${amountExpr}) / 3.68, 2)`;
      const hasExpenseLoc = expenseCols.has('LOCATION_ID');
      const hasExpenseCurr = expenseCols.has('CURRENCY_CODE');

      const dateList = pastMonthDates.map(d => `'${d}'`).join(',');
      const actualSql = `
        SELECT
          EXTRACT(YEAR  FROM e.CAL_MONTH_START_DATE) AS FISCAL_YEAR,
          EXTRACT(MONTH FROM e.CAL_MONTH_START_DATE) AS MONTH_NUM,
          e.DEPARTMENT_ID    AS DEPARTMENT_ID,
          COALESCE(d.DEPARTMENT_NAME, 'Unassigned') AS DEPARTMENT,
          ${hasExpenseLoc && hasDimLocation ? `COALESCE(l.LOCATION_NAME, 'Unassigned')` : `'Unassigned'`} AS LOCATION,
          ${hasExpenseCurr ? `COALESCE(e.CURRENCY_CODE, 'ILS')` : `'ILS'`} AS CURRENCY,
          g.GL_ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
          g.GL_ACCOUNT_NAME   AS ACCOUNT_NAME,
          g.GL_ACCOUNT_ID     AS NETSUITE_INTERNAL_NUMBER,
          g.PARENT_GL_ACCOUNT_NAME AS CATEGORY,
          g.IS_PAYROLL        AS IS_PAYROLL,
          ${amountExpr} AS MONTH_AMOUNT_ILS,
          ${amountExprEur} AS MONTH_AMOUNT_EUR,
          e.SUBSIDIARY_ID     AS SUBSIDIARY_ID
        FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE e
        JOIN      DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT  g ON e.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
        LEFT JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT  d ON e.DEPARTMENT_ID = d.DEPARTMENT_ID
        ${hasExpenseLoc && hasDimLocation ? `LEFT JOIN DL_PRODUCTION.FINANCE.DIM_LOCATION l ON e.LOCATION_ID = l.LOCATION_ID` : ''}
        WHERE e.SUBSIDIARY_ID = ${subsidiary}
          AND e.SOURCE NOT IN ('future_cost_override', 'future_cost_increment')
          AND e.CAL_MONTH_START_DATE IN (${dateList})
          -- PR-G: accrual view. Capture FULL payroll actuals (IS_PAYROLL) plus
          -- vendor/opex actuals (Expense-typed, non-800x except 800029). Actual
          -- payroll posts to GL accounts that are largely NOT typed 'Expense',
          -- so the earlier GL_ACCOUNT_TYPE='Expense' filter silently dropped
          -- ~80% of past-month payroll. Pulling payroll via IS_PAYROLL=TRUE
          -- captures the true Snowflake P&L payroll actual (matches the salary
          -- modal's "Actual (Snowflake)" figure), while the non-payroll branch
          -- keeps the dashboard's outflow definition for vendors.
          AND g.GL_ACCOUNT_NUMBER NOT IN ('780502')
          AND (
            g.IS_PAYROLL = TRUE
            OR (
              g.GL_ACCOUNT_TYPE = 'Expense'
              AND (g.GL_ACCOUNT_NUMBER NOT LIKE '800%' OR g.GL_ACCOUNT_NUMBER = '800029')
            )
          )
        GROUP BY
          EXTRACT(YEAR  FROM e.CAL_MONTH_START_DATE),
          EXTRACT(MONTH FROM e.CAL_MONTH_START_DATE),
          e.DEPARTMENT_ID, d.DEPARTMENT_NAME,
          ${hasExpenseLoc && hasDimLocation ? 'l.LOCATION_NAME,' : ''}
          ${hasExpenseCurr ? 'e.CURRENCY_CODE,' : ''}
          g.GL_ACCOUNT_NUMBER, g.GL_ACCOUNT_NAME, g.GL_ACCOUNT_ID,
          g.PARENT_GL_ACCOUNT_NAME, g.IS_PAYROLL,
          e.SUBSIDIARY_ID
      `;
      const actualRows: any[] = await sf.query(actualSql);

      // For each (year, sub, dept, loc, acct, curr) in actuals, overwrite the past month's
      // value in the corresponding group. Create new groups for actuals rows that have no
      // matching FCT_BUDGET row (e.g. an unbudgeted expense category in past months).
      for (const r of actualRows) {
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
            monthlyRaw: {},
            monthlyEur: {},
          };
          groups.set(key, g);
        }
        const mkey = String(Number(r.MONTH_NUM)).padStart(2, '0');
        const actualAmt = Number(r.MONTH_AMOUNT_ILS) || 0;
        const actualEur = Number(r.MONTH_AMOUNT_EUR) || 0;
        const oldVal = g.monthly[mkey] || 0;
        g.monthly[mkey] = actualAmt;
        g.monthlyRaw[mkey] = actualAmt; // actuals ARE the raw value for past months
        g.monthlyEur[mkey] = actualEur; // native EUR actual for past months
        g.annual = g.annual - oldVal + actualAmt;
      }
      return null; // success
    } catch (actualsErr: any) {
      const msg = actualsErr?.message || String(actualsErr);
      logger.error?.(`PR-B: past-month actuals overlay FAILED: ${msg}`);
      return msg; // surfaced in the sync response so it isn't silently swallowed
    }
  }
  const overlayError = await applyActualsForPastMonths();

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
              netsuite_internal_number, source_amount_ils, monthly_source_ils, monthly_source_eur, subsidiary_id
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
      // For the fallback path, "raw" = previous year's monthly_raw_ils (or its source if absent),
      // and the adjusted value applies the target-year scenario % to that raw.
      const rawSource: Record<string, any> = typeof r.monthly_raw_ils === 'string'
        ? (() => { try { return JSON.parse(r.monthly_raw_ils); } catch { return {}; } })()
        : (r.monthly_raw_ils
            || (typeof r.monthly_source_ils === 'string'
              ? (() => { try { return JSON.parse(r.monthly_source_ils); } catch { return {}; } })()
              : (r.monthly_source_ils || {})));
      // PR-H: prior-year native EUR, scaled by the same factor as ILS.
      const rawEurSource: Record<string, any> = typeof r.monthly_source_eur === 'string'
        ? (() => { try { return JSON.parse(r.monthly_source_eur); } catch { return {}; } })()
        : (r.monthly_source_eur || {});
      const adjustedMonthly: Record<string, number> = {};
      const monthlyRawCopy: Record<string, number> = {};
      const adjustedMonthlyEur: Record<string, number> = {};
      let annual = 0;
      for (const mkey of MONTH_KEYS_LIST) {
        const raw = Number(rawSource[mkey]) || 0;
        const rawEur = Number(rawEurSource[mkey]) || 0;
        let amt = raw;
        const ymKey = `${targetYear}-${mkey}`;
        const mNumLocal = parseInt(mkey, 10);
        let scenarioPct = 0;
        if (meta.isPayroll) scenarioPct = salaryDeptAdj[ymKey]?.[r.department] || 0;
        else if (meta.category) scenarioPct = vendorCatAdj[ymKey]?.[meta.category] || 0;
        if (scenarioPct) amt = amt * (1 + scenarioPct / 100);
        // Layer 4 + 5 also apply in the fallback path so 2027 figures match the dashboard's 2027 projections.
        if (meta.isPayroll) {
          const manualPct = Number(salaryAdjPctByMonth[String(mNumLocal - 1)]) || 0;
          if (manualPct) amt = amt * (1 + manualPct / 100);
          const cumDelta = cumulativeHc[ymKey]?.[r.department] || 0;
          const hc = Number(deptHeadcount[r.department]?.count) || 0;
          if (cumDelta !== 0 && hc > 0) {
            const hcPct = (cumDelta / hc) * 100;
            amt = amt * (1 + hcPct / 100);
          }
        }
        const amtEur = raw !== 0 ? rawEur * (amt / raw) : rawEur;
        if (amt !== 0) adjustedMonthly[mkey] = amt;
        if (raw !== 0) monthlyRawCopy[mkey] = raw;
        if (amtEur !== 0) adjustedMonthlyEur[mkey] = amtEur;
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
        monthlyRaw: monthlyRawCopy,
        monthly: adjustedMonthly,
        monthlyEur: adjustedMonthlyEur,
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
          netsuite_internal_number, source_amount_ils, monthly_source_ils, monthly_raw_ils, monthly_source_eur, subsidiary_id, source_synced_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12, $13)
         ON CONFLICT (fiscal_year, subsidiary_id, department, location, account_number, currency)
         DO UPDATE SET
           account_name = EXCLUDED.account_name,
           netsuite_internal_number = EXCLUDED.netsuite_internal_number,
           source_amount_ils = EXCLUDED.source_amount_ils,
           monthly_source_ils = EXCLUDED.monthly_source_ils,
           monthly_raw_ils = EXCLUDED.monthly_raw_ils,
           monthly_source_eur = EXCLUDED.monthly_source_eur,
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
          JSON.stringify(g.monthlyRaw),
          JSON.stringify(g.monthlyEur || {}),
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
    overlayError, // null on success; the FCT_EXPENSE actuals-overlay error message otherwise
    pastMonthsApplied: overlayError == null,
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
      salaryAdjPctByMonth: body.salaryAdjPctByMonth && typeof body.salaryAdjPctByMonth === 'object' ? body.salaryAdjPctByMonth : undefined,
      headcountAdj: body.headcountAdj && typeof body.headcountAdj === 'object' ? body.headcountAdj : undefined,
      deptHeadcount: body.deptHeadcount && typeof body.deptHeadcount === 'object' ? body.deptHeadcount : undefined,
    };
    // PR-F: dashboard-driven scaling. Frontend sends exact bucket totals per month.
    const dashboardTotals = body.dashboardTotals && typeof body.dashboardTotals === 'object'
      ? body.dashboardTotals
      : undefined;
    await ensureBudgetTablesExist();
    const result = await populateBudgetTargets({ subsidiary, years, scenarioAdj, dashboardTotals });
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
              monthly_raw_ils AS "MONTHLY_RAW_ILS",
              monthly_source_eur AS "MONTHLY_SOURCE_EUR",
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
      MONTHLY_RAW_ILS: r.MONTHLY_RAW_ILS || null,
      MONTHLY_SOURCE_EUR: r.MONTHLY_SOURCE_EUR || null,
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
