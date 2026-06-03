-- ============================================================================
-- Table:    DL_PRODUCTION.FINANCE.FCT_BUDGET_TARGET_BY_DEPT_ACCT
-- Purpose:  Annual budget targets per Department x Account x Location.
-- Scope:    Subsidiary 3 (LSports), fiscal years 2026 and 2027.
-- Grain:    One row per (FISCAL_YEAR, DEPARTMENT, LOCATION, ACCOUNT_NUMBER).
-- Source:   DL_PRODUCTION.FINANCE.FCT_BUDGET aggregated to annual totals.
--
-- Before running:
--   1. Confirm the executing role has CREATE TABLE on DL_PRODUCTION.FINANCE
--      and SELECT on FCT_BUDGET / DIM_DEPARTMENT / DIM_GL_ACCOUNT / DIM_LOCATION.
--   2. Run scripts/inspect-budget-schema.cjs to verify the source columns
--      assumed below actually exist (LOCATION_ID, CURRENCY_CODE).
--   3. If LOCATION_ID or CURRENCY_CODE are not present in FCT_BUDGET, adjust
--      the INSERT below (see notes inline).
-- ============================================================================

CREATE TABLE IF NOT EXISTS DL_PRODUCTION.FINANCE.FCT_BUDGET_TARGET_BY_DEPT_ACCT (
    FISCAL_YEAR                  NUMBER(4, 0)    NOT NULL,
    DEPARTMENT                   VARCHAR(255),
    LOCATION                     VARCHAR(255),
    CURRENCY                     VARCHAR(3),
    ACCOUNT_NUMBER               VARCHAR(50),
    ACCOUNT_NAME                 VARCHAR(255),
    NETSUITE_INTERNAL_NUMBER     NUMBER(38, 0),
    ANNUAL_BUDGET_TARGET_AMOUNT  NUMBER(18, 2),
    SUBSIDIARY_ID                NUMBER(10, 0)   NOT NULL,
    LOADED_AT                    TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Annual budget targets per department, location, and GL account. Source: FCT_BUDGET, aggregated to fiscal year. Subsidiary 3 (LSports), FY2026 + FY2027.';

-- ----------------------------------------------------------------------------
-- Populate FY2026 and FY2027 from FCT_BUDGET.
-- Amount is summed in ILS (LSports functional currency); change AMOUNT_ILS_CC
-- to AMOUNT_EUR_CC if you want EUR-denominated targets instead.
-- ----------------------------------------------------------------------------
INSERT INTO DL_PRODUCTION.FINANCE.FCT_BUDGET_TARGET_BY_DEPT_ACCT (
    FISCAL_YEAR,
    DEPARTMENT,
    LOCATION,
    CURRENCY,
    ACCOUNT_NUMBER,
    ACCOUNT_NAME,
    NETSUITE_INTERNAL_NUMBER,
    ANNUAL_BUDGET_TARGET_AMOUNT,
    SUBSIDIARY_ID
)
SELECT
    EXTRACT(YEAR FROM b.BUDGET_MONTH_DATE)::NUMBER(4, 0)   AS FISCAL_YEAR,
    COALESCE(d.DEPARTMENT_NAME, 'Unassigned')              AS DEPARTMENT,
    COALESCE(l.LOCATION_NAME, 'Unassigned')                AS LOCATION,
    -- If FCT_BUDGET has no CURRENCY_CODE column, replace the next line with: 'ILS' AS CURRENCY,
    COALESCE(b.CURRENCY_CODE, 'ILS')                       AS CURRENCY,
    g.GL_ACCOUNT_NUMBER                                    AS ACCOUNT_NUMBER,
    g.GL_ACCOUNT_NAME                                      AS ACCOUNT_NAME,
    g.GL_ACCOUNT_ID                                        AS NETSUITE_INTERNAL_NUMBER,
    ROUND(SUM(b.AMOUNT_ILS_CC), 2)                         AS ANNUAL_BUDGET_TARGET_AMOUNT,
    b.SUBSIDIARY_ID                                        AS SUBSIDIARY_ID
FROM DL_PRODUCTION.FINANCE.FCT_BUDGET b
JOIN      DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT  g ON b.GL_ACCOUNT_ID  = g.GL_ACCOUNT_ID
LEFT JOIN DL_PRODUCTION.FINANCE.DIM_DEPARTMENT  d ON b.DEPARTMENT_ID  = d.DEPARTMENT_ID
-- If FCT_BUDGET has no LOCATION_ID column, drop this join and replace l.LOCATION_NAME with NULL.
LEFT JOIN DL_PRODUCTION.FINANCE.DIM_LOCATION    l ON b.LOCATION_ID    = l.LOCATION_ID
WHERE b.SUBSIDIARY_ID = 3
  AND b.BUDGET_MONTH_DATE >= '2026-01-01'
  AND b.BUDGET_MONTH_DATE <= '2027-12-31'
GROUP BY
    EXTRACT(YEAR FROM b.BUDGET_MONTH_DATE),
    d.DEPARTMENT_NAME,
    l.LOCATION_NAME,
    b.CURRENCY_CODE,
    g.GL_ACCOUNT_NUMBER,
    g.GL_ACCOUNT_NAME,
    g.GL_ACCOUNT_ID,
    b.SUBSIDIARY_ID
HAVING ABS(SUM(b.AMOUNT_ILS_CC)) > 0
ORDER BY FISCAL_YEAR, DEPARTMENT, ACCOUNT_NUMBER;

-- ----------------------------------------------------------------------------
-- Sanity checks (optional, run after the INSERT).
-- ----------------------------------------------------------------------------
-- SELECT FISCAL_YEAR, COUNT(*) AS ROW_COUNT, SUM(ANNUAL_BUDGET_TARGET_AMOUNT) AS TOTAL_ILS
-- FROM DL_PRODUCTION.FINANCE.FCT_BUDGET_TARGET_BY_DEPT_ACCT
-- GROUP BY FISCAL_YEAR
-- ORDER BY FISCAL_YEAR;
--
-- SELECT * FROM DL_PRODUCTION.FINANCE.FCT_BUDGET_TARGET_BY_DEPT_ACCT
-- ORDER BY FISCAL_YEAR, DEPARTMENT, ACCOUNT_NUMBER
-- LIMIT 20;
