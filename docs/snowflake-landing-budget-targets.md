# Bank Dashboard → Snowflake: Budget Targets landing table

The Bank Dashboard writes its **Budget Targets** (including manual overrides and
scenario assumptions) back into Snowflake so other teams can consume the same
numbers the dashboard shows.

- **Table:** `RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS`
- **Refresh:** rewritten in full every time someone clicks **Sync** on the dashboard.
- **Method:** OVERWRITE — the whole table is replaced atomically with the current
  snapshot (one `INSERT OVERWRITE`). It is a *current-state mirror*, not a history.
  After a Sync, the table equals what the dashboard shows for the synced year(s).
- **Grain:** one row per **fiscal year × subsidiary × department × location ×
  account number × currency**, and each row carries the **12 monthly values** in
  `MONTHLY_SOURCE_ILS` / `MONTHLY_SOURCE_EUR`. So every
  (department × account × month) cell has its own amount, in **both ILS and EUR**.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `FISCAL_YEAR` | NUMBER | Budget year (e.g. 2026, 2027) |
| `SUBSIDIARY_ID` | NUMBER | NetSuite subsidiary (3 = LSports) |
| `DEPARTMENT` | STRING | Department (`Unassigned` if none) |
| `LOCATION` | STRING | Location (`Unassigned` if none) |
| `CURRENCY` | STRING | Source currency code |
| `ACCOUNT_NUMBER` | STRING | GL account number (payroll = `76xxxx`) |
| `ACCOUNT_NAME` | STRING | GL account name |
| `NETSUITE_INTERNAL_NUMBER` | NUMBER | NetSuite GL account internal id |
| `CATEGORY` | STRING | Parent GL category (NULL for fallback years) |
| `SOURCE_AMOUNT_ILS` | FLOAT | Annual amount (ILS), before override |
| `USER_OVERRIDE_AMOUNT_ILS` | FLOAT | Absolute manual override, ILS (NULL if none) |
| `USER_OVERRIDE_PCT` | FLOAT | Percent manual adjustment (NULL if none) |
| `ANNUAL_BUDGET_TARGET_AMOUNT` | FLOAT | **Effective annual target, ILS** — override-aware. Use this for ILS. |
| `MONTHLY_SOURCE_ILS` | STRING (JSON) | Month → ILS amount, e.g. `{"01":1234,...,"12":...}` |
| `SOURCE_AMOUNT_EUR` | FLOAT | Annual amount (EUR) = sum of the monthly EUR |
| `MONTHLY_SOURCE_EUR` | STRING (JSON) | Month → EUR amount, e.g. `{"01":335,...,"12":...}` |
| `USER_EDITED_BY` | STRING | Who last edited the override (email) |
| `USER_EDITED_AT` | STRING | When the override was last edited |
| `SOURCE_SYNCED_AT` | STRING | When the row was last pulled from source |
| `SRC_UPDATED_AT` | TIMESTAMP_NTZ | Source last-change time (override edit time, else sync time). Use for freshness/CDC. |

### Currencies

Both currencies are stored and reconcile to the dashboard:
- **ILS** — `ANNUAL_BUDGET_TARGET_AMOUNT` (annual, override-aware) and `MONTHLY_SOURCE_ILS` (per month).
- **EUR** — `SOURCE_AMOUNT_EUR` (annual) and `MONTHLY_SOURCE_EUR` (per month). EUR is the
  dashboard's native EUR (scaled independently from ILS), not an ILS÷rate conversion, so
  it matches the dashboard's EUR view to the cent.

### How a year's numbers are produced

- **Years with a real budget in Snowflake `FCT_BUDGET`** (e.g. 2026): pulled from the
  budget, then scaled so each month's salary/vendor totals and each department's salary
  match the dashboard (the dashboard is the source of truth for the cash view).
- **Projection years with no budget yet** (e.g. 2027): there is no account-level budget in
  the source. The values are the dashboard's **scenario projection**: each month's salary
  per department is set to the dashboard's figure, the vendor bucket to the dashboard's
  vendor total, and the per-account split follows the most-recent-actual (prior-year) mix —
  exactly what the dashboard's drilldown modals show. True independent per-account budgets
  for a projection year only appear once that year's budget is built in NetSuite.

## Querying

```sql
-- Full current snapshot for a year (ILS + EUR annual)
SELECT department, account_number, account_name, category,
       annual_budget_target_amount AS annual_ils, source_amount_eur AS annual_eur
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS
WHERE  fiscal_year = 2027 AND subsidiary_id = 3
ORDER  BY department, account_number;
```

```sql
-- Salary vs vendors per month (ILS), to tie out against the dashboard cash view
SELECT m.key AS month,
       ROUND(SUM(CASE WHEN account_number LIKE '76%' THEN m.value::float END)) AS salary_ils,
       ROUND(SUM(CASE WHEN account_number NOT LIKE '76%' THEN m.value::float END)) AS vendors_ils
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS t,
       LATERAL FLATTEN(input => PARSE_JSON(t.monthly_source_ils)) m
WHERE  t.fiscal_year = 2027 AND t.subsidiary_id = 3
GROUP  BY m.key ORDER BY m.key;
```

```sql
-- Same, in EUR (compare to the dashboard's EUR toggle)
SELECT m.key AS month,
       ROUND(SUM(CASE WHEN account_number LIKE '76%' THEN m.value::float END)) AS salary_eur,
       ROUND(SUM(CASE WHEN account_number NOT LIKE '76%' THEN m.value::float END)) AS vendors_eur
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS t,
       LATERAL FLATTEN(input => PARSE_JSON(t.monthly_source_eur)) m
WHERE  t.fiscal_year = 2027 AND t.subsidiary_id = 3
GROUP  BY m.key ORDER BY m.key;
```

```sql
-- One department, per account, per month (EUR) — matches the salary modal
SELECT account_number, account_name, m.key AS month, ROUND(m.value::float) AS eur
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS t,
       LATERAL FLATTEN(input => PARSE_JSON(t.monthly_source_eur)) m
WHERE  t.fiscal_year = 2027 AND t.department = 'Playmakers' AND t.account_number LIKE '76%'
ORDER  BY account_number, m.key;
```

```sql
-- Analyst-overridden rows (ILS overrides), with who/when
SELECT department, account_number, source_amount_ils,
       annual_budget_target_amount, user_edited_by, src_updated_at
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS
WHERE  user_override_amount_ils IS NOT NULL OR user_override_pct IS NOT NULL;
```

```sql
-- Freshness
SELECT fiscal_year, MAX(src_updated_at) AS last_source_change
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS
GROUP  BY fiscal_year ORDER BY fiscal_year;
```

## Notes for consumers

- **Snapshot, not history.** Every Sync overwrites the table. The figures reflect the
  dashboard state **at the moment of the last Sync** — if an analyst changes the scenario
  and hasn't re-synced, the table is behind. Watch `SRC_UPDATED_AT` for freshness.
- **Refresh is manual**, triggered by an analyst clicking Sync per fiscal year shown on the
  dashboard. A year only appears after it's been synced at least once.
- **Salary vs vendors:** payroll accounts are `76xxxx`; everything else is vendors/opex.
- **Both ILS and EUR** are present per account/department/month.
- **Read access:** ask the data platform owner for SELECT on `RAW.LANDING_FINANCE`
  (the table is written by the `LOADER_FINANCE` role).
