# Bank Dashboard → Snowflake: Budget Targets landing table

The Bank Dashboard writes its **Budget Targets** (including manual overrides) back
into Snowflake so other teams can consume the same numbers the dashboard shows.

- **Table:** `RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS`
- **Refresh:** rewritten in full every time someone clicks **Sync** on the dashboard.
- **Method:** OVERWRITE — the whole table is replaced atomically with the current
  snapshot. It is a *current-state mirror*, not an append-only history. There is no
  partial/incremental update: after a Sync, the table equals the dashboard exactly.
- **Grain (one row per):** fiscal year × subsidiary × department × location ×
  account number × currency.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `FISCAL_YEAR` | NUMBER | Budget year (e.g. 2026) |
| `SUBSIDIARY_ID` | NUMBER | NetSuite subsidiary (3 = LSports) |
| `DEPARTMENT` | STRING | Department name (`Unassigned` if none) |
| `LOCATION` | STRING | Location (`Unassigned` if none) |
| `CURRENCY` | STRING | Source currency code |
| `ACCOUNT_NUMBER` | STRING | GL account number |
| `ACCOUNT_NAME` | STRING | GL account name |
| `NETSUITE_INTERNAL_NUMBER` | NUMBER | NetSuite GL account internal id |
| `CATEGORY` | STRING | Parent GL category (e.g. Outsourcing, SW Licenses) |
| `SOURCE_AMOUNT_ILS` | FLOAT | Annual budget from Snowflake `FCT_BUDGET`, before any override |
| `USER_OVERRIDE_AMOUNT_ILS` | FLOAT | Absolute manual override (NULL if not overridden) |
| `USER_OVERRIDE_PCT` | FLOAT | Percent manual adjustment (NULL if not used) |
| `ANNUAL_BUDGET_TARGET_AMOUNT` | FLOAT | **Effective target** — use this one. See note below. |
| `MONTHLY_SOURCE_ILS` | STRING | JSON map of month → source amount, e.g. `{"01":1234,...}` |
| `USER_EDITED_BY` | STRING | Who last edited the override (email) |
| `USER_EDITED_AT` | STRING | When the override was last edited |
| `SOURCE_SYNCED_AT` | STRING | When the row was last pulled from `FCT_BUDGET` |
| `SRC_UPDATED_AT` | TIMESTAMP_NTZ | When the source row last changed — the override edit time if present, else the sync time. Use this for freshness/CDC. |

### Which amount to use

`ANNUAL_BUDGET_TARGET_AMOUNT` is the value the dashboard treats as the target. It is:

```
COALESCE(
  USER_OVERRIDE_AMOUNT_ILS,                              -- absolute override wins
  SOURCE_AMOUNT_ILS * (1 + COALESCE(USER_OVERRIDE_PCT,0)/100.0),  -- else % adj on source
  SOURCE_AMOUNT_ILS                                      -- else raw source
)
```

If you want the un-edited budget, use `SOURCE_AMOUNT_ILS`. Rows where
`USER_OVERRIDE_AMOUNT_ILS` or `USER_OVERRIDE_PCT` is non-NULL are analyst-adjusted.

## Querying

```sql
-- Full current snapshot for 2026, LSports
SELECT department, account_number, account_name, category,
       source_amount_ils, annual_budget_target_amount
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS
WHERE  fiscal_year = 2026
  AND  subsidiary_id = 3
ORDER  BY department, account_number;
```

```sql
-- Target by department
SELECT department, ROUND(SUM(annual_budget_target_amount)) AS target_ils
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS
WHERE  fiscal_year = 2026 AND subsidiary_id = 3
GROUP  BY department
ORDER  BY target_ils DESC;
```

```sql
-- Only analyst-overridden rows, with who/when
SELECT department, account_number, source_amount_ils,
       annual_budget_target_amount, user_edited_by, src_updated_at
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS
WHERE  user_override_amount_ils IS NOT NULL OR user_override_pct IS NOT NULL;
```

```sql
-- Monthly breakdown: explode the JSON map into rows
SELECT t.fiscal_year, t.department, t.account_number,
       m.key::INT AS month_num, m.value::FLOAT AS month_amount_ils
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS t,
       LATERAL FLATTEN(input => PARSE_JSON(t.monthly_source_ils)) m
WHERE  t.fiscal_year = 2026 AND t.subsidiary_id = 3;
```

```sql
-- How fresh is the data?
SELECT MAX(src_updated_at) AS last_source_change
FROM   RAW.LANDING_FINANCE.BANK_DASHBOARD_BUDGET_TARGETS;
```

## Notes for consumers

- **Snapshot, not history.** Every Sync overwrites the table. If you need history,
  snapshot it yourself downstream (e.g. a scheduled task into a dated table or a
  stream) — this landing table only ever holds the latest state.
- **Refresh is manual**, triggered by an analyst clicking Sync, not on a schedule.
  Watch `SRC_UPDATED_AT` (or `SOURCE_SYNCED_AT`) for staleness rather than assuming
  a fixed cadence.
- **Amounts are in ILS.**
- **Read access:** ask the data platform owner for SELECT on
  `RAW.LANDING_FINANCE` (the table is written by the `LOADER_FINANCE` role).
