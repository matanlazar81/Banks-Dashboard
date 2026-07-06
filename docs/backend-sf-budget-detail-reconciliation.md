# Drill-down reconciliation — `fetchBudgetCategoryDetail` includes future-cost overrides

**Where:** `snowflake-api.cjs` `fetchBudgetCategoryDetail(month, category)` (shared Snowflake client,
in this repo). Consumed by `/api/sf-budget-detail` on both the dev Vite server
(`server/api-routes.cjs`) and the production `finance-it-backend`.

## Why

The "Snowflake Budget Breakdown" category list (`/api/sf-budget`) merges FCT_EXPENSE **future-cost
overrides / increments** into each category total (`server/api-routes.cjs`, the `sf-budget` handler:
`Override` sets the category total, `Increment` adds to it). The category drill-down
(`/api/sf-budget-detail` → `fetchBudgetCategoryDetail`) returned **raw `FCT_BUDGET`** rows only, with
no overrides — so the drill-down summed LOW and disagreed with the number shown in the list.

Concrete case: **Cloud Infrastructure & DevOps, Sep 2026** — category list €606,371 vs drill-down
€434,942, a **€171,429** forward-cost increment that lived only in FCT_EXPENSE.

## Fix

`fetchBudgetCategoryDetail` now, after building the raw detail rows, computes the exact category base
(`fetchBudgetByCategory(year).byMonth[month][category]`) + applies the matching FCT_EXPENSE overrides
(same merge as `/api/sf-budget`), then appends ONE reconciliation row for the delta:

```
department: 'Future-cost override'
name:       'Forward-cost override / increment (not in base budget)'
amountEUR:  catTotal − Σ(detail rows)          // e.g. +171,429
isOverride: true
```

So the drill-down rows now sum to the category-list total. The row is display-only: its `account` is
empty and `accountId` is null, so the row-click bill fetch is skipped, and it renders like any other
detail line. Guarded in a try/catch — if the extra lookups fail, the drill-down degrades to the raw
detail (today's behavior), never an error.

## Deploy

The change is in the shared `snowflake-api.cjs` **method**, not a route — so a normal backend
**rebuild + restart** picks it up. **No route patch is needed** (unlike the churn `quarterly` fix,
which was a route-shape change). No frontend rebuild is required: `App.tsx` already renders the detail
rows generically. A full Pull & Build is harmless and also fine.

```bash
cd /home/ubuntu/finance-it/backend && npm run build && pm2 restart finance-it-backend
```

## Verify

`GET /api/sf-budget-detail?month=2026-09&category=Cloud Infrastructure & DevOps` → the detail rows
plus the appended "Future-cost override" row sum to **€606,371** (the category-list value).
