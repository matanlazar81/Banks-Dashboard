# BI-3228 Phase 2 — scope all Snowflake reads to CONSUMER_HUB__FINANCE (CFO note)

**Date:** 2026-07-15
**Scope:** Bank Dashboard — every Snowflake-backed figure
**Status:** Code ready on a feature branch. Deploy is gated on the dbt views existing and on a
clean parity run; it must land BEFORE the grant cutover.

## What changed

The dashboard now reads all Salesforce/finance/HR warehouse data from one governed schema,
`DL_PRODUCTION.CONSUMER_HUB__FINANCE`, instead of the raw `FINANCE` / `HR` / `CORE` schemas. Each
table is a passthrough view suffixed `__FINANCE` (e.g. `FCT_BUDGET__FINANCE`), an explicit
column-list copy with no filters — so values are identical to the source.

13 tables moved (budget, expense, GL accounts, departments, revenue target, opportunities, monthly
opportunity/customer, MRR snapshot, employees, headcount, customers, and the Phase-1 revenue SCD
table). This isolates the dashboard from upstream BI schema changes: after cutover the `finance`
warehouse user can read only `CONSUMER_HUB__FINANCE`.

## Also in this change

- Removed two dead debug endpoints (`/api/sf-discover`, `/api/sf-query`) and their helpers — the
  latter allowed arbitrary read SQL and was registered on the production server.
- Removed all runtime schema introspection (the old `PROBABILITY`-column probe and the budget-sync
  column discovery); column sets are now guaranteed by the dbt view contract.
- Dropped the location join from the budget path: there is no financial location dimension in dbt,
  and the join was silently skipped in production already.

## What we validated

`scripts/verify-phase2-migration.cjs` compares old source vs new view for all 13 tables in one run
(counts + keyed sums). Passthrough views must be exact — any non-zero diff is a view defect and
blocks the deploy. Endpoint-level before/after (`scripts/capture-dashboard-snapshot.cjs`) is
expected to show zero diffs: this migration is value-neutral.

## Rollout order (the hard constraint)

1. dbt PR merged and built — all 13 `__FINANCE` views exist.
2. Parity run clean (`verify-phase2-migration.cjs` → PASS).
3. **Deploy this app** (pull master, `npm install && npm run build`, `pm2 restart finance-it-backend`).
4. Only then: BI revokes the `finance` user's access to `FINANCE` / `HR` / `CORE` / `STATSCORE`,
   leaving `CONSUMER_HUB__FINANCE`.

Deploying before step 1, or revoking before step 3, breaks every Snowflake-backed card until the
order is corrected. NetSuite-backed figures (bank balances, cashflow actuals) are unaffected.

## Still open (deferred, not day-1)

The budget-target sync (`POST /api/sync-budget-targets`) runs from a code block pasted into the
parent finance-it backend (`docs/parent-backend-budget-targets.ts` is the reference copy). It has
its own inline Snowflake SQL and must be re-pointed to the `__FINANCE` views and re-spliced
separately. Until then, budget-target reads keep serving from Postgres (last synced values); only
an admin-triggered sync would error. Handle before the next budget refresh.
