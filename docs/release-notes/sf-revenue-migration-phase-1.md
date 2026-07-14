# Revenue data source migration — Phase 1 (CFO note)

**Date:** 2026-07-14
**Scope:** Bank Dashboard revenue figures (LSports)
**Status:** Code merged, pending post-migration parity sign-off before deploy.

## What changed

The dashboard's Salesforce revenue numbers now read from one governed table instead of three
retiring ones. Old sources (being decommissioned by the data team):

- `FCT_MONTHLY_REVENUE`
- `FCT_MONTHLY_REVENUE__SUBSET_PAID`
- `FCT_REVENUE__MONTHLY_ACTUAL_VS_TARGET`

New single source:

- `DL_PRODUCTION.CONSUMER_HUB__BANKS_DASHBOARD.FCT_OPPORTUNITY_MONTHLY_REVENUE__SCD_DAILY__BANKS_DASHBOARD`

The revenue target table (`FCT_REVENUE_TARGET`) is unchanged and stays the source for monthly targets.

## Why

The three old tables are frozen (last refreshed 2026-06-23) and scheduled to be dropped. The new
table is the data team's supported, daily-refreshed replacement with a one-to-one column mapping,
so the dashboard keeps working past the drop and gets fresher numbers.

## What we validated

We captured every revenue number before the switch and diffed old vs new at the source:

- **Total revenue: stable.** 2025 and January–May 2026 match within 1%.
- **Calibration factor: identical.** The pipeline projection factor is unchanged (0.6830), with a
  byte-identical numerator — so the forecast math is untouched.
- **Zero genuine regressions.** No unexplained differences remained after review.

## Where numbers may move (all expected)

- **June / July 2026 revenue:** small differences vs the frozen snapshot. These are legitimate
  Salesforce corrections that landed after the old tables froze — the new live table reflects them.
- **Paid / unpaid / customer counts:** drift as invoices get paid and opportunities are edited.
  This is live status catching up, not a data error.
- **AI revenue-summary text (elapsed months):** now reads the live monthly figure. Visible only in
  the summary narrative; it does not feed any forecast math.

## Safeguards

- All queries are read-only; no writes to Salesforce or Snowflake.
- The pipeline calibration keeps its sanity clamp and fallback factor.
- Before/after parity is captured with `scripts/verify-mr-migration.cjs` and
  `scripts/capture-dashboard-snapshot.cjs`; the diff must show changes only in the revenue impact
  zones above before we deploy.

## The ask

Approve the post-migration "after" parity run so we can deploy. After deploy, the old tables can be
dropped by the data team without affecting the dashboard.
