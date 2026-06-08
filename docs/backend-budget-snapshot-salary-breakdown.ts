// ============================================================================
// SUPERSEDED. The salary baseline for projection years is now derived on the
// FRONTEND from source-year per-department actuals (last 3 actual months avg,
// keyed under a synthetic '${srcYear}-AVG' entry that the existing Last Actual
// projection path consumes). The salary modal renders the same six-department
// view as the live "Last Actual" mode, dashboard total == modal total, and
// each department is adjustable.
//
// This backend bake (sfSalaryBreakdown via FCT_BUDGET) only ever produced ONE
// row (Playmakers / 760001 Gross Salaries) because FCT_BUDGET isn't split per
// department — that's why the projection-year modal showed only one row.
//
// The patch already applied in finance-it does no harm (the frontend ignores
// the baked field), so no urgent revert is required. If you want to clean up
// when convenient:
//   • drop the `sfSalaryBreakdown` field from the snapshot object
//   • drop the breakdown-bake try/catch block
//   • the `sfSalaryBudget` flat baseline (avg Oct-Dec source-year monthly
//     totals) can stay — it's the safety fallback when the frontend's
//     per-dept actuals fetch returns empty.
// ============================================================================
