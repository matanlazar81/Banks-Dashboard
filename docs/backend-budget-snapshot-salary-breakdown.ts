// ============================================================================
// finance-it backend patch — bake the per-account salary breakdown into the
// LSports budget-snapshot roll-forward (POST /api/budget-snapshot).
//
// WHY
//   A projection year (e.g. 2027) has NO live Snowflake rows. The salary
//   drilldown for a projection-year month therefore came back empty, and the
//   vendor projection used a flat 12-month average instead of mirroring the
//   source year month-for-month. The dashboard now reads everything from the
//   snapshot ("bake into snapshot"):
//     • Vendors  — already per-month in the snapshot (sfBudget byMonth/totalByMonth
//                  are remapMonths(2026 budget), so each projection month mirrors
//                  the same source-year month). No backend change needed.
//     • Salary   — flat last-3-month (Oct-Dec) average. This patch ALSO bakes the
//                  per-account breakdown (avg of Oct-Dec source-year salary budget
//                  breakdown) so the salary modal renders the same detail the total
//                  is built from, with NO live query, and ties to the dashboard.
//
// FRONTEND FALLBACK (already shipped): if a snapshot has no `sfSalaryBreakdown`
//   (older snapshots, or prod before this patch), the salary modal fetches the
//   source-year Oct/Nov/Dec budget breakdown live and scales it to the dashboard
//   total. So the modal works in prod TODAY. Apply this patch + re-roll 2027 to
//   make the snapshot fully self-contained (no live query at all).
//
// WHERE
//   In the finance-it copy of the LSports roll-forward (the `company === 'lsports'`
//   branch of the budget-snapshot POST handler — mirrors bank-dashboard/vite.config.ts).
//
// HOW (mirror of vite.config.ts) — two edits:
//
//   1) After the SF Promise.all that loads sfBudget/sfSalaryBudget/... and BEFORE
//      building the `snapshot` object, compute the baked breakdown. `sf` is the
//      Snowflake client; `sourceYear` is in scope:
//
//        let sfSalaryBreakdown: any[] = [];
//        try {
//          if (sf && sf.fetchSalaryBudgetBreakdown) {
//            const brkMonths = [10, 11, 12].map(m => `${sourceYear}-${String(m).padStart(2, '0')}`);
//            const brks = await Promise.all(brkMonths.map((mm: string) => sf.fetchSalaryBudgetBreakdown(mm).catch(() => [])));
//            const acc: Record<string, any> = {};
//            for (const rows of brks) for (const row of (rows || [])) {
//              const key = row.account || row.name;
//              if (!acc[key]) acc[key] = { department: row.department, account: row.account, accountId: row.accountId, name: row.name, amountEUR: 0, amountILS: 0 };
//              acc[key].amountEUR += (row.amountEUR || 0);
//              acc[key].amountILS += (row.amountILS || 0);
//            }
//            sfSalaryBreakdown = Object.values(acc).map((x: any) => ({ ...x, amountEUR: Math.round(x.amountEUR / 3), amountILS: Math.round(x.amountILS / 3) }));
//          }
//        } catch (e: any) { console.warn(`[Budget] LS salary breakdown bake failed: ${e.message}`); }
//        const sfSalaryBreakdownSum = sfSalaryBreakdown.reduce((s: number, x: any) => s + (x.amountEUR || 0), 0);
//
//   2) In the `snapshot` object, add the `sfSalaryBreakdown` field and set the flat
//      salary baseline to the breakdown's sum (so the modal total == the dashboard
//      salary exactly), falling back to the avg of the 3 monthly totals:
//
//        // Per-account salary breakdown (avg Oct-Dec source year) — drives the projection-year salary modal.
//        sfSalaryBreakdown,
//        sfSalaryBudget: (() => {
//          const octSal = getByMonthIdx(sfSalaryBudget, 9)?.eur || salByIdx[9] || lastSal;
//          const novSal = getByMonthIdx(sfSalaryBudget, 10)?.eur || salByIdx[10] || lastSal;
//          const decSal = getByMonthIdx(sfSalaryBudget, 11)?.eur || salByIdx[11] || lastSal;
//          const avgSal = sfSalaryBreakdownSum > 0 ? sfSalaryBreakdownSum : (Math.round((octSal + novSal + decSal) / 3) || lastSal);
//          const flat: Record<string, { eur: number }> = {};
//          for (let m = 1; m <= 12; m++) flat[`${targetYear}-${String(m).padStart(2, '0')}`] = { eur: avgSal };
//          return flat;
//        })(),
//
// AFTER APPLYING
//   cd /home/ubuntu/finance-it/backend && npm run build && pm2 restart finance-it-backend
//   Then re-roll 2027 from the dashboard (Budget Targets → roll forward / sync) so
//   the new snapshot includes sfSalaryBreakdown. Verify: the 2027 salary modal shows
//   per-account rows whose total equals the cashflow Salary cell.
//
// IDEMPOTENCY: only apply once. Guard with: if the snapshot object already has a
//   `sfSalaryBreakdown` field, this patch is already present — skip.
// ============================================================================
