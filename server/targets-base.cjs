// ─────────────────────────────────────────────────────────────────────────────
// The baseline of the projection-year targets (src/forecast/targets.mjs) from a projection's engine
// data: the Plan's projection months with payroll split by the basis month's departments and operating
// expenses split by the current year's vendor budget categories of the same month. Both projection
// pages carry it as payload.targetsBase; it holds totals by department and category only.
// ─────────────────────────────────────────────────────────────────────────────

const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const ratio = (a, b) => (Math.abs(num(b)) >= 0.5 ? num(a) / num(b) : 0);

/** { 'MM': { category: € } } from the engine's vendor budget by month ('YYYY-MM' of the current year). */
function categoriesByMonth(sfBudgetByMonth, Y) {
  const out = {};
  for (const [mKey, cats] of Object.entries(sfBudgetByMonth || {})) {
    if (!mKey.startsWith(`${Y}-`)) continue;
    out[mKey.slice(5)] = Object.fromEntries(Object.entries(cats || {}).map(([c, v]) => [c, num(v && typeof v === 'object' ? v.eur : v)]));
  }
  return out;
}

/** { dept: € } of the projection year's payroll basis month. */
function deptAmounts(inputsT) {
  const byDept = (inputsT.salaryActualsByDept || {})[inputsT.lastActualSalaryMonth || ''] || {};
  return Object.fromEntries(Object.entries(byDept).map(([d, v]) => [d || '(no department)', num(v && v.eur)]));
}

/** Cash page: revenue = the expected revenue before the collection %, which the deltas then apply. */
function cashTargetsBase(targets, { Y, T, rowsT, inputsT, sfBudgetByMonth }) {
  return targets.buildTargetsBase({
    year: T,
    months: rowsT.map((r, i) => ({
      mKey: r.mKey,
      revenue: num(r.collectionsRevenue) > 0 ? num(r.collectionsRevenue) : num(r.collectionsForecast),
      collPct: num(((inputsT.collPctByMonth || {})[i]) ?? 100),
      payroll: num(r.salary),
      opex: num(r.vendors),
      ilsRate: ratio(r.salaryILS, r.salary) || ratio(r.vendorsILS, r.vendors) || ratio(r.collectionsILS, r.collections),
    })),
    deptAmounts: deptAmounts(inputsT),
    categoryAmountsByMonth: categoriesByMonth(sfBudgetByMonth, Y),
  });
}

/** Cloud (640xxx) as a % of customer revenue over the current year's NetSuite months, or null. */
function serverRatioYtd(details) {
  let cloud = 0;
  let revenue = 0;
  for (const [mKey, byLine] of Object.entries(details.accounts || {})) {
    for (const a of byLine.opex || []) if (String(a.acct).startsWith('640')) cloud -= num(a.eur); // profit-signed cost
    const t = details.rules && details.rules.totals && details.rules.totals[mKey];
    revenue += num(t && t.revenue && t.revenue.eur);
  }
  return revenue >= 0.5 ? Math.round((cloud / revenue) * 10000) / 100 : null;
}

/** P&L page: revenue = customer revenue (no collection %). */
function pnlTargetsBase(targets, { Y, T, monthsT, inputsT, sfBudgetByMonth, details }) {
  return targets.buildTargetsBase({
    year: T,
    months: monthsT.map((m) => ({
      mKey: m.mKey,
      revenue: num(m.eur.revenue),
      collPct: 100,
      payroll: num(m.eur.payroll),
      opex: num(m.eur.opex),
      ilsRate: ratio(m.ils.revenue, m.eur.revenue) || ratio(m.ils.payroll, m.eur.payroll) || ratio(m.ils.opex, m.eur.opex),
    })),
    deptAmounts: deptAmounts(inputsT),
    categoryAmountsByMonth: categoriesByMonth(sfBudgetByMonth, Y),
    serverRatioYtd: details ? serverRatioYtd(details) : null,
  });
}

module.exports = { cashTargetsBase, pnlTargetsBase, serverRatioYtd, categoriesByMonth };
