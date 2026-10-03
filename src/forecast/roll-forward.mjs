// ─────────────────────────────────────────────────────────────────────────────
// Roll-forward: turn the CURRENT year's forecast into the NEXT year's engine inputs.
//
// Pure (no I/O, no wall clock). The New Bank Dashboard's server (server/cash-projection.cjs)
// runs the shared engine for the current year, hands the 12 rows here, and runs the engine
// again on the returned inputs for the projection year.
//
// This is a port of the old dashboard's projection-year loader, which only ever ran in the
// browser:
//   • src/App.tsx:2633-2856 — projection-year branch of fetchData (opening carry, salary
//     run-rate basis, vendor mirror, inflow baseline, zero pipeline/churn)
//   • src/App.tsx:1516-1552 + 2159-2196 — which scenario knobs a projection year sees
//   • server/api-routes.cjs:2125-2307 — the "→ 2027" snapshot remaps
// Rebuilt from live current-year data on every call, it equals the old dashboard's projection
// year right after "↻ from <year>". snapshotFieldsFromFile() reproduces a stored snapshot file
// instead, for side-by-side parity checks.
// ─────────────────────────────────────────────────────────────────────────────

const pad2 = (n) => String(n).padStart(2, '0');
const monthKeys = (year) => Array.from({ length: 12 }, (_, i) => `${year}-${pad2(i + 1)}`);
const EMPTY_CONVERSION = { yearly: [], stages: [], customers: [], projection: [] };

// Re-key every 'YYYY-MM' key to targetYear-MM (api-routes.cjs:2126-2133). Later keys win when two
// source years map onto the same month, exactly like the snapshot.
function remapMonthKeys(obj, targetYear) {
  const out = {};
  for (const [key, val] of Object.entries(obj || {})) {
    out[key.replace(/^\d{4}(-\d{2})$/, `${targetYear}$1`)] = val;
  }
  return out;
}

// First key ending in -MM, whatever its year (api-routes.cjs:2135-2141). Kept first-match on
// purpose: feeds that also carry the prior year resolve to the prior year's month, as before.
function getByMonthIdx(obj, mi) {
  const mm = pad2(mi + 1);
  for (const key of Object.keys(obj || {})) {
    if (key.endsWith(`-${mm}`)) return obj[key];
  }
  return undefined;
}

// Month-index-keyed scenario maps for a projection year. The old dashboard's year switch keeps the
// target year's own map when it has one and otherwise starts from the current year's map (salary %,
// collection %, currency-defense %); pipeline % never inherits (App.tsx:1516-1552). Legacy scenarios
// without year buckets store their flat maps as current-year values (App.tsx:2185-2195), so the
// projection year inherits them the same way.
function inheritProjectionMaps(sd, targetYear, sourceYear) {
  const nonEmpty = (m) => !!m && typeof m === 'object' && Object.keys(m).length > 0;
  const aby = sd && sd.adjustmentsByYear;
  if (aby) {
    const t = aby[String(targetYear)] || {};
    const s = aby[String(sourceYear)] || {};
    const inherit = (k) => (nonEmpty(t[k]) ? t[k] : (s[k] || {}));
    return {
      salaryAdjPctByMonth: inherit('salaryAdjPctByMonth'),
      collPctByMonth: inherit('collPctByMonth'),
      currencyDefensePctByMonth: inherit('currencyDefensePctByMonth'),
      pipelineAdjPctByMonth: t.pipelineAdjPctByMonth || {},
    };
  }
  const flat = sd || {};
  return {
    salaryAdjPctByMonth: flat.salaryAdjPctByMonth || {},
    collPctByMonth: flat.collPctByMonth || {},
    currencyDefensePctByMonth: flat.currencyDefensePctByMonth || {},
    pipelineAdjPctByMonth: {},
  };
}

// Projection-year salary basis (App.tsx:2785-2820 + setSalaryFrom 2756-2766): the source year's
// Oct–Dec payroll budget by department, scaled so its monthly average equals the Oct–Dec salary the
// source-year forecast shows, then flattened. Rounding order matters for euro-exact parity: scale
// and round the 3-month department sums first, then divide by the months that returned data.
//   breakdowns: [octRows, novRows, decRows] from snowflake fetchSalaryBudgetBreakdown(month)
// Returns null when no month returned rows (caller falls back to the flat snapshot budget).
function synthesizeSalaryBasis(breakdowns, srcRows, sourceYear) {
  const perDept = {};
  let monthsWithData = 0;
  for (const rows of breakdowns || []) {
    const list = Array.isArray(rows) ? rows : [];
    if (list.length > 0) monthsWithData++;
    for (const row of list) {
      const dept = row.department || 'Unassigned';
      if (!perDept[dept]) perDept[dept] = { eur: 0, ils: 0 };
      perDept[dept].eur += (row.amountEUR || 0);
      perDept[dept].ils += (row.amountILS || 0);
    }
  }
  if (monthsWithData === 0) return null;

  let scaleEur = 1;
  let scaleIls = 1;
  if (Array.isArray(srcRows) && srcRows.length === 12) {
    const tgtEur = (srcRows[9].salary + srcRows[10].salary + srcRows[11].salary) / 3;
    const tgtIls = (srcRows[9].salaryILS + srcRows[10].salaryILS + srcRows[11].salaryILS) / 3;
    const curEur = Object.values(perDept).reduce((s, v) => s + v.eur, 0) / monthsWithData;
    const curIls = Object.values(perDept).reduce((s, v) => s + v.ils, 0) / monthsWithData;
    scaleEur = curEur > 0 && tgtEur > 0 ? tgtEur / curEur : 1;
    scaleIls = curIls > 0 && tgtIls > 0 ? tgtIls / curIls : 1;
    for (const d of Object.keys(perDept)) {
      perDept[d].eur = Math.round(perDept[d].eur * scaleEur);
      perDept[d].ils = Math.round(perDept[d].ils * scaleIls);
    }
  }
  const div = Math.max(1, monthsWithData);
  const synth = {};
  for (const [d, v] of Object.entries(perDept)) synth[d] = { eur: Math.round(v.eur / div), ils: Math.round(v.ils / div) };
  const basisKey = `${sourceYear}-AVG`; // sorts before every projection-year month key
  return {
    salaryActualsByDept: { [basisKey]: synth },
    lastActualSalaryMonth: basisKey,
    flat: {
      eur: Object.values(synth).reduce((s, v) => s + v.eur, 0),
      ils: Object.values(synth).reduce((s, v) => s + v.ils, 0),
    },
    monthsWithData,
    scale: { eur: scaleEur, ils: scaleIls },
  };
}

// The fields the "→ <year>" snapshot file would hold, rebuilt from live source-year inputs
// (api-routes.cjs:2143-2307, LSports branch). rawSfBudget / rawSfSalaryBudget must be the plain
// Snowflake fetches: gatherInputs() merges budget overrides into its own copies in place, and the
// snapshot never applied them.
function snapshotFieldsFromLive({ sourceYear, targetYear, srcInputs, rawSfBudget, rawSfSalaryBudget, now }) {
  const T = targetYear;
  const src = srcInputs || {};
  const salaryData = src.salaryData || [];
  const sfActualsSplit = src.sfActualsSplit || {};
  const sfRevenuePaid = src.sfRevenuePaid || {};
  const sfRevenue = src.sfRevenue || {};
  const collections = src.actualCollections || {};
  const salBudget = rawSfSalaryBudget || {};

  // The snapshot's server-side month loop, kept only for its "last known" fallbacks
  // (api-routes.cjs:2183-2253; NS budget is always empty for LSports).
  const curMonthIdx = now.getMonth();
  const salByIdx = {};
  for (const s of salaryData) {
    if (s && s.month && s.amountEUR > 0) {
      const m = parseInt(s.month.split('-')[1], 10) - 1;
      if (!isNaN(m)) salByIdx[m] = s.amountEUR;
    }
  }
  const collByIdx = {};
  for (const [k, v] of Object.entries(collections)) {
    if (k.startsWith(`${sourceYear}`)) {
      const m = parseInt(k.substring(5, 7), 10) - 1;
      if (!isNaN(m)) collByIdx[m] = v;
    }
  }
  let lastSal = 0;
  let lastColl = 0;
  for (let mi = 0; mi < 12; mi++) {
    const isPast = mi < curMonthIdx;
    const split = getByMonthIdx(sfActualsSplit, mi);
    const salBud = getByMonthIdx(salBudget, mi);
    let sal = 0;
    if (isPast && split && split.salary > 0) sal = split.salary;
    else if (isPast && salByIdx[mi] > 0) sal = salByIdx[mi];
    else if (salBud && salBud.eur > 0) sal = salBud.eur;
    else sal = lastSal;
    if (sal > 0) lastSal = sal;
    const revPaid = getByMonthIdx(sfRevenuePaid, mi);
    const revBud = getByMonthIdx(sfRevenue.budget || {}, mi);
    let coll = 0;
    if (isPast && collByIdx[mi] > 0) coll = collByIdx[mi];
    else if (revPaid && revPaid.revenue > 0) coll = revPaid.revenue;
    else if (revBud && revBud.eur > 0) coll = revBud.eur;
    if (coll > 0) lastColl = coll;
  }

  const tKeys = monthKeys(T);
  const salAt = (mi) => (getByMonthIdx(salBudget, mi) || {}).eur || salByIdx[mi] || lastSal;
  const avgSal = Math.round((salAt(9) + salAt(10) + salAt(11)) / 3) || lastSal;
  const inflowAt = (mi) => (getByMonthIdx(sfRevenuePaid, mi) || {}).revenue || (getByMonthIdx(sfRevenue.budget || {}, mi) || {}).eur || 0;
  const avgInflow = Math.round((inflowAt(9) + inflowAt(10) + inflowAt(11)) / 3) || lastColl;

  return {
    sfBudgetByMonth: remapMonthKeys((rawSfBudget && rawSfBudget.byMonth) || {}, T),
    sfSalaryBudget: Object.fromEntries(tKeys.map((k) => [k, { eur: avgSal }])),
    sfRevenue: { budget: Object.fromEntries(tKeys.map((k) => [k, { eur: avgInflow }])), targets: remapMonthKeys(sfRevenue.targets || {}, T) },
    sfActualsSplit: remapMonthKeys(sfActualsSplit, T),
    nsBudget: { byMonth: {} },
    sfPipeline: src.sfPipeline || [],
    sfConversion: src.sfConversion || EMPTY_CONVERSION,
    salary: salaryData.map((s) => ({ ...s, month: s.month ? s.month.replace(/^\d{4}/, `${T}`) : s.month })),
    vendorHistory: (src.vendorHistory || []).map((v) => ({ ...v, paidDate: v.paidDate ? v.paidDate.replace(/^\d{4}/, `${T}`) : v.paidDate })),
    collections: remapMonthKeys(collections, T),
  };
}

// The same fields read from a stored snapshot file (data/budgets/<year>-lsports.json), so a parity
// run can reproduce what the old dashboard shows from that file.
function snapshotFieldsFromFile(snap) {
  const s = snap || {};
  return {
    sfBudgetByMonth: (s.sfBudget && s.sfBudget.byMonth) || {},
    sfSalaryBudget: s.sfSalaryBudget || {},
    sfRevenue: s.sfRevenue || {},
    sfActualsSplit: s.sfActualsSplit || {},
    nsBudget: s.nsBudget || { byMonth: {} },
    sfPipeline: s.sfPipeline || [],
    sfConversion: s.sfConversion || EMPTY_CONVERSION,
    salary: s.salary || [],
    vendorHistory: s.vendorHistory || [],
    collections: s.collections || {},
  };
}

// Engine inputs for the projection year (App.tsx:2633-2856). srcRows are the source year's 12
// operating-view rows (dividend excluded) — the same rows the old dashboard carried forward.
//   knobs: scenario knobs for the target year (scenarioKnobs + inheritProjectionMaps)
//   salaryBasis: synthesizeSalaryBasis() result, or null to fall back to the flat snapshot budget
function buildNextYearInputs({ sourceYear, targetYear, srcInputs, srcRows, snapshot, salaryBasis, knobs, now, ilsRevalRate }) {
  if (!Array.isArray(srcRows) || srcRows.length !== 12) throw new Error('buildNextYearInputs: srcRows must hold 12 months');
  const T = targetYear;
  const tKeys = monthKeys(T);
  const dec = srcRows[11];

  // Opening = source-year December closing. EUR kept unrounded, ILS rounded (App.tsx:2649-2659).
  // currentBalance carries the same values: the engine derives the projection year's EUR→ILS
  // rate from them unless the scenario sets fxRateByYear[T].
  const openEur = dec.closingBalance;
  const openIls = Math.round(dec.closingBalanceILS);

  // Inflows: flat average of the source year's Oct–Dec collections (App.tsx:2688-2699).
  const avgColl = Math.round((srcRows[9].collections + srcRows[10].collections + srcRows[11].collections) / 3);
  const sfRevenuePaid = Object.fromEntries(tKeys.map((k) => [k, { revenue: avgColl, customers: dec.customers || 0, paid: avgColl, unpaid: 0 }]));

  // Vendors: the source year mirrored month by month (App.tsx:2831-2840).
  const totalByMonth = Object.fromEntries(tKeys.map((k, m) => [k, { eur: Math.round(srcRows[m].vendors), ils: Math.round(srcRows[m].vendorsILS) }]));

  // Salary: run-rate basis when the Oct–Dec breakdown loaded, else the flat snapshot budget with no
  // last-actual basis (App.tsx:2804, 2822).
  let salaryActualsByDept = {};
  let lastActualSalaryMonth = '';
  let sfSalaryBudget = snapshot.sfSalaryBudget || {};
  if (salaryBasis) {
    salaryActualsByDept = salaryBasis.salaryActualsByDept;
    lastActualSalaryMonth = salaryBasis.lastActualSalaryMonth;
    sfSalaryBudget = Object.fromEntries(tKeys.map((k) => [k, { ...salaryBasis.flat }]));
  }

  return {
    ...knobs,
    activeYear: T,
    currentYear: sourceYear,
    now,
    asOfDate: null,
    ilsRevalRate,

    book: { openingBalance: openEur, currentBalance: openEur, dailyBalances: [] },
    bookLocal: { openingBalance: openIls, currentBalance: openIls, dailyBalances: [] },
    yearStartBalance: null,     // the engine ignores it for a projection year
    prevMonthEndBalance: null,  // no bank anchor in a projection year
    liveFxRate: srcInputs.liveFxRate,

    salaryData: snapshot.salary,
    salaryActualsByDept,
    lastActualSalaryMonth,
    salaryDeptBudgets: {},
    sfSalaryOverrides: [],
    sfSalaryBudget,
    monthlyHCImpact: {},
    sfActualsSplit: snapshot.sfActualsSplit,

    vendorBills: [],
    vendorActuals: [],
    nsPaidVendors: { byMonth: {} },
    vendorHistory: snapshot.vendorHistory,
    sfBudget: { byMonth: snapshot.sfBudgetByMonth, totalByMonth },
    nsBudget: snapshot.nsBudget,
    expenseCategories: { byMonth: {}, categories: [] },

    sfRevenuePaid,
    actualCollections: snapshot.collections,
    sfRevenue: snapshot.sfRevenue,
    revenueActuals: [],
    customerReceipts: {},

    // A projection year models no new pipeline and no churn (App.tsx:2733-2740); the open
    // pipeline still feeds Inflows through the pipelineMinProb filter, as before.
    sfPipeline: snapshot.sfPipeline,
    sfConversion: snapshot.sfConversion,
    pipelineMethodology: { byMonth: {} },
    sfChurnQuarterly: [],
    churnData: [],
    churnMonthlyAvg: 0,

    // The old projection year always ended up with an empty finance budget (App.tsx:2684-2685),
    // so its currency-defense reval is 0. Kept for parity.
    monthlyReval: { byMonth: {}, preYear: { eur: 0, ils: 0 } },
    nsBankClassified: { byMonth: {} },
    sfFinanceBudget: {},
    dividendExclusions: null,
  };
}

// Compact table rows for one year: only the figures the dashboard shows (no deal names, no bank
// line labels), rounded to cents so every identity below holds.
//   net      = net change incl. reval = inflows − outflows + reval
//   reanchor = opening − previous closing; non-zero only in the live current month, where the
//              engine re-anchors the opening to the NetSuite bank balance
// prevClosing: { eur, ils } of the preceding December (projection year), or null.
function shapeYear(rows, { year, kind, prevClosing = null }) {
  const c = (n) => Math.round((Number(n) || 0) * 100) / 100;
  const side = (r, ils) => (ils
    ? { opening: r.openingBalanceILS, collections: r.collectionsILS, pipeline: r.pipelineWeightedILS, churn: r.churnDeductionILS,
        salary: r.salaryILS, vendors: r.vendorsILS, other: r.otherILS, reval: r.revalImpactILS, net: r.netILS + r.revalImpactILS, closing: r.closingBalanceILS }
    : { opening: r.openingBalance, collections: r.collections, pipeline: r.pipelineWeighted, churn: r.churnDeduction,
        salary: r.salary, vendors: r.vendors, other: r.other, reval: r.revalImpact, net: r.net + r.revalImpact, closing: r.closingBalance });
  const out = rows.map((r, i) => {
    const prev = i > 0
      ? { eur: rows[i - 1].closingBalance, ils: rows[i - 1].closingBalanceILS }
      : prevClosing;
    const eur = side(r, false);
    const ils = side(r, true);
    const shaped = { eur: {}, ils: {} };
    for (const k of Object.keys(eur)) { shaped.eur[k] = c(eur[k]); shaped.ils[k] = c(ils[k]); }
    shaped.eur.reanchor = prev ? c(r.openingBalance - prev.eur) : 0;
    shaped.ils.reanchor = prev ? c(r.openingBalanceILS - prev.ils) : 0;
    return {
      mKey: r.mKey,
      status: r.isPast ? 'actual' : r.isCurrent ? 'current' : 'forecast',
      dividendExcluded: c(r.dividendExcluded || 0),
      eur: shaped.eur,
      ils: shaped.ils,
    };
  });
  return { year, kind, rows: out };
}

export {
  remapMonthKeys,
  getByMonthIdx,
  inheritProjectionMaps,
  synthesizeSalaryBasis,
  snapshotFieldsFromLive,
  snapshotFieldsFromFile,
  buildNextYearInputs,
  shapeYear,
};
