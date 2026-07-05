// ============================================================================
// forecast-core.cjs — the cashflow forecast engine, extracted verbatim.
//
// This is a faithful 1:1 port of the `cashflowForecast` useMemo in
// src/App.tsx (the ~470-line block that produces the 12 monthly rows).
// It is a SINGLE SOURCE OF TRUTH: the frontend memo builds an `inputs`
// object and calls this, and the nightly server-side job (which has no
// browser) builds the same object from NetSuite/Snowflake and calls this.
// Because both paths run the exact same code, the on-screen number and the
// number pushed to Snowflake at 23:00 can never diverge.
//
// Purity contract (why this can run in Node):
//   - No setState / refs / window / document / localStorage / fetch.
//   - No wall-clock: the caller passes `now` (a Date) and `currentYear`.
//     The frontend passes `now = asOfDate ? new Date(asOfDate+'T12:00:00')
//     : new Date()` and its `currentYear` state; the Node job passes the
//     real current instant. (An omitted `now` falls back to `new Date()`
//     ONLY so the frontend stays byte-identical if it forgets to pass it —
//     Node/tests must always pass `now` for determinism.)
//   - The hardcoded 3.59 ILS reval rate is now the `ilsRevalRate` input.
//
// Delta vs. the original memo's dependency array:
//   + adjustedCurrent / adjustedCurrentLocal — derived from book/bookLocal
//     just above the memo in App.tsx (lines 3108-3109); passed via book.
//   + customerReceipts, sfSalaryOverrides — USED in the body but were
//     missing from the dep array; they are real inputs and included here.
//   - arForecast, monthEndBalances — in the dep array but NOT read in the
//     body; dropped as stale.
//   - getCollPct(i) — inlined as `collPctByMonth[i] ?? 100`.
//
// See src/forecast/forecast-core.d.ts for the input/output shapes and
// scripts/test-forecast-core.cjs for the golden-value gate.
// ============================================================================

/**
 * Compute the 12-month cashflow forecast rows.
 * @param {import('./forecast-core').ForecastInputs} inputs
 * @returns {import('./forecast-core').ForecastRow[]} 12 rows, Jan..Dec of activeYear
 */
function computeCashflowForecast(inputs) {
  const {
    // ── time / config (injected; no wall-clock inside) ──
    activeYear,
    asOfDate = null,
    ilsRevalRate = 3.59,

    // ── balances ──
    book = null,
    bookLocal = null,
    yearStartBalance = null,
    prevMonthEndBalance = null,
    liveFxRate = 0,
    fxRateByYear = {},

    // ── salary ──
    salaryData = [],
    salaryProjectionMode = 'lastActual',
    lastActualSalaryMonth = '',
    salaryActualsByDept = {},
    salaryDeptBudgets = {},
    salaryDeptAdj = {},
    salaryAdjPctByMonth = {},
    sfSalaryOverrides = [],
    sfSalaryBudget = {},
    salaryManualILS = {},
    monthlyHCImpact = {},
    sfActualsSplit = {},

    // ── vendors ──
    vendorBills = [],
    vendorActuals = [],
    nsPaidVendors = { byMonth: {} },
    vendorHistory = [],
    sfBudget = { totalByMonth: {}, byMonth: {} },
    nsBudget = { byMonth: {} },
    expenseCategories = { byMonth: {} },
    vendorCatAdj = {},
    vendorDetailAdj = {},

    // ── collections / revenue ──
    sfRevenuePaid = {},
    actualCollections = {},
    collPctByMonth = {},
    sfRevenue = { budget: {} },
    revenueActuals = [],
    customerReceipts = {},

    // ── pipeline ──
    sfPipeline = [],
    pipelineMinProb = 0,
    sfConversion = { yearly: [] },
    pipelineAdjPctByMonth = {},
    revenueMethodology = 'legacy',
    pipelineMethodology = {},

    // ── churn ──
    sfChurnQuarterly = [],
    churnData = [],
    churnMonthlyAvg = 0,
    churnOverride = {},

    // ── reval / currency defense ──
    monthlyReval = {},
    nsBankClassified = { byMonth: {} },
    currencyDefensePct = 0,
    currencyDefensePctByMonth = {},
    sfFinanceBudget = {},
  } = inputs;

  // Injected "now": the frontend passes asOfDate-derived or real Date; the
  // wall-clock fallback exists only so the frontend stays identical if it
  // omits `now`. Node/tests always pass `inputs.now`.
  const now = asOfDate ? new Date(asOfDate + 'T12:00:00') : (inputs.now || new Date());
  const currentYear = inputs.currentYear != null ? inputs.currentYear : now.getFullYear();

  // getCollPct(i) inlined from App.tsx:1458 (default 100%).
  const getCollPct = (i) => collPctByMonth[i] ?? 100;

  // adjustedCurrent / adjustedCurrentLocal — App.tsx:3108-3109 (just above the memo).
  const adjustedCurrent = book?.adjustedCurrentBalance || book?.currentBalance || 0;
  const adjustedCurrentLocal = bookLocal?.adjustedCurrentBalance || bookLocal?.currentBalance || 0;

  // ────────────────────────────────────────────────────────────────────────
  // Body — verbatim from App.tsx cashflowForecast (TS annotations stripped).
  // ────────────────────────────────────────────────────────────────────────
  const forecastYear = activeYear;
  const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

  // Fallbacks from NS (only used when Snowflake has no data)
  const completedSalaries = salaryData.filter(s => s.month < currentMonth && s.amountEUR > 0);
  const lastSalary = completedSalaries.length > 0 ? completedSalaries[completedSalaries.length - 1].amountEUR : 0;
  const openBillsTotal = vendorBills.reduce((s, b) => s + b.amountEUR, 0);

  // Pipeline impact: filtered opps add recurring MRR from their close month onward
  const filteredPipeline = pipelineMinProb > 0 ? sfPipeline.filter(o => o.probability >= pipelineMinProb) : sfPipeline;
  const pipelineByMonth = {};
  // Low-conf pipeline: opps below threshold, weighted by historical win rate with delay
  // Use calculated values from Snowflake conversion analysis when available
  const recentYears = sfConversion.yearly.filter(y => y.year >= 2023);
  const calcWinRate = recentYears.length > 0 ? Math.round(recentYears.reduce((s, y) => s + y.winRate, 0) / recentYears.length) : 33;
  const calcAvgDays = recentYears.length > 0 ? Math.round(recentYears.reduce((s, y) => s + (y.avgWonDays || 0), 0) / recentYears.length) : 60;
  const pipelineHistWinRate = calcWinRate; // historical close-won ratio %
  const pipelineDelayMonths = Math.max(1, Math.round(calcAvgDays / 30)); // avg days to close → months
  const lowConfPipeline = sfPipeline.filter(o => o.probability < pipelineMinProb && o.probability > 0);
  const pipelineLowByMonth = {};
  for (let mi = 0; mi < 12; mi++) {
    const mKey = `${forecastYear}-${String(mi + 1).padStart(2, '0')}`;
    // Opps closing on or before this month → recurring revenue
    pipelineByMonth[mKey] = filteredPipeline.filter(o => o.closeDate.substring(0, 7) <= mKey).reduce((s, o) => s + o.amount, 0);
    // Low-conf: opps that closed at least delayMonths ago (shifted), weighted by win rate
    const delayedMonth = new Date(forecastYear, mi - pipelineDelayMonths, 1);
    const delayedKey = `${delayedMonth.getFullYear()}-${String(delayedMonth.getMonth() + 1).padStart(2, '0')}`;
    const matchingOpps = lowConfPipeline.filter(o => o.closeDate.substring(0, 7) <= delayedKey);
    const total = matchingOpps.reduce((s, o) => s + o.amount, 0);
    pipelineLowByMonth[mKey] = {
      weighted: Math.round(total * pipelineHistWinRate / 100),
      total,
      count: matchingOpps.length,
      opps: matchingOpps,
    };
  }

  // EUR→ILS ratio from bank balances. For a projection year, an explicit per-year override
  // wins (EUR is the base → ILS = EUR × rate), so the user can set the planning rate.
  const derivedEurIls = (adjustedCurrent > 0 && adjustedCurrentLocal > 0) ? adjustedCurrentLocal / adjustedCurrent : liveFxRate;
  const eurIlsRatio = (forecastYear !== currentYear && (fxRateByYear[forecastYear] || 0) > 0) ? fxRateByYear[forecastYear] : derivedEurIls;

  // Jan 1 opening: prefer NS Bank+CC actual at year-start (yearStartBalance). Falls back to
  // book.openingBalance + cumulative pre-year reval if the NS as-of query hasn't returned yet.
  let runningBalance = yearStartBalance?.eur ?? ((book?.openingBalance || 0) + (monthlyReval.preYear?.eur || 0));
  let runningBalanceILS = yearStartBalance?.ils ?? ((bookLocal?.openingBalance || 0) + (monthlyReval.preYear?.ils || 0));
  const rows = [];
  let prevMonthSalary = 0;
  let prevMonthUnpaid = 0; // unpaid from previous month rolls forward
  // Pipeline-methodology cumulative: monthly MRR (projectedMrr × factor) accumulates
  // forward — a deal closing in July keeps contributing Aug…Dec, so each future month
  // carries all prior future cohorts plus its own. Pyramids up to match cash timing,
  // unlike Column D which front-loads each cohort's full annual total into one month.
  let pipelineMethodCum = 0;

  // Monthly churn run-rate: latest completed quarter / 3, else current-year monthlyImpact, else 6m avg.
  // Each forecast month deducts rate × (forecast-month index), so the cumulative MRR-lost grows
  // alongside the cumulative pipeline wins (which are also <= mKey). Manual overrides bypass.
  let monthlyChurnRate = 0;
  {
    const latestQ = sfChurnQuarterly.filter(q => !q.partial).sort((a, b) => b.qs.localeCompare(a.qs))[0];
    const quarterlyMonthly = latestQ ? Math.round(latestQ.amount / 3) : 0;
    if (quarterlyMonthly > 0) monthlyChurnRate = quarterlyMonthly;
    else {
      const cyChurn = churnData.find(c => c.year === activeYear);
      monthlyChurnRate = cyChurn && cyChurn.monthlyImpact > 0 ? cyChurn.monthlyImpact : churnMonthlyAvg;
    }
  }
  let forecastMonthIndex = 0;

  for (let mi = 0; mi < 12; mi++) {
    const i = mi;
    const d = new Date(forecastYear, mi, 1);
    const mKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    const label = `${monthNames[d.getMonth()]} ${d.getFullYear()}`;
    const isCurMonth = forecastYear === now.getFullYear() && mi === now.getMonth();
    const isPastMonth = forecastYear < now.getFullYear() || (forecastYear === now.getFullYear() && mi < now.getMonth());
    const isClosed = isPastMonth || isCurMonth;
    // Anchor current month to actual previous month-end bank balance (includes FxReval)
    // Only in live mode — in historical (asOfDate) mode, let running balance flow from opening
    if (isCurMonth && prevMonthEndBalance && !asOfDate) {
      runningBalance = prevMonthEndBalance.eur;
      runningBalanceILS = prevMonthEndBalance.ils;
    }
    const openingBalance = runningBalance;
    // Bank-classified data for this month (past months only). Overrides salary/vendors/collections/reval
    // with bank-side truth so closing balance equals NS bank delta by construction.
    const bcm = isPastMonth ? (nsBankClassified.byMonth?.[mKey] || null) : null;

    const monthAdj = salaryAdjPctByMonth[i] || 0;
    const monthMultiplier = 1 + (monthAdj / 100);
    // Per-department salary adjustment delta — cascades from earlier months
    // For each department, use the adjustment from this month if set, otherwise inherit from the most recent earlier month
    const effectiveDeptAdj = {};
    const allAdjMonths = Object.keys(salaryDeptAdj).filter(k => k <= mKey && k.slice(0,4) === mKey.slice(0,4)).sort();
    for (const adjMKey of allAdjMonths) {
      for (const [dept, pct] of Object.entries(salaryDeptAdj[adjMKey])) {
        if (pct !== 0) effectiveDeptAdj[dept] = pct;
        else delete effectiveDeptAdj[dept]; // explicitly set to 0 = clear
      }
    }
    // Dept adjustment delta — uses last-actual dept amounts when in lastActual mode
    let deptAdjDelta = 0;
    const useLastActual = salaryProjectionMode === 'lastActual' && lastActualSalaryMonth && salaryActualsByDept[lastActualSalaryMonth] && mKey > lastActualSalaryMonth;
    const deptBasis = useLastActual ? salaryActualsByDept[lastActualSalaryMonth] : null;
    if (Object.keys(effectiveDeptAdj).length > 0) {
      const deptSource = deptBasis
        ? Object.fromEntries(Object.entries(deptBasis).map(([d, v]) => [d, v.eur]))
        : salaryDeptBudgets[mKey];
      if (deptSource) {
        for (const [dept, pct] of Object.entries(effectiveDeptAdj)) {
          const deptBudget = deptSource[dept] || 0;
          deptAdjDelta += Math.round(deptBudget * (pct / 100));
        }
      }
    }

    // ── SALARY: NS actuals (past/current — full 76xxx GL) → SF actuals → lastActual → SF/NS budget ──
    // NS preferred over Snowflake FCT_EXPENSE because the mart is missing some
    // non-recurring payroll accounts (760017 Bonus, 760019 Maternity).
    let salary;
    let salaryBase; // base salary WITHOUT scenario adjustments (for delta display)
    const actualSalaryEntry = salaryData.find(s => s.month === mKey);
    // NS GL 76xxx direct query (salaryData) is preferred — Snowflake FCT_EXPENSE
    // is missing some non-recurring payroll accounts (760017 Bonus, 760019
    // Maternity). For Jan 2026 that's a €441K bonus that would be dropped.
    if (isClosed && actualSalaryEntry && actualSalaryEntry.amountEUR > 0) {
      salary = actualSalaryEntry.amountEUR;
      salaryBase = salary;
    } else if (isClosed && sfActualsSplit[mKey]?.salary > 0) {
      salary = sfActualsSplit[mKey].salary;
      salaryBase = salary;
    } else if (useLastActual && !isPastMonth) {
      // "Last Actual" mode: project last actual month's recurring salary per dept
      // (applies to current month too if no actual data yet)
      const lastActualBase = Object.values(salaryActualsByDept[lastActualSalaryMonth]).reduce((s, v) => s + v.eur, 0);
      // GSheets salary overrides — applied as fixed delta (manual % does NOT scale them)
      const monthOvrs = sfSalaryOverrides.filter(o => o.mKey === mKey);
      let overrideDelta = 0;
      for (const ov of monthOvrs) {
        if (ov.mode === 'Override') overrideDelta += (ov.newVal - ov.oldVal);
        else overrideDelta += ov.amountEUR;
      }
      // Headcount event impact — applied as fixed delta (manual % does NOT scale it)
      const hcImpactILS = monthlyHCImpact[mKey]?.running || 0;
      const hcImpactEUR = eurIlsRatio > 0 ? Math.round(hcImpactILS / eurIlsRatio) : 0;
      salaryBase = Math.round(lastActualBase + overrideDelta + hcImpactEUR);
      // Manual % applies only to the base; override + HC + dept-adj layer on top
      salary = Math.round(lastActualBase * monthMultiplier) + deptAdjDelta + overrideDelta + hcImpactEUR;
    } else if (sfSalaryBudget[mKey]?.eur > 0) {
      salaryBase = Math.round(sfSalaryBudget[mKey].eur);
      salary = Math.round(sfSalaryBudget[mKey].eur * monthMultiplier) + deptAdjDelta;
    } else if (nsBudget.byMonth[mKey]?.salary > 0) {
      salaryBase = Math.round(nsBudget.byMonth[mKey].salary);
      salary = Math.round(nsBudget.byMonth[mKey].salary * monthMultiplier);
    } else {
      if (actualSalaryEntry && actualSalaryEntry.amountEUR > 0) {
        salary = actualSalaryEntry.amountEUR;
        salaryBase = salary;
      } else if (prevMonthSalary > 0) {
        salaryBase = prevMonthSalary;
        salary = Math.round(prevMonthSalary * monthMultiplier);
      } else {
        salary = lastSalary;
        salaryBase = salary;
      }
    }
    // Apply manual ILS override as additive adjustment on top of calculated salary
    if (salaryManualILS[mKey] !== undefined && !isPastMonth) {
      const manualDeltaEUR = eurIlsRatio > 0 ? Math.round(salaryManualILS[mKey] / eurIlsRatio) : 0;
      salary += manualDeltaEUR;
    }
    prevMonthSalary = salary;

    // ── VENDORS: SF Actuals (past) → NS paid bills → NS vendor actuals → SF/NS budget (future) ──
    // Past months: prefer SF Actuals (FCT_EXPENSE accrual) so the grid matches the Vendor
    // Expenses modal's 'Snowflake Actual (this month)' line. NS paid bills is a cash-basis
    // fallback for subsidiaries / months without SF coverage.
    // Current month: use budget, not partial actuals (bills post throughout the month).
    let vendors;
    // Snowflake Actual (FCT_EXPENSE accrual, cleaned) — preferred for past months on SF-covered subsidiaries.
    // Matches "Snowflake Actual (this month)" in the Vendor modal.
    // NS GL accrual (6xxx+7xxx ex 76xxx) — fallback for subs without SF coverage,
    // matches P&L "Total Overheads − Payroll". Tends to run higher than SF Actual
    // because it includes I/C charges and other line items SF filters out.
    const nsVendorActualGL = isPastMonth ? (vendorActuals.find(v => v.month === mKey)?.amountEUR || 0) : 0;
    const nsPaidByMonth = isPastMonth ? (nsPaidVendors.byMonth[mKey] || 0) : 0;
    const nsVendorActual = isPastMonth ? vendorHistory.filter(v => v.paidDate.startsWith(mKey)).reduce((s, v) => s + v.amountEUR, 0) : 0;
    if (isPastMonth && sfActualsSplit[mKey]?.vendors > 0) {
      vendors = sfActualsSplit[mKey].vendors;
    } else if (isPastMonth && nsVendorActualGL > 0) {
      vendors = nsVendorActualGL;
    } else if (isPastMonth && nsPaidByMonth > 0) {
      vendors = nsPaidByMonth;
    } else if (isPastMonth && nsVendorActual > 0) {
      // NS actual vendor payments (used for non-SF subsidiaries like Statscore)
      vendors = nsVendorActual;
    } else if (sfBudget.totalByMonth[mKey] && Number.isFinite(sfBudget.totalByMonth[mKey].eur)) {
      vendors = Math.round(sfBudget.totalByMonth[mKey].eur);
    } else if (nsBudget.byMonth[mKey]?.vendors) {
      vendors = Math.round(nsBudget.byMonth[mKey].vendors);
    } else if (expenseCategories.byMonth[mKey]) {
      vendors = Object.values(expenseCategories.byMonth[mKey]).reduce((s, v) => s + v, 0);
    } else {
      vendors = isCurMonth ? openBillsTotal : 0;
    }
    let vendorsBase = vendors; // base vendors BEFORE scenario adjustments (reassigned by bank-classified override for past months)

    // Apply per-category vendor adjustments from scenario (includes auto-set HR categories from salary dept adj)
    if (!isPastMonth && Object.keys(vendorCatAdj).length > 0) {
      const effectiveVendorAdj = {};
      const allVendorAdjMonths = Object.keys(vendorCatAdj).filter(k => k <= mKey && k.slice(0,4) === mKey.slice(0,4)).sort();
      for (const adjM of allVendorAdjMonths) {
        for (const [cat, pct] of Object.entries(vendorCatAdj[adjM])) {
          if (pct !== 0) effectiveVendorAdj[cat] = pct;
          else delete effectiveVendorAdj[cat];
        }
      }
      if (Object.keys(effectiveVendorAdj).length > 0) {
        const catData = sfBudget.byMonth?.[mKey] || nsBudget.byMonth[mKey]?.categories || expenseCategories.byMonth?.[mKey] || {};
        let vendorDelta = 0;
        for (const [cat, pct] of Object.entries(effectiveVendorAdj)) {
          const catBudget = catData[cat] || 0;
          const p = Number(pct); // pct may be a mid-typing string ('' / '-') from the input
          if (Number.isFinite(catBudget) && Number.isFinite(p)) vendorDelta += Math.round(catBudget * (p / 100));
        }
        vendors += vendorDelta;
      }
    }

    // Apply per-department vendor detail adjustments from scenario
    if (!isPastMonth && Object.keys(vendorDetailAdj).length > 0) {
      const effectiveDetailAdj = {};
      const allDetailAdjMonths = Object.keys(vendorDetailAdj).filter(k => k <= mKey && k.slice(0,4) === mKey.slice(0,4)).sort();
      for (const adjM of allDetailAdjMonths) {
        for (const [key, val] of Object.entries(vendorDetailAdj[adjM])) {
          if (val.pct !== 0) effectiveDetailAdj[key] = val;
          else delete effectiveDetailAdj[key];
        }
      }
      let detailDelta = 0;
      for (const [, val] of Object.entries(effectiveDetailAdj)) {
        // base / pct can be non-numeric if a scenario stored a mid-typing value
        // (e.g. pct '-' → '-'/100 = NaN). Skip non-finite so one bad line can't
        // poison the whole vendor total (and cascade NaN into closing balances).
        const base = Number(val.base);
        const pct = Number(val.pct);
        if (Number.isFinite(base) && Number.isFinite(pct)) detailDelta += Math.round(base * (pct / 100));
      }
      if (detailDelta !== 0) vendors += detailDelta;
    }

    // ── INFLOWS: NS collections for past/current, SF REVENUE_AMOUNT_EUR for future ──
    // Unpaid carry only from past months where there's real paid data (not future where unpaid=revenue)
    const revPaid = sfRevenuePaid[mKey];
    const actualColl = (isCurMonth || isPastMonth) ? (actualCollections[mKey] || 0) : 0;
    const collPct = getCollPct(i);
    const collMultiplier = collPct / 100;
    let collections;
    let collectionsActual = 0;
    let collectionsRemaining = 0;
    let collectionsForecast = revPaid?.revenue || sfRevenue.budget?.[mKey]?.eur || nsBudget.byMonth[mKey]?.revenue || 0;
    let collectionsRevenue = revPaid?.revenue || nsBudget.byMonth[mKey]?.revenue || 0;
    // Carry is obsolete now that NS inflows use cash-basis on closedate — late payments
    // from prior months naturally land in the month they're paid (and counted there),
    // so adding them again here would double-count.
    let collectionsUnpaidCarry = 0;
    const collectionsUnpaidCarryMonth = '';
    // Pipeline contribution to inflows: ALWAYS legacy win-rate pipeline
    // (pipelineByMonth), independent of the revenueMethodology toggle. The
    // toggle drives the displayed Pipeline column (pipelineWeighted) only, so
    // Inflows (AR) stays stable when switching Win-rate <-> Pipeline.
    const collectionsPipeline = (!isPastMonth && !isCurMonth)
      ? (pipelineByMonth[mKey] || 0)
      : 0;
    const customers = revPaid?.customers || 0;
    // NS GL accrual for revenue (4xxx) — matches P&L "Total - 400000 - REVENUES".
    const nsRevenueActualGL = (isPastMonth || isCurMonth) ? (revenueActuals.find(v => v.month === mKey)?.amountEUR || 0) : 0;
    // Customer cash receipts (CustPymt + CashSale bank debits) — matches bank statement
    // filtered to customer money. Includes AR catch-up from prior periods.
    const nsCustomerReceipts = (isPastMonth || isCurMonth) ? (customerReceipts[mKey] || 0) : 0;
    if (isPastMonth && nsCustomerReceipts > 0) {
      // Past (preferred): real bank deposits from customers
      collections = nsCustomerReceipts;
      collectionsActual = nsCustomerReceipts;
    } else if (isPastMonth && nsRevenueActualGL > 0) {
      // Past fallback: NS GL revenue (accrual basis, matches P&L)
      collections = nsRevenueActualGL;
      collectionsActual = nsRevenueActualGL;
    } else if (isPastMonth && actualColl > 0) {
      // Past last-resort: NS Income credits on cash-basis (incl I/C)
      collections = actualColl;
      collectionsActual = actualColl;
    } else if (isCurMonth && actualColl > 0) {
      // Current: NS actual so far + remaining projected × %
      collectionsActual = actualColl;
      collectionsRemaining = Math.max(0, Math.round(collectionsForecast * collMultiplier) - actualColl);
      collections = actualColl + collectionsRemaining + collectionsUnpaidCarry;
    } else if (collectionsRevenue > 0) {
      // Future: REVENUE_AMOUNT_EUR × collection% + unpaid carry + pipeline impact
      collections = Math.round(collectionsRevenue * collMultiplier) + collectionsUnpaidCarry + collectionsPipeline;
    } else if (collectionsForecast > 0) {
      collections = Math.round(collectionsForecast * collMultiplier) + collectionsPipeline;
    } else {
      collections = collectionsPipeline;
    }
    // Only carry forward unpaid from fully completed past months
    // A month is "complete" if it's past AND most of its revenue was collected (paid > 50% of revenue)
    // This avoids carrying forward months where paid is tiny (just started) or future (paid=null)
    if (isPastMonth && revPaid && revPaid.paid > 0 && revPaid.revenue > 0 && revPaid.paid / revPaid.revenue > 0.5) {
      prevMonthUnpaid = revPaid.unpaid || 0;
    } else {
      prevMonthUnpaid = 0;
    }
    // Low-conf pipeline for this month (not added to collections — shown separately)
    const pipelineLow = pipelineLowByMonth[mKey] || { weighted: 0, total: 0, count: 0, opps: [] };
    const pipelineAdjPct = pipelineAdjPctByMonth[i] ?? 100; // default 100% = full pipeline, 0% = zero
    // The displayed Pipeline column reflects the revenueMethodology toggle:
    //   'legacy'   → historical low-confidence win-rate weighted pipeline
    //   'pipeline' → Column B methodology, CUMULATIVE monthly MRR: each future month
    //                adds projectedMrr × factor (the new MRR coming online) to the
    //                running total of prior future cohorts. Pyramids up to match cash
    //                timing; annual total equals Σ Column D. Future months only.
    //                Drives the column AND its Net contribution. Inflows (AR) is
    //                unaffected (collectionsPipeline stays legacy).
    if (revenueMethodology === 'pipeline' && !isPastMonth && !isCurMonth) {
      pipelineMethodCum += Math.round(pipelineMethodology?.byMonth?.[mKey]?.monthlyContribution || 0);
    }
    const pipelineBaseWeighted = revenueMethodology === 'pipeline'
      ? ((!isPastMonth && !isCurMonth) ? pipelineMethodCum : 0)
      : pipelineLow.weighted;
    const pipelineWeighted = Math.round(pipelineBaseWeighted * pipelineAdjPct / 100);
    const pipelineWeightedILS = Math.round(pipelineWeighted * eurIlsRatio);
    const pipelineTotal = pipelineLow.total;
    const pipelineCount = pipelineLow.count;
    const pipelineOpps = pipelineLow.opps;

    // Prorate current month when as-of date is mid-month
    const daysInMonth = new Date(d.getFullYear(), d.getMonth() + 1, 0).getDate();
    const prorateFactor = (asOfDate && isCurMonth) ? now.getDate() / daysInMonth : 1;
    if (prorateFactor < 1) {
      salary = Math.round(salary * prorateFactor);
      vendors = Math.round(vendors * prorateFactor);
      collections = Math.round(collections * prorateFactor);
    }

    // Bank-classified override (past months only): every NS bank line goes into exactly one
    // bucket (Salary / Vendors / Collections / Reval / Other), so closing balance equals the
    // actual NS month-end bank delta. 'other' surfaces what doesn't fit the first four
    // (manual checks, tax journals, transfers, fees, interest, refunds).
    //
    // Collections is *kept* from the existing NS collection-data source so the grid matches the
    // Revenue Forecast modal's 'NetSuite Collections (actual cash received)' line. The delta
    // between that and the bank-side collections bucket (interest journals, AR-adjustments
    // that the modal classifies separately) is absorbed into 'other' so the closing balance
    // still reconciles to the actual NS bank delta.
    // Past-month override: salary/vendors/collections stay as already computed
    // (NS actuals for salary, SF accrual for vendors, NS collections); the
    // bank-line residual is classified into 'other' so closing balance still
    // reconciles to the NS month-end bank delta.
    let other = 0;
    let otherILS = 0;
    if (bcm) {
      other = -bcm.other.eur;
      otherILS = -bcm.other.ils;
    }
    // Cascade-stopper: never let a non-finite component reach net/closing. A single NaN
    // (from a malformed scenario adjustment or a budget month with an undefined amount)
    // would otherwise propagate net → closing → every later month's opening, blanking
    // the rest of the year. Fall back to the pre-adjustment base (or 0) per component.
    if (!Number.isFinite(salary)) salary = Number.isFinite(salaryBase) ? salaryBase : 0;
    if (!Number.isFinite(vendors)) vendors = Number.isFinite(vendorsBase) ? vendorsBase : 0;
    if (!Number.isFinite(collections)) collections = 0;
    if (!Number.isFinite(other)) other = 0;
    let totalOutflow = salary + vendors + Math.max(0, other);
    // Cumulative churn: every customer that churned in earlier forecast months is still gone,
    // so each month deducts rate × (number of forecast months elapsed). Mirrors the pipeline,
    // which sums all opps closing <= mKey. Manual override per month bypasses the cumulation.
    let churnDeduction = 0;
    if (!isPastMonth && !isCurMonth) {
      forecastMonthIndex++;
      if (churnOverride[mKey] !== undefined) {
        churnDeduction = churnOverride[mKey];
      } else {
        churnDeduction = monthlyChurnRate * forecastMonthIndex;
      }
    }
    const churnDeductionILS = Math.round(churnDeduction * eurIlsRatio);
    let net = collections - salary - vendors - other + pipelineWeighted - churnDeduction;
    // salaryILS picks the same source priority as the salary EUR above
    // (NS actuals → SF actualsSplit) for closed months, else derives.
    let salaryILS;
    if (isClosed && actualSalaryEntry?.amountILS > 0) salaryILS = actualSalaryEntry.amountILS;
    else if (isClosed && sfActualsSplit[mKey]?.salaryILS > 0) salaryILS = sfActualsSplit[mKey].salaryILS;
    else salaryILS = Math.round(salary * eurIlsRatio);
    let vendorsILS = Math.round(vendors * eurIlsRatio); // vendors uses SF Actuals × ratio (matches modal)
    const collectionsILS = Math.round(collections * eurIlsRatio); // always derive from displayed collections × ratio
    // otherILS already set from bcm.other.ils above; no collections-gap adjustment.
    let totalOutflowILS = salaryILS + vendorsILS + Math.max(0, otherILS);
    let netILS = collectionsILS - salaryILS - vendorsILS - otherILS + pipelineWeightedILS - churnDeductionILS;
    runningBalance += net;
    runningBalanceILS += netILS;

    // Revaluation impact:
    //  - Past months: bank-classified FxReval (preferred) or NS GL-derived monthly reval when complete.
    //  - Current / future months: forecast only (currency defense budget × pct). Partial-month NS reval
    //    is ignored because the month isn't closed -- using opening-balance-only reval would project
    //    a misleading large gain/loss onto the row.
    // ── Reval both-ends guard ──
    // A month's FX-reval cycle = an opening reversal (posted the 1st, reversing the prior
    // month's mark) + a new month-end mark (last day). We only recognize a PAST month's reval
    // when BOTH ends are present (monthlyReval.hasBothEnds → reval txns on >=2 distinct dates).
    // This stops a stale/mid-month snapshot that captured only the opening reversal from posting
    // a phantom one-sided swing (e.g. June showing -€2.53M, which was just May's mark being
    // reversed, with June's own +€2.75M month-end mark not yet in the snapshot).
    const revalHasBothEnds = isPastMonth ? (monthlyReval.byMonth?.[mKey]?.hasBothEnds || false) : false;
    const monthlyRevalEur = monthlyReval.byMonth?.[mKey]?.eur || 0;
    const monthlyRevalIls = monthlyReval.byMonth?.[mKey]?.ils || 0;
    // Prefer the bank-classified reval (bcm) — it's the FX impact on the bank accounts
    // specifically — but only when it's a complete cycle AND consistent with the P&L reval.
    // If bcm is absent, or diverges from the P&L reval by far more than a normal month's
    // ~€50-100K gap (a stale single-sided snapshot), fall back to the guarded P&L monthly reval
    // so the row can never show a one-sided phantom.
    let revalImpact = 0, revalImpactILS = 0;
    if (revalHasBothEnds) {
      if (bcm && Math.abs(bcm.reval.eur - monthlyRevalEur) < 500000) {
        // bcm present and consistent with the P&L reval → use the bank-side figure.
        revalImpact = bcm.reval.eur;
        revalImpactILS = bcm.reval.ils || 0;
      } else {
        // bcm absent or a stale single-sided snapshot → use the guarded P&L monthly reval.
        revalImpact = monthlyRevalEur;
        revalImpactILS = monthlyRevalIls;
      }
    }
    // Current + future: currency defense budget × pct
    if (!isPastMonth) {
      // Coerce (the per-month input may hold '' or '-' mid-edit) and allow NEGATIVE pct so the
      // reval projection can be set to a loss: defBudget is positive, so a negative pct yields a
      // negative defenseAmount → negative reval. Number.isFinite guards against '' / '-'.
      const monthPct = Number(currencyDefensePctByMonth[i] ?? currencyDefensePct);
      if (Number.isFinite(monthPct) && monthPct !== 0) {
        let defBudget = 0;
        if (sfFinanceBudget[mKey] && sfFinanceBudget[mKey].eur !== 0) {
          defBudget = Math.abs(sfFinanceBudget[mKey].eur);
        } else {
          const catData = sfBudget.byMonth?.[mKey] || nsBudget.byMonth[mKey]?.categories || {};
          const fin800 = catData['Other (800)'] || 0;
          if (fin800 !== 0) defBudget = Math.abs(fin800);
        }
        if (defBudget > 0) {
          const defenseAmount = Math.round(defBudget * monthPct / 100);
          revalImpact += defenseAmount;
          revalImpactILS += Math.round(defenseAmount * ilsRevalRate);
        }
      }
    }
    runningBalance += revalImpact;
    runningBalanceILS += revalImpactILS;

    const openingBalanceILS = runningBalanceILS - netILS - revalImpactILS;
    // Mode 3: each column independent, Closing = Opening + Inflows - Outflows + Reval
    // (pure row math). No anchor to NS bank balance — Closing may diverge from the
    // NS Balance Sheet by AR/AP timing, but every column reads its own raw source.
    const wcDelta = 0;
    const wcDeltaILS = 0;
    rows.push({ month: label, mKey, openingBalance, openingBalanceILS, salary, salaryBase, salaryILS, vendors, vendorsBase, vendorsILS, other, otherILS, otherDetails: bcm?.details || [], totalOutflow, totalOutflowILS, collections, collectionsILS, collectionsActual, collectionsRemaining, collectionsForecast, collectionsRevenue, collectionsUnpaidCarry, collectionsUnpaidCarryMonth, collectionsPipeline, customers, pipelineWeighted, pipelineWeightedILS, pipelineTotal, pipelineCount, pipelineOpps, pipelineHistWinRate, pipelineDelayMonths, churnDeduction, churnDeductionILS, net, netILS, revalImpact, revalImpactILS, revalHasBothEnds, closingBalance: runningBalance, closingBalanceILS: runningBalanceILS, wcDelta, wcDeltaILS, isCurrent: isCurMonth, isPast: isPastMonth });
  }
  return rows;
}

module.exports = { computeCashflowForecast };
