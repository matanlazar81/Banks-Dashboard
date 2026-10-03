// ─────────────────────────────────────────────────────────────────────────────
// GET /api/cash-projection/breakdown — what makes up one cell of the New Bank Dashboard.
//
//   ?line=collections|pipeline|churn|salary|vendors|other|reval|dividend
//   &period=YYYY-MM | FY-YYYY        a month of the projection, or a full year (sum of its months)
//   &variant=plan|base  &ccy=eur|ils
//
// Every answer adds up to the table cell: the listed rows, then labelled adjustment rows for what the
// rows do not explain (cash timing in actual months, hires/leavers and plan changes in forecast months,
// the collection rate, …). A last "FX conversion difference" row only appears in ₪ when a forecast
// adjustment had to be converted at the month's rate.
//
// Sources:
//   • engine details captured while the projection is computed (captureDetails) and stored in the
//     same cache entry as the payload. They are never sent with /api/cash-projection itself.
//   • Snowflake, read on demand and cached for CASH_PROJECTION_TTL_MIN: NetSuite GL accounts of booked
//     expenses (FCT_EXPENSE) and of the vendor budget (FCT_BUDGET), revenue by customer, and the
//     opportunities churned in a quarter. Each query mirrors the filters of the snowflake-api.cjs
//     function that feeds the matching table line, so the rows describe the same money.
//
// Responses (JSON, Cache-Control: no-store): 200 breakdown | 202 computing | 400 bad parameters |
// 404 period not in the projection | 405 not GET | 200 { ok:false, status:'error' } on a failed read.
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const MIN = 60 * 1000;
const DETAILS_VERSION = 1;

const LINES = ['collections', 'pipeline', 'churn', 'salary', 'vendors', 'other', 'reval', 'dividend'];
const LINE_LABELS = {
  collections: 'Collections (AR)', pipeline: 'Pipeline', churn: 'Churn', salary: 'Salary', vendors: 'Vendors',
  other: 'Other (tax, I/C, fees)', reval: 'Reval (FX)', dividend: 'Dividend paid',
};
const MONTHS_LONG = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

const pad2 = (n) => String(n).padStart(2, '0');
const cents = (n) => Math.round((Number(n) || 0) * 100) / 100;
const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const monthLong = (mKey) => {
  const [y, m] = String(mKey).split('-');
  return `${MONTHS_LONG[Number(m) - 1] || m} ${y}`;
};
const fmtEur = (n) => `€${Math.round(n).toLocaleString('en-US')}`;

class BreakdownError extends Error {}

// ── 1. capture (runs inside computeCashProjection) ──────────────────────────
// Mirrors forecast-core's own guard for a complete bank-classified month (bcmCashValid).
function bankMonthComplete(bcm) {
  return !!bcm && Math.abs((bcm.salary && bcm.salary.eur) || 0) > 50000
    && ((bcm.collections && bcm.collections.eur) || 0) > 50000
    && Number.isFinite(bcm.other && bcm.other.eur);
}

// Which branch of forecast-core produced each line for this month. Only the branch CONDITIONS are
// mirrored (to pick labels and sources); amounts always come from the engine row, and every breakdown
// is tied out to the cell, so a mismatch here can mislabel a row but never break the total.
function sourcesFor(r, inputs) {
  const mKey = r.mKey;
  const bcm = r.isPast ? (inputs.nsBankClassified && inputs.nsBankClassified.byMonth && inputs.nsBankClassified.byMonth[mKey]) || null : null;
  const bank = bankMonthComplete(bcm);
  const isClosed = r.isPast || r.isCurrent;
  const lam = inputs.lastActualSalaryMonth || '';
  const basis = (inputs.salaryProjectionMode || 'lastActual') === 'lastActual' && lam ? (inputs.salaryActualsByDept || {})[lam] : null;
  const basisSum = basis ? Object.values(basis).reduce((s, v) => s + ((v && v.eur) || 0), 0) : 0;
  const useLastActual = !!basis && basisSum > 50000 && mKey > lam;
  const salaryEntry = (inputs.salaryData || []).find((s) => s.month === mKey);
  const split = (inputs.sfActualsSplit || {})[mKey] || {};

  let salary;
  if (r.isPast && bank) salary = 'bank';
  else if (isClosed && salaryEntry && salaryEntry.amountEUR > 0) salary = 'nsActual';
  else if (isClosed && split.salary > 0) salary = 'sfActual';
  else if (useLastActual && !r.isPast) salary = 'lastActual';
  else salary = 'budget';

  let vendors;
  if (r.isPast && bank) vendors = 'bank';
  else if (r.isPast && split.vendors > 0) vendors = 'sfActual';
  else if (r.isPast) vendors = 'nsActual';
  else vendors = 'budget';

  let collections;
  if (r.isPast && bank) collections = 'bank';
  else if (r.isPast) collections = 'receipts';
  else if (r.isCurrent && r.collectionsActual > 0) collections = 'current';
  else collections = 'forecast';

  let reval = 'defense';
  if (r.isPast) {
    const pnl = (inputs.monthlyReval && inputs.monthlyReval.byMonth && inputs.monthlyReval.byMonth[mKey]) || {};
    if (!r.revalHasBothEnds) reval = 'none';
    else if (bcm && bcm.reval && Math.abs(num(bcm.reval.eur) - num(pnl.eur)) < 500000) reval = 'bank';
    else reval = 'pnl';
  }
  return { salary, vendors, collections, reval };
}

// The currency-defense budget and % behind a forecast month's reval (forecast-core, "Current + future").
function defenseFor(inputs, mKey, i) {
  const pct = Number(((inputs.currencyDefensePctByMonth || {})[i]) ?? inputs.currencyDefensePct ?? 0);
  const fin = (inputs.sfFinanceBudget || {})[mKey];
  let budget = 0;
  if (fin && fin.eur !== 0) budget = Math.abs(num(fin.eur));
  else {
    const catData = (inputs.sfBudget && inputs.sfBudget.byMonth && inputs.sfBudget.byMonth[mKey])
      || (inputs.nsBudget && inputs.nsBudget.byMonth && inputs.nsBudget.byMonth[mKey] && inputs.nsBudget.byMonth[mKey].categories) || {};
    budget = Math.abs(num(catData['Other (800)']));
  }
  return { budget, pct: Number.isFinite(pct) ? pct : 0 };
}

function captureYear(rows, inputs) {
  const months = {};
  let forecastIndex = 0;
  rows.forEach((r, i) => {
    const status = r.isPast ? 'actual' : r.isCurrent ? 'current' : 'forecast';
    if (status === 'forecast') forecastIndex++;
    months[r.mKey] = {
      status,
      src: sourcesFor(r, inputs),
      salary: num(r.salary), salaryBase: num(r.salaryBase), salaryILS: num(r.salaryILS),
      vendors: num(r.vendors), vendorsBase: num(r.vendorsBase), vendorsILS: num(r.vendorsILS),
      collections: num(r.collections), collectionsILS: num(r.collectionsILS),
      collectionsActual: num(r.collectionsActual), collectionsRemaining: num(r.collectionsRemaining),
      collectionsRevenue: num(r.collectionsRevenue), collectionsForecast: num(r.collectionsForecast),
      collectionsPipeline: num(r.collectionsPipeline), collPct: num(((inputs.collPctByMonth || {})[i]) ?? 100),
      pipelineWeighted: num(r.pipelineWeighted), pipelineAdjPct: num(((inputs.pipelineAdjPctByMonth || {})[i]) ?? 100),
      churnDeduction: num(r.churnDeduction), churnIndex: status === 'forecast' ? forecastIndex : 0,
      churnOverride: (inputs.churnOverride || {})[r.mKey] !== undefined,
      defense: status === 'actual' ? null : defenseFor(inputs, r.mKey, i),
    };
  });
  const lam = inputs.lastActualSalaryMonth || '';
  const byDept = lam ? (inputs.salaryActualsByDept || {})[lam] : null;
  const pm = inputs.pipelineMethodology || {};
  return {
    months,
    salaryBasis: byDept ? {
      month: lam,
      byDept: Object.fromEntries(Object.entries(byDept).map(([d, v]) => [d, { eur: num(v && v.eur), ils: num(v && v.ils) }])),
    } : null,
    pipeline: {
      factor: num(pm.calibrationFactor),
      byMonth: Object.fromEntries(Object.entries(pm.byMonth || {}).map(([k, v]) => [k, {
        state: v && v.state, projectedMrr: num(v && v.projectedMrr), monthlyContribution: num(v && v.monthlyContribution),
      }])),
    },
    openDeals: {
      minProb: num(inputs.pipelineMinProb),
      deals: (inputs.sfPipeline || []).map((o) => ({
        name: String(o.name || ''), amount: num(o.amount), probability: num(o.probability), closeMonth: String(o.closeDate || '').slice(0, 7),
      })),
    },
  };
}

/**
 * Server-only details for the breakdowns of one computation. Called per variant with the engine rows
 * and inputs of both years; `into` accumulates (first call creates it).
 */
function captureDetails(into, { variant, Y, T, rowsY, rowsT, inputsY, inputsT }) {
  const d = into || { version: DETAILS_VERSION, years: [Y, T], variants: {}, bankLines: {}, dividends: {}, churn: null };
  d.variants[variant] = { [Y]: captureYear(rowsY, inputsY), [T]: captureYear(rowsT, inputsT) };
  if (!into) {
    const bc = (inputsY.nsBankClassified && inputsY.nsBankClassified.byMonth) || {};
    for (const [mKey, bcm] of Object.entries(bc)) {
      if (!String(mKey).startsWith(`${Y}-`) || !bcm || !Array.isArray(bcm.details)) continue;
      d.bankLines[mKey] = bcm.details.map((x) => ({ label: String(x.label || ''), bucket: String(x.bucket || 'other'), eur: num(x.eur), ils: num(x.ils) }));
    }
    const div = (inputsY.dividendExclusions && inputsY.dividendExclusions.byMonth) || {};
    for (const [mKey, x] of Object.entries(div)) {
      d.dividends[mKey] = {
        distEur: Math.round(Math.abs(num(x && x.distributionEUR))), whtEur: Math.round(Math.abs(num(x && x.whtEUR))),
        distIls: Math.round(Math.abs(num(x && x.distributionILS))), whtIls: Math.round(Math.abs(num(x && x.whtILS))),
      };
    }
    const cy = (inputsY.churnData || []).find((c) => c.year === Y);
    d.churn = {
      quarters: (inputsY.sfChurnQuarterly || []).map((q) => ({ q: q.q, qs: q.qs, amount: num(q.amount), opps: num(q.opps), partial: !!q.partial })),
      cyMonthlyImpact: num(cy && cy.monthlyImpact),
      monthlyAvg: num(inputsY.churnMonthlyAvg),
    };
  }
  return d;
}

// ── 2. Snowflake reads (each mirrors the feed of its table line) ────────────
function tables() {
  return require(path.join(ROOT, 'snowflake-api.cjs')).SF_TABLES;
}
const yearOf = (y) => {
  const n = Number(y);
  if (!Number.isInteger(n) || n < 2000 || n > 2100) throw new BreakdownError('Invalid year.');
  return n;
};

// Booked expenses by NetSuite account and month — same table and filters as fetchMonthlyActualsSplit
// (Salary = payroll accounts, Finance = 800xxx, Vendors = the rest).
async function sfExpenseByAccount(sf, year) {
  const y = yearOf(year);
  const T = tables();
  const rows = await sf.query(`
    SELECT TO_VARCHAR(DATE_TRUNC('month', e.CAL_MONTH_START_DATE), 'YYYY-MM') AS M,
           CASE WHEN g.IS_PAYROLL THEN 'Salary'
                WHEN g.GL_ACCOUNT_NUMBER LIKE '800%' THEN 'Finance'
                ELSE 'Vendors' END AS KIND,
           g.PARENT_GL_ACCOUNT_NAME AS CATEGORY,
           g.GL_ACCOUNT_NUMBER AS ACCT,
           g.GL_ACCOUNT_NAME AS NAME,
           SUM(e.AMOUNT_EUR) AS EUR,
           SUM(e.AMOUNT_ILS) AS ILS
    FROM ${T.FCT_EXPENSE} e
    JOIN ${T.DIM_GL_ACCOUNT} g ON e.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
    WHERE e.SUBSIDIARY_ID = 3
      AND e.SOURCE = 'netsuite'
      AND e.CAL_MONTH_START_DATE >= '${y}-01-01'
      AND e.CAL_MONTH_START_DATE < '${y + 1}-01-01'
    GROUP BY 1, 2, 3, 4, 5
  `);
  return rows.map((r) => ({
    month: String(r.M || ''), kind: String(r.KIND || ''), category: String(r.CATEGORY || 'Other'),
    acct: String(r.ACCT || ''), name: String(r.NAME || ''), eur: num(r.EUR), ils: num(r.ILS),
  }));
}

// Vendor budget by NetSuite account and month — same table and filters as fetchBudgetByCategory.
async function sfBudgetByAccount(sf, year) {
  const y = yearOf(year);
  const T = tables();
  const rows = await sf.query(`
    SELECT TO_VARCHAR(b.BUDGET_MONTH_DATE, 'YYYY-MM') AS M,
           g.PARENT_GL_ACCOUNT_NAME AS CATEGORY,
           g.GL_ACCOUNT_NUMBER AS ACCT,
           g.GL_ACCOUNT_NAME AS NAME,
           SUM(b.AMOUNT_EUR_CC) AS EUR,
           SUM(b.AMOUNT_ILS_CC) AS ILS
    FROM ${T.FCT_BUDGET} b
    JOIN ${T.DIM_GL_ACCOUNT} g ON b.GL_ACCOUNT_ID = g.GL_ACCOUNT_ID
    WHERE b.SUBSIDIARY_ID = 3
      AND g.GL_ACCOUNT_TYPE = 'Expense'
      AND g.IS_PAYROLL = FALSE
      AND g.GL_ACCOUNT_NUMBER NOT LIKE '800%'
      AND g.GL_ACCOUNT_NUMBER NOT IN ('780502')
      AND b.BUDGET_MONTH_DATE >= '${y}-01-01'
      AND b.BUDGET_MONTH_DATE <= '${y}-12-31'
    GROUP BY 1, 2, 3, 4
  `);
  return rows.map((r) => ({
    month: String(r.M || ''), category: String(r.CATEGORY || 'Other'), acct: String(r.ACCT || ''),
    name: String(r.NAME || ''), eur: num(r.EUR), ils: num(r.ILS),
  }));
}

// Revenue by customer and month — same table and month filter as fetchMonthlyRevenuePaid (the
// "expected revenue" of a forecast month); names as fetchRevenueBreakdown shows them.
async function sfRevenueByCustomer(sf, year) {
  const y = yearOf(year);
  const T = tables();
  const rows = await sf.query(`
    SELECT TO_VARCHAR(CAL_MONTH_START_DATE, 'YYYY-MM') AS M,
           COALESCE(OPPORTUNITY_NAME, TO_VARCHAR(CUSTOMER_ID), '(no name)') AS NAME,
           SUM(REVENUE_AMOUNT_EUR) AS EUR
    FROM ${T.MONTHLY_REVENUE}
    WHERE CAL_MONTH_START_DATE >= '${y}-01-01' AND CAL_MONTH_START_DATE < '${y + 1}-01-01'
    GROUP BY 1, 2
  `);
  return rows.map((r) => ({ month: String(r.M || ''), name: String(r.NAME || ''), eur: num(r.EUR) }));
}

// Opportunities churned in the quarter starting `qs` — same table and filter as fetchQuarterlyChurnMRR.
async function sfChurnedInQuarter(sf, qs) {
  if (!/^\d{4}-\d{2}-01$/.test(String(qs))) throw new BreakdownError('Invalid quarter.');
  const T = tables();
  const rows = await sf.query(`
    SELECT COALESCE(OPPORTUNITY_NAME, '(no name)') AS NAME, SUM(opportunity_amount) AS AMT
    FROM ${T.DIM_OPPORTUNITY}
    WHERE is_opportunity_churned = TRUE
      AND opportunity_churn_month_start_date >= '${qs}'
      AND opportunity_churn_month_start_date < DATEADD('month', 3, TO_DATE('${qs}'))
    GROUP BY 1
  `);
  return rows.map((r) => ({ name: String(r.NAME || ''), amount: num(r.AMT) }));
}

// ── 3. building blocks ──────────────────────────────────────────────────────
// A row carries both currencies so one build serves € and ₪ and full years can be summed by key.
const item = (key, label, eur, ils, extra = {}) => ({ key, label, eur: num(eur), ils: num(ils), kind: 'item', ...extra });
const adjust = (key, label, eur, ils, hint) => ({ key, label, eur: num(eur), ils: num(ils), kind: 'adjust', hint });
const sum = (rows, ccy) => rows.reduce((s, r) => s + r[ccy], 0);
const ratioOf = (cell) => (Math.abs(cell.eur) >= 0.5 ? cell.ils / cell.eur : 0);

const bySize = (a, b) => Math.abs(b.eur) - Math.abs(a.eur);

// Item rows grouped for display: groups by size, rows by size within each group. Rows without a group
// keep their order after the groups; adjustment rows go last (used after merging months into a year).
function orderRows(rows) {
  const groupTotal = new Map();
  for (const r of rows) if (r.kind === 'item' && r.group) groupTotal.set(r.group, (groupTotal.get(r.group) || 0) + r.eur);
  const grouped = rows.filter((r) => r.kind === 'item' && r.group)
    .sort((a, b) => (Math.abs(groupTotal.get(b.group)) - Math.abs(groupTotal.get(a.group))) || String(a.group).localeCompare(String(b.group)) || bySize(a, b));
  return [...grouped, ...rows.filter((r) => r.kind === 'item' && !r.group), ...rows.filter((r) => r.kind !== 'item')];
}

function accountRows(list, sign = 1) {
  return orderRows(list.map((a) => item(`acct:${a.acct}`, a.name || a.acct, sign * a.eur, sign * a.ils, { ref: a.acct, group: a.category || 'Other' })));
}
function bankRows(lines, bucket, sign) {
  return lines.filter((l) => l.bucket === bucket).map((l) => item(`bank:${l.label}`, l.label, sign * l.eur, sign * l.ils));
}

function bankSection(ctx, bucket, sign, extraRows = []) {
  const lines = ctx.details.bankLines[ctx.mKey] || [];
  const rows = [...bankRows(lines, bucket, sign), ...extraRows];
  if (!rows.length) return null;
  return {
    id: 'bank', title: 'Bank lines (cash, NetSuite)', collapsed: true,
    note: 'The NetSuite bank movements this month was classified into. The table cell is built from these.',
    rows, tie: { key: 'bank-diff', label: 'Other differences' },
  };
}

const TIMING = {
  key: 'timing', label: 'Cash timing (booked vs paid)',
  hint: 'The cell is the cash that left or reached the bank this month (NetSuite bank lines). Booked amounts can be paid in an earlier or later month.',
};

// ── 4. per-line builders: ctx → { sections:[main, …], notes? } (main must explain the cell) ──
async function buildSalary(ctx) {
  const { M, cell, sfx, year } = ctx;
  if (M.status === 'actual' || (M.status === 'current' && (M.src.salary === 'nsActual' || M.src.salary === 'sfActual'))) {
    const accts = (await sfx.expense(year)).filter((a) => a.month === ctx.mKey && a.kind === 'Salary');
    const bank = M.src.salary === 'bank';
    return {
      sections: [{
        id: 'accounts', title: 'Booked payroll by NetSuite account', note: 'Snowflake FCT_EXPENSE, payroll accounts.',
        rows: accountRows(accts),
        tie: bank ? TIMING : {
          key: 'diff', label: 'Difference to NetSuite payroll',
          hint: 'The cell is NetSuite payroll (all 76xxx accounts). Snowflake can miss non-recurring payroll accounts.',
        },
      }, bank ? bankSection(ctx, 'salary', -1) : null],
    };
  }
  const basis = ctx.yearDetails.salaryBasis;
  const ratio = ratioOf(cell);
  if (M.src.salary === 'lastActual' && basis) {
    const deptTotal = Object.values(basis.byDept).reduce((s, v) => s + v.eur, 0);
    let rows;
    let title;
    let note;
    if (/^\d{4}-\d{2}$/.test(basis.month)) {
      const accts = (await sfx.expense(basis.month.slice(0, 4))).filter((a) => a.month === basis.month && a.kind === 'Salary');
      rows = accountRows(accts);
      title = `Basis: ${monthLong(basis.month)} payroll by NetSuite account`;
      note = 'Forecast salary starts from the last closed payroll month (Snowflake FCT_EXPENSE).';
      const diff = deptTotal - sum(rows, 'eur');
      if (Math.abs(diff) >= 0.5) {
        rows.push(adjust('basis-diff', 'Difference to the by-department basis', diff, diff * ratio,
          'The forecast uses this month\'s payroll by department from the same table; accounts without a department make the difference.'));
      }
    } else {
      rows = Object.entries(basis.byDept).map(([dept, v]) => item(`dept:${dept}`, dept || '(no department)', v.eur, v.ils));
      title = 'Run-rate basis by department';
      note = `${ctx.year} salary is the ${ctx.prevYear} October–December payroll by department, flat across the year.`;
    }
    const hc = M.salaryBase - deptTotal;
    const plan = M.salary - M.salaryBase;
    rows.push(adjust('hc', 'Hires, leavers and salary overrides', hc, hc * ratio, 'Headcount events and salary-sheet overrides from this month on.'));
    rows.push(adjust('plan', 'Plan changes (salary %)', plan, plan * ratio, 'The plan\'s salary % and department % for this month, plus any manual ₪ override.'));
    return { sections: [{ id: 'accounts', title, note, rows }] };
  }
  const plan = M.salary - M.salaryBase;
  return {
    sections: [{
      id: 'accounts', title: 'Salary budget', note: 'No usable closed payroll month, so this month uses the salary budget.',
      rows: [item('budget', 'Salary budget (Snowflake)', M.salaryBase, M.salaryBase * ratio), adjust('plan', 'Plan changes (salary %)', plan, plan * ratio)],
    }],
  };
}

async function buildVendors(ctx) {
  const { M, cell, sfx, year } = ctx;
  if (M.status === 'actual') {
    const accts = (await sfx.expense(year)).filter((a) => a.month === ctx.mKey && a.kind === 'Vendors');
    const bank = M.src.vendors === 'bank';
    const div = ctx.details.dividends[ctx.mKey];
    const divRow = div && div.distEur ? [adjust('dividend', 'Dividend distribution (shown under Dividend paid)', -div.distEur, -div.distIls)] : [];
    return {
      sections: [{
        id: 'accounts', title: 'Booked vendor costs by NetSuite account', note: 'Snowflake FCT_EXPENSE: expense accounts except payroll and 800xxx finance.',
        rows: accountRows(accts),
        tie: bank ? TIMING : { key: 'diff', label: 'Difference to the NetSuite vendor figure', hint: 'This month has no complete bank classification; the cell comes from NetSuite.' },
      }, bank ? bankSection(ctx, 'vendors', -1, divRow) : null],
    };
  }
  const ratio = ratioOf(cell);
  if (ctx.yearKind === 'current') {
    const accts = (await sfx.budget(year)).filter((a) => a.month === ctx.mKey);
    const rows = accountRows(accts);
    const ovr = M.vendorsBase - sum(rows, 'eur');
    const plan = M.vendors - M.vendorsBase;
    rows.push(adjust('overrides', 'Budget overrides and other differences', ovr, ovr * ratio, 'Budget overrides from the budget sheet, or budget lines outside these accounts.'));
    rows.push(adjust('plan', 'Plan adjustments', plan, plan * ratio, 'The plan\'s category and account % for vendors.'));
    return { sections: [{ id: 'accounts', title: 'Vendor budget by NetSuite account', note: 'Snowflake FCT_BUDGET.', rows }] };
  }
  // Projection year: each month mirrors the same month of the current year (roll-forward).
  const srcKey = `${ctx.prevYear}-${ctx.mKey.slice(5)}`;
  const srcCtx = makeCtx(ctx.common, srcKey);
  if (!srcCtx) throw new BreakdownError(`${monthLong(srcKey)} is not part of the projection.`);
  const src = await buildMonth(srcCtx);
  const rows = src.sections[0].rows.map((r) => ({ ...r, key: `mirror:${r.key}` }));
  const plan = M.vendors - M.vendorsBase;
  rows.push(adjust('plan', `Plan adjustments (${ctx.year})`, plan, plan * ratio));
  return {
    sections: [{
      id: 'accounts', title: `Same month of ${ctx.prevYear}: ${monthLong(srcKey)}`,
      note: `${ctx.year} vendors repeat ${ctx.prevYear} month by month. The rows below are ${monthLong(srcKey)}'s breakdown.`, rows,
    }],
  };
}

async function buildCollections(ctx) {
  const { M, cell, sfx, year } = ctx;
  const ratio = ratioOf(cell);
  if (M.status === 'actual') {
    if (M.src.collections === 'bank') {
      return {
        sections: [{
          id: 'bank', title: 'Received in the bank (NetSuite bank lines)',
          note: 'Bank lines classified as collections. NetSuite groups them by type, not by customer.',
          rows: bankRows(ctx.details.bankLines[ctx.mKey] || [], 'collections', 1), tie: { key: 'diff', label: 'Other differences' },
        }],
      };
    }
    return { sections: [{ id: 'bank', title: 'Customer receipts', rows: [item('receipts', 'Customer receipts (NetSuite)', cell.eur, cell.ils)] }] };
  }
  if (M.src.collections === 'current') {
    return {
      sections: [{
        id: 'forecast', title: 'This month so far and still expected',
        rows: [
          item('actual', 'Collected so far this month (NetSuite)', M.collectionsActual, M.collectionsActual * ratio),
          item('remaining', 'Expected in the rest of the month', M.collectionsRemaining, M.collectionsRemaining * ratio),
        ],
      }],
    };
  }
  const revenueUsed = M.collectionsRevenue > 0 ? M.collectionsRevenue : M.collectionsForecast;
  let rows;
  if (ctx.yearKind === 'current') {
    rows = (await sfx.revenue(year)).filter((r) => r.month === ctx.mKey)
      .map((r) => item(`cust:${r.name}`, r.name, r.eur, r.eur * ratio, { group: 'Expected revenue by customer' }))
      .sort(bySize);
    const diff = revenueUsed - sum(rows, 'eur');
    if (Math.abs(diff) >= 0.5) rows.push(adjust('rev-diff', 'Difference to the revenue used', diff, diff * ratio));
  } else {
    rows = [item('avg', `Expected revenue: ${ctx.prevYear} October–December average collections`, revenueUsed, revenueUsed * ratio)];
  }
  const pctEffect = Math.round(revenueUsed * M.collPct / 100) - revenueUsed;
  if (Math.abs(pctEffect) >= 0.5) rows.push(adjust('coll-pct', `Collection rate (${M.collPct}%)`, pctEffect, pctEffect * ratio, 'Share of the month\'s expected revenue assumed to be collected in the month.'));
  if (M.collectionsPipeline) {
    const od = ctx.yearDetails.openDeals;
    const deals = od.deals.filter((o) => (od.minProb > 0 ? o.probability >= od.minProb : true) && o.closeMonth && o.closeMonth <= ctx.mKey);
    rows.push(...deals.map((o) => item(`deal:${o.name}|${o.closeMonth}|${o.amount}`, o.name || '(no name)', o.amount, o.amount * ratio, {
      group: 'Open pipeline deals (full amount from the close month)', ref: `${o.probability}% · closes ${o.closeMonth}`,
    })).sort(bySize));
    const diff = M.collectionsPipeline - deals.reduce((s, o) => s + o.amount, 0);
    if (Math.abs(diff) >= 0.5) rows.push(adjust('deal-diff', 'Difference to the pipeline used', diff, diff * ratio));
  }
  return {
    sections: [{
      id: 'forecast', title: 'Expected collections',
      note: ctx.yearKind === 'current' ? 'Expected revenue by customer (Snowflake) × collection rate, plus open pipeline deals.' : undefined, rows,
    }],
  };
}

async function buildPipeline(ctx) {
  const { M, cell } = ctx;
  const ratio = ratioOf(cell);
  const pm = ctx.yearDetails.pipeline;
  const rows = [];
  for (const [mKey, m] of Object.entries(ctx.yearMonths)) {
    if (m.status !== 'forecast' || mKey > ctx.mKey) continue;
    const p = pm.byMonth[mKey];
    if (!p || !p.monthlyContribution) continue;
    rows.push(item(`cohort:${mKey}`, `New MRR from ${monthLong(mKey)}`, p.monthlyContribution, p.monthlyContribution * ratio, {
      ref: p.projectedMrr > 0 && pm.factor > 0 ? `${fmtEur(p.projectedMrr)} × ${pm.factor.toFixed(2)}` : undefined,
    }));
  }
  const pct = cell.eur - sum(rows, 'eur');
  if (Math.abs(pct) >= 0.5) rows.push(adjust('pct', `Pipeline % (${M.pipelineAdjPct}%)`, pct, pct * ratio, 'The plan\'s pipeline % for this month.'));
  return {
    sections: [{
      id: 'cohorts', title: 'New business from the open pipeline, cumulative',
      note: 'Each forecast month adds its projected new MRR (stage-weighted open pipeline, at least 80% of last year\'s wins for later quarters) × the calibration factor, and keeps it in later months.',
      rows,
    }],
  };
}

async function buildChurn(ctx) {
  const { M, cell, sfx } = ctx;
  const ratio = ratioOf(cell);
  if (M.churnOverride) return { sections: [{ id: 'calc', title: 'Churn deduction', rows: [item('override', 'Manual churn override', cell.eur, cell.ils)] }] };
  const n = M.churnIndex || 1;
  const rate = M.churnDeduction / n;
  const ch = ctx.details.churn || { quarters: [] };
  const latestQ = ch.quarters.filter((q) => !q.partial).sort((a, b) => String(b.qs).localeCompare(String(a.qs)))[0];
  const fromQuarter = latestQ && Math.round(latestQ.amount / 3) > 0;
  const qStart = fromQuarter ? (/^\d{4}-\d{2}$/.test(latestQ.qs) ? `${latestQ.qs}-01` : String(latestQ.qs).slice(0, 10)) : '';
  const qLabel = fromQuarter ? (latestQ.q || `the quarter from ${monthLong(qStart.slice(0, 7))}`) : '';
  const source = fromQuarter ? `${qLabel} churned MRR ${fmtEur(latestQ.amount)} ÷ 3` : (ch.cyMonthlyImpact > 0 ? 'this year\'s monthly churn impact' : 'recent monthly average');
  const sections = [{
    id: 'calc', title: 'Churn deduction',
    note: 'Customers lost in earlier forecast months stay lost, so the deduction grows each forecast month.',
    rows: [item('calc', `Monthly churn run-rate × ${n} forecast month${n === 1 ? '' : 's'}`, -M.churnDeduction, -M.churnDeduction * ratio, {
      ref: `${fmtEur(rate)} × ${n} (run-rate: ${source})`,
    })],
  }];
  const notes = [];
  if (fromQuarter) {
    // Context only, so a failed read becomes a note instead of failing the whole breakdown.
    const lost = await sfx.churned(qStart).catch((e) => {
      if (!(e instanceof BreakdownError)) console.error(`[cash-projection] churned customers ${qStart}: ${e && e.message}`);
      notes.push('The list of churned customers could not be loaded from Snowflake.');
      return null;
    });
    if (lost) {
      sections.push({
        id: 'customers', title: `Customers lost in ${qLabel} (Snowflake)`, informational: true,
        note: 'The run-rate is this quarter\'s churned MRR ÷ 3. Monthly amounts.',
        rows: lost.map((c) => item(`cust:${c.name}`, c.name, c.amount, c.amount * ratio)).sort(bySize),
      });
    }
  }
  return { sections, notes };
}

async function buildOther(ctx) {
  const div = ctx.details.dividends[ctx.mKey];
  const rows = bankRows(ctx.details.bankLines[ctx.mKey] || [], 'other', -1);
  if (div && div.whtEur) rows.push(adjust('wht', 'Dividend withholding tax (shown under Dividend paid)', -div.whtEur, -div.whtIls));
  return {
    sections: [{
      id: 'bank', title: 'Bank lines (cash, NetSuite)', rows, tie: { key: 'diff', label: 'Other differences' },
      note: 'Bank movements outside salary, vendors, collections and FX. Outflows positive, inflows in parentheses.',
    }],
  };
}

async function buildReval(ctx) {
  const { M, cell } = ctx;
  if (M.status === 'actual') {
    if (M.src.reval === 'bank') {
      return {
        sections: [{
          id: 'bank', title: 'FX revaluation on the bank accounts (NetSuite bank lines)',
          rows: bankRows(ctx.details.bankLines[ctx.mKey] || [], 'reval', 1), tie: { key: 'diff', label: 'Other differences' },
        }],
      };
    }
    return { sections: [{ id: 'bank', title: 'Booked FX revaluation', rows: [item('pnl', 'FX revaluation (NetSuite P&L)', cell.eur, cell.ils)] }] };
  }
  const d = M.defense || { budget: 0, pct: 0 };
  return {
    sections: [{
      id: 'calc', title: 'Currency defense', note: 'Forecast reval = the currency-defense budget × the plan\'s defense %.',
      rows: [item('defense', 'Currency-defense budget × defense %', cell.eur, cell.ils, { ref: `${fmtEur(d.budget)} × ${d.pct}%` })],
    }],
  };
}

async function buildDividend(ctx) {
  const div = ctx.details.dividends[ctx.mKey] || { distEur: 0, whtEur: 0, distIls: 0, whtIls: 0 };
  return {
    sections: [{
      id: 'dividend', title: 'Dividend paid', note: 'NetSuite dividend distributions and the withholding tax paid on them.',
      rows: [item('dist', 'Dividend distribution', -div.distEur, -div.distIls), item('wht', 'Withholding tax on the dividend', -div.whtEur, -div.whtIls)],
    }],
  };
}

const BUILDERS = {
  salary: buildSalary, vendors: buildVendors, collections: buildCollections, pipeline: buildPipeline,
  churn: buildChurn, other: buildOther, reval: buildReval, dividend: buildDividend,
};

// The displayed cell of a line for one month (same signs as the table: churn and dividends negative).
function cellOf(line, row) {
  const v = (ccy) => {
    const f = row[ccy];
    switch (line) {
      case 'churn': return -f.churn;
      case 'dividend': return -f.dividend;
      default: return f[line];
    }
  };
  return { eur: num(v('eur')), ils: num(v('ils')) };
}

// Ties every section that asks for it (the main one always does) to its target: residual per currency.
function tieOut(section, target, fallbackTie) {
  const tie = section.tie || fallbackTie;
  const de = target.eur - sum(section.rows, 'eur');
  const di = target.ils - sum(section.rows, 'ils');
  if (Math.abs(de) >= 0.5 || Math.abs(di) >= 0.5) section.rows.push(adjust(tie.key, tie.label, de, di, tie.hint));
}

const FX_TIE = { key: 'fx', label: 'FX conversion difference', hint: 'Forecast adjustments are converted to ₪ at the month\'s rate; booked ₪ amounts were converted at their own rates.' };

async function buildMonth(ctx) {
  const out = await BUILDERS[ctx.line](ctx);
  const sections = out.sections.filter(Boolean);
  if (!sections.length) throw new BreakdownError('Nothing to break down for this cell.');
  tieOut(sections[0], ctx.cell, FX_TIE);
  for (const s of sections.slice(1)) if (s.tie) tieOut(s, ctx.cell, s.tie);
  return { sections, notes: out.notes || [] };
}

// Titles of a full year's main section, where its months explain the line in different ways.
const FY_TITLES = {
  salary: 'By NetSuite account (booked in actual months, basis month in forecast months)',
  vendors: 'By NetSuite account (booked in actual months, budget in forecast months)',
  collections: 'Received in actual months, expected in forecast months',
  reval: 'Booked FX in actual months, currency defense in forecast months',
};

// Full year: each month's breakdown merged by row key. Every month's main section (which adds up to
// its cell) goes into the year's main section, so that one adds up to the FY cell; the secondary
// sections merge by id.
async function buildYear(ctx) {
  const main = { id: 'main', title: '', rows: [] };
  const secondary = [];
  const mainIds = new Set();
  const notes = new Set();
  let months = 0;
  const mergeRows = (target, rows) => {
    for (const r of rows) {
      const hit = target.rows.find((x) => x.key === r.key);
      if (hit) { hit.eur += r.eur; hit.ils += r.ils; } else target.rows.push({ ...r });
    }
  };
  for (const [mKey, row] of ctx.yearRowsByKey) {
    if (Math.abs(cellOf(ctx.line, row)[ctx.ccy]) < 0.5) continue;
    months++;
    const one = await buildMonth(makeCtx(ctx.common, mKey));
    one.notes.forEach((n) => notes.add(n));
    const [first, ...rest] = one.sections;
    if (!mainIds.size) Object.assign(main, { id: first.id, title: first.title });
    mainIds.add(first.id);
    mergeRows(main, first.rows);
    for (const s of rest) {
      let target = secondary.find((m) => m.id === s.id);
      if (!target) { target = { ...s, rows: [] }; secondary.push(target); }
      mergeRows(target, s.rows);
    }
  }
  if (!months) throw new BreakdownError('Nothing to break down for this year.');
  if (ctx.yearKind === 'current' && FY_TITLES[ctx.line] && (mainIds.size > 1 || mainIds.has('accounts'))) main.title = FY_TITLES[ctx.line];
  if (ctx.yearKind === 'projection' && ctx.line === 'vendors') main.title = `Same months of ${ctx.prevYear}, by NetSuite account`;
  const sections = [main, ...secondary];
  for (const s of sections) {
    s.note = `Sum of the ${months} month${months === 1 ? '' : 's'} with an amount.`;
    s.rows = orderRows(s.rows);
  }
  return { sections, notes: [...notes] };
}

// Everything a builder needs about one period. common: { entry, line, variant, ccy, sfx }. null when
// the period is not part of the projection.
function makeCtx(common, period) {
  const { entry, line, variant } = common;
  const fyMatch = /^FY-(\d{4})$/.exec(period);
  const year = fyMatch ? Number(fyMatch[1]) : Number(String(period).slice(0, 4));
  const block = entry.payload.variants[variant].years.find((y) => y.year === year);
  const yearDetails = entry.details.variants[variant] && entry.details.variants[variant][year];
  if (!block || !yearDetails) return null;
  const ctx = {
    ...common, common, details: entry.details, year, prevYear: year - 1, yearKind: block.kind,
    yearDetails, yearMonths: yearDetails.months, yearRowsByKey: new Map(block.rows.map((r) => [r.mKey, r])),
    period, fy: !!fyMatch, mKey: null,
  };
  if (fyMatch) {
    ctx.cell = block.rows.reduce((s, r) => { const c = cellOf(line, r); return { eur: s.eur + c.eur, ils: s.ils + c.ils }; }, { eur: 0, ils: 0 });
    return ctx;
  }
  const row = ctx.yearRowsByKey.get(period);
  if (!row || !yearDetails.months[period]) return null;
  return Object.assign(ctx, { mKey: period, row, M: yearDetails.months[period], cell: cellOf(line, row) });
}

/** One breakdown from a cache entry. sfx: { expense(year), budget(year), revenue(year), churned(qs) }. */
async function buildBreakdown({ entry, line, period, variant, ccy, sfx }) {
  const ctx = makeCtx({ entry, line, variant, ccy, sfx }, period);
  if (!ctx) return null;
  const built = ctx.fy ? await buildYear(ctx) : await buildMonth(ctx);
  const { cell, year } = ctx;
  const fyMatch = ctx.fy;
  // Project to the requested currency; hide rows that round to nothing.
  const sections = built.sections.map((s) => {
    const rows = s.rows
      .map((r) => ({ key: r.key, label: r.label, ref: r.ref || null, group: r.group || null, kind: r.kind, hint: r.hint || null, amount: cents(r[ccy]) }))
      .filter((r) => Math.abs(r.amount) >= 0.5);
    return {
      id: s.id, title: s.title, note: s.note || null, collapsed: !!s.collapsed, informational: !!s.informational,
      rows, total: cents(rows.reduce((t, r) => t + r.amount, 0)),
    };
  });
  const status = fyMatch ? 'fy' : ctx.M.status;
  return {
    ok: true, status: 'ready', line, lineLabel: LINE_LABELS[line], period,
    periodLabel: fyMatch ? `FY ${year}` : monthLong(period), periodStatus: status, variant, ccy,
    cell: cents(cell[ccy]), sections, notes: built.notes,
  };
}

// ── 5. handler ──────────────────────────────────────────────────────────────
function envMinutes(name, fallback) {
  const v = parseFloat(process.env[name] || '');
  return Number.isFinite(v) && v > 0 ? v : fallback;
}

// Snowflake reads shared by all requests, each cached per argument for ttlMs (a failure is not cached).
function snowflakeReads(getSf, clock, ttlMs) {
  const memo = new Map();
  const cached = (name, fn) => (arg) => {
    const key = `${name}:${arg}`;
    const hit = memo.get(key);
    if (hit && clock() - hit.at < ttlMs) return hit.p;
    const sf = getSf();
    if (!sf) return Promise.reject(new BreakdownError('Snowflake is not configured on this server.'));
    const p = fn(sf, arg);
    memo.set(key, { at: clock(), p });
    p.catch(() => { if (memo.get(key) && memo.get(key).p === p) memo.delete(key); });
    return p;
  };
  return {
    expense: cached('expense', sfExpenseByAccount),
    budget: cached('budget', sfBudgetByAccount),
    revenue: cached('revenue', sfRevenueByCustomer),
    churned: cached('churned', sfChurnedInQuarter),
  };
}

/**
 * Express/connect handler for GET /api/cash-projection/breakdown. deps (all optional):
 *   getSfClient()  — share the API module's Snowflake client
 *   sfx            — override all Snowflake reads (tests): { expense, budget, revenue, churned }
 *   cacheFile      — the projection cache (default: the one server/cash-projection.cjs writes)
 *   clock(), ttlMs
 */
function createCashProjectionBreakdownHandler(deps = {}) {
  const cp = require('./cash-projection.cjs');
  const clock = deps.clock || Date.now;
  const ttlMs = deps.ttlMs ?? envMinutes('CASH_PROJECTION_TTL_MIN', 30) * MIN;
  const cacheFile = deps.cacheFile === undefined ? cp.DEFAULT_CACHE_FILE : deps.cacheFile;
  const getSf = deps.getSfClient || (() => {
    // Loads the checkout's .env (as the projection does) before the default client reads it.
    require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs'));
    return cp.defaultGetSfClient();
  });
  const sfx = deps.sfx || snowflakeReads(getSf, clock, ttlMs);
  const state = { entry: null, mtimeMs: 0 };

  function currentEntry() {
    let mtimeMs;
    try { mtimeMs = fs.statSync(cacheFile).mtimeMs; } catch { return state.entry; }
    if (mtimeMs > state.mtimeMs) {
      state.mtimeMs = mtimeMs;
      const e = cp.readCacheEntry(cacheFile);
      if (e) state.entry = e;
    }
    return state.entry;
  }

  function send(res, status, body) {
    res.statusCode = status;
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'no-store');
    res.end(JSON.stringify(body));
  }

  return async function breakdownHandler(req, res) {
    if ((req.method || 'GET').toUpperCase() !== 'GET') {
      res.setHeader('Allow', 'GET');
      send(res, 405, { ok: false, status: 'error', error: 'Method not allowed' });
      return;
    }
    let q;
    try { q = new URL(req.url || '', 'http://localhost').searchParams; } catch { q = new URLSearchParams(); }
    const line = q.get('line') || '';
    const period = q.get('period') || '';
    const variant = q.get('variant') || 'plan';
    const ccy = q.get('ccy') || 'eur';
    if (!LINES.includes(line) || !/^(\d{4}-(0[1-9]|1[0-2])|FY-\d{4})$/.test(period) || !['plan', 'base'].includes(variant) || !['eur', 'ils'].includes(ccy)) {
      send(res, 400, { ok: false, status: 'error', error: 'Unknown line, period, variant or currency.' });
      return;
    }
    const entry = currentEntry();
    if (!entry || !entry.details || entry.details.version !== DETAILS_VERSION) {
      send(res, 202, { ok: true, status: 'computing' });
      return;
    }
    try {
      const out = await buildBreakdown({ entry, line, period, variant, ccy, sfx });
      if (!out) { send(res, 404, { ok: false, status: 'error', error: 'This period is not part of the projection.' }); return; }
      send(res, 200, { ...out, generatedAt: entry.payload.generatedAt });
    } catch (e) {
      const safe = e instanceof BreakdownError;
      if (!safe) console.error(`[cash-projection] breakdown ${line} ${period} failed: ${e && e.stack ? e.stack : e}`);
      send(res, 200, { ok: false, status: 'error', error: safe ? e.message : 'The breakdown could not be loaded. The server log has the details.' });
    }
  };
}

module.exports = {
  createCashProjectionBreakdownHandler,
  captureDetails,
  buildBreakdown,
  BreakdownError,
  LINES,
  DETAILS_VERSION,
};
