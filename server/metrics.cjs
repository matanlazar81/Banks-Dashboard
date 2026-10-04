// ─────────────────────────────────────────────────────────────────────────────
// GET /api/metrics — the Metrics page (Business Tools → Metrics): one short pack, the same shape every
// month, computed live from what the dashboards already have. Every figure says whether it is actual
// or forecast.
//
//   Revenue, EBITDA       P&L Projection, Plan (NetSuite actual months, engine forecast)
//   Net cash              New Bank Dashboard, Plan: bank at the last month-end (actual), December
//                         closings (forecast). There is no debt data, so net cash is the cash in the bank.
//   ARR                   Snowflake MRR × 12 now (actual); December customer revenue × 12 (forecast)
//   NRR                   Snowflake revenue by customer: this month's revenue from customers who had
//                         revenue 12 months earlier ÷ their revenue then (GRR caps each at its old amount)
//   Churn                 churned MRR by quarter (Snowflake), and the revenue the forecast loses to churn
//   Cloud vs cap          cloud (NetSuite 640xxx; forecast: the budget's cloud share of operating
//                         expenses) against cap % × projected FY revenue
//   Payroll / revenue,    each closed month through the last one whose payroll JE is posted in NetSuite
//   revenue per employee  (76xxxx, gross of capitalised salaries; P&L total revenue), plus year to date;
//                         employees = HiBob (Snowflake) employees of the P&L's company active at month-end
//   Projection year       the 2027 targets saved on the New Bank Dashboard apply, as in both pages'
//                         Targets view (src/forecast/targets.mjs): revenue, EBITDA, December cash, ARR,
//                         and cloud (server costs as a % of revenue, or the category's % change)
//   Innovation envelope   an amount for a year, spread over its forecast months from a start month, in
//                         or out of the forecast (EBITDA and net cash are shown both ways)
//   USD/EUR planning rate saved setting, next to today's ECB rate
//   FX conversions        last month's NetSuite transfers between accounts of different currencies
//   Deposit confirmations the page's tracker: deposits whose confirmation has not come back
//
// The projections come from their cached handlers (no second NetSuite pull); the other reads are cached
// for METRICS_TTL_MIN (default 30). GET/PUT /api/metrics/settings and /api/metrics/deposits save what
// the page edits (server/json-store.cjs, server/metrics-settings.cjs).
// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');
const { readDoc, createDocHandler, send } = require('./json-store.cjs');
const { emptySettings, validateSettings, validateDeposits } = require('./metrics-settings.cjs');
const projectionTargets = require('./projection-targets.cjs');

const ROOT = path.resolve(__dirname, '..');
const SETTINGS_FILE = path.join(ROOT, 'data', 'metrics-settings.json');
const DEPOSITS_FILE = path.join(ROOT, 'data', 'metrics-deposits.json');
const MIN = 60 * 1000;
const MONTHS_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const round2 = (n) => Math.round(num(n) * 100) / 100;
const pad2 = (n) => String(n).padStart(2, '0');
const monthShort = (mKey) => `${MONTHS_SHORT[Number(String(mKey).slice(5)) - 1]} ${String(mKey).slice(0, 4)}`;
/** 'YYYY-MM' shifted by n months. */
function shiftMonth(mKey, n) {
  const [y, m] = String(mKey).split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}`;
}
/** The last closed month at nowMs. */
const lastClosedMonth = (nowMs) => {
  const d = new Date(nowMs);
  return shiftMonth(`${d.getFullYear()}-${pad2(d.getMonth() + 1)}`, -1);
};
const cell = (value, status, label, extra = {}) => ({ value: value === null ? null : round2(value), status, label, ...extra });
const isActual = (r) => r.status === 'actual';
const sumOf = (rows, f) => rows.reduce((s, r) => s + num(f(r)), 0);

// ── innovation envelope ─────────────────────────────────────────────────────
/** € per month the envelope adds to costs: its amount spread evenly from the start month to December,
 *  applied to the year's forecast months only (a closed month keeps what was booked). */
function envelopeMonths(innovation, blocks, defaultYear) {
  const out = new Map();
  const year = innovation.year || defaultYear;
  const amount = num(innovation.amountEur);
  if (!amount) return { byMonth: out, year, monthly: 0 };
  const start = innovation.startMonth || 1;
  const monthly = amount / (13 - start);
  const block = blocks.find((b) => b.year === year);
  for (const r of block ? block.rows : []) {
    if (Number(r.mKey.slice(5)) >= start && !isActual(r)) out.set(r.mKey, monthly);
  }
  return { byMonth: out, year, monthly };
}

// ── cloud ───────────────────────────────────────────────────────────────────
function cloudCategoryOf(settings, categories, targetsBase) {
  if (settings.cloudCategory) return settings.cloudCategory;
  return categories.find((c) => /cloud/i.test(c)) || (targetsBase && targetsBase.serverCategory) || '';
}
const shareOf = (cats, name) => {
  const total = Object.values(cats || {}).reduce((s, v) => s + num(v && typeof v === 'object' ? v.eur : v), 0);
  const v = cats ? cats[name] : 0;
  return Math.abs(total) >= 0.5 ? num(v && typeof v === 'object' ? v.eur : v) / total : 0;
};

/**
 * The projection year's cloud under the saved targets, per month: server costs set as a % of revenue
 * replace the category (the month's `server` of computeTargetDeltas); otherwise the category's % change
 * scales it. null when the targets leave the cloud category alone.
 */
function cloudTargetsOf(targets, deltas, category) {
  if (!targets || !deltas || !category) return null;
  const server = !!(targets.server.enabled && targets.server.category === category);
  const pct = num(targets.opex.categoryPct[category]);
  if (!server && !pct) return null;
  const byKey = new Map(deltas.map((d) => [d.mKey, d]));
  return {
    kind: server ? 'server' : 'category', pct: server ? num(targets.server.pctOfRevenue) : pct,
    adjust: (mKey, baseline) => {
      const d = byKey.get(mKey);
      if (!d) return baseline;
      return server ? num(d.server) : baseline * (1 + pct / 100);
    },
  };
}

/** One year of cloud: NetSuite 640xxx in closed months, the budget's cloud share of opex in the others
 *  (block: the Plan's year; withTargets: the projection year's targets, cloudTargetsOf). */
function cloudYear({ block, details, targetsBase, category, capPct, revenue, withTargets = null }) {
  let actual = 0;
  let forecast = 0;
  block.rows.forEach((r, i) => {
    if (isActual(r)) {
      const opex = ((details.accounts || {})[r.mKey] || {}).opex || [];
      actual += opex.filter((a) => String(a.acct).startsWith('640')).reduce((s, a) => s - num(a.eur), 0);
      return;
    }
    const capM = (((details.variants || {}).plan || {})[block.year] || {}).months || {};
    const cats = capM[r.mKey] && capM[r.mKey].categories;
    let month = 0;
    if (cats) month = num(r.eur.opex) * shareOf(cats, category);
    else if (targetsBase && targetsBase.year === block.year && targetsBase.months[i]) month = num(targetsBase.months[i].opexByCategory[category]);
    forecast += withTargets ? withTargets.adjust(r.mKey, month) : month;
  });
  const total = actual + forecast;
  const cap = (capPct / 100) * revenue;
  return {
    year: block.year, status: actual && forecast ? 'actual+forecast' : forecast ? 'forecast' : 'actual',
    actual: round2(actual), forecast: round2(forecast), total: round2(total), revenue: round2(revenue),
    capPct, cap: round2(cap), headroom: round2(cap - total), pctOfRevenue: revenue ? Math.round((total / revenue) * 10000) / 100 : null,
    within: total <= cap,
    targets: withTargets ? { kind: withTargets.kind, pct: withTargets.pct } : null,
  };
}

// ── NRR ─────────────────────────────────────────────────────────────────────
/** NRR and GRR for each month from revenue by customer: [{ month, customer, rev }] (actual months). */
function nrrSeries(rows, months) {
  const by = new Map();
  for (const r of rows) {
    if (!by.has(r.month)) by.set(r.month, new Map());
    const m = by.get(r.month);
    m.set(r.customer, (m.get(r.customer) || 0) + num(r.rev));
  }
  return months.map((month) => {
    const before = by.get(shiftMonth(month, -12));
    const now = by.get(month);
    if (!before || !now) return null;
    let base = 0;
    let kept = 0;
    let gross = 0;
    let customers = 0;
    for (const [c, v] of before) {
      if (v <= 0) continue;
      const cur = Math.max(0, now.get(c) || 0);
      base += v;
      kept += cur;
      gross += Math.min(cur, v);
      customers++;
    }
    return base > 0 ? { month, nrr: Math.round((kept / base) * 10000) / 100, grr: Math.round((gross / base) * 10000) / 100, customers } : null;
  }).filter(Boolean);
}

/** Revenue by customer and month (actual months, test customers excluded), from..to inclusive. */
async function sfCustomerRevenue(sf, from, to) {
  if (!/^\d{4}-\d{2}$/.test(from) || !/^\d{4}-\d{2}$/.test(to)) throw new Error('sfCustomerRevenue: bad months');
  const T = require(path.join(ROOT, 'snowflake-api.cjs')).SF_TABLES;
  const rows = await sf.query(`
    SELECT TO_VARCHAR(DATE_TRUNC('month', m.CAL_MONTH_START_DATE), 'YYYY-MM') AS M,
           m.CUSTOMER_ID AS C,
           SUM(m.REVENUE) AS REV
    FROM ${T.FCT_CUSTOMER_MONTHLY} m
    JOIN ${T.DIM_CUSTOMER} c ON c.CUSTOMER_ID = m.CUSTOMER_ID
    WHERE m.DATE_STATUS = 'actual'
      AND c.IS_TEST = FALSE
      AND m.CAL_MONTH_START_DATE >= '${from}-01'
      AND m.CAL_MONTH_START_DATE < '${shiftMonth(to, 1)}-01'
    GROUP BY 1, 2
  `);
  return rows.map((r) => ({ month: String(r.M || ''), customer: String(r.C || ''), rev: num(r.REV) }));
}

// ── payroll and revenue per employee ────────────────────────────────────────
const lastDayOf = (mKey) => `${mKey}-${pad2(new Date(Date.UTC(Number(mKey.slice(0, 4)), Number(mKey.slice(5)), 0)).getUTCDate())}`;

/** Employees active at each month-end ({ start, end, type }: 'YYYY-MM-DD', end null while employed):
 *  started on or before the last day and not left before it. { mKey: { total, byType } } */
function headcountByMonth(spans, months) {
  return Object.fromEntries(months.map((mKey) => {
    const day = lastDayOf(mKey);
    const byType = {};
    let total = 0;
    for (const s of spans) {
      if (!s.start || s.start > day || (s.end && s.end < day)) continue;
      total++;
      const t = s.type || 'Unspecified';
      byType[t] = (byType[t] || 0) + 1;
    }
    return [mKey, { total, byType }];
  }));
}

/**
 * Payroll / revenue and revenue per employee, month by month through the last payroll JE (pure).
 *   rows: the P&L's current-year rows (eur: payroll = 76xxxx gross, totalRevenue); only closed months count.
 *   A closed month's payroll counts as posted when it is at least half the year's largest month, so a
 *   partly posted JE does not end the series early or late.
 *   spans: employees ({ start, end, type }) or null when they could not be read.
 */
function peopleMetrics(rows, spans, company) {
  const closed = rows.filter(isActual);
  const top = Math.max(0, ...closed.map((r) => num(r.eur.payroll)));
  const lastIdx = top > 0 ? closed.reduce((last, r, i) => (num(r.eur.payroll) >= top / 2 ? i : last), -1) : -1;
  const shown = closed.slice(0, lastIdx + 1);
  const heads = spans ? headcountByMonth(spans, shown.map((r) => r.mKey)) : null;
  const months = shown.map((r) => {
    const payroll = num(r.eur.payroll);
    const revenue = num(r.eur.totalRevenue);
    const hc = heads ? heads[r.mKey] : null;
    return {
      mKey: r.mKey, payroll: round2(payroll), revenue: round2(revenue),
      payrollPct: revenue > 0 ? Math.round((payroll / revenue) * 10000) / 100 : null,
      headcount: hc ? hc.total : null, byType: hc ? hc.byType : null,
      revenuePerEmployee: hc && hc.total > 0 ? round2(revenue / hc.total) : null,
    };
  });
  const payroll = sumOf(months, (m) => m.payroll);
  const revenue = sumOf(months, (m) => m.revenue);
  const counted = months.filter((m) => m.headcount);
  const headMonths = sumOf(counted, (m) => m.headcount);
  const avgHeadcount = counted.length ? headMonths / counted.length : null;
  const revCounted = sumOf(counted, (m) => m.revenue);
  const n = months.length;
  return {
    company,
    through: n ? months[n - 1].mKey : null,
    pending: closed.slice(lastIdx + 1).map((r) => r.mKey),
    months,
    ytd: n ? {
      label: n > 1 ? `Jan–${monthShort(months[n - 1].mKey)}` : monthShort(months[0].mKey),
      payroll: round2(payroll), revenue: round2(revenue),
      payrollPct: revenue > 0 ? Math.round((payroll / revenue) * 10000) / 100 : null,
      avgHeadcount: avgHeadcount === null ? null : Math.round(avgHeadcount * 10) / 10,
      // Comparable with the monthly bars: revenue a month per employee, over the months counted.
      revenuePerEmployeeMonthly: headMonths > 0 ? round2(revCounted / headMonths) : null,
      // Accumulated: the months' revenue per average employee, and that pace over a full year.
      revenuePerEmployee: avgHeadcount ? round2(revCounted / avgHeadcount) : null,
      revenuePerEmployeeAnnualised: avgHeadcount && counted.length ? round2((revCounted / avgHeadcount) * (12 / counted.length)) : null,
    } : null,
  };
}

/** HiBob employees of one company with their start and leave dates (Snowflake; no names or ids). */
async function sfEmployeeSpans(sf, company) {
  const T = require(path.join(ROOT, 'snowflake-api.cjs')).SF_TABLES;
  const rows = await sf.query(`
    SELECT TO_VARCHAR(START_DATE, 'YYYY-MM-DD') AS S,
           TO_VARCHAR(TERMINATION_DATE, 'YYYY-MM-DD') AS E,
           EMPLOYMENT_TYPE AS T
    FROM ${T.DIM_EMPLOYEE}
    WHERE COMPANY_NAME = ? AND START_DATE IS NOT NULL
  `, [company]);
  return rows.map((r) => ({ start: String(r.S || ''), end: r.E ? String(r.E) : null, type: r.T ? String(r.T) : null }));
}

// ── the pack ────────────────────────────────────────────────────────────────
/**
 * The Metrics payload from its inputs (pure; unit-tested).
 *   cash, pnl: the projection payloads (Plan); pnlDetails: the P&L's server-side details
 *   extras: { arr, churnQuarters, nrr: [{ month, nrr, grr, customers }], fx: [...], fxMonth, usdLive,
 *             employees: [{ start, end, type }], company }
 *   failed: names of the extras that could not be read
 *   targets: the saved targets store ({ years: { 'YYYY': targets }, updatedAt, updatedBy }) or null;
 *   targetsLib: src/forecast/targets.mjs (without it, or without saved targets, the Plan as it is)
 */
function buildMetrics({ nowMs, cash, pnl, pnlDetails, settings, deposits, extras, failed = [], targets = null, targetsLib = null }) {
  const [Y, T] = pnl.years;
  // The projection year follows the targets saved on the New Bank Dashboard, as both pages' Targets view.
  const tl = targetsLib;
  const savedRaw = tl && targets && targets.years ? targets.years[String(T)] : null;
  const saved = savedRaw ? tl.validateTargets(savedRaw).targets : null;
  const tActive = !!(saved && !tl.isEmptyTargets(saved));
  const blocksOf = (payload, apply) => (tActive && payload.targetsBase && payload.targetsBase.year === T
    ? tl.variantWithTargets(payload.variants.plan, payload.targetsBase, saved, apply).years
    : payload.variants.plan.years);
  const pBlocks = blocksOf(pnl, tl && tl.applyPnlTargets);
  const cBlocks = blocksOf(cash, tl && tl.applyCashTargets);
  const pnlTargeted = pBlocks !== pnl.variants.plan.years;
  const cashTargeted = cBlocks !== cash.variants.plan.years;
  const withT = (text, on = pnlTargeted) => (on ? `${text}; FY ${T} with the ${T} targets` : text);
  const pY = pBlocks.find((b) => b.year === Y);
  const pT = pBlocks.find((b) => b.year === T);
  const cY = cBlocks.find((b) => b.year === Y);
  const cT = cBlocks.find((b) => b.year === T);
  const through = pnl.actuals && pnl.actuals.through;
  const closed = pY.rows.filter(isActual);
  const lastRow = through ? pY.rows.find((r) => r.mKey === through) : null;
  const ytdLabel = through ? (closed.length > 1 ? `Jan–${monthShort(through)}` : monthShort(through)) : null;
  const inv = settings.innovation;
  const env = envelopeMonths(inv, pBlocks, T);
  const envIn = (rows) => rows.reduce((s, r) => s + (env.byMonth.get(r.mKey) || 0), 0);
  const useEnv = !!inv.included;

  const fyOf = (block, field, withEnvelope) => {
    const actual = sumOf(block.rows.filter(isActual), (r) => r.eur[field]);
    const forecast = sumOf(block.rows.filter((r) => !isActual(r)), (r) => r.eur[field]) - (withEnvelope ? envIn(block.rows) : 0);
    return cell(actual + forecast, actual && forecast ? 'actual+forecast' : forecast ? 'forecast' : 'actual', `FY ${block.year}`,
      { actual: round2(actual), forecast: round2(forecast) });
  };
  const lastAndYtd = (field) => ({
    lastMonth: lastRow ? cell(lastRow.eur[field], 'actual', monthShort(through)) : null,
    ytd: closed.length ? cell(sumOf(closed, (r) => r.eur[field]), 'actual', ytdLabel) : null,
  });

  // Net cash: the bank at the last month-end; December closings carry the envelope's cash out.
  const envThrough = (year) => [...env.byMonth.entries()].filter(([k]) => Number(k.slice(0, 4)) <= year).reduce((s, [, v]) => s + v, 0);
  const decClose = (block) => {
    const v = num(block.rows[block.rows.length - 1].eur.closing) - (useEnv ? envThrough(block.year) : 0);
    return cell(v, 'forecast', `Dec ${block.year}`);
  };
  const bank = cash.bankToday;
  const janOpen = cY && cY.rows[0] ? num(cY.rows[0].eur.opening) : null;

  // ARR: Snowflake now; forecast = December customer revenue (after pipeline and churn) × 12.
  const decRunRate = (block) => {
    const r = block.rows[block.rows.length - 1].eur;
    return cell((num(r.revenue) + num(r.pipeline) - num(r.churn)) * 12, 'forecast', `Dec ${block.year} × 12`);
  };
  const arr = extras.arr;
  const nrrLast = extras.nrr && extras.nrr.length ? extras.nrr[extras.nrr.length - 1] : null;
  const quarters = (extras.churnQuarters || []).slice().sort((a, b) => String(a.qs).localeCompare(String(b.qs)));
  const lastFullQ = quarters.filter((q) => !q.partial).pop();
  const churnYtd = quarters.filter((q) => String(q.qs).startsWith(`${Y}-`));

  const metrics = [
    { key: 'revenue', label: 'Revenue', unit: 'eur', note: withT('Total revenue (P&L, Plan)'), ...lastAndYtd('totalRevenue'), fy: [fyOf(pY, 'totalRevenue', false), fyOf(pT, 'totalRevenue', false)] },
    {
      key: 'ebitda', label: 'EBITDA', unit: 'eur',
      note: withT(useEnv && inv.amountEur ? 'P&L, Plan, with the innovation envelope' : 'P&L, Plan'),
      ...lastAndYtd('ebitda'), fy: [fyOf(pY, 'ebitda', useEnv), fyOf(pT, 'ebitda', useEnv)],
    },
    {
      key: 'netCash', label: 'Net cash', unit: 'eur', note: withT('Cash in the bank (no debt); forecast: New Bank Dashboard, Plan', cashTargeted),
      lastMonth: bank ? cell(bank.eur, 'actual', `Bank, ${bank.asOf}`) : null,
      ytd: bank && janOpen !== null ? cell(num(bank.eur) - janOpen, 'actual', 'Change since 1 Jan') : null,
      fy: [cY ? decClose(cY) : null, cT ? decClose(cT) : null],
    },
    {
      key: 'arr', label: 'ARR', unit: 'eur', note: withT('MRR × 12'),
      lastMonth: arr ? cell(arr.arr, 'actual', `Now (${arr.liveDate || arr.snapDate || 'latest'})`) : null,
      ytd: null,
      fy: [decRunRate(pY), decRunRate(pT)],
    },
    {
      key: 'nrr', label: 'NRR', unit: 'pct', note: nrrLast ? `Trailing 12 months; GRR ${nrrLast.grr}%` : 'Trailing 12 months',
      lastMonth: nrrLast ? cell(nrrLast.nrr, 'actual', monthShort(nrrLast.month), { grr: nrrLast.grr, customers: nrrLast.customers }) : null,
      ytd: null, fy: [null, null],
    },
    {
      key: 'churn', label: 'Churn', unit: 'eur', note: 'Churned MRR (actual); revenue lost to churn (forecast)',
      lastMonth: lastFullQ ? cell(lastFullQ.amount, 'actual', lastFullQ.q || String(lastFullQ.qs)) : null,
      ytd: churnYtd.length ? cell(sumOf(churnYtd, (q) => q.amount), 'actual', `${Y} so far`) : null,
      fy: [cell(sumOf(pY.rows.filter((r) => !isActual(r)), (r) => r.eur.churn), 'forecast', `FY ${Y} forecast months`), null],
    },
  ];

  const categories = (pnl.targetsBase && pnl.targetsBase.categories) || [];
  const category = cloudCategoryOf(settings, categories, pnl.targetsBase);
  const capPct = num(settings.cloudCapPct);
  // The baseline cloud comes from the Plan's year (its opex already holds no targets); the projection
  // year then takes the targets' cloud, and the cap the revenue after the targets.
  const deltas = pnlTargeted ? tl.computeTargetDeltas(pnl.targetsBase, saved) : null;
  const cloudTargets = cloudTargetsOf(saved, deltas, category);
  const planBlock = (year) => pnl.variants.plan.years.find((b) => b.year === year);
  const cloud = {
    category, categories, capPct, accounts: '640xxx',
    years: [pY, pT].map((b) => cloudYear({
      block: planBlock(b.year), details: pnlDetails || {}, targetsBase: pnl.targetsBase, category, capPct,
      revenue: fyOf(b, 'totalRevenue', false).value, withTargets: b.year === T ? cloudTargets : null,
    })),
  };

  const fyEbitda = (b) => fyOf(b, 'ebitda', false).value;
  const envBlock = pBlocks.find((b) => b.year === env.year);
  const cashEnvBlock = cBlocks.find((b) => b.year === env.year);
  const applied = round2(envIn(envBlock ? envBlock.rows : []));
  const innovation = {
    amountEur: num(inv.amountEur), year: env.year, startMonth: inv.startMonth || 1, included: useEnv,
    monthly: round2(env.monthly), months: env.byMonth.size, applied,
    ebitda: envBlock ? { year: env.year, without: fyEbitda(envBlock), with: round2(fyEbitda(envBlock) - applied) } : null,
    netCash: cashEnvBlock ? {
      year: env.year,
      without: round2(cashEnvBlock.rows[11].eur.closing),
      with: round2(num(cashEnvBlock.rows[11].eur.closing) - envThrough(env.year)),
    } : null,
  };

  const fx = (extras.fx || []);
  const totals = new Map();
  for (const c of fx) {
    const k = `${c.fromCurrency}→${c.toCurrency}|${c.currency}`;
    const t = totals.get(k) || { pair: `${c.fromCurrency} → ${c.toCurrency}`, currency: c.currency, amount: 0, eur: 0, count: 0 };
    t.amount += c.amount;
    t.eur += c.eur;
    t.count++;
    totals.set(k, t);
  }

  const open = (deposits || []).filter((d) => !d.confirmed).sort((a, b) => String(a.placedOn).localeCompare(String(b.placedOn)));
  const warnings = failed.map((f) => ({
    arr: 'ARR could not be read from Snowflake right now.',
    churn: 'Churn by quarter could not be read from Snowflake right now.',
    nrr: 'NRR could not be computed from Snowflake right now.',
    fx: 'Last month\'s FX conversions could not be read from NetSuite right now.',
    usd: 'Today\'s ECB rate is unavailable right now.',
    targets: `The ${T} targets could not be applied right now: FY ${T} shows the Plan.`,
    employees: 'Employees could not be read from HiBob (Snowflake) right now: revenue per employee is empty.',
  }[f] || `${f} is unavailable right now.`));

  return {
    ok: true, status: 'ready', generatedAt: new Date(nowMs).toISOString(), years: [Y, T],
    asOf: { lastClosed: through, cash: cash.generatedAt, pnl: pnl.generatedAt, plan: pnl.plan ? pnl.plan.name : null },
    targets: {
      year: T, active: pnlTargeted || cashTargeted,
      updatedAt: savedRaw && targets.updatedAt ? targets.updatedAt : null,
      updatedBy: savedRaw && targets.updatedBy ? targets.updatedBy : null,
      assumptions: pnlTargeted || cashTargeted ? tl.describeTargets(saved) : [],
    },
    metrics,
    nrrTrend: extras.nrr || [],
    cloud,
    innovation,
    people: peopleMetrics(pY.rows, extras.employees || null, extras.company || null),
    rates: { usdEurPlanning: settings.usdEurPlanningRate, usdEurLive: extras.usdLive || null },
    fx: {
      month: extras.fxMonth, items: fx,
      totals: [...totals.values()].map((t) => ({
        pair: t.pair, currency: t.currency, count: t.count, amount: round2(t.amount), eur: round2(t.eur),
        rate: t.currency !== 'EUR' && t.eur > 0 ? Math.round((t.amount / t.eur) * 10000) / 10000 : null,
      })),
    },
    deposits: { open, openCount: open.length, total: (deposits || []).length },
    settings,
    warnings,
  };
}

// ── handler ─────────────────────────────────────────────────────────────────
function envMinutes(name, fallback) {
  const v = parseFloat(process.env[name] || '');
  return Number.isFinite(v) && v > 0 ? v : fallback;
}

async function fetchUsdLive() {
  const fetchJson = async (u) => {
    const r = await fetch(u, { signal: AbortSignal.timeout(5000) });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  };
  try {
    const d = await fetchJson('https://api.frankfurter.app/latest?from=EUR&to=USD');
    if (d && d.rates && Number.isFinite(d.rates.USD)) return { rate: Math.round(d.rates.USD * 10000) / 10000, date: d.date, source: 'ECB (Frankfurter)' };
  } catch { /* backup below */ }
  const d = await fetchJson('https://open.er-api.com/v6/latest/EUR');
  if (d && d.rates && Number.isFinite(d.rates.USD)) return { rate: Math.round(d.rates.USD * 10000) / 10000, date: String(d.time_last_update_utc || '').slice(0, 16), source: 'open.er-api.com' };
  throw new Error('no USD rate');
}

/**
 * Express/connect handler for GET /api/metrics. deps (all optional):
 *   cash, pnl        the projection handlers (their .current()); default: own instances on the same caches
 *   getSfClient(), getNsClient(sub), queueNsCall(fn)
 *   reads            override every external read (tests): { arr, churnQuarters, customerRevenue(from, to), fx(month), usdLive, employees(company) }
 *   company          the HiBob company of the P&L's subsidiary (default METRICS_HEADCOUNT_COMPANY or 'LSports')
 *   settingsFile, depositsFile, targetsFile (the saved 2027 targets), clock(), ttlMs
 */
function createMetricsHandler(deps = {}) {
  const clock = deps.clock || Date.now;
  const ttlMs = deps.ttlMs ?? envMinutes('METRICS_TTL_MIN', 30) * MIN;
  const settingsFile = deps.settingsFile || SETTINGS_FILE;
  const depositsFile = deps.depositsFile || DEPOSITS_FILE;
  const targetsFile = deps.targetsFile || projectionTargets.DEFAULT_FILE;
  const company = deps.company || process.env.METRICS_HEADCOUNT_COMPANY || 'LSports';
  // Without the page handlers (finance-it's own route file), own instances read the same cache files and
  // compute only when there is no usable cache at all; refreshing stale figures stays with the pages.
  let own = null;
  const shared = !!(deps.cash && deps.pnl);
  const projections = () => {
    if (shared) return { cash: deps.cash, pnl: deps.pnl };
    if (!own) {
      const cp = require('./cash-projection.cjs');
      const pnl = require('./pnl-projection.cjs');
      own = { cash: cp.createCashProjectionHandler(), pnl: pnl.createPnlProjectionHandler() };
    }
    return own;
  };
  const reads = deps.reads || (() => {
    const cmp = () => require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs')); // loads the checkout's .env
    const sf = () => {
      cmp();
      const c = deps.getSfClient ? deps.getSfClient() : require('./cash-projection.cjs').defaultGetSfClient();
      if (!c) throw new Error('Snowflake is not configured');
      return c;
    };
    const ns = () => {
      const sub = cmp().SUBSIDIARY;
      const c = deps.getNsClient ? deps.getNsClient(sub) : require('./cash-projection.cjs').defaultGetNsClient(sub);
      if (!c) throw new Error('NetSuite is not configured');
      return c;
    };
    const queue = deps.queueNsCall || require('./cash-projection.cjs').defaultQueueNsCall;
    return {
      arr: () => sf().fetchCurrentARR(),
      churnQuarters: () => sf().fetchQuarterlyChurnMRR(),
      customerRevenue: (from, to) => sfCustomerRevenue(sf(), from, to),
      fx: (month) => queue(() => ns().fetchFxConversions({ month })),
      usdLive: fetchUsdLive,
      employees: (co) => sfEmployeeSpans(sf(), co),
    };
  })();

  const memo = new Map();
  const cached = (key, fn, ms = ttlMs) => {
    const hit = memo.get(key);
    if (hit && clock() - hit.at < ms) return hit.p;
    const p = Promise.resolve().then(fn);
    memo.set(key, { at: clock(), p });
    p.catch(() => { if (memo.get(key) && memo.get(key).p === p) memo.delete(key); });
    return p;
  };

  return async function metricsHandler(req, res) {
    if ((req.method || 'GET').toUpperCase() !== 'GET') {
      send(res, 405, { ok: false, status: 'error', error: 'Method not allowed' }, { Allow: 'GET' });
      return;
    }
    let refresh = false;
    try { refresh = /^(1|true)$/.test(new URL(req.url || '', 'http://localhost').searchParams.get('refresh') || ''); } catch { /* plain read */ }
    if (refresh) memo.clear();

    const { cash, pnl } = projections();
    const c = cash.current({ refreshStale: shared });
    const p = pnl.current({ refreshStale: shared });
    if (c.computing || p.computing) {
      const started = Math.min(c.startedMs || Infinity, p.startedMs || Infinity);
      send(res, 202, { ok: true, status: 'computing', startedAt: Number.isFinite(started) ? new Date(started).toISOString() : null, elapsedSec: Number.isFinite(started) ? Math.round((clock() - started) / 1000) : 0 }, { 'Retry-After': '5' });
      return;
    }
    if (c.error || p.error) {
      send(res, 200, { ok: false, status: 'error', error: c.error || p.error });
      return;
    }
    const nowMs = clock();
    const closed = lastClosedMonth(nowMs);
    const nrrMonths = Array.from({ length: 6 }, (_, i) => shiftMonth(closed, i - 5));
    const settled = await Promise.allSettled([
      cached('arr', reads.arr),
      cached('churn', reads.churnQuarters),
      cached(`nrr:${closed}`, async () => nrrSeries(await reads.customerRevenue(shiftMonth(nrrMonths[0], -12), closed), nrrMonths)),
      cached(`fx:${closed}`, () => reads.fx(closed)),
      cached('usd', reads.usdLive, Math.min(ttlMs, 60 * MIN)),
      cached(`employees:${company}`, () => reads.employees(company)),
    ]);
    const names = ['arr', 'churn', 'nrr', 'fx', 'usd', 'employees'];
    const failed = [];
    const val = (i) => {
      if (settled[i].status === 'fulfilled') return settled[i].value;
      failed.push(names[i]);
      console.warn(`[metrics] ${names[i]} unavailable: ${settled[i].reason && settled[i].reason.message}`);
      return null;
    };
    const extras = { arr: val(0), churnQuarters: val(1), nrr: val(2), fx: val(3), fxMonth: closed, usdLive: val(4), employees: val(5), company };
    const settingsDoc = readDoc(settingsFile);
    const depositsDoc = readDoc(depositsFile);
    // The saved targets, read on every request so a save on the New Bank Dashboard shows at once.
    const targets = projectionTargets.readStore(targetsFile);
    let targetsLib = null;
    if (targets) {
      try { targetsLib = await projectionTargets.loadTargetsModule(); } catch (e) {
        console.warn(`[metrics] targets module unavailable: ${e && e.message}`);
        failed.push('targets');
      }
    }
    try {
      const out = buildMetrics({
        nowMs, cash: c.entry.payload, pnl: p.entry.payload, pnlDetails: p.entry.details,
        settings: { ...emptySettings(), ...((settingsDoc && settingsDoc.value) || {}) },
        deposits: (depositsDoc && depositsDoc.value) || [],
        extras, failed, targets, targetsLib,
      });
      out.refreshing = !!(c.refreshing || p.refreshing);
      send(res, 200, out);
    } catch (e) {
      console.error(`[metrics] build failed: ${e && e.stack ? e.stack : e}`);
      send(res, 200, { ok: false, status: 'error', error: 'The metrics could not be computed. The server log has the details.' });
    }
  };
}

function createMetricsSettingsHandler(deps = {}) {
  return createDocHandler({ file: deps.file || SETTINGS_FILE, label: 'metrics-settings', validate: validateSettings, empty: emptySettings, clock: deps.clock });
}
function createMetricsDepositsHandler(deps = {}) {
  return createDocHandler({ file: deps.file || DEPOSITS_FILE, label: 'metrics-deposits', maxBytes: 128 * 1024, validate: validateDeposits, empty: () => [], clock: deps.clock });
}

module.exports = {
  createMetricsHandler, createMetricsSettingsHandler, createMetricsDepositsHandler,
  buildMetrics, nrrSeries, envelopeMonths, shiftMonth, lastClosedMonth, cloudTargetsOf, headcountByMonth, peopleMetrics,
};
