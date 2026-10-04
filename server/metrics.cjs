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
//
// GET /api/metrics?detail=<item> answers what one figure is made of (NRR, the churn cells): the formula,
// the source, the steps and the customers behind it (DETAILS, buildDetail).
//
// The projections come from their cached handlers (no second NetSuite pull); the other reads are cached
// for METRICS_TTL_MIN (default 30). GET/PUT /api/metrics/settings saves what the page edits (the cloud
// cap; server/json-store.cjs, server/metrics-settings.cjs). /api/metrics/deposits stays mounted for
// finance-it's route file, but the page no longer has the deposit tracker.
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

/**
 * How one month's NRR is made (pure): the bridge from the base customers' revenue a year earlier to
 * their revenue now, and the customers behind each step. rows: [{ month, customer, name, rev }].
 * The figures equal nrrSeries' for the month. limit: customers listed per group (the rest are counted).
 */
function nrrDetail(rows, month, { limit = 40 } = {}) {
  const prev = shiftMonth(month, -12);
  const at = (m) => {
    const out = new Map();
    for (const r of rows) {
      if (r.month !== m) continue;
      const c = out.get(r.customer) || { name: r.name || r.customer, rev: 0 };
      c.rev += num(r.rev);
      out.set(r.customer, c);
    }
    return out;
  };
  const before = at(prev);
  const now = at(month);
  if (!before.size || !now.size) return null;
  const groups = { churned: [], contracted: [], expanded: [], flat: [], added: [] };
  let base = 0;
  let kept = 0;
  let gross = 0;
  for (const [id, b] of before) {
    if (b.rev <= 0) continue;
    const cur = Math.max(0, now.has(id) ? now.get(id).rev : 0);
    base += b.rev;
    kept += cur;
    gross += Math.min(cur, b.rev);
    const row = { customer: b.name, then: round2(b.rev), now: round2(cur), change: round2(cur - b.rev) };
    if (cur <= 0) groups.churned.push(row);
    else if (Math.abs(cur - b.rev) < 0.5) groups.flat.push(row);
    else (cur > b.rev ? groups.expanded : groups.contracted).push(row);
  }
  for (const [id, c] of now) {
    if (c.rev > 0 && !(before.has(id) && before.get(id).rev > 0)) groups.added.push({ customer: c.name, then: 0, now: round2(c.rev), change: round2(c.rev) });
  }
  if (!(base > 0)) return null;
  const total = (list, f) => round2(sumOf(list, f));
  const expansion = total(groups.expanded, (r) => r.change);
  const contraction = total(groups.contracted, (r) => -r.change);
  const churn = total(groups.churned, (r) => r.then);
  const P = monthShort(prev);
  const N = monthShort(month);
  const byImpact = (list) => list.slice().sort((a, b) => Math.abs(b.change) - Math.abs(a.change));
  const table = (title, list, note) => {
    const sorted = byImpact(list);
    return {
      title: `${title} (${list.length})`, note,
      columns: [{ key: 'customer', label: 'Customer', unit: 'text' }, { key: 'then', label: P, unit: 'eur' }, { key: 'now', label: N, unit: 'eur' }, { key: 'change', label: 'Change', unit: 'eur' }],
      rows: sorted.slice(0, limit), more: Math.max(0, sorted.length - limit),
      total: { customer: 'Total', then: total(list, (r) => r.then), now: total(list, (r) => r.now), change: total(list, (r) => r.change) },
    };
  };
  const customers = groups.churned.length + groups.contracted.length + groups.expanded.length + groups.flat.length;
  return {
    item: 'nrr', title: `NRR, ${N}`, subtitle: `Last year's customers: ${P} → ${N}`,
    value: { value: Math.round((kept / base) * 10000) / 100, unit: 'pct' },
    formula: [
      `NRR = the revenue in ${N} of the customers who had revenue in ${P}, ÷ their revenue in ${P}.`,
      `GRR = the same with each customer capped at its ${P} revenue, so growth doesn't count: only what was kept.`,
      `Customers new since ${P} are left out (listed at the end, not counted).`,
    ],
    source: [
      'Snowflake FCT_CUSTOMER__MONTHLY__FINANCE: revenue by customer and month, actual months only (DATE_STATUS = \'actual\').',
      'Test customers excluded (DIM_CUSTOMER__FINANCE.IS_TEST = FALSE); names from DIM_CUSTOMER__FINANCE.',
      'Read when the page loads and kept for 30 minutes; Refresh reads it again.',
    ],
    summary: [
      { label: `Customers with revenue in ${P}`, value: customers, unit: 'int' },
      { label: `Their revenue, ${P}`, value: round2(base), unit: 'eur' },
      { label: `+ Expansion (${groups.expanded.length} customers)`, value: expansion, unit: 'eur', sign: true },
      { label: `− Contraction (${groups.contracted.length} customers)`, value: -contraction, unit: 'eur', sign: true },
      { label: `− Churned (${groups.churned.length} customers, no revenue in ${N})`, value: -churn, unit: 'eur', sign: true },
      { label: `= Their revenue, ${N}`, value: round2(kept), unit: 'eur', strong: true },
      { label: `NRR = ${N} ÷ ${P}`, value: Math.round((kept / base) * 10000) / 100, unit: 'pct', strong: true },
      { label: 'GRR (each capped at its old revenue)', value: Math.round((gross / base) * 10000) / 100, unit: 'pct' },
      { label: `Not counted: ${groups.added.length} new customers, revenue ${N}`, value: total(groups.added, (r) => r.now), unit: 'eur' },
    ],
    tables: [
      table('Churned', groups.churned, `Revenue in ${P}, none in ${N}.`),
      table('Contracted', groups.contracted, `Less revenue in ${N} than in ${P}.`),
      table('Expanded', groups.expanded, `More revenue in ${N} than in ${P}.`),
      table('New since then (not counted)', groups.added, `No revenue in ${P}: outside NRR.`),
    ],
  };
}

// ── churn: what each figure is made of ──────────────────────────────────────
const quarterLabel = (mKey) => `Q${Math.ceil(Number(mKey.slice(5)) / 3)} ${mKey.slice(0, 4)}`;
const quarterMonths = (qs) => [0, 1, 2].map((i) => shiftMonth(qs.slice(0, 7), i));
const OPP_SOURCE = [
  'Snowflake DIM_OPPORTUNITY__FINANCE: opportunities with IS_OPPORTUNITY_CHURNED = TRUE, by their churn month (OPPORTUNITY_CHURN_MONTH_START_DATE); MRR = OPPORTUNITY_AMOUNT.',
  'Customer names from DIM_CUSTOMER__FINANCE (by CUSTOMER_ID). The same read the New Bank Dashboard\'s churn run-rate uses.',
];
const oppTable = (title, list, { withQuarter = false, note } = {}) => ({
  title, note,
  columns: [
    ...(withQuarter ? [{ key: 'quarter', label: 'Quarter', unit: 'text' }] : []),
    { key: 'customer', label: 'Customer', unit: 'text' }, { key: 'opportunity', label: 'Opportunity', unit: 'text' },
    { key: 'month', label: 'Churn month', unit: 'text' }, { key: 'currency', label: 'Currency', unit: 'text' },
    { key: 'amount', label: 'MRR', unit: 'eur' },
  ],
  rows: list.map((o) => ({ quarter: quarterLabel(o.month), customer: o.customer || '–', opportunity: o.opportunity, month: monthShort(o.month), currency: o.currency || '–', amount: round2(o.amount) })),
  more: 0,
  total: { [withQuarter ? 'quarter' : 'customer']: 'Total', amount: round2(sumOf(list, (o) => o.amount)) },
});
// Amounts in other currencies are added as they are, as the quarterly figure does: say so when it happens.
const currencyNote = (list) => {
  const other = [...new Set(list.map((o) => o.currency).filter((c) => c && c !== 'EUR'))];
  return other.length ? `Some opportunities are in ${other.join(', ')}: their amounts are added as they are, as in the pack's figure.` : null;
};

/** The last full quarter's churned MRR, opportunity by opportunity (pure). opps: [{ month, opportunity, customer, currency, amount }]. */
function churnQuarterDetail(quarter, opps) {
  const months = quarterMonths(quarter.qs);
  const list = opps.filter((o) => months.includes(o.month)).sort((a, b) => b.amount - a.amount);
  const total = round2(sumOf(list, (o) => o.amount));
  const notes = [currencyNote(list), Math.abs(total - num(quarter.amount)) >= 0.5
    ? `The pack shows ${round2(quarter.amount)} (read earlier); this list was read now.` : null].filter(Boolean);
  return {
    item: 'churn-quarter', title: `Churn, ${quarter.q}`, subtitle: `Churned MRR, ${monthShort(months[0])}–${monthShort(months[2])}`,
    value: { value: total, unit: 'eur' },
    formula: [
      `Churned MRR, ${quarter.q} = the MRR of every opportunity marked churned whose churn month is in ${quarter.q}.`,
      'One row per opportunity: a customer can have several.',
    ],
    source: [...OPP_SOURCE, 'The last full quarter: the quarter in progress is not complete yet.'],
    summary: [
      { label: 'Opportunities churned', value: list.length, unit: 'int' },
      { label: 'Customers', value: new Set(list.map((o) => o.customer || o.opportunity)).size, unit: 'int' },
      { label: `Churned MRR, ${quarter.q}`, value: total, unit: 'eur', strong: true },
    ],
    notes,
    tables: [oppTable(`Opportunities churned in ${quarter.q} (${list.length})`, list)],
  };
}

/** This year's churned MRR so far, quarter by quarter (the quarter in progress included). */
function churnYtdDetail(year, quarters, opps) {
  const qs = quarters.filter((q) => String(q.qs).startsWith(`${year}-`)).sort((a, b) => String(a.qs).localeCompare(String(b.qs)));
  const inYear = opps.filter((o) => o.month.startsWith(`${year}-`))
    .sort((a, b) => a.month.slice(0, 7).localeCompare(b.month.slice(0, 7)) || b.amount - a.amount);
  const total = round2(sumOf(inYear, (o) => o.amount));
  return {
    item: 'churn-ytd', title: `Churn, ${year} so far`, subtitle: `Churned MRR, ${qs.map((q) => q.q).join(' + ') || year}`,
    value: { value: total, unit: 'eur' },
    formula: [
      `Churned MRR, ${year} so far = the churned MRR of each quarter of ${year}, the quarter in progress included.`,
      'Each quarter: the MRR of every opportunity marked churned whose churn month is in it.',
    ],
    source: OPP_SOURCE,
    summary: [
      ...qs.map((q) => {
        const sum = round2(sumOf(inYear.filter((o) => quarterLabel(o.month) === q.q), (o) => o.amount));
        return { label: `${q.q}${q.partial ? ' (in progress)' : ''}: ${inYear.filter((o) => quarterLabel(o.month) === q.q).length} opportunities`, value: sum, unit: 'eur' };
      }),
      { label: `Churned MRR, ${year} so far`, value: total, unit: 'eur', strong: true },
    ],
    notes: [currencyNote(inYear)].filter(Boolean),
    tables: [oppTable(`Opportunities churned in ${year} (${inYear.length})`, inYear, { withQuarter: true })],
  };
}

/**
 * The forecast's churn for the year: a run-rate, not a list of customers. Each forecast month loses the
 * run-rate × its place in the forecast (churn piles up); a month the Plan sets by hand keeps its figure.
 *   rows: the P&L year (eur.churn); months: details.variants.plan[year].months ({ churnIndex, churnOverride });
 *   quarters: the churn quarters; opps: the churned opportunities (context only).
 */
function churnForecastDetail(year, rows, months, quarters, opps) {
  const fc = rows.filter((r) => !isActual(r)).map((r) => {
    const d = (months || {})[r.mKey] || {};
    return { mKey: r.mKey, n: num(d.churnIndex), override: !!d.churnOverride, churn: num(r.eur.churn) };
  });
  const ruled = fc.find((m) => !m.override && m.n > 0 && m.churn > 0);
  const rate = ruled ? ruled.churn / ruled.n : 0;
  // The quarter the run-rate comes from: the latest full quarter before the forecast starts.
  const first = fc.length ? fc[0].mKey : null;
  const fromQ = first ? quarters.filter((q) => !q.partial && quarterMonths(q.qs)[2] < first).sort((a, b) => String(a.qs).localeCompare(String(b.qs))).pop() : null;
  const matches = fromQ && Math.abs(num(fromQ.amount) / 3 - rate) < 1;
  const total = round2(sumOf(fc, (m) => m.churn));
  const context = fromQ ? opps.filter((o) => quarterMonths(fromQ.qs).includes(o.month)).sort((a, b) => b.amount - a.amount) : [];
  return {
    item: 'churn-forecast', title: `Churn, FY ${year} forecast months`, subtitle: 'Revenue the forecast loses to churn (a run-rate, not a list of customers)',
    value: { value: total, unit: 'eur' },
    formula: [
      matches ? `Run-rate = ${fromQ.q} churned MRR ÷ 3 (a month of it).` : 'Run-rate = the forecast engine\'s monthly churn.',
      'Each forecast month loses the run-rate × its place in the forecast (1st month × 1, 2nd × 2, …): a customer lost in one month stays lost in the next.',
      'A month the Plan sets by hand keeps its own figure.',
      `FY ${year} forecast months = the sum of those months.`,
    ],
    source: [
      'The P&L Projection, Plan: the churn line of each forecast month (the same engine as the New Bank Dashboard).',
      ...(matches ? [`${fromQ.q} churned MRR: ${OPP_SOURCE[0]}`] : []),
    ],
    summary: [
      ...(matches ? [{ label: `${fromQ.q} churned MRR`, value: round2(fromQ.amount), unit: 'eur' }] : []),
      { label: matches ? 'Run-rate a month (÷ 3)' : 'Run-rate a month', value: round2(rate), unit: 'eur' },
      { label: 'Forecast months', value: fc.length, unit: 'int' },
      { label: `Churn, FY ${year} forecast months`, value: total, unit: 'eur', strong: true },
    ],
    notes: [],
    tables: [
      {
        title: 'By forecast month', note: null,
        columns: [{ key: 'month', label: 'Month', unit: 'text' }, { key: 'how', label: 'How', unit: 'text' }, { key: 'churn', label: 'Churn', unit: 'eur' }],
        rows: fc.map((m) => ({ month: monthShort(m.mKey), how: m.override ? 'Set in the Plan' : `${m.n} × run-rate`, churn: round2(m.churn) })),
        more: 0, total: { month: 'Total', churn: total },
      },
      ...(context.length ? [{
        ...oppTable(`For context: churned in ${fromQ.q} (${context.length})`, context),
        note: `The run-rate comes from these. The forecast does not name customers: it assumes ${fromQ.q}'s pace goes on.`,
      }] : []),
    ],
  };
}

/** Churned opportunities with a churn month from..to (YYYY-MM, inclusive), the quarterly figure's filters. */
async function sfChurnedOpportunities(sf, from, to) {
  if (!/^\d{4}-\d{2}$/.test(from) || !/^\d{4}-\d{2}$/.test(to)) throw new Error('sfChurnedOpportunities: bad months');
  const T = require(path.join(ROOT, 'snowflake-api.cjs')).SF_TABLES;
  const rows = await sf.query(`
    SELECT TO_VARCHAR(DATE_TRUNC('month', o.OPPORTUNITY_CHURN_MONTH_START_DATE), 'YYYY-MM') AS M,
           COALESCE(o.OPPORTUNITY_NAME, '(no name)') AS OPP,
           c.CUSTOMER_NAME AS CUST,
           o.CURRENCY AS CUR,
           SUM(o.OPPORTUNITY_AMOUNT) AS AMT
    FROM ${T.DIM_OPPORTUNITY} o
    LEFT JOIN (SELECT CUSTOMER_ID, MAX(CUSTOMER_NAME) AS CUSTOMER_NAME FROM ${T.DIM_CUSTOMER} GROUP BY 1) c ON c.CUSTOMER_ID = o.CUSTOMER_ID
    WHERE o.IS_OPPORTUNITY_CHURNED = TRUE
      AND o.OPPORTUNITY_CHURN_MONTH_START_DATE >= '${from}-01'
      AND o.OPPORTUNITY_CHURN_MONTH_START_DATE < '${shiftMonth(to, 1)}-01'
    GROUP BY 1, 2, 3, 4
  `);
  return rows.map((r) => ({ month: String(r.M || ''), opportunity: String(r.OPP || ''), customer: r.CUST ? String(r.CUST) : null, currency: r.CUR ? String(r.CUR) : null, amount: num(r.AMT) }));
}

/** Revenue by customer and month (actual months, test customers excluded), from..to inclusive. */
async function sfCustomerRevenue(sf, from, to) {
  if (!/^\d{4}-\d{2}$/.test(from) || !/^\d{4}-\d{2}$/.test(to)) throw new Error('sfCustomerRevenue: bad months');
  const T = require(path.join(ROOT, 'snowflake-api.cjs')).SF_TABLES;
  const rows = await sf.query(`
    SELECT TO_VARCHAR(DATE_TRUNC('month', m.CAL_MONTH_START_DATE), 'YYYY-MM') AS M,
           m.CUSTOMER_ID AS C,
           MAX(c.CUSTOMER_NAME) AS N,
           SUM(m.REVENUE) AS REV
    FROM ${T.FCT_CUSTOMER_MONTHLY} m
    JOIN ${T.DIM_CUSTOMER} c ON c.CUSTOMER_ID = m.CUSTOMER_ID
    WHERE m.DATE_STATUS = 'actual'
      AND c.IS_TEST = FALSE
      AND m.CAL_MONTH_START_DATE >= '${from}-01'
      AND m.CAL_MONTH_START_DATE < '${shiftMonth(to, 1)}-01'
    GROUP BY 1, 2
  `);
  return rows.map((r) => ({ month: String(r.M || ''), customer: String(r.C || ''), name: r.N ? String(r.N) : null, rev: num(r.REV) }));
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
 *   extras: { arr, churnQuarters, nrr: [{ month, nrr, grr, customers }],
 *             employees: [{ start, end, type }], company }
 *   failed: names of the extras that could not be read
 *   targets: the saved targets store ({ years: { 'YYYY': targets }, updatedAt, updatedBy }) or null;
 *   targetsLib: src/forecast/targets.mjs (without it, or without saved targets, the Plan as it is)
 */
function buildMetrics({ nowMs, cash, pnl, pnlDetails, settings, extras, failed = [], targets = null, targetsLib = null }) {
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

  const fyOf = (block, field) => {
    const actual = sumOf(block.rows.filter(isActual), (r) => r.eur[field]);
    const forecast = sumOf(block.rows.filter((r) => !isActual(r)), (r) => r.eur[field]);
    return cell(actual + forecast, actual && forecast ? 'actual+forecast' : forecast ? 'forecast' : 'actual', `FY ${block.year}`,
      { actual: round2(actual), forecast: round2(forecast) });
  };
  const lastAndYtd = (field) => ({
    lastMonth: lastRow ? cell(lastRow.eur[field], 'actual', monthShort(through)) : null,
    ytd: closed.length ? cell(sumOf(closed, (r) => r.eur[field]), 'actual', ytdLabel) : null,
  });

  // Net cash: the bank at the last month-end; the December closings.
  const decClose = (block) => cell(num(block.rows[block.rows.length - 1].eur.closing), 'forecast', `Dec ${block.year}`);
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
    { key: 'revenue', label: 'Revenue', unit: 'eur', note: withT('Total revenue (P&L, Plan)'), ...lastAndYtd('totalRevenue'), fy: [fyOf(pY, 'totalRevenue'), fyOf(pT, 'totalRevenue')] },
    { key: 'ebitda', label: 'EBITDA', unit: 'eur', note: withT('P&L, Plan'), ...lastAndYtd('ebitda'), fy: [fyOf(pY, 'ebitda'), fyOf(pT, 'ebitda')] },
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
      lastMonth: nrrLast ? cell(nrrLast.nrr, 'actual', monthShort(nrrLast.month), { grr: nrrLast.grr, customers: nrrLast.customers, detail: 'nrr' }) : null,
      ytd: null, fy: [null, null],
    },
    {
      key: 'churn', label: 'Churn', unit: 'eur', note: 'Churned MRR (actual); revenue lost to churn (forecast)',
      lastMonth: lastFullQ ? cell(lastFullQ.amount, 'actual', lastFullQ.q || String(lastFullQ.qs), { detail: 'churn-quarter' }) : null,
      ytd: churnYtd.length ? cell(sumOf(churnYtd, (q) => q.amount), 'actual', `${Y} so far`, { detail: 'churn-ytd' }) : null,
      fy: [cell(sumOf(pY.rows.filter((r) => !isActual(r)), (r) => r.eur.churn), 'forecast', `FY ${Y} forecast months`, { detail: 'churn-forecast' }), null],
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
      revenue: fyOf(b, 'totalRevenue').value, withTargets: b.year === T ? cloudTargets : null,
    })),
  };

  const warnings = failed.map((f) => ({
    arr: 'ARR could not be read from Snowflake right now.',
    churn: 'Churn by quarter could not be read from Snowflake right now.',
    nrr: 'NRR could not be computed from Snowflake right now.',
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
    people: peopleMetrics(pY.rows, extras.employees || null, extras.company || null),
    settings,
    warnings,
  };
}

// ── ?detail=: what one figure is made of ────────────────────────────────────
const DETAILS = ['nrr', 'churn-quarter', 'churn-ytd', 'churn-forecast'];

/**
 * The breakdown of one pack figure, from the same reads as the pack (null when there is nothing yet).
 *   ctx: { pnl, pnlDetails, closed, nrrMonths, quarters(), customerRevenue(), churnedOpps(from, to) }
 */
async function buildDetail(item, ctx) {
  const Y = ctx.pnl.years[0];
  if (item === 'nrr') {
    const rows = await ctx.customerRevenue();
    const series = nrrSeries(rows, ctx.nrrMonths);
    const last = series[series.length - 1];
    if (!last) return null;
    const d = nrrDetail(rows, last.month);
    if (d) {
      d.tables.push({
        title: 'NRR, the last 6 months', note: 'Each month against the same month a year earlier.',
        columns: [{ key: 'month', label: 'Month', unit: 'text' }, { key: 'customers', label: 'Base customers', unit: 'int' }, { key: 'nrr', label: 'NRR', unit: 'pct' }, { key: 'grr', label: 'GRR', unit: 'pct' }],
        rows: series.map((s) => ({ month: monthShort(s.month), customers: s.customers, nrr: s.nrr, grr: s.grr })), more: 0, total: null,
      });
    }
    return d;
  }
  const quarters = ((await ctx.quarters()) || []).slice().sort((a, b) => String(a.qs).localeCompare(String(b.qs)));
  const lastFull = quarters.filter((q) => !q.partial).pop();
  // One read of the churned opportunities covers the last full quarter and this year so far.
  const from = [lastFull ? String(lastFull.qs).slice(0, 7) : null, `${Y}-01`].filter(Boolean).sort()[0];
  const to = shiftMonth(ctx.closed, 3);
  const opps = await ctx.churnedOpps(from, to);
  if (item === 'churn-quarter') return lastFull ? churnQuarterDetail(lastFull, opps) : null;
  if (item === 'churn-ytd') return churnYtdDetail(Y, quarters, opps);
  if (item === 'churn-forecast') {
    const block = ctx.pnl.variants.plan.years.find((b) => b.year === Y);
    const months = (((ctx.pnlDetails || {}).variants || {}).plan || {})[Y];
    return block ? churnForecastDetail(Y, block.rows, months ? months.months : {}, quarters, opps) : null;
  }
  return null;
}

// ── handler ─────────────────────────────────────────────────────────────────
function envMinutes(name, fallback) {
  const v = parseFloat(process.env[name] || '');
  return Number.isFinite(v) && v > 0 ? v : fallback;
}

/**
 * Express/connect handler for GET /api/metrics. deps (all optional):
 *   cash, pnl        the projection handlers (their .current()); default: own instances on the same caches
 *   getSfClient()
 *   reads            override every external read (tests): { arr, churnQuarters, customerRevenue(from, to), churnedOpps(from, to), employees(company) }
 *   company          the HiBob company of the P&L's subsidiary (default METRICS_HEADCOUNT_COMPANY or 'LSports')
 *   settingsFile, targetsFile (the saved 2027 targets), clock(), ttlMs
 */
function createMetricsHandler(deps = {}) {
  const clock = deps.clock || Date.now;
  const ttlMs = deps.ttlMs ?? envMinutes('METRICS_TTL_MIN', 30) * MIN;
  const settingsFile = deps.settingsFile || SETTINGS_FILE;
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
    return {
      arr: () => sf().fetchCurrentARR(),
      churnQuarters: () => sf().fetchQuarterlyChurnMRR(),
      customerRevenue: (from, to) => sfCustomerRevenue(sf(), from, to),
      churnedOpps: (from, to) => sfChurnedOpportunities(sf(), from, to),
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
    let detail = null;
    try {
      const q = new URL(req.url || '', 'http://localhost').searchParams;
      refresh = /^(1|true)$/.test(q.get('refresh') || '');
      detail = q.get('detail');
    } catch { /* plain read */ }
    if (detail !== null && !DETAILS.includes(detail)) {
      send(res, 404, { ok: false, status: 'error', error: 'There is no breakdown for that figure.' });
      return;
    }
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
    // Revenue by customer is kept as read: the NRR series and its breakdown both come from it.
    const customerRevenue = () => cached(`custrev:${closed}`, () => reads.customerRevenue(shiftMonth(nrrMonths[0], -12), closed));

    if (detail !== null) {
      try {
        const out = await buildDetail(detail, {
          pnl: p.entry.payload, pnlDetails: p.entry.details, closed, nrrMonths,
          quarters: () => cached('churn', reads.churnQuarters),
          customerRevenue,
          churnedOpps: (from, to) => cached(`churnOpps:${from}:${to}`, () => reads.churnedOpps(from, to)),
        });
        if (out) send(res, 200, { ok: true, status: 'ready', detail: out });
        else send(res, 200, { ok: false, status: 'error', error: 'There is nothing to break down for this figure yet.' });
      } catch (e) {
        console.warn(`[metrics] detail ${detail} unavailable: ${e && e.message}`);
        send(res, 200, { ok: false, status: 'error', error: 'The breakdown could not be read from Snowflake right now. Try again in a minute.' });
      }
      return;
    }

    const settled = await Promise.allSettled([
      cached('arr', reads.arr),
      cached('churn', reads.churnQuarters),
      customerRevenue().then((rows) => nrrSeries(rows, nrrMonths)),
      cached(`employees:${company}`, () => reads.employees(company)),
    ]);
    const names = ['arr', 'churn', 'nrr', 'employees'];
    const failed = [];
    const val = (i) => {
      if (settled[i].status === 'fulfilled') return settled[i].value;
      failed.push(names[i]);
      console.warn(`[metrics] ${names[i]} unavailable: ${settled[i].reason && settled[i].reason.message}`);
      return null;
    };
    const extras = { arr: val(0), churnQuarters: val(1), nrr: val(2), employees: val(3), company };
    const settingsDoc = readDoc(settingsFile);
    // Settings saved before the innovation envelope and the planning rate were removed still carry them.
    const { innovation: _envelope, usdEurPlanningRate: _rate, ...savedSettings } = (settingsDoc && settingsDoc.value) || {};
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
        settings: { ...emptySettings(), ...savedSettings },
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
  buildMetrics, buildDetail, nrrSeries, nrrDetail, churnQuarterDetail, churnYtdDetail, churnForecastDetail, DETAILS,
  shiftMonth, lastClosedMonth, cloudTargetsOf, headcountByMonth, peopleMetrics,
};
