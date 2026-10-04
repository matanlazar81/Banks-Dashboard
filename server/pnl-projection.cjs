// ─────────────────────────────────────────────────────────────────────────────
// GET /api/pnl-projection — the P&L Projection page's one data call.
//
// The New Bank Dashboard's projection on an accrual (P&L) basis, with the accumulated net profit:
// the current year (actuals + forecast) and the next year rolled forward from December, for the
// official plan and the base forecast.
//
//   • actual months  — NetSuite GL by account (ns.fetchPnlActuals), mapped to lines by
//                      server/pnl-lines.cjs. Every line equals NetSuite exactly; EBITDA is the
//                      "EBITDA_Profit and Loss" Operating Profit and Net profit the sum of every P&L
//                      account.
//   • forecast months — the same engine and inputs as the cash projection (shared input stage), run
//                      with the current month projected as a whole month and the collection rate at
//                      100% (revenue is recognised, not collected). Lines the cash engine does not
//                      model come from NetSuite run-rates or the Snowflake budget (see forecastExtras).
//   • next year      — the cash projection's roll-forward (src/forecast/roll-forward.mjs), fed with
//                      this year's P&L months instead of its cash months.
//
// Responses, caching and refresh behave exactly like /api/cash-projection (createProjectionHandler):
// cache file data/pnl-projection-cache.json, PNL_PROJECTION_TTL_MIN / _TIMEOUT_MIN / _PREWARM
// (default: the CASH_PROJECTION_* values). NetSuite actuals are grouped by accounting (posting)
// period, as NetSuite's Profit and Loss report does; PNL_NS_DATE_BASIS=trandate groups them by
// transaction date instead.
// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');
const cp = require('./cash-projection.cjs');
const { loadProjectionInputs, wrapClient } = require('./projection-inputs.cjs');
const { sumByLine, classifyAccount } = require('./pnl-lines.cjs');

const ROOT = path.resolve(__dirname, '..');
const SCHEMA_VERSION = 1;
const DETAILS_VERSION = 1;
const DEFAULT_CACHE_FILE = path.join(ROOT, 'data', 'pnl-projection-cache.json');
const { ProjectionError } = cp;

const pad2 = (n) => String(n).padStart(2, '0');
const cents = (n) => Math.round((Number(n) || 0) * 100) / 100;
const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const mk = (y, mi) => `${y}-${pad2(mi + 1)}`;
const ZERO = () => ({ eur: 0, ils: 0 });
const CCYS = ['eur', 'ils'];

// The month figures every row carries (per currency), in display convention:
//   revenue, pipeline, otherRevenue, totalRevenue  income positive (churn: amount lost, shown as a deduction)
//   payroll, capex, opex, totalCosts               costs positive (the Salaries CAPEX credit is negative)
//   fx, finance, depreciation, taxOther            profit-signed (gain positive, cost negative)
//   ebitda, net, accOpening, accClosing
const BASE_KEYS = ['revenue', 'pipeline', 'churn', 'otherRevenue', 'payroll', 'capex', 'opex', 'fx', 'finance', 'depreciation', 'taxOther'];

const FEED_LABELS = {
  ...cp.FEED_LABELS,
  'ns.fetchPnlActuals': 'NetSuite P&L actuals',
  'sf.fetchPnlBudgetExtras': 'Snowflake depreciation and tax budget',
};

function finish(f) {
  const o = {};
  for (const k of BASE_KEYS) o[k] = cents(f[k]);
  o.totalRevenue = cents(o.revenue + o.pipeline - o.churn + o.otherRevenue);
  o.totalCosts = cents(o.payroll + o.capex + o.opex);
  o.ebitda = cents(o.totalRevenue - o.totalCosts);
  o.net = cents(o.ebitda + o.fx + o.finance + o.depreciation + o.taxOther);
  return o;
}

// One NetSuite month (line totals, profit-signed) in display convention.
function actualFigures(totals, ccy) {
  const t = (k) => num(totals[k] && totals[k][ccy]);
  return {
    revenue: t('revenue'), pipeline: 0, churn: 0, otherRevenue: t('otherRevenue'),
    payroll: -t('payroll'), capex: -t('capex'), opex: -t('opex'),
    fx: t('fx'), finance: t('finance'), depreciation: t('depreciation'), taxOther: t('taxOther'),
  };
}

// One engine month plus the non-engine lines (extras, profit-signed) in display convention.
function forecastFigures(r, extras, ccy) {
  const ils = ccy === 'ils';
  return {
    revenue: num(ils ? r.collectionsILS : r.collections),
    pipeline: num(ils ? r.pipelineWeightedILS : r.pipelineWeighted),
    churn: num(ils ? r.churnDeductionILS : r.churnDeduction),
    otherRevenue: extras.otherRevenue[ccy],
    payroll: num(ils ? r.salaryILS : r.salary),
    capex: -extras.capex[ccy],
    opex: num(ils ? r.vendorsILS : r.vendors),
    fx: num(ils ? r.revalImpactILS : r.revalImpact),
    finance: extras.finance[ccy],
    depreciation: extras.depreciation[ccy],
    taxOther: extras.taxOther[ccy],
  };
}

/** The n month keys before (year, monthIdx), most recent last. */
function monthsBefore(year, monthIdx, n) {
  const out = [];
  for (let k = n; k >= 1; k--) {
    const d = new Date(year, monthIdx - k, 1);
    out.push(mk(d.getFullYear(), d.getMonth()));
  }
  return out;
}
const shortLabel = (mKey) => new Date(`${mKey}-15T12:00:00`).toLocaleString('en-GB', { month: 'short', year: 'numeric' });

/**
 * The lines the cash engine does not model, for forecast months (variant-independent). From the NetSuite
 * actuals (profit-signed line totals by month) and the Snowflake budget rows.
 *   otherRevenue, finance  average of the last 3 closed months
 *   capex                  the last closed month with a Salaries CAPEX amount, carried flat
 *   depreciation           the year's budget when it has one, else the average of the last 3 closed months
 *                          (posted monthly; a longer window would lag behind changes in the asset base)
 *   taxOther               the year's budget when it has one, else nothing
 */
function makeForecastRules({ totalsByMonth, budgetRows, currentYear, currentIdx }) {
  const last3 = monthsBefore(currentYear, currentIdx, 3);
  const last12 = monthsBefore(currentYear, currentIdx, 12);
  const avg = (line, keys) => {
    const s = ZERO();
    for (const k of keys) for (const c of CCYS) s[c] += num(totalsByMonth[k] && totalsByMonth[k][line] && totalsByMonth[k][line][c]);
    return { eur: s.eur / keys.length, ils: s.ils / keys.length };
  };
  let capexFrom = null;
  for (let i = last12.length - 1; i >= 0 && !capexFrom; i--) {
    const c = totalsByMonth[last12[i]] && totalsByMonth[last12[i]].capex;
    if (c && Math.abs(c.eur) >= 0.5) capexFrom = last12[i];
  }
  const capex = capexFrom ? { ...totalsByMonth[capexFrom].capex } : ZERO();

  // Budget by month and line (profit-signed), plus which years carry any budget for a line.
  const budget = {};
  const budgetYears = {};
  for (const r of budgetRows || []) {
    const line = classifyAccount(r.acct, r.type);
    if (line !== 'depreciation' && line !== 'taxOther') continue;
    const m = budget[r.month] || (budget[r.month] = {});
    const cell = m[line] || (m[line] = { eur: 0, ils: 0, accounts: [] });
    cell.eur -= num(r.eur);
    cell.ils -= num(r.ils);
    cell.accounts.push({ acct: r.acct, name: r.name, eur: -num(r.eur), ils: -num(r.ils) });
    (budgetYears[line] || (budgetYears[line] = new Set())).add(r.month.slice(0, 4));
  }
  const fromBudget = (line, mKey) => !!(budgetYears[line] && budgetYears[line].has(mKey.slice(0, 4)));
  const budgetOf = (line, mKey) => {
    const c = budget[mKey] && budget[mKey][line];
    return c ? { eur: c.eur, ils: c.ils } : ZERO();
  };

  const avgOther = avg('otherRevenue', last3);
  const avgFinance = avg('finance', last3);
  const avgDep = avg('depreciation', last3);
  return {
    last3, capexFrom,
    extrasFor(mKey) {
      return {
        otherRevenue: avgOther,
        finance: avgFinance,
        capex,
        depreciation: fromBudget('depreciation', mKey) ? budgetOf('depreciation', mKey) : avgDep,
        taxOther: fromBudget('taxOther', mKey) ? budgetOf('taxOther', mKey) : ZERO(),
      };
    },
    // For the breakdown: how each non-engine line of a month was built.
    methodFor(mKey) {
      const accounts = (line) => ((budget[mKey] && budget[mKey][line] && budget[mKey][line].accounts) || []).map((a) => ({ ...a }));
      return {
        depreciation: fromBudget('depreciation', mKey) ? { method: 'budget', accounts: accounts('depreciation') } : { method: 'average', months: last3 },
        taxOther: fromBudget('taxOther', mKey) ? { method: 'budget', accounts: accounts('taxOther') } : { method: 'none' },
      };
    },
  };
}

// Server-only facts for the breakdown of a forecast month (engine values; the cells come from the payload).
function captureForecastMonth(r, inputs, i, forecastIndex) {
  const pct = (m, def) => num((m || {})[i] ?? def);
  return {
    mr: num(r.collections) - num(r.collectionsPipeline),
    deals: num(r.collectionsPipeline),
    pipelinePct: pct(inputs.pipelineAdjPctByMonth, 100),
    churnIndex: forecastIndex,
    churnOverride: (inputs.churnOverride || {})[r.mKey] !== undefined,
    salaryBase: num(r.salaryBase),
    vendorsBase: num(r.vendorsBase),
    categories: (inputs.sfBudget && inputs.sfBudget.byMonth && inputs.sfBudget.byMonth[r.mKey]) || null,
    defense: (() => {
      const p = Number(((inputs.currencyDefensePctByMonth || {})[i]) ?? inputs.currencyDefensePct ?? 0);
      const fin = (inputs.sfFinanceBudget || {})[r.mKey];
      const catData = (inputs.sfBudget && inputs.sfBudget.byMonth && inputs.sfBudget.byMonth[r.mKey]) || {};
      const budget = fin && fin.eur !== 0 ? Math.abs(num(fin.eur)) : Math.abs(num(catData['Other (800)']));
      return { budget, pct: Number.isFinite(p) ? p : 0 };
    })(),
  };
}

function pipelineCohorts(rows, inputs) {
  const byMonth = (inputs.pipelineMethodology && inputs.pipelineMethodology.byMonth) || {};
  const out = {};
  for (const r of rows) {
    if (r.isPast || r.isCurrent) continue;
    out[r.mKey] = Math.round(num(byMonth[r.mKey] && byMonth[r.mKey].monthlyContribution));
  }
  return out;
}

/**
 * Compute the full payload. opts (as computeCashProjection):
 *   now, getNsClient(sub), getSfClient(), queueNsCall(fn), computeModule (tests), includeRaw (CLI)
 */
async function computePnlProjection(opts) {
  const now = opts.now || new Date();
  const cmp = opts.computeModule || require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs'));
  const { computeCashflowForecast, rf } = await cp.loadForecastModules();

  if (!process.env.NETSUITE_ACCOUNT_ID) throw new ProjectionError('NetSuite is not configured on this server.');
  const nsClient = opts.getNsClient(cmp.SUBSIDIARY);
  const sfClient = opts.getSfClient();
  if (!nsClient) throw new ProjectionError('The NetSuite client is unavailable on this server.');
  if (!sfClient) throw new ProjectionError('Snowflake is not configured on this server.');

  const Y = now.getFullYear();
  const T = Y + 1;
  const curIdx = now.getMonth();
  const scenarioName = process.env.NET_CASH_SCENARIO_NAME || cp.DEFAULT_SCENARIO;
  const basis = process.env.PNL_NS_DATE_BASIS === 'trandate' ? 'trandate' : 'period';

  const shared = await loadProjectionInputs({ now, cmp, nsClient, sfClient, queueNsCall: opts.queueNsCall, scenarioName });
  const { inputs, meta, extras, scenario } = shared;
  const failures = [...shared.failures];
  const ns = wrapClient(nsClient, 'ns', failures, opts.queueNsCall);
  const sf = wrapClient(sfClient, 'sf', failures, null);
  const [actuals, budgetRows] = await Promise.all([
    ns.fetchPnlActuals({ fromYear: Y - 1, toYear: Y, basis }).catch(() => null),
    sf.fetchPnlBudgetExtras(Y, T).catch(() => []),
  ]);
  if (!actuals || !actuals.byMonth) {
    throw new ProjectionError('NetSuite P&L actuals are unavailable, so the actual months cannot be shown.');
  }

  // NetSuite line totals and accounts per month (profit-signed).
  const totalsByMonth = {};
  const accountsByMonth = {};
  for (const [mKey, accts] of Object.entries(actuals.byMonth)) {
    const s = sumByLine(accts);
    totalsByMonth[mKey] = s.totals;
    accountsByMonth[mKey] = s.accounts;
  }
  const rules = makeForecastRules({ totalsByMonth, budgetRows, currentYear: Y, currentIdx: curIdx });
  const statusOf = (mi) => (mi < curIdx ? 'actual' : mi === curIdx ? 'current' : 'forecast');

  const snapshot = rf.snapshotFieldsFromLive({
    sourceYear: Y,
    targetYear: T,
    srcInputs: inputs,
    rawSfBudget: extras.rawSfBudget || { byMonth: (inputs.sfBudget || {}).byMonth || {} },
    rawSfSalaryBudget: extras.rawSfSalaryBudget || inputs.sfSalaryBudget || {},
    now,
  });

  // The engine sees the last day of the previous month as "now": the current month is then a whole
  // forecast month (its partial postings are not a month's P&L). Closed months are NetSuite's anyway.
  const pnlNow = new Date(Y, curIdx, 0, 12);
  const planData = (scenario && scenario.data) || {};
  const planLoaded = Object.keys(planData).length > 0;
  const variants = {};
  const raw = {};
  const details = {
    version: DETAILS_VERSION, years: [Y, T], basis: actuals.basis, accountIds: actuals.accountIds || {},
    accounts: Object.fromEntries(Object.entries(accountsByMonth).filter(([k]) => k.startsWith(`${Y}-`) && k < mk(Y, curIdx))),
    rules: { last3: rules.last3, capexFrom: rules.capexFrom, totals: totalsByMonth },
    variants: {},
  };

  for (const [variant, sd] of [['plan', planData], ['base', {}]]) {
    const inputsY = {
      ...inputs,
      ...cmp.scenarioKnobs(sd, Y),
      collPctByMonth: {},
      activeYear: Y,
      currentYear: Y,
      now: pnlNow,
      asOfDate: null,
      lastActualSalaryMonth: meta.lastActualSalaryMonth,
      ilsRevalRate: cmp.ILS_REVAL_RATE,
    };
    const rowsY = computeCashflowForecast(inputsY);

    const capY = {};
    let fIdx = 0;
    const monthsY = rowsY.map((r, mi) => {
      const status = statusOf(mi);
      if (status === 'actual') {
        const totals = totalsByMonth[r.mKey] || {};
        return { mKey: r.mKey, status, eur: finish(actualFigures(totals, 'eur')), ils: finish(actualFigures(totals, 'ils')) };
      }
      const x = rules.extrasFor(r.mKey);
      if (!r.isPast && !r.isCurrent) fIdx++;
      capY[r.mKey] = { ...captureForecastMonth(r, inputsY, mi, fIdx), ...rules.methodFor(r.mKey) };
      return { mKey: r.mKey, status, eur: finish(forecastFigures(r, x, 'eur')), ils: finish(forecastFigures(r, x, 'ils')) };
    });

    // Next year: the cash roll-forward, fed with this year's P&L months (customer revenue run-rate incl.
    // the pipeline and churn already in it, payroll, operating expenses mirrored month by month).
    const srcRows = rowsY.map((r, mi) => {
      const m = monthsY[mi];
      return {
        ...r,
        collections: m.eur.revenue + m.eur.pipeline - m.eur.churn,
        collectionsILS: m.ils.revenue + m.ils.pipeline - m.ils.churn,
        salary: m.eur.payroll, salaryILS: m.ils.payroll,
        vendors: m.eur.opex, vendorsILS: m.ils.opex,
      };
    });
    const salaryBasis = rf.synthesizeSalaryBasis(extras.breakdowns, srcRows, Y);
    const knobsT = {
      ...cmp.scenarioKnobs(sd, T),
      ...rf.inheritProjectionMaps(sd, T, Y),
      collPctByMonth: {},
      revenueMethodology: 'pipeline',
      salaryProjectionMode: 'lastActual',
    };
    const inputsT = rf.buildNextYearInputs({
      sourceYear: Y, targetYear: T, srcInputs: inputsY, srcRows, snapshot, salaryBasis,
      knobs: knobsT, now, ilsRevalRate: cmp.ILS_REVAL_RATE,
    });
    const rowsT = computeCashflowForecast(inputsT);
    const capT = {};
    let tIdx = 0;
    const monthsT = rowsT.map((r, mi) => {
      const x = rules.extrasFor(r.mKey);
      tIdx++;
      capT[r.mKey] = { ...captureForecastMonth(r, inputsT, mi, tIdx), ...rules.methodFor(r.mKey) };
      return { mKey: r.mKey, status: 'forecast', eur: finish(forecastFigures(r, x, 'eur')), ils: finish(forecastFigures(r, x, 'ils')) };
    });

    // Accumulated net profit: from 0 on 1 January of the current year, carried into the next year.
    const acc = ZERO();
    for (const m of [...monthsY, ...monthsT]) {
      for (const c of CCYS) {
        m[c].accOpening = cents(acc[c]);
        acc[c] += m[c].net;
        m[c].accClosing = cents(acc[c]);
      }
    }
    const dec = monthsY[11];
    variants[variant] = {
      years: [{ year: Y, kind: 'current', rows: monthsY }, { year: T, kind: 'projection', rows: monthsT }],
      rollForward: {
        from: `${Y}-12`,
        to: `${T}-01`,
        accumulated: { eur: dec.eur.accClosing, ils: dec.ils.accClosing },
        salaryBasis: salaryBasis
          ? { method: 'run-rate', monthsWithData: salaryBasis.monthsWithData, scale: salaryBasis.scale }
          : { method: 'flat-budget', monthsWithData: 0, scale: null },
      },
    };
    details.variants[variant] = {
      [Y]: { months: capY, pipeline: pipelineCohorts(rowsY, inputsY), salaryBasis: basisOf(inputsY) },
      [T]: { months: capT, pipeline: {}, salaryBasis: basisOf(inputsT) },
    };
    if (opts.includeRaw) raw[variant] = { rows: { [Y]: rowsY, [T]: rowsT }, inputs: { [Y]: inputsY, [T]: inputsT } };
  }

  const actualThrough = curIdx > 0 ? mk(Y, curIdx - 1) : null;
  const warnings = [];
  if (!planLoaded) warnings.push(`The plan "${scenarioName}" was not found, so Plan shows the base forecast.`);
  if (!meta.lastActualSalaryMonth) warnings.push('No closed payroll month to project salary from; payroll falls back to the budget.');
  if (actualThrough && !Object.keys(actuals.byMonth).some((k) => k === actualThrough)) {
    warnings.push(`NetSuite has no P&L postings for ${shortLabel(actualThrough)} yet.`);
  }
  if (!extras.breakdowns.some((b) => Array.isArray(b) && b.length > 0)) {
    warnings.push(`No Oct–Dec ${Y} payroll budget by department; ${T} payroll uses the flat budget average.`);
  }
  const failedFeeds = [...new Set(failures)];
  if (failedFeeds.length) {
    warnings.push(`Some data sources failed and fell back to defaults: ${failedFeeds.map((f) => FEED_LABELS[f] || f).join(', ')}.`);
  }

  const src = String((scenario && scenario.source) || '');
  const payload = {
    ok: true,
    schemaVersion: SCHEMA_VERSION,
    status: 'ready',
    generatedAt: new Date().toISOString(),
    computedMonth: `${Y}-${pad2(curIdx + 1)}`,
    company: cp.COMPANY,
    years: [Y, T],
    plan: { name: scenarioName, loaded: planLoaded, source: !planLoaded ? 'none' : src.startsWith('Postgres') ? 'postgres' : 'file' },
    actuals: { source: 'netsuite', basis: actuals.basis, through: actualThrough },
    degraded: failedFeeds.length > 0,
    failedFeeds,
    warnings,
    variants,
  };
  return opts.includeRaw ? { payload, details, raw } : { payload, details };
}

function basisOf(inputs) {
  const lam = inputs.lastActualSalaryMonth || '';
  const byDept = lam ? (inputs.salaryActualsByDept || {})[lam] : null;
  return byDept
    ? { month: lam, byDept: Object.fromEntries(Object.entries(byDept).map(([d, v]) => [d, { eur: num(v && v.eur), ils: num(v && v.ils) }])) }
    : null;
}

/**
 * Express/connect handler for GET /api/pnl-projection. deps as createCashProjectionHandler:
 *   getNsClient, getSfClient, queueNsCall, compute(nowDate), clock(), cacheFile, ttlMs, timeoutMs, …
 */
function createPnlProjectionHandler(deps = {}) {
  return cp.createProjectionHandler({
    ...deps,
    schemaVersion: SCHEMA_VERSION,
    logTag: 'pnl-projection',
    envPrefix: 'PNL_PROJECTION',
    cacheFile: deps.cacheFile === undefined ? DEFAULT_CACHE_FILE : deps.cacheFile,
    compute: deps.compute || ((nowDate) => computePnlProjection({
      now: nowDate,
      getNsClient: deps.getNsClient || cp.defaultGetNsClient,
      getSfClient: deps.getSfClient || cp.defaultGetSfClient,
      queueNsCall: deps.queueNsCall || cp.defaultQueueNsCall,
    })),
  });
}

module.exports = {
  createPnlProjectionHandler,
  computePnlProjection,
  makeForecastRules,
  monthsBefore,
  SCHEMA_VERSION,
  DETAILS_VERSION,
  DEFAULT_CACHE_FILE,
  BASE_KEYS,
};
