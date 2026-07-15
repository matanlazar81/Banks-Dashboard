// Phase-0 parity check for the SF revenue-table migration.
//
//   node scripts/verify-mr-migration.cjs [--year=2026] [--asof=YYYY-MM-DD] [--months=2026-05,2026-06,2025-06]
//
// Compares the OLD Salesforce revenue tables (DL_PRODUCTION.FINANCE.*, frozen 2026-06-23)
// against the NEW replacement table, at the SOURCE, in a single run (both are queryable now).
// Read-only. Writes a machine JSON + a reviewer-ready report under data/migration-snapshots/.
//
// It mirrors the exact aggregations the app runs today (snowflake-api.cjs):
//   fetchMonthlyRevenuePaid, fetchYoYRevenue, fetchRevenueBreakdown, fetchRevenueProjection,
//   and the pipeline calibration numerator + sfContribByMonth.
//
// Expected per the migration plan: 2025 + Jan–May 2026 near-exact; June/July 2026 diffs are
// legitimate SF corrections after the 2026-06-23 freeze (GAP 4). Projection diffs are GAP 5/6.
//
// NOTE (BI-3228 phase 2): the consumer-hub schema was since renamed
// CONSUMER_HUB__BANKS_DASHBOARD → CONSUMER_HUB__FINANCE (views suffixed __FINANCE). This script
// deliberately still reads the OLD DL_PRODUCTION.FINANCE.* tables for the phase-1 parity record;
// it is UNRUNNABLE after the grant cutover (those schemas are revoked from the finance user).
// Kept as a historical artifact — do not repoint its constants.

const path = require('path');
const fs = require('fs');
const { createSnowflakeClient } = require('../snowflake-api.cjs');

// ── env (identical walk to scripts/diagnose-calibration.cjs) ──────────────────
function loadEnv() {
  const candidates = [];
  let dir = __dirname;
  for (let i = 0; i < 6; i++) {
    for (const name of ['backend/.env', '.env']) {
      const p = path.join(dir, name);
      if (fs.existsSync(p)) candidates.push(p);
    }
    dir = path.dirname(dir);
  }
  for (const p of candidates) {
    const txt = fs.readFileSync(p, 'utf-8');
    if (/^\s*SNOWFLAKE_ACCOUNT\s*=/m.test(txt)) {
      require('dotenv').config({ path: p, override: true });
      return { path: p, hasSnowflake: true };
    }
  }
  if (candidates.length) { require('dotenv').config({ path: candidates[0], override: true }); return { path: candidates[0], hasSnowflake: false }; }
  return null;
}

// ── CLI args ──────────────────────────────────────────────────────────────────
function arg(name, def) {
  const hit = process.argv.slice(2).find(a => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : def;
}

const OLD_SUBSET = 'DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE__SUBSET_PAID';
const OLD_AVT    = 'DL_PRODUCTION.FINANCE.FCT_REVENUE__MONTHLY_ACTUAL_VS_TARGET';
const NEW_TABLE  = 'DL_PRODUCTION.CONSUMER_HUB__BANKS_DASHBOARD.FCT_OPPORTUNITY_MONTHLY_REVENUE__SCD_DAILY__BANKS_DASHBOARD';
const TARGET     = 'DL_PRODUCTION.FINANCE.FCT_REVENUE_TARGET';
const DIM_OPP    = 'DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY';

const num = (v) => (v == null ? 0 : Number(v) || 0);
const round = (v) => Math.round(num(v));
function diff(oldV, newV) {
  const o = num(oldV), n = num(newV);
  const abs = n - o;
  const pct = o !== 0 ? (abs / Math.abs(o)) * 100 : (n !== 0 ? Infinity : 0);
  return { old: round(o), new: round(n), abs: round(abs), pct: Number.isFinite(pct) ? Math.round(pct * 10) / 10 : null, flag: !Number.isFinite(pct) ? (n !== 0) : Math.abs(pct) > 1 };
}

(async () => {
  const envInfo = loadEnv();
  console.log(`[verify-mr] env: ${envInfo ? envInfo.path : '(none)'} (snowflake=${envInfo?.hasSnowflake ? 'yes' : 'no'})`);
  const sf = createSnowflakeClient(process.env);
  if (!sf) { console.error('Snowflake client unavailable (check SNOWFLAKE_* env / key path)'); process.exit(1); }
  const q = sf.query.bind(sf);

  const year = parseInt(arg('year', '2026'), 10);
  const priorYr = year - 1;
  const asof = arg('asof', new Date().toISOString().slice(0, 10));
  const curMonth = parseInt(asof.slice(5, 7), 10) || 12;
  const sampleMonths = arg('months', `${year}-05,${year}-06,${priorYr}-06`).split(',').map(s => s.trim()).filter(Boolean);

  console.log(`[verify-mr] year=${year} priorYr=${priorYr} asof=${asof} (curMonth=${curMonth}) samples=${sampleMonths.join(', ')}`);

  const report = { generatedAt: new Date().toISOString(), year, priorYr, asof, oldTables: { subset: OLD_SUBSET, avt: OLD_AVT }, newTable: NEW_TABLE, sections: {}, newTableReadable: true };

  // Guard: confirm the new table is grant-readable before doing everything else.
  try {
    await q(`SELECT 1 FROM ${NEW_TABLE} LIMIT 1`);
  } catch (e) {
    report.newTableReadable = false;
    report.newTableError = e.message;
    console.error(`\n[verify-mr] ⚠ Cannot read the NEW table (${NEW_TABLE}):\n   ${e.message}\n   → The role likely lacks a grant on CONSUMER_HUB__BANKS_DASHBOARD. Old-side capture will still run.`);
  }

  // Which integration filter does the live calibration use today? (GAP 3)
  let integrationCol = null;
  try {
    const meth = await sf.fetchPipelineMethodology(year);
    integrationCol = meth?.mrColumnsResolved?.integration || null;
    report.sections.integrationDecision = { mrColumnsResolved: meth?.mrColumnsResolved || null, keepIntegrationFilter: !!integrationCol };
    console.log(`[verify-mr] GAP-3 integration column resolved today: ${integrationCol || '(none)'} → calibration filter ${integrationCol ? 'KEPT' : 'OMITTED'}`);
  } catch (e) {
    console.warn(`[verify-mr] could not read live methodology for integration decision: ${e.message}`);
  }

  // Probe whether each table exposes IS_INTEGRATION_MONTH so we mirror the filter only where valid.
  async function hasIntegration(fqTable) {
    const [db, schema, table] = fqTable.split('.');
    try {
      const rows = await q(`SELECT COLUMN_NAME FROM ${db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${table}' AND COLUMN_NAME = 'IS_INTEGRATION_MONTH'`);
      return rows.length > 0;
    } catch { return false; }
  }

  // ── 1. Monthly revenue/paid/unpaid/customers (mirror fetchMonthlyRevenuePaid) ──
  async function monthly(table, yr) {
    const rows = await q(`
      SELECT CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR,
             ROUND(SUM(REVENUE_AMOUNT_EUR)) AS TOTAL_REV,
             ROUND(SUM(PAID_REVENUE_AMOUNT_EUR)) AS PAID_REV,
             ROUND(SUM(UNPAID_REVENUE_AMOUNT_EUR)) AS UNPAID_REV,
             COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMERS
      FROM ${table}
      WHERE CAL_MONTH_START_DATE >= '${yr}-01-01' AND CAL_MONTH_START_DATE < '${yr + 1}-01-01'
      GROUP BY CAL_MONTH_START_DATE ORDER BY CAL_MONTH_START_DATE
    `);
    const out = {};
    for (const r of rows) out[(r.MONTH_STR || '').slice(0, 7)] = { revenue: round(r.TOTAL_REV), paid: round(r.PAID_REV), unpaid: round(r.UNPAID_REV), customers: round(r.CUSTOMERS) };
    return out;
  }

  for (const yr of [year, priorYr]) {
    const oldM = await monthly(OLD_SUBSET, yr).catch(e => ({ __error: e.message }));
    const newM = report.newTableReadable ? await monthly(NEW_TABLE, yr).catch(e => ({ __error: e.message })) : {};
    const months = [...new Set([...Object.keys(oldM), ...Object.keys(newM)])].filter(m => m !== '__error').sort();
    report.sections[`monthly_${yr}`] = months.map(m => ({
      month: m,
      revenue: diff(oldM[m]?.revenue, newM[m]?.revenue),
      paid: diff(oldM[m]?.paid, newM[m]?.paid),
      unpaid: diff(oldM[m]?.unpaid, newM[m]?.unpaid),
      customers: diff(oldM[m]?.customers, newM[m]?.customers),
    }));
    if (oldM.__error) report.sections[`monthly_${yr}`] = { __oldError: oldM.__error };
  }

  // ── 2. YoY aggregates (mirror fetchYoYRevenue) ────────────────────────────────
  async function yoy(table) {
    const rows = await q(`
      SELECT YEAR(CAL_MONTH_START_DATE) AS YR,
             ROUND(SUM(REVENUE_AMOUNT_EUR)) AS TOTAL_REV,
             ROUND(SUM(PAID_REVENUE_AMOUNT_EUR)) AS PAID_REV,
             COUNT(DISTINCT CUSTOMER_ID) AS CUSTOMERS
      FROM ${table}
      WHERE (YEAR(CAL_MONTH_START_DATE) = ${year} AND MONTH(CAL_MONTH_START_DATE) <= ${curMonth})
         OR (YEAR(CAL_MONTH_START_DATE) = ${priorYr} AND MONTH(CAL_MONTH_START_DATE) <= ${curMonth})
      GROUP BY YEAR(CAL_MONTH_START_DATE) ORDER BY YR
    `);
    const out = {};
    for (const r of rows) out[num(r.YR)] = { revenue: round(r.TOTAL_REV), paid: round(r.PAID_REV), customers: round(r.CUSTOMERS) };
    return out;
  }
  {
    const oldY = await yoy(OLD_SUBSET).catch(e => ({ __error: e.message }));
    const newY = report.newTableReadable ? await yoy(NEW_TABLE).catch(e => ({ __error: e.message })) : {};
    report.sections.yoy = [year, priorYr].map(yr => ({
      year: yr,
      revenue: diff(oldY[yr]?.revenue, newY[yr]?.revenue),
      paid: diff(oldY[yr]?.paid, newY[yr]?.paid),
      customers: diff(oldY[yr]?.customers, newY[yr]?.customers),
    }));
    if (oldY.__error) report.sections.yoy = { __oldError: oldY.__error };
  }

  // ── 3. Per-customer breakdown for sample months (mirror fetchRevenueBreakdown) ─
  async function perCustomer(table, month) {
    const rows = await q(`
      SELECT OPPORTUNITY_NAME AS CUST_NAME,
             ROUND(SUM(REVENUE_AMOUNT_EUR)) AS REV_EUR,
             ROUND(SUM(PAID_REVENUE_AMOUNT_EUR)) AS PAID_EUR
      FROM ${table}
      WHERE CAL_MONTH_START_DATE = TO_DATE('${month}-01') AND REVENUE_AMOUNT_EUR > 0
      GROUP BY OPPORTUNITY_NAME ORDER BY SUM(REVENUE_AMOUNT_EUR) DESC LIMIT 100
    `);
    const out = {};
    for (const r of rows) out[r.CUST_NAME || '(null)'] = { revenue: round(r.REV_EUR), paid: round(r.PAID_EUR) };
    return out;
  }
  report.sections.perCustomer = {};
  for (const m of sampleMonths) {
    const oldC = await perCustomer(OLD_SUBSET, m).catch(e => ({ __error: e.message }));
    const newC = report.newTableReadable ? await perCustomer(NEW_TABLE, m).catch(e => ({ __error: e.message })) : {};
    const names = [...new Set([...Object.keys(oldC), ...Object.keys(newC)])].filter(n => n !== '__error');
    const custRows = names.map(name => ({ customer: name, revenue: diff(oldC[name]?.revenue, newC[name]?.revenue) }))
      .filter(r => r.revenue.flag).sort((a, b) => Math.abs(b.revenue.abs) - Math.abs(a.revenue.abs));
    report.sections.perCustomer[m] = {
      oldCount: Object.keys(oldC).filter(k => k !== '__error').length,
      newCount: Object.keys(newC).filter(k => k !== '__error').length,
      oldTotal: Object.values(oldC).reduce((s, v) => s + (v.revenue || 0), 0),
      newTotal: Object.values(newC).reduce((s, v) => s + (v.revenue || 0), 0),
      diffs: custRows, // only customers differing > 1%
    };
    if (oldC.__error) report.sections.perCustomer[m] = { __oldError: oldC.__error };
  }

  // ── 4. Revenue projection (mirror fetchRevenueProjection) ─────────────────────
  // OLD budget = FORECAST_EUR || SF_REV_EUR ; OLD actual = ACTUAL_EUR || NS_REV_EUR (from AVT).
  // NEW budget = NEW actual = SUM(REVENUE_AMOUNT_EUR) by month (same table).
  {
    const oldRows = await q(`
      SELECT CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR,
             ROUND(REVENUE_EUR_ACTUAL) AS ACTUAL_EUR, ROUND(REVENUE_EUR_FORECAST) AS FORECAST_EUR,
             ROUND(SF_REVENUE_AMOUNT_EUR) AS SF_REV_EUR, ROUND(NS_REVENUE_AMOUNT_EUR) AS NS_REV_EUR
      FROM ${OLD_AVT}
      WHERE CAL_MONTH_START_DATE >= '${priorYr}-01-01' AND CAL_MONTH_START_DATE <= '${year}-12-31'
      ORDER BY CAL_MONTH_START_DATE
    `).catch(e => ({ __error: e.message }));
    const newRows = report.newTableReadable ? await q(`
      SELECT CAL_MONTH_START_DATE::VARCHAR AS MONTH_STR, ROUND(SUM(REVENUE_AMOUNT_EUR)) AS REV_EUR
      FROM ${NEW_TABLE}
      WHERE CAL_MONTH_START_DATE >= '${priorYr}-01-01' AND CAL_MONTH_START_DATE <= '${year}-12-31'
      GROUP BY CAL_MONTH_START_DATE ORDER BY CAL_MONTH_START_DATE
    `).catch(e => ({ __error: e.message })) : [];
    if (oldRows.__error) {
      report.sections.projection = { __oldError: oldRows.__error };
    } else {
      const oldBudget = {}, oldActual = {}, newSum = {};
      for (const r of oldRows) { const m = (r.MONTH_STR || '').slice(0, 7); oldBudget[m] = round(r.FORECAST_EUR || r.SF_REV_EUR || 0); oldActual[m] = round(r.ACTUAL_EUR || r.NS_REV_EUR || 0); }
      for (const r of (newRows.__error ? [] : newRows)) newSum[(r.MONTH_STR || '').slice(0, 7)] = round(r.REV_EUR);
      const months = [...new Set([...Object.keys(oldBudget), ...Object.keys(newSum)])].sort();
      report.sections.projection = months.map(m => ({ month: m, budget: diff(oldBudget[m], newSum[m]), actual: diff(oldActual[m], newSum[m]) }));
    }
  }

  // ── 5. Pipeline calibration numerator + sfContribByMonth (old vs new) ─────────
  const oldHasInteg = await hasIntegration(OLD_SUBSET);
  const newHasInteg = report.newTableReadable ? await hasIntegration(NEW_TABLE) : false;
  const mrWhere = (useInteg) => {
    const parts = ['COALESCE(mr.REVENUE_AMOUNT_EUR, 0) <> 0'];
    if (integrationCol && useInteg) parts.push('COALESCE(mr.IS_INTEGRATION_MONTH, FALSE) = FALSE');
    return 'AND ' + parts.join('\n            AND ');
  };
  async function numerator(table, useInteg) {
    const rows = await q(`
      WITH opp AS (
        SELECT OPPORTUNITY_ID FROM ${DIM_OPP}
        WHERE IS_OPPORTUNITY_WON = TRUE AND EXTRACT(YEAR FROM CLOSED_WON_DATE) = ${priorYr}
          AND OPPORTUNITY_AMOUNT > 0 AND COALESCE(IS_OPPORTUNITY_REVENUE_SHARED, FALSE) = FALSE AND COALESCE(IS_PRICE_UPDATE, FALSE) = FALSE
        GROUP BY OPPORTUNITY_ID
      )
      SELECT SUM(ROUND(mr.REVENUE_AMOUNT_EUR)) AS NUMER
      FROM ${table} mr
      WHERE EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${priorYr}
        AND mr.OPPORTUNITY_ID IN (SELECT OPPORTUNITY_ID FROM opp)
        ${mrWhere(useInteg)}
    `);
    return num(rows[0]?.NUMER);
  }
  async function contrib(table, useInteg) {
    const rows = await q(`
      WITH opp AS (
        SELECT OPPORTUNITY_ID, ANY_VALUE(TO_VARCHAR(CLOSED_WON_DATE, 'YYYY-MM')) AS CLOSE_MONTH
        FROM ${DIM_OPP}
        WHERE IS_OPPORTUNITY_WON = TRUE AND EXTRACT(YEAR FROM CLOSED_WON_DATE) = ${year}
        GROUP BY OPPORTUNITY_ID
      )
      SELECT opp.CLOSE_MONTH AS CLOSE_MONTH, SUM(ROUND(mr.REVENUE_AMOUNT_EUR)) AS CONTRIB
      FROM ${table} mr JOIN opp ON mr.OPPORTUNITY_ID = opp.OPPORTUNITY_ID
      WHERE EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${year} ${mrWhere(useInteg)}
      GROUP BY 1
    `);
    const out = {};
    for (const r of rows) out[(r.CLOSE_MONTH || '').slice(0, 7)] = round(r.CONTRIB);
    return out;
  }
  try {
    const denomRows = await q(`
      WITH opp AS (
        SELECT OPPORTUNITY_ID, ANY_VALUE(ROUND(OPPORTUNITY_AMOUNT)) AS AMT, ANY_VALUE(EXTRACT(MONTH FROM CLOSED_WON_DATE)) AS MO
        FROM ${DIM_OPP}
        WHERE IS_OPPORTUNITY_WON = TRUE AND EXTRACT(YEAR FROM CLOSED_WON_DATE) = ${priorYr}
          AND OPPORTUNITY_AMOUNT > 0 AND COALESCE(IS_OPPORTUNITY_REVENUE_SHARED, FALSE) = FALSE AND COALESCE(IS_PRICE_UPDATE, FALSE) = FALSE
        GROUP BY OPPORTUNITY_ID
      )
      SELECT SUM(AMT * (12 - (MO - 1))) AS DENOM FROM opp
    `);
    const denom = num(denomRows[0]?.DENOM);
    const numerOld = await numerator(OLD_SUBSET, oldHasInteg).catch(() => 0);
    const numerNew = report.newTableReadable ? await numerator(NEW_TABLE, newHasInteg).catch(() => 0) : 0;
    const clamp = (raw) => (raw >= 0.3 && raw <= 2.0 ? raw : null);
    const factorOld = denom > 0 && numerOld > 0 ? numerOld / denom : null;
    const factorNew = denom > 0 && numerNew > 0 ? numerNew / denom : null;
    const contribOld = await contrib(OLD_SUBSET, oldHasInteg).catch(() => ({}));
    const contribNew = report.newTableReadable ? await contrib(NEW_TABLE, newHasInteg).catch(() => ({})) : {};
    const cMonths = [...new Set([...Object.keys(contribOld), ...Object.keys(contribNew)])].sort();
    report.sections.calibration = {
      denom: round(denom),
      numer: diff(numerOld, numerNew),
      factorOldRaw: factorOld, factorNewRaw: factorNew,
      factorOldClamped: factorOld ? clamp(factorOld) : null, factorNewClamped: factorNew ? clamp(factorNew) : null,
      integrationFilterKept: !!integrationCol, oldHasIntegrationCol: oldHasInteg, newHasIntegrationCol: newHasInteg,
      sfContribByMonth: cMonths.map(m => ({ month: m, contrib: diff(contribOld[m], contribNew[m]) })),
    };
  } catch (e) {
    report.sections.calibration = { __error: e.message };
  }

  // ── Write outputs ─────────────────────────────────────────────────────────────
  const outDir = path.resolve(__dirname, '..', 'data', 'migration-snapshots');
  fs.mkdirSync(outDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const jsonPath = path.join(outDir, `mr-parity-${ts}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  // Reviewer-ready report (drivers + where-you-see-it + how-to-verify).
  const WHERE = {
    monthly: '"Cash collected vs Revenue" widget + cashflow REVENUE column',
    yoy: 'OKR YoY revenue card',
    perCustomer: 'inflows drilldown → "Load customer breakdown" modal',
    projection: 'Budget/Actual bridge revenue rows + dept revenue budget-vs-actual + forecast collections fallback',
    calibration: 'Pipeline button label ×N.NN + methodology panel Column B',
  };
  const inStableWindow = (m) => m < `${year}-06`; // 2025-* and Jan–May of the current year should be near-exact
  // Metric-aware classification. `expected:true` → benign, review-only. `expected:false` → genuine
  // regression to investigate (goes on the short "must explain" list).
  function classify(metric, period) {
    if (metric === 'monthly.paid' || metric === 'monthly.unpaid' || metric === 'monthly.customers' || metric === 'yoy.paid' || metric === 'yoy.customers')
      return { expected: true, driver: 'live SF payment/customer status (expected — paid/unpaid split & customer set reflect current collection state)' };
    if (metric === 'monthly.revenue' || metric === 'yoy.revenue')
      return inStableWindow(period)
        ? { expected: false, driver: 'INVESTIGATE — total revenue should be near-exact for 2025 & Jan–May 2026' }
        : { expected: true, driver: 'GAP 4: SF edits after the 2026-06-23 freeze' };
    if (metric === 'projection.budget') return { expected: true, driver: 'GAP 6: live MRs vs frozen Apr-2025 pipeline snapshot' };
    if (metric === 'projection.actual') return { expected: true, driver: 'GAP 5: SF MRs vs NetSuite recognized revenue (AI-summary text only — no widget/forecast reads it)' };
    if (metric === 'calibration.numer') return { expected: false, driver: 'INVESTIGATE — prior-year numerator changes the calibration factor' };
    if (metric === 'calibration.sfContrib') return inStableWindow(period)
      ? { expected: true, driver: 'live SF MR edits (Column B contribution reflects current SF state)' }
      : { expected: true, driver: 'GAP 4: SF edits after the 2026-06-23 freeze' };
    return { expected: false, driver: 'INVESTIGATE' };
  }
  const flagged = [];
  const add = (metric, key, d, where) => { const c = classify(metric, key); flagged.push({ metric, key, ...d, where, driver: c.driver, expected: c.expected }); };
  for (const yr of [year, priorYr]) {
    const sec = report.sections[`monthly_${yr}`];
    if (Array.isArray(sec)) for (const row of sec) for (const k of ['revenue', 'paid', 'unpaid', 'customers']) if (row[k].flag) add(`monthly.${k}`, row.month, row[k], WHERE.monthly);
  }
  if (Array.isArray(report.sections.yoy)) for (const row of report.sections.yoy) for (const k of ['revenue', 'paid', 'customers']) if (row[k].flag) add(`yoy.${k}`, String(row.year), row[k], WHERE.yoy);
  if (Array.isArray(report.sections.projection)) for (const row of report.sections.projection) for (const k of ['budget', 'actual']) if (row[k].flag) add(`projection.${k}`, row.month, row[k], WHERE.projection);
  if (report.sections.calibration && !report.sections.calibration.__error) {
    const c = report.sections.calibration;
    if (c.numer.flag) add('calibration.numer', `${priorYr}`, c.numer, WHERE.calibration);
    for (const row of c.sfContribByMonth) if (row.contrib.flag) add('calibration.sfContrib', row.month, row.contrib, WHERE.calibration);
  }
  const investigate = flagged.filter(f => !f.expected);
  const expectedRows = flagged.filter(f => f.expected);

  // Top-line summary
  const cal = report.sections.calibration && !report.sections.calibration.__error ? report.sections.calibration : null;
  const revenueTotalStable = !flagged.some(f => (f.metric === 'monthly.revenue' || f.metric === 'yoy.revenue') && !f.expected);
  const calibrationIdentical = cal ? (cal.numer.pct === 0 || (Math.abs(cal.numer.abs) <= 1)) : null;
  const L = [];
  L.push(`# SF Revenue Migration — Parity Report`);
  L.push(`Generated ${report.generatedAt} · year=${year} · asof=${asof} · new table readable: ${report.newTableReadable ? 'YES' : 'NO'}`);
  if (!report.newTableReadable) L.push(`\n⚠ NEW TABLE NOT READABLE: ${report.newTableError}\n   Grant read on CONSUMER_HUB__BANKS_DASHBOARD, then re-run. Only OLD-side numbers were captured.`);
  L.push(`\n## Verdict`);
  L.push(`- Total revenue stable (2025 + Jan–May 2026): ${revenueTotalStable ? 'YES ✓' : 'NO ✗ — see investigate list'}`);
  L.push(`- Calibration factor identical: ${calibrationIdentical == null ? 'n/a' : calibrationIdentical ? 'YES ✓' : 'NO ✗'}`);
  L.push(`- Genuine "investigate" rows: ${investigate.length}${investigate.length === 0 ? ' ✓ (all other diffs are expected/benign)' : ''}`);
  L.push(`- GAP-3 integration filter: ${integrationCol ? `KEEP (resolved column: ${integrationCol})` : 'OMIT (no integration column resolved today)'}`);
  const tbl = (rows) => {
    const out = [`| Metric | Period | Old | New | Δ | Δ% | Driver | Where you see it |`, `|---|---|--:|--:|--:|--:|---|---|`];
    for (const f of rows) out.push(`| ${f.metric} | ${f.key} | ${f.old.toLocaleString()} | ${f.new.toLocaleString()} | ${f.abs >= 0 ? '+' : ''}${f.abs.toLocaleString()} | ${f.pct == null ? 'new' : f.pct + '%'} | ${f.driver} | ${f.where} |`);
    return out.join('\n');
  };
  L.push(`\n## ⚠ Investigate (should be empty) — ${investigate.length}\n`);
  L.push(investigate.length ? tbl(investigate) : '_none — every flagged diff is an expected/benign driver below._');
  L.push(`\n## Expected diffs (review, don't block) — ${expectedRows.length}\n`);
  L.push(expectedRows.length ? tbl(expectedRows) : '_none_');
  L.push(`\nHow to verify any row: open the named screen on prod (old data) vs localhost (new data) and compare, or re-run the matching query from snowflake-api.cjs.`);
  if (cal) {
    L.push(`\n## Calibration factor\n- denom (shared): ${cal.denom.toLocaleString()}\n- numerator: old ${cal.numer.old.toLocaleString()} → new ${cal.numer.new.toLocaleString()} (${cal.numer.pct == null ? 'new' : cal.numer.pct + '%'})\n- factor: old ${cal.factorOldRaw?.toFixed(4) ?? 'n/a'} → new ${cal.factorNewRaw?.toFixed(4) ?? 'n/a'} (clamped to [0.3,2.0]: old ${cal.factorOldClamped?.toFixed(4) ?? 'fallback'} / new ${cal.factorNewClamped?.toFixed(4) ?? 'fallback'})`);
  }
  report.summary = { revenueTotalStable, calibrationIdentical, investigateCount: investigate.length, expectedCount: expectedRows.length };
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const mdPath = path.join(outDir, `mr-parity-${ts}.md`);
  fs.writeFileSync(mdPath, L.join('\n'));

  console.log(`\n${L.join('\n')}`);
  console.log(`\n[verify-mr] wrote:\n  ${jsonPath}\n  ${mdPath}`);
  process.exit(0);
})().catch(e => { console.error('ERROR:', e); process.exit(1); });
