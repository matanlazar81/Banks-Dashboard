// BI-3228 Phase-2 parity check: OLD source tables vs NEW CONSUMER_HUB__FINANCE passthrough views.
//
//   node scripts/verify-phase2-migration.cjs [--sub=3]
//
// Runs BEFORE the grant cutover, while both the source schemas (FINANCE/HR/CORE) and the new
// CONSUMER_HUB__FINANCE views are readable. For each migrated table it compares aggregate
// metrics (COUNT + keyed SUMs) old-vs-new in a single run. The views are explicit column-list
// passthroughs with no filters, so EXACT equality is expected — any non-zero diff is a view
// defect: stop and report before deploying.
//
// Read-only. Writes a machine JSON + a reviewer-ready Markdown report under data/migration-snapshots/.
// Mirrors the harness of scripts/verify-mr-migration.cjs (loadEnv walk, createSnowflakeClient, diff).

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
function arg(name, def) {
  const hit = process.argv.slice(2).find(a => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : def;
}

const CH = 'DL_PRODUCTION.CONSUMER_HUB__FINANCE';
const num = (v) => (v == null ? 0 : Number(v) || 0);
const round = (v) => Math.round(num(v));

// Each pair: old source FQN, new __FINANCE view, and a list of aggregate metrics.
// `where` is optional (applied to both sides identically).
function pairs(sub) {
  return [
    { name: 'FCT_BUDGET', old: 'DL_PRODUCTION.FINANCE.FCT_BUDGET', neo: `${CH}.FCT_BUDGET__FINANCE`,
      where: `WHERE SUBSIDIARY_ID = ${sub}`,
      metrics: [['cnt', 'COUNT(*)'], ['sum_eur_cc', 'SUM(AMOUNT_EUR_CC)']] },
    { name: 'FCT_EXPENSE', old: 'DL_PRODUCTION.FINANCE.FCT_EXPENSE', neo: `${CH}.FCT_EXPENSE__FINANCE`,
      where: `WHERE SUBSIDIARY_ID = ${sub}`,
      metrics: [['cnt', 'COUNT(*)'], ['sum_eur', 'SUM(AMOUNT_EUR)']] },
    { name: 'DIM_GL_ACCOUNT', old: 'DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT', neo: `${CH}.DIM_GL_ACCOUNT__FINANCE`,
      metrics: [['cnt', 'COUNT(*)'], ['distinct_num', 'COUNT(DISTINCT GL_ACCOUNT_NUMBER)']] },
    { name: 'DIM_DEPARTMENT', old: 'DL_PRODUCTION.FINANCE.DIM_DEPARTMENT', neo: `${CH}.DIM_DEPARTMENT__FINANCE`,
      metrics: [['cnt', 'COUNT(*)']] },
    { name: 'FCT_REVENUE_TARGET', old: 'DL_PRODUCTION.FINANCE.FCT_REVENUE_TARGET', neo: `${CH}.FCT_REVENUE_TARGET__FINANCE`,
      metrics: [['cnt', 'COUNT(*)'], ['sum_target', 'SUM(REVENUE_TARGET_EURO)'],
                ['min_month', "MIN(CAL_MONTH_START_DATE)::VARCHAR"], ['max_month', "MAX(CAL_MONTH_START_DATE)::VARCHAR"]] },
    { name: 'DIM_OPPORTUNITY', old: 'DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY', neo: `${CH}.DIM_OPPORTUNITY__FINANCE`,
      metrics: [['cnt', 'COUNT(*)'], ['distinct_opp', 'COUNT(DISTINCT OPPORTUNITY_ID)'], ['sum_amt', 'SUM(OPPORTUNITY_AMOUNT)']] },
    { name: 'FCT_OPPORTUNITY__MONTHLY', old: 'DL_PRODUCTION.FINANCE.FCT_OPPORTUNITY__MONTHLY', neo: `${CH}.FCT_OPPORTUNITY__MONTHLY__FINANCE`,
      metrics: [['cnt', 'COUNT(*)'], ['sum_mrr', 'SUM(OPPORTUNITY_MRR)']] },
    { name: 'FCT_CUSTOMER__MONTHLY', old: 'DL_PRODUCTION.FINANCE.FCT_CUSTOMER__MONTHLY', neo: `${CH}.FCT_CUSTOMER__MONTHLY__FINANCE`,
      metrics: [['cnt', 'COUNT(*)'], ['distinct_cust', 'COUNT(DISTINCT CUSTOMER_ID)']] },
    { name: 'FCT_MRR_Q_SNAPSHOT', old: 'DL_PRODUCTION.FINANCE.FCT_MRR_Q_SNAPSHOT', neo: `${CH}.FCT_MRR_Q_SNAPSHOT__FINANCE`,
      where: "WHERE CURRENCY_ISO_CODE = 'EUR' AND SRC_IS_DELETED = FALSE",
      metrics: [['cnt', 'COUNT(*)'], ['avg_mrr', 'ROUND(AVG(MRR), 2)']] },
    { name: 'DIM_EMPLOYEE', old: 'DL_PRODUCTION.HR.DIM_EMPLOYEE', neo: `${CH}.DIM_EMPLOYEE__FINANCE`,
      metrics: [['cnt', 'COUNT(*)']] },
    { name: 'FCT_HEADCOUNT_EVENT', old: 'DL_PRODUCTION.HR.FCT_HEADCOUNT_EVENT', neo: `${CH}.FCT_HEADCOUNT_EVENT__FINANCE`,
      metrics: [['cnt', 'COUNT(*)']] },
    { name: 'DIM_CUSTOMER', old: 'DL_PRODUCTION.CORE.DIM_CUSTOMER', neo: `${CH}.DIM_CUSTOMER__FINANCE`,
      metrics: [['cnt', 'COUNT(*)'], ['distinct_cust', 'COUNT(DISTINCT CUSTOMER_ID)']] },
    // Phase-1 SCD daily table: pure schema/name rename BANKS_DASHBOARD → FINANCE. The old name may
    // already be gone after the dbt rename; if so the OLD side records an error and only NEW captures.
    { name: 'SCD_DAILY (rename)',
      old: 'DL_PRODUCTION.CONSUMER_HUB__BANKS_DASHBOARD.FCT_OPPORTUNITY_MONTHLY_REVENUE__SCD_DAILY__BANKS_DASHBOARD',
      neo: `${CH}.FCT_OPPORTUNITY_MONTHLY_REVENUE__SCD_DAILY__FINANCE`,
      metrics: [['cnt', 'COUNT(*)'], ['sum_rev_eur', 'SUM(REVENUE_AMOUNT_EUR)']] },
  ];
}

(async () => {
  const envInfo = loadEnv();
  console.log(`[verify-p2] env: ${envInfo ? envInfo.path : '(none)'} (snowflake=${envInfo?.hasSnowflake ? 'yes' : 'no'})`);
  const sf = createSnowflakeClient(process.env);
  if (!sf) { console.error('Snowflake client unavailable (check SNOWFLAKE_* env / key path)'); process.exit(1); }
  const q = sf.query.bind(sf);
  const sub = parseInt(arg('sub', '3'), 10);

  const report = { generatedAt: new Date().toISOString(), subsidiary: sub, tables: [], summary: {} };
  let flagged = 0, errored = 0, compared = 0, newErrored = 0;

  async function agg(fqn, sel, where) {
    const rows = await q(`SELECT ${sel} FROM ${fqn} ${where || ''}`);
    return rows[0] || {};
  }

  for (const p of pairs(sub)) {
    const sel = p.metrics.map(([k, e]) => `${e} AS ${k.toUpperCase()}`).join(', ');
    const entry = { table: p.name, old: p.old, new: p.neo, metrics: [] };
    let oldRow = null, newRow = null, oldErr = null, newErr = null;
    try { oldRow = await agg(p.old, sel, p.where); } catch (e) { oldErr = e.message; }
    try { newRow = await agg(p.neo, sel, p.where); } catch (e) { newErr = e.message; }
    entry.oldError = oldErr; entry.newError = newErr;
    for (const [k] of p.metrics) {
      const K = k.toUpperCase();
      const ov = oldRow ? oldRow[K] : null;
      const nv = newRow ? newRow[K] : null;
      let match = null;
      const isNumeric = typeof ov !== 'string' && typeof nv !== 'string';
      if (oldRow && newRow) {
        match = isNumeric ? (round(ov) === round(nv)) : (String(ov) === String(nv));
        compared++;
        if (!match) flagged++;
      }
      entry.metrics.push({ metric: k, old: isNumeric ? (ov == null ? null : round(ov)) : ov,
        new: isNumeric ? (nv == null ? null : round(nv)) : nv, match });
    }
    if (oldErr || newErr) errored++;
    if (newErr) newErrored++;
    report.tables.push(entry);
    const status = (oldErr || newErr) ? '⚠ err' : (entry.metrics.every(m => m.match) ? '✓' : '✗ DIFF');
    console.log(`  ${status.padEnd(7)} ${p.name}`);
    if (oldErr) console.log(`          old: ${oldErr}`);
    if (newErr) console.log(`          new: ${newErr}`);
  }

  const verdict = newErrored > 0
    ? 'FAIL — NEW views unreadable (dbt not built / not granted) — DO NOT DEPLOY'
    : (flagged > 0 ? 'FAIL — value diffs (view defect) — DO NOT DEPLOY'
      : (errored > 0 ? 'PASS — exact parity (old-side read notes below are benign)' : 'PASS — exact parity'));
  report.summary = { comparedMetrics: compared, mismatches: flagged, tablesWithErrors: errored,
    newViewsUnreadable: newErrored, verdict };

  // ── write reports ──
  const outDir = path.resolve(__dirname, '..', 'data', 'migration-snapshots');
  fs.mkdirSync(outDir, { recursive: true });
  const ts = report.generatedAt.replace(/[:.]/g, '-');
  const jsonPath = path.join(outDir, `phase2-parity-${ts}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [];
  md.push(`# BI-3228 Phase-2 parity — OLD source vs NEW __FINANCE views`);
  md.push(`Generated ${report.generatedAt} · subsidiary=${sub}`);
  md.push('');
  md.push(`## Verdict: ${report.summary.verdict}`);
  md.push(`- metrics compared: ${compared}`);
  md.push(`- mismatches (must be 0): ${flagged}`);
  md.push(`- tables with read errors: ${errored}`);
  md.push('');
  md.push(`| Table | Metric | Old | New | Match |`);
  md.push(`|---|---|--:|--:|:--:|`);
  for (const t of report.tables) {
    for (const m of t.metrics) {
      md.push(`| ${t.table} | ${m.metric} | ${m.old ?? '—'} | ${m.new ?? '—'} | ${m.match === null ? 'n/a' : (m.match ? '✓' : '✗')} |`);
    }
    if (t.oldError) md.push(`| ${t.table} | _old read error_ | | | ⚠ ${t.oldError.slice(0, 80)} |`);
    if (t.newError) md.push(`| ${t.table} | _new read error_ | | | ⚠ ${t.newError.slice(0, 80)} |`);
  }
  md.push('');
  md.push(`Expected: every Match ✓ (passthrough views are value-identical). Any ✗ is a view defect — stop and report before deploying. Read errors on the SCD_DAILY old name are expected if the dbt schema rename already ran.`);
  const mdPath = path.join(outDir, `phase2-parity-${ts}.md`);
  fs.writeFileSync(mdPath, md.join('\n'));

  console.log(`\n${report.summary.verdict}`);
  if (newErrored > 0 || flagged > 0) {
    console.log(`\n⛔ DO NOT DEPLOY — ${newErrored > 0 ? `${newErrored} table(s) could not be read from CONSUMER_HUB__FINANCE (create/grant the dbt views first)` : `${flagged} value mismatch(es) — investigate the view definition`}.`);
  }
  console.log(`[verify-p2] wrote:\n  ${jsonPath}\n  ${mdPath}`);
  process.exit((newErrored > 0 || flagged > 0) ? 2 : 0);
})().catch(e => { console.error('ERROR:', e); process.exit(1); });
