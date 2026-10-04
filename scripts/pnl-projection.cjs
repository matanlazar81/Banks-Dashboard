#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// P&L Projection from the command line: the same computation as GET /api/pnl-projection
// (server/pnl-projection.cjs), without a web server.
//
//   node scripts/pnl-projection.cjs --dry-run       print both years (plan and base), € thousands
//   node scripts/pnl-projection.cjs --json          print the API payload
//   node scripts/pnl-projection.cjs --write-cache   compute and write data/pnl-projection-cache.json;
//                                                   a running server serves it on its next request
//   node scripts/pnl-projection.cjs --reconcile [--year=2026]
//        the NetSuite P&L of every closed month by line (Sales, Overheads, Operating Profit = EBITDA,
//        below-EBITDA lines, Net profit), by transaction date AND by posting period, in €, to compare
//        with NetSuite's "EBITDA_Profit and Loss" report. PNL_NS_DATE_BASIS (trandate | period) picks
//        the one the page uses.
//
// Needs the same .env as the nightly job (NetSuite, Snowflake, and DATABASE_URL for the plan).
// Output contains internal financial figures: keep it on the server.
// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
try { require('dotenv').config({ path: path.join(ROOT, '.env') }); } catch { /* env may already be exported */ }

const { computePnlProjection, DEFAULT_CACHE_FILE, SCHEMA_VERSION } = require(path.join(ROOT, 'server', 'pnl-projection.cjs'));
const { makeEntry, writeCacheEntry } = require(path.join(ROOT, 'server', 'cash-projection.cjs'));
const { sumByLine } = require(path.join(ROOT, 'server', 'pnl-lines.cjs'));

function arg(name) {
  const hit = process.argv.slice(2).find((a) => a === `--${name}` || a.startsWith(`--${name}=`));
  if (!hit) return null;
  return hit.includes('=') ? hit.slice(hit.indexOf('=') + 1) : true;
}

const k = (n) => {
  const v = Math.round((Number(n) || 0) / 1000);
  return (v < 0 ? `(${Math.abs(v).toLocaleString('en-US')})` : v.toLocaleString('en-US')).padStart(9);
};
const eur = (n) => (Math.round((Number(n) || 0) * 100) / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).padStart(16);

function printYear(label, block) {
  console.log(`\n${label} ${block.year} (${block.kind}) — € thousands`);
  console.log('  month    status    revenue    costs   EBITDA      fx  finance  deprec.  tax&oth      net  accumul.');
  for (const r of block.rows) {
    const f = r.eur;
    console.log(`  ${r.mKey}  ${r.status.padEnd(8)}${k(f.totalRevenue)}${k(f.totalCosts)}${k(f.ebitda)}${k(f.fx)}${k(f.finance)}${k(f.depreciation)}${k(f.taxOther)}${k(f.net)}${k(f.accClosing)}`);
  }
}

// The EBITDA report's sections from line totals (profit-signed).
function sections(totals) {
  const t = (key) => totals[key].eur;
  const sales = t('revenue') + t('otherRevenue');
  const overheads = -(t('payroll') + t('capex') + t('opex'));
  const ebitda = sales - overheads;
  const below = t('fx') + t('finance') + t('depreciation') + t('taxOther');
  return { sales, overheads, ebitda, fx: t('fx'), finance: t('finance'), depreciation: t('depreciation'), taxOther: t('taxOther'), net: ebitda + below };
}

async function reconcile(ns) {
  const now = new Date();
  const year = parseInt(arg('year') || now.getFullYear(), 10);
  const lastMonth = year < now.getFullYear() ? 12 : now.getMonth(); // closed months only
  if (lastMonth < 1) { console.log(`No closed month in ${year} yet.`); return; }
  const [byDate, byPeriod] = [await ns.fetchPnlActuals({ fromYear: year, toYear: year, basis: 'trandate' }), await ns.fetchPnlActuals({ fromYear: year, toYear: year, basis: 'period' })];
  const using = process.env.PNL_NS_DATE_BASIS === 'period' ? 'posting period' : 'transaction date';
  console.log(`\nNetSuite P&L ${year}, subsidiary LSports Data, € (primary book). The page uses: ${using}.`);
  console.log('Compare "Operating Profit" with the EBITDA_Profit and Loss report for the same month.\n');
  const cols = ['sales', 'overheads', 'ebitda', 'fx', 'finance', 'depreciation', 'taxOther', 'net'];
  console.log(`  month    basis        ${cols.map((c) => c.padStart(16)).join('')}`);
  const ytd = { trandate: Object.fromEntries(cols.map((c) => [c, 0])), period: Object.fromEntries(cols.map((c) => [c, 0])) };
  for (let m = 1; m <= lastMonth; m++) {
    const mKey = `${year}-${String(m).padStart(2, '0')}`;
    for (const [basis, data] of [['trandate', byDate], ['period', byPeriod]]) {
      const s = sections(sumByLine(data.byMonth[mKey] || {}).totals);
      cols.forEach((c) => { ytd[basis][c] += s[c]; });
      console.log(`  ${mKey}  ${basis.padEnd(12)}${cols.map((c) => eur(s[c])).join('')}`);
    }
  }
  console.log('');
  for (const basis of ['trandate', 'period']) console.log(`  YTD      ${basis.padEnd(12)}${cols.map((c) => eur(ytd[basis][c])).join('')}`);
}

async function main() {
  const { createNetSuiteClient } = require(path.join(ROOT, 'netsuite-api.cjs'));
  const sub = parseInt(process.env.NET_CASH_SUBSIDIARY || '3', 10) || 3;
  const ns = createNetSuiteClient(process.env, sub);

  if (arg('reconcile')) {
    await reconcile(ns);
    if (!arg('json') && !arg('dry-run') && !arg('write-cache')) return;
  }

  const { createSnowflakeClient } = require(path.join(ROOT, 'snowflake-api.cjs'));
  const sf = createSnowflakeClient(process.env);
  const t0 = Date.now();
  const { payload, details } = await computePnlProjection({
    now: new Date(),
    getNsClient: () => ns,
    getSfClient: () => sf,
    queueNsCall: (fn) => fn(),
  });
  console.log(`\n[pnl-projection] computed in ${((Date.now() - t0) / 1000).toFixed(1)}s — plan "${payload.plan.name}" ${payload.plan.loaded ? `loaded (${payload.plan.source})` : 'NOT found (plan = base)'}; actuals through ${payload.actuals.through || '–'} by ${payload.actuals.basis}`);
  for (const w of payload.warnings) console.warn(`[pnl-projection] ⚠ ${w}`);

  if (arg('json')) console.log(JSON.stringify(payload, null, 2));

  if (arg('dry-run')) {
    for (const v of ['plan', 'base']) {
      for (const block of payload.variants[v].years) printYear(v.toUpperCase(), block);
      const rf = payload.variants[v].rollForward;
      console.log(`  roll-forward ${rf.from} → ${rf.to}: accumulated €${Math.round(rf.accumulated.eur).toLocaleString('en-US')} (payroll basis: ${rf.salaryBasis.method})`);
    }
  }

  if (arg('write-cache')) {
    writeCacheEntry(DEFAULT_CACHE_FILE, makeEntry(payload, Date.now(), details, SCHEMA_VERSION));
    console.log(`[pnl-projection] ✓ wrote ${path.relative(ROOT, DEFAULT_CACHE_FILE)}; the server serves it on its next request.`);
  }

  if (!arg('json') && !arg('dry-run') && !arg('write-cache') && !arg('reconcile')) {
    console.log('Nothing to do: pass --dry-run, --json, --write-cache or --reconcile.');
  }
}

main().catch((e) => {
  console.error(`[pnl-projection] FAILED: ${e && e.stack ? e.stack : e}`);
  process.exit(1);
});
