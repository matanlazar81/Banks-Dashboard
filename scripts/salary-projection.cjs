#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// Salary Projection from the command line: the same computation as GET /api/salary-projection
// (server/salary-projection.cjs), without a web server.
//
//   node scripts/salary-projection.cjs --dry-run       print both years by account, ₪ thousands (Total payroll
//                                                      adds the planned hires, leavers and returns from leave
//                                                      and the override file's increments, as the page does)
//   node scripts/salary-projection.cjs --json          print the API payload
//   node scripts/salary-projection.cjs --write-cache   compute and write data/salary-projection-cache.json;
//                                                      a running server serves it on its next request
//   node scripts/salary-projection.cjs --reconcile [--year=2026]
//        Snowflake's payroll (FCT_EXPENSE) against NetSuite's P&L by account, every closed month, in ₪:
//        lists each account and month that differ by more than ₪1. The page reads NetSuite because
//        Snowflake misses some one-off accounts (760017 bonus, 760019 maternity); this shows when it is fixed.
//
// Needs the server's .env (NetSuite; Snowflake for the override file and --reconcile).
// Output contains internal financial figures: keep it on the server.
// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
try { require('dotenv').config({ path: path.join(ROOT, '.env') }); } catch { /* env may already be exported */ }

const { computeSalaryProjection, loadSalaryModule, DEFAULT_CACHE_FILE, SCHEMA_VERSION } = require(path.join(ROOT, 'server', 'salary-projection.cjs'));
const { makeEntry, writeCacheEntry } = require(path.join(ROOT, 'server', 'cash-projection.cjs'));

function arg(name) {
  const hit = process.argv.slice(2).find((a) => a === `--${name}` || a.startsWith(`--${name}=`));
  if (!hit) return null;
  return hit.includes('=') ? hit.slice(hit.indexOf('=') + 1) : true;
}

const k = (n) => {
  const v = Math.round((Number(n) || 0) / 1000) || 0; // no "-0"
  return (v < 0 ? `(${Math.abs(v).toLocaleString('en-US')})` : v.toLocaleString('en-US')).padStart(8);
};
const ils = (n) => (Math.round((Number(n) || 0) * 100) / 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).padStart(16);
const eur = (n) => `€${Math.round(Number(n) || 0).toLocaleString('en-US')}`.padStart(14);

// planned: Map(label → Map(mKey → ₪)) the planned changes that count in Total payroll, as on the page:
// Snowflake's hires, leavers and returns from leave (countedChangeRows) and the override file's increments
// (countedIncrementRows).
function printYear(payload, block, planned) {
  console.log(`\n${block.year} (${block.kind}) — ₪ thousands`);
  console.log(`  ${'account'.padEnd(44)}${block.rows.map((r) => `${r.mKey.slice(2)}${r.status === 'actual' ? 'A' : r.status === 'current' ? 'C' : 'F'}`.padStart(8)).join('')}${'FY'.padStart(9)}`);
  const sumRow = (accts) => block.rows.map((r) => accts.reduce((s, a) => s + (r.ils[a] || 0), 0));
  const plannedRow = (labels) => block.rows.map((r) => labels.reduce((s, l) => s + (planned.get(l).get(r.mKey) || 0), 0));
  const labels = [...planned.keys()].filter((l) => block.rows.some((r) => planned.get(l).has(r.mKey)));
  const print = (label, values) => console.log(`  ${label.slice(0, 43).padEnd(44)}${values.map(k).join('')} ${k(values.reduce((s, v) => s + v, 0))}`);
  for (const group of ['permanent', 'temporary', 'other']) {
    const list = payload.accounts.filter((a) => a.group === group);
    if (list.length) {
      for (const a of list) print(`${a.acct} - ${a.name}`, sumRow([a.acct]));
      print(`Total ${group}`, sumRow(list.map((a) => a.acct)));
    }
    if (group === 'permanent' && labels.length) {
      for (const l of labels) print(l, plannedRow([l]));
      print('Total planned changes', plannedRow(labels));
    }
  }
  const changes = plannedRow(labels);
  print('Total payroll', sumRow(payload.accounts.map((a) => a.acct)).map((v, i) => v + changes[i]));
}

async function reconcile(ns) {
  const { createSnowflakeClient } = require(path.join(ROOT, 'snowflake-api.cjs'));
  const sf = createSnowflakeClient(process.env);
  const now = new Date();
  const year = parseInt(arg('year') || now.getFullYear(), 10);
  const lastMonth = year < now.getFullYear() ? 12 : now.getMonth(); // closed months only
  if (lastMonth < 1) { console.log(`No closed month in ${year} yet.`); return; }
  const [sfRows, nsData] = await Promise.all([sf.fetchPayrollByAccount(year, year), ns.fetchPnlActuals({ fromYear: year, toYear: year, basis: 'period' })]);
  const sfBy = {};
  for (const r of sfRows) (sfBy[r.month] || (sfBy[r.month] = {}))[r.acct] = r.ils;
  console.log(`\nPayroll ${year} (76xxxx), ₪: Snowflake vs NetSuite P&L by posting period. Only differences over ₪1 are listed.\n`);
  console.log(`  month    account   ${'Snowflake'.padStart(16)}${'NetSuite'.padStart(16)}${'difference'.padStart(16)}`);
  let diffs = 0;
  for (let m = 1; m <= lastMonth; m++) {
    const mKey = `${year}-${String(m).padStart(2, '0')}`;
    const nsBy = {};
    for (const a of Object.values(nsData.byMonth[mKey] || {})) if (String(a.acct).startsWith('76')) nsBy[a.acct] = -a.ils; // profit-signed → cost positive
    let sfTotal = 0;
    let nsTotal = 0;
    for (const acct of [...new Set([...Object.keys(sfBy[mKey] || {}), ...Object.keys(nsBy)])].sort()) {
      const s = (sfBy[mKey] || {})[acct] || 0;
      const n = nsBy[acct] || 0;
      sfTotal += s;
      nsTotal += n;
      if (Math.abs(s - n) > 1) { diffs++; console.log(`  ${mKey}  ${acct.padEnd(8)}${ils(s)}${ils(n)}${ils(s - n)}`); }
    }
    console.log(`  ${mKey}  total   ${ils(sfTotal)}${ils(nsTotal)}${ils(sfTotal - nsTotal)}`);
  }
  console.log(diffs ? `\n${diffs} account-month(s) differ.` : '\nSnowflake equals NetSuite for every payroll account and closed month.');
}

async function main() {
  const { createNetSuiteClient } = require(path.join(ROOT, 'netsuite-api.cjs'));
  const sub = parseInt(process.env.NET_CASH_SUBSIDIARY || '3', 10) || 3;
  const ns = createNetSuiteClient(process.env, sub);

  if (arg('reconcile')) {
    await reconcile(ns);
    if (!arg('json') && !arg('dry-run') && !arg('write-cache')) return;
  }

  const t0 = Date.now();
  const getSfClient = () => require(path.join(ROOT, 'snowflake-api.cjs')).createSnowflakeClient(process.env);
  const { payload } = await computeSalaryProjection({ now: new Date(), getNsClient: () => ns, getSfClient, queueNsCall: (fn) => fn() });
  console.log(`\n[salary-projection] computed in ${((Date.now() - t0) / 1000).toFixed(1)}s — actuals through ${payload.actuals.through || '–'}; permanent run-rate ${payload.basis.runRateMonth}; ₪${payload.basis.ilsPerEur} per €`);
  for (const w of payload.warnings) console.warn(`[salary-projection] ⚠ ${w}`);

  if (arg('json')) console.log(JSON.stringify(payload, null, 2));
  if (arg('dry-run')) {
    const ov = payload.overrides;
    const ch = payload.changes || { rows: [], error: null };
    const sm = await loadSalaryModule();
    const countedIncrements = new Set(sm.countedIncrementRows(payload.blocks, ov.rows));
    const countedChanges = new Set(sm.countedChangeRows(payload.blocks, ch.rows));
    const planned = new Map();
    const add = (label, r) => {
      const m = planned.get(label) || new Map();
      m.set(r.month, (m.get(r.month) || 0) + (Number(r.ils) || 0));
      planned.set(label, m);
    };
    const CHANGE_LABEL = { 'Open position': 'Planned hires', Terminated: 'Leavers', 'Leave end': 'Returns from leave' };
    for (const source of Object.keys(CHANGE_LABEL)) for (const r of countedChanges) if (r.source === source) add(`${CHANGE_LABEL[source]} (Snowflake)`, r);
    for (const r of countedIncrements) add(`${r.acct} increment (override file)`, r);
    for (const block of payload.blocks) printYear(payload, block, planned);
    console.log(`\nPlanned hires, leavers and returns from leave on payroll accounts (Snowflake): ${ch.error ? ch.error : `${countedChanges.size} of ${ch.rows.length} account-month row(s) counted (the projected months)`}`);
    console.log(`\nOverride file, payroll rows from January ${payload.years[0]}: ${ov.error ? ov.error : `${ov.rows.length} row(s)`}`);
    console.log('  (+ = an increment counted in Total payroll; increments in closed months are not counted, overrides never are)');
    for (const r of ov.rows) {
      console.log(`${countedIncrements.has(r) ? '+ ' : '  '}${r.month}  ${r.acct.padEnd(8)}${r.mode.padEnd(10)}${eur(r.eur)}  ${(r.department || '–').slice(0, 24).padEnd(25)}${r.memo.slice(0, 50)}`);
    }
  }

  if (arg('write-cache')) {
    writeCacheEntry(DEFAULT_CACHE_FILE, makeEntry(payload, Date.now(), null, SCHEMA_VERSION));
    console.log(`[salary-projection] ✓ wrote ${path.relative(ROOT, DEFAULT_CACHE_FILE)}; the server serves it on its next request.`);
  }

  if (!arg('json') && !arg('dry-run') && !arg('write-cache') && !arg('reconcile')) {
    console.log('Nothing to do: pass --dry-run, --json, --write-cache or --reconcile.');
  }
}

main().catch((e) => {
  console.error(`[salary-projection] FAILED: ${e && e.stack ? e.stack : e}`);
  process.exit(1);
});
