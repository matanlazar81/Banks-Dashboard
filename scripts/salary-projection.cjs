#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// Salary Projection from the command line: the same computation as GET /api/salary-projection
// (server/salary-projection.cjs), without a web server.
//
//   node scripts/salary-projection.cjs --dry-run       print both years by account, ₪ thousands (Total payroll
//                                                      adds the planned hires, leavers and returns from leave
//                                                      and the override file's increments, as the page does),
//                                                      then Deel and Payroll + Deel, the headcount (HiBob) by
//                                                      month, and what HiBob's events and Deel's vendors were
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
// (countedIncrementRows). deel: Map(mKey → { ils: { [acct]: ₪ } }) Deel as the page shows it (deelByMonth).
function printYear(payload, block, planned, deel) {
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
  const total = sumRow(payload.accounts.map((a) => a.acct)).map((v, i) => v + changes[i]);
  print('Total payroll', total);
  // As the page: the Deel accounts with an amount in the projection's months.
  const deelRow = (accts) => block.rows.map((r) => accts.reduce((s, a) => s + ((deel.get(r.mKey) || { ils: {} }).ils[a] || 0), 0));
  const shown = (a) => [...deel.values()].some((m) => Math.abs(m.ils[a] || 0) >= 0.5);
  const deelAccts = [...new Set(payload.deel.rows.map((r) => r.acct))].filter(shown).sort();
  if (deelAccts.length) {
    const names = new Map(payload.deel.rows.map((r) => [r.acct, r.name]));
    for (const a of deelAccts) print(`Deel ${a} - ${names.get(a) || ''}`, deelRow([a]));
    const deelTotal = deelRow(deelAccts);
    print('Total Deel', deelTotal);
    print('Payroll + Deel', total.map((v, i) => v + deelTotal[i]));
  }
}

// The headcount (HiBob) of one year: people, not thousands.
function printHeadcount(payload, block) {
  const byKey = new Map(payload.headcount.months.map((m) => [m.mKey, m]));
  const cell = (v) => String(v === null || v === undefined ? '–' : v).padStart(8);
  console.log(`\n${block.year} headcount (HiBob ${payload.headcount.company}) — people`);
  console.log(`  ${''.padEnd(44)}${block.rows.map((r) => `${r.mKey.slice(2)}${r.mKey < payload.computedMonth ? 'A' : r.mKey === payload.computedMonth ? 'C' : 'F'}`.padStart(8)).join('')}`);
  for (const [label, k] of [['Employees at month-end', 'employees'], ['Hires', 'hires'], ['Leavers', 'leavers'], ['Open positions (not counted)', 'openings'], ['Planned leavers, no employee (not counted)', 'cuts'], ['Returns from leave', 'returns'], ['Leave starts', 'leaveStarts']]) {
    console.log(`  ${label.padEnd(44)}${block.rows.map((r) => cell((byKey.get(r.mKey) || {})[k])).join('')}`);
  }
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
    const deel = sm.deelByMonth(payload.blocks, payload.deel.rows, payload.basis.runRateMonth);
    for (const block of payload.blocks) printYear(payload, block, planned, deel);

    const dl = payload.deel;
    const rate = sm.deelRunRate(dl.rows, payload.basis.runRateMonth);
    console.log(`\nDeel (NetSuite): ${dl.error ? dl.error : `${dl.vendors.length} vendor(s): ${dl.vendors.join(', ') || '–'}`}`);
    for (const a of [...new Set(dl.rows.map((r) => r.acct))].sort()) {
      const of = dl.rows.filter((r) => r.acct === a);
      console.log(`  ${a.padEnd(8)}${(of[0].name || '').slice(0, 40).padEnd(41)}${of.length} row(s), ${of[0].month} to ${of[of.length - 1].month}`);
    }
    for (const a of dl.onPayroll) console.log(`  ${a.acct.padEnd(8)}${(a.name || '').slice(0, 40).padEnd(41)}payroll account: already in Total payroll, not in Deel`);
    console.log(`  Forecast months: ${rate.months.length ? `the average of ${rate.months.join(', ')}` : '0 (no Deel postings in the 12 months up to the run-rate month)'}`);

    const hc = payload.headcount;
    console.log(`\nHeadcount (HiBob ${hc.company}): ${hc.error ? hc.error : `${hc.today === null ? '–' : hc.today} employees on ${hc.asOf}`}`);
    if (hc.months.some((m) => m.employees !== null)) for (const block of payload.blocks) printHeadcount(payload, block);
    console.log(`\nHiBob's planned events from January ${payload.years[0]} (FCT_HEADCOUNT_EVENT), by type, sub-type, employee and opening status, and how each counts:`);
    const kinds = new Map();
    for (const e of hc.events) {
      const past = e.month < payload.computedMonth;
      const k = `${e.type}|${e.subType}|${e.employee}|${e.status}|${past}`;
      const g = kinds.get(k) || { ...e, past, count: 0, first: e.month, last: e.month };
      g.count += e.count;
      if (e.month < g.first) g.first = e.month;
      if (e.month > g.last) g.last = e.month;
      kinds.set(k, g);
    }
    if (!kinds.size) console.log('  (none)');
    const LINE = {
      openings: 'Open positions (shown, not counted)', cuts: 'Planned leavers, no employee (shown, not counted)',
      employee: 'in HiBob\'s employees already (their dates)',
      returns: 'Returns from leave (shown, count unchanged)', leaveStarts: 'Leave starts (shown, count unchanged)',
    };
    for (const g of kinds.values()) {
      const kind = sm.headcountKind(g.type, g.subType, g.employee);
      // As headcountSeries: before the current month, only openings still open show (in the current month).
      const line = g.past && kind === 'openings' ? (/^open/i.test(g.status) ? `Open positions in ${payload.computedMonth} (still open; shown, not counted)` : 'not shown (an earlier month, not open)')
        : g.past && kind === 'cuts' ? 'not shown (an earlier month)'
          : LINE[kind] || 'not shown (unknown type)';
      console.log(`  ${g.type.padEnd(10)}${(g.subType || '–').slice(0, 24).padEnd(25)}${(g.employee ? 'employee' : 'no employee').padEnd(12)}${(g.status || '–').slice(0, 14).padEnd(15)}${String(g.count).padStart(5)}  ${g.first} to ${g.last}  → ${line}`);
    }
    const cur = hc.months.find((m) => m.mKey === payload.computedMonth);
    if (cur && hc.today !== null) console.log(`  Today ${hc.today} employees; ${payload.computedMonth} month-end ${cur.employees} by HiBob's dates (${cur.hires} hire(s), ${cur.leavers} leaver(s) this month; ${cur.openings} open position(s) not counted)`);
    console.log(`\nPlanned hires, leavers and returns from leave on payroll accounts (Snowflake): ${ch.error ? ch.error : `${countedChanges.size} of ${ch.rows.length} account-month row(s) counted (marked TRUE, in the projected months)`}`);
    if (!ch.error) {
      // How each source splits: TRUE / FALSE (IS_SOURCE_ACTUAL), in the projected months and in closed ones.
      const projected = sm.projectedMonths(payload.blocks);
      for (const source of Object.keys(CHANGE_LABEL)) {
        const of = ch.rows.filter((r) => r.source === source);
        if (!of.length) continue;
        const part = (keep) => { const rows = of.filter(keep); return `${rows.length} row(s) ₪${Math.round(rows.reduce((s, r) => s + r.ils, 0)).toLocaleString('en-US')}`; };
        console.log(`  ${source.padEnd(15)} TRUE: ${part((r) => r.actual && projected.has(r.month))} counted, ${part((r) => r.actual && !projected.has(r.month))} in closed months · FALSE: ${part((r) => !r.actual)} (not counted)`);
      }
    }
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
