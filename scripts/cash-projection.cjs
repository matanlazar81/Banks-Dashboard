#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// New Bank Dashboard projection from the command line: the same computation as
// GET /api/cash-projection (server/cash-projection.cjs), without a web server.
//
//   node scripts/cash-projection.cjs --dry-run       print both years (plan and base), € thousands
//   node scripts/cash-projection.cjs --json          print the API payload
//   node scripts/cash-projection.cjs --write-cache   compute and write data/cash-projection-cache.json;
//                                                    a running server serves it on its next request
//                                                    (e.g. cron at 06:10, after the nightly net-cash job)
//   node scripts/cash-projection.cjs --dry-run --snapshot-file=data/budgets/2027-lsports.json
//        build the projection year from a stored "→ 2027" snapshot instead of live data, to
//        reproduce exactly what the old dashboard shows from that file
//   node scripts/cash-projection.cjs --compare=<rows.json> [--variant=plan|base]
//        diff against old-dashboard rows captured with ?fccapture=1 (copy(JSON.stringify(window.__fcRows)));
//        the year is taken from the captured rows. Exit 1 when any figure differs by more than €1.
//
// Needs the same .env as the nightly job (NetSuite, Snowflake, and DATABASE_URL for the plan).
// Output contains internal financial figures: keep it on the server.
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
try { require('dotenv').config({ path: path.join(ROOT, '.env') }); } catch { /* env may already be exported */ }

const { computeCashProjection, makeEntry, writeCacheEntry, DEFAULT_CACHE_FILE } = require(path.join(ROOT, 'server', 'cash-projection.cjs'));

function arg(name) {
  const hit = process.argv.slice(2).find((a) => a === `--${name}` || a.startsWith(`--${name}=`));
  if (!hit) return null;
  return hit.includes('=') ? hit.slice(hit.indexOf('=') + 1) : true;
}

const GOLDEN_FIELDS = [
  'openingBalance', 'collections', 'pipelineWeighted', 'churnDeduction', 'salary', 'vendors', 'other',
  'revalImpact', 'net', 'closingBalance',
  'openingBalanceILS', 'collectionsILS', 'salaryILS', 'vendorsILS', 'revalImpactILS', 'netILS', 'closingBalanceILS',
];

const k = (n) => {
  const v = Math.round((Number(n) || 0) / 1000);
  return (v < 0 ? `(${Math.abs(v).toLocaleString('en-US')})` : v.toLocaleString('en-US')).padStart(9);
};

function printYear(label, block) {
  console.log(`\n${label} ${block.year} (${block.kind}) — € thousands`);
  console.log('  month    status    opening  inflows outflows   reval  net chg dividend  closing  re-anchor');
  for (const r of block.rows) {
    const f = r.eur;
    const inflows = f.collections + f.pipeline - f.churn;
    const outflows = f.salary + f.vendors + f.other;
    console.log(`  ${r.mKey}  ${r.status.padEnd(8)}${k(f.opening)}${k(inflows)}${k(outflows)}${k(f.reval)}${k(f.net)}${k(-f.dividend)}${k(f.closing)}${k(f.reanchor)}`);
  }
}

function loadCapturedRows(file) {
  const raw = JSON.parse(fs.readFileSync(path.resolve(file), 'utf-8'));
  const rows = Array.isArray(raw) ? raw : (raw.rows || raw.expected || raw.__fcRows);
  if (!Array.isArray(rows) || rows.length !== 12 || !rows[0].mKey) throw new Error(`${file}: expected the 12 rows of window.__fcRows`);
  return rows;
}

async function main() {
  const compareFile = arg('compare');
  const snapshotArg = arg('snapshot-file');
  const variant = arg('variant') === 'base' ? 'base' : 'plan';
  if (arg('write-cache') && snapshotArg) throw new Error('--write-cache cannot be combined with --snapshot-file (parity runs must not replace the live cache).');

  let snapshotFile = null;
  if (typeof snapshotArg === 'string') {
    const parsed = JSON.parse(fs.readFileSync(path.resolve(snapshotArg), 'utf-8'));
    snapshotFile = parsed && parsed.data && parsed.exists !== undefined ? parsed.data : parsed; // file or /api/budget-snapshot body
  }

  const { createNetSuiteClient } = require(path.join(ROOT, 'netsuite-api.cjs'));
  const { createSnowflakeClient } = require(path.join(ROOT, 'snowflake-api.cjs'));
  const sub = parseInt(process.env.NET_CASH_SUBSIDIARY || '3', 10) || 3;
  const ns = createNetSuiteClient(process.env, sub);
  const sf = createSnowflakeClient(process.env);

  const t0 = Date.now();
  // NetSuite is throttled inside gatherInputs (3 at a time, like the nightly job), so no extra queue here.
  const { payload, raw } = await computeCashProjection({
    now: new Date(),
    getNsClient: () => ns,
    getSfClient: () => sf,
    queueNsCall: (fn) => fn(),
    snapshotFile,
    includeRaw: true,
  });
  console.log(`\n[cash-projection] computed in ${((Date.now() - t0) / 1000).toFixed(1)}s — plan "${payload.plan.name}" ${payload.plan.loaded ? `loaded (${payload.plan.source})` : 'NOT found (plan = base)'}`);
  for (const w of payload.warnings) console.warn(`[cash-projection] ⚠ ${w}`);

  if (arg('json')) {
    console.log(JSON.stringify(payload, null, 2));
  }

  if (arg('dry-run')) {
    for (const v of ['plan', 'base']) {
      for (const block of payload.variants[v].years) printYear(v.toUpperCase(), block);
      const rf = payload.variants[v].rollForward;
      console.log(`  roll-forward ${rf.from} → ${rf.to}: €${Math.round(rf.closing.eur).toLocaleString('en-US')} (source: ${rf.source}, salary basis: ${rf.salaryBasis.method})`);
    }
  }

  if (compareFile) {
    const captured = loadCapturedRows(compareFile);
    const year = Number(String(captured[0].mKey).slice(0, 4));
    const ours = raw[variant].rows[year];
    if (!ours) throw new Error(`no ${year} rows in this projection (years: ${payload.years.join(', ')})`);
    let diffs = 0;
    for (let i = 0; i < 12; i++) {
      for (const f of GOLDEN_FIELDS) {
        const got = Math.round(Number(ours[i][f]) || 0);
        const exp = Math.round(Number(captured[i][f]) || 0);
        if (Math.abs(got - exp) > 1) {
          diffs++;
          console.log(`  ✗ ${ours[i].mKey} ${f}: new ${got.toLocaleString('en-US')} vs old ${exp.toLocaleString('en-US')} (Δ ${(got - exp).toLocaleString('en-US')})`);
        }
      }
    }
    console.log(diffs === 0
      ? `\n[cash-projection] ✓ ${variant} ${year} matches the captured old-dashboard rows (all fields within €1).`
      : `\n[cash-projection] ✗ ${diffs} difference(s) vs the captured old-dashboard rows (${variant} ${year}).`);
    if (diffs) process.exitCode = 1;
  }

  if (arg('write-cache')) {
    const file = DEFAULT_CACHE_FILE;
    writeCacheEntry(file, makeEntry(payload, Date.now()));
    console.log(`[cash-projection] ✓ wrote ${path.relative(ROOT, file)}; the server serves it on its next request.`);
  }

  if (!arg('json') && !arg('dry-run') && !compareFile && !arg('write-cache')) {
    console.log('Nothing to do: pass --dry-run, --json, --write-cache or --compare=<file>.');
  }
}

main().catch((e) => {
  console.error(`[cash-projection] FAILED: ${e && e.stack ? e.stack : e}`);
  process.exit(1);
});
