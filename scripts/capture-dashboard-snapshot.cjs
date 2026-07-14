// Phase-0 end-to-end dashboard snapshot for the SF revenue-table migration.
//
// CAPTURE (run BEFORE on prod, AFTER on localhost):
//   node scripts/capture-dashboard-snapshot.cjs --label=before --base=http://localhost:8790 --asof=2026-06-30
//   node scripts/capture-dashboard-snapshot.cjs --label=before --base=http://localhost:8790 --asof=live
//   node scripts/capture-dashboard-snapshot.cjs --label=after  --base=http://localhost:5176 --asof=2026-06-30
//   node scripts/capture-dashboard-snapshot.cjs --label=after  --base=http://localhost:5176 --asof=live
//
// DIFF (offline comparison):
//   node scripts/capture-dashboard-snapshot.cjs --diff=before,after --asof=2026-06-30
//
// Captures the raw JSON of every load-path API endpoint (Layer 1 — deterministic given fixed
// subsidiary/year/asOf). Writes to data/migration-snapshots/<label>/<asofTag>/<endpoint>.json.
//
// Layer 2 (client-computed forecast + cards) is NOT an API response — capture it in the browser:
// open  <base>/?fccapture=1&asOf=<asof>  and in the console run
//   copy(JSON.stringify(window.__fcRows))   // then paste into <label>/<asofTag>/__fcRows.json
// and record the on-screen KR5 / Budget-Bridge revenue / YoY / Pipeline ×factor / dept-revenue
// numbers. See docs/sf-revenue-migration-capture.md. No auth header is needed for read endpoints.

const path = require('path');
const fs = require('fs');

function arg(name, def) {
  const hit = process.argv.slice(2).find(a => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : def;
}
const SNAP_ROOT = path.resolve(__dirname, '..', 'data', 'migration-snapshots');

// Revenue-touching endpoints — changes here are EXPECTED impact zones after migration.
const IMPACT = new Set(['sf-revenue', 'sf-revenue-paid', 'sf-yoy-revenue', 'sf-pipeline-methodology', 'sf-revenue-breakdown']);

function prevMonth(ymd) {
  const y = parseInt(ymd.slice(0, 4), 10), m = parseInt(ymd.slice(5, 7), 10);
  const d = new Date(Date.UTC(y, m - 1, 1)); // asof month is the last completed month → lastActual = that month
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}

function endpointList({ subsidiary, year, priorYear, asof, lastActual, sampleMonths }) {
  const sub = `subsidiary=${subsidiary}`;
  const asofQ = asof ? `&asOfDate=${asof}` : '';
  const list = [
    // NetSuite family
    ['ns-config', `/api/ns-config?${sub}`],
    ['bank-balance', `/api/bank-balance?${sub}`],
    ['bank-accounts', `/api/bank-accounts?${sub}`],
    ['vendor-bills', `/api/vendor-bills?${sub}`],
    ['ar-forecast', `/api/ar-forecast?${sub}`],
    ['salary-data', `/api/salary-data?${sub}`],
    ['vendor-history', `/api/vendor-history?${sub}`],
    ['expense-categories', `/api/expense-categories?${sub}`],
    ['banks-collection-data', `/api/banks-collection-data?${sub}`],
    ['monthly-reval', `/api/monthly-reval?${sub}`],
    ['ns-paid-vendors-yearly', `/api/ns-paid-vendors-yearly?${sub}`],
    ['ns-bank-classified-yearly', `/api/ns-bank-classified-yearly?${sub}`],
    ['ns-vendor-actuals', `/api/ns-vendor-actuals?${sub}`],
    ['ns-revenue-actuals', `/api/ns-revenue-actuals?${sub}`],
    ['ns-customer-receipts', `/api/ns-customer-receipts?${sub}`],
    ['ns-budget', `/api/ns-budget?${sub}`],
    // Snowflake family
    ['sf-budget', `/api/sf-budget?${sub}`],
    ['sf-revenue', `/api/sf-revenue?year=${year}`],
    ['sf-revenue-prior', `/api/sf-revenue?year=${priorYear}`],
    ['sf-actuals-split', `/api/sf-actuals-split?${sub}`],
    ['sf-salary-budget', `/api/sf-salary-budget?${sub}`],
    ['sf-revenue-paid', `/api/sf-revenue-paid?year=${year}`],
    ['sf-revenue-paid-prior', `/api/sf-revenue-paid?year=${priorYear}`],
    ['sf-pipeline', `/api/sf-pipeline?${sub}`],
    ['sf-conversion', `/api/sf-conversion?${sub}`],
    ['sf-churn-analysis', `/api/sf-churn-analysis?${sub}`],
    ['sf-yoy-revenue', `/api/sf-yoy-revenue?${sub}${asofQ}`],
    ['sf-finance-budget', `/api/sf-finance-budget?${sub}`],
    ['arr-current', `/api/arr-current?year=${year}`],
    ['sf-salary-actuals-by-dept', `/api/sf-salary-actuals-by-dept?${sub}`],
    ['sf-monthly-hc-impact', `/api/sf-monthly-hc-impact?lastActual=${lastActual}`],
    ['sf-pipeline-methodology', `/api/sf-pipeline-methodology?year=${year}`],
    // Anchors + on-demand
    ['ns-bank-accounts-asof', `/api/ns-bank-accounts-asof?date=${asof || new Date().toISOString().slice(0, 10)}&${sub}`],
  ];
  for (const m of sampleMonths) list.push([`sf-revenue-breakdown-${m}`, `/api/sf-revenue-breakdown?month=${m}`]);
  return list;
}

async function fetchJson(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 60000);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    const text = await res.text();
    let body; try { body = JSON.parse(text); } catch { body = { __nonJson: text.slice(0, 500) }; }
    return { ok: res.ok, status: res.status, body };
  } catch (e) {
    return { ok: false, status: 0, error: e.message };
  } finally { clearTimeout(t); }
}

// ── DIFF mode ───────────────────────────────────────────────────────────────
function flatten(obj, prefix, out) {
  if (obj === null || typeof obj !== 'object') { out[prefix] = obj; return out; }
  if (Array.isArray(obj)) { obj.forEach((v, i) => flatten(v, `${prefix}[${i}]`, out)); return out; }
  for (const k of Object.keys(obj)) flatten(obj[k], prefix ? `${prefix}.${k}` : k, out);
  return out;
}
function diffDirs(aDir, bDir) {
  const files = [...new Set([...fs.readdirSync(aDir).filter(f => f.endsWith('.json')), ...fs.readdirSync(bDir).filter(f => f.endsWith('.json'))])].sort();
  const changedByEndpoint = [];
  for (const f of files) {
    if (f === 'manifest.json') continue;
    const endpoint = f.replace(/\.json$/, '').replace(/-\d{4}-\d{2}$/, ''); // strip sample-month suffix
    const aP = path.join(aDir, f), bP = path.join(bDir, f);
    const a = fs.existsSync(aP) ? JSON.parse(fs.readFileSync(aP, 'utf8')) : null;
    const b = fs.existsSync(bP) ? JSON.parse(fs.readFileSync(bP, 'utf8')) : null;
    const fa = flatten(a?.body ?? a, '', {}), fb = flatten(b?.body ?? b, '', {});
    const keys = [...new Set([...Object.keys(fa), ...Object.keys(fb)])];
    const changes = [];
    for (const k of keys) {
      const va = fa[k], vb = fb[k];
      if (JSON.stringify(va) !== JSON.stringify(vb)) changes.push({ key: k, before: va, after: vb });
    }
    if (changes.length) changedByEndpoint.push({ file: f, endpoint, expected: IMPACT.has(endpoint), count: changes.length, sample: changes.slice(0, 12) });
  }
  return changedByEndpoint;
}

(async () => {
  const diffArg = arg('diff', null);
  const asof = arg('asof', '2026-06-30');
  const asofTag = (asof === 'live' || asof === '') ? 'live' : asof;

  if (diffArg) {
    const [aLabel, bLabel] = diffArg.split(',');
    const aDir = path.join(SNAP_ROOT, aLabel, asofTag);
    const bDir = path.join(SNAP_ROOT, bLabel, asofTag);
    if (!fs.existsSync(aDir) || !fs.existsSync(bDir)) { console.error(`Missing snapshot dir(s):\n  ${aDir} (${fs.existsSync(aDir)})\n  ${bDir} (${fs.existsSync(bDir)})`); process.exit(1); }
    const changed = diffDirs(aDir, bDir);
    const expected = changed.filter(c => c.expected), unexpected = changed.filter(c => !c.expected);
    console.log(`\n# Dashboard snapshot diff — ${aLabel} → ${bLabel} @ asof=${asofTag}\n`);
    console.log(`Endpoints changed: ${changed.length} (expected impact zones: ${expected.length}, SHOULD-BE-UNCHANGED: ${unexpected.length})\n`);
    console.log(`## Expected (revenue impact zones — review, don't block)`);
    for (const c of expected) console.log(`  • ${c.file}: ${c.count} value(s) changed`);
    console.log(`\n## Unexpected (must be identical — investigate any entry below)`);
    if (!unexpected.length) console.log('  ✓ none — all non-revenue endpoints identical');
    for (const c of unexpected) {
      console.log(`  ✗ ${c.file}: ${c.count} value(s) changed`);
      for (const s of c.sample) console.log(`       ${s.key}: ${JSON.stringify(s.before)} → ${JSON.stringify(s.after)}`);
    }
    const outPath = path.join(SNAP_ROOT, `diff-${aLabel}-vs-${bLabel}-${asofTag}.json`);
    fs.writeFileSync(outPath, JSON.stringify({ aLabel, bLabel, asof: asofTag, changed }, null, 2));
    console.log(`\nWrote ${outPath}`);
    process.exit(0);
  }

  // CAPTURE mode
  const label = arg('label', null);
  if (!label) { console.error('Provide --label=before|after (or --diff=before,after).'); process.exit(1); }
  const base = arg('base', 'http://localhost:5176').replace(/\/$/, '');
  const subsidiary = parseInt(arg('subsidiary', '3'), 10);
  const year = parseInt(arg('year', '2026'), 10);
  const priorYear = year - 1;
  const liveAsof = (asof === 'live' || asof === '');
  const asofVal = liveAsof ? '' : asof;
  const lastActual = arg('lastActual', liveAsof ? prevMonth(new Date().toISOString().slice(0, 10)) : `${asof.slice(0, 7)}`);
  const sampleMonths = arg('months', `${year}-05,${year}-06,${priorYear}-06`).split(',').map(s => s.trim()).filter(Boolean);

  const outDir = path.join(SNAP_ROOT, label, asofTag);
  fs.mkdirSync(outDir, { recursive: true });
  const list = endpointList({ subsidiary, year, priorYear, asof: asofVal, lastActual, sampleMonths });
  console.log(`[capture] ${label} @ asof=${asofTag} · base=${base} · ${list.length} endpoints → ${outDir}`);

  const manifest = { label, base, subsidiary, year, asof: asofTag, lastActual, capturedAt: new Date().toISOString(), endpoints: {} };
  for (const [name, pathq] of list) {
    const url = base + pathq;
    const r = await fetchJson(url);
    fs.writeFileSync(path.join(outDir, `${name}.json`), JSON.stringify({ url: pathq, ...r }, null, 2));
    manifest.endpoints[name] = { url: pathq, ok: r.ok, status: r.status, error: r.error || null };
    console.log(`  ${r.ok ? '✓' : '✗'} ${name} (${r.status || r.error})`);
  }
  fs.writeFileSync(path.join(outDir, 'manifest.json'), JSON.stringify(manifest, null, 2));
  console.log(`\n[capture] done. Layer-2 (client-computed) reminder:`);
  console.log(`  open ${base}/?fccapture=1${liveAsof ? '' : `&asOf=${asof}`}  → console: copy(JSON.stringify(window.__fcRows))`);
  console.log(`  save it as ${path.join(outDir, '__fcRows.json')} and jot the KR5 / bridge / YoY / pipeline-factor numbers alongside.`);
  process.exit(0);
})().catch(e => { console.error('ERROR:', e); process.exit(1); });
