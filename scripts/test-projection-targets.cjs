#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// Checks for the projection-year targets: the driver formulas and their effect on both pages
// (src/forecast/targets.mjs) and the shared store (server/projection-targets.cjs). Synthetic data only.
//   node scripts/test-projection-targets.cjs
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const os = require('os');
const path = require('path');
const { pathToFileURL } = require('url');

const ROOT = path.resolve(__dirname, '..');
let failures = 0;
function check(ok, label, detail) {
  if (ok) console.log(`  ✓ ${label}`);
  else { failures++; console.log(`  ✗ ${label}${detail !== undefined ? ` — ${detail}` : ''}`); }
}
const near = (a, b, tol = 0.011) => Math.abs(a - b) <= tol;

const T = 2027;
const mk = (m) => `${T}-${String(m).padStart(2, '0')}`;
const R = 3.6;

function baseOf(t) {
  return t.buildTargetsBase({
    year: T,
    months: Array.from({ length: 12 }, (_, i) => ({ mKey: mk(i + 1), revenue: 5_000_000, collPct: i < 6 ? 100 : 90, payroll: 2_000_000, opex: 1_000_000, ilsRate: R })),
    deptAmounts: { 'R&D': 1_200_000, Sales: 600_000, 'G&A': 200_000 },
    categoryAmountsByMonth: Object.fromEntries(Array.from({ length: 12 }, (_, i) => [String(i + 1).padStart(2, '0'), { 'Cloud Infrastructure & DevOps': 400_000, 'SW Licenses': 200_000, Marketing: 400_000 }])),
    serverRatioYtd: 7.5,
  });
}

// A year of the cash page: opening 10M, each month +500K net.
function cashYear() {
  let open = 10_000_000;
  const rows = Array.from({ length: 12 }, (_, i) => {
    const f = { opening: open, collections: 4_000_000, pipeline: 0, churn: 0, salary: 2_000_000, vendors: 1_000_000, other: 500_000, reval: 0, net: 500_000, dividend: 0, closing: open + 500_000, reanchor: 0 };
    open += 500_000;
    const ils = Object.fromEntries(Object.entries(f).map(([k, v]) => [k, v * R]));
    return { mKey: mk(i + 1), status: 'forecast', dividendExcluded: 0, eur: f, ils };
  });
  return { year: T, kind: 'projection', rows };
}
function pnlYear() {
  let acc = 1_000_000;
  const rows = Array.from({ length: 12 }, (_, i) => {
    const f = { accOpening: acc, revenue: 5_000_000, pipeline: 0, churn: 0, otherRevenue: 80_000, totalRevenue: 5_080_000, payroll: 2_000_000, capex: -1_000_000, opex: 1_000_000, totalCosts: 2_000_000, ebitda: 3_080_000, fx: 0, finance: 8_000, depreciation: -950_000, taxOther: 0, net: 2_138_000, accClosing: acc + 2_138_000 };
    acc += 2_138_000;
    const ils = Object.fromEntries(Object.entries(f).map(([k, v]) => [k, v * R]));
    return { mKey: mk(i + 1), status: 'forecast', eur: f, ils };
  });
  return { year: T, kind: 'projection', rows };
}

async function testFormulas(t) {
  console.log('\nFORMULAS: drivers → monthly changes (src/forecast/targets.mjs)');
  const base = baseOf(t);
  check(base.categories.length === 3 && base.serverCategory === 'Cloud Infrastructure & DevOps' && base.departments.join() === 'G&A,R&D,Sales',
    'baseline: categories and departments from the shares; the server category defaults to the cloud one');
  check(near(base.months[0].payrollByDept['R&D'], 1_200_000) && near(base.months[0].opexByCategory.Marketing, 400_000), 'baseline: payroll and opex split by shares');

  const none = t.computeTargetDeltas(base, t.emptyTargets());
  check(none.every((d) => d.dRevenue === 0 && d.dPayroll === 0 && d.dOpex === 0 && d.dServer === 0), 'empty targets change nothing');
  check(t.isEmptyTargets(t.emptyTargets()), 'empty targets read as empty');

  const g = t.emptyTargets();
  g.revenue.mode = 'growth';
  g.revenue.growthPct = Array(12).fill(1);
  let d = t.computeTargetDeltas(base, g);
  check(near(d[0].dRevenue, 50_000) && near(d[11].dRevenue, 5_000_000 * (1.01 ** 12 - 1), 0.05), 'growth %: compounds month by month', d[11].dRevenue);

  const n = t.emptyTargets();
  n.revenue.mode = 'newMrr';
  n.revenue.newMrr = Array(12).fill(100_000);
  n.revenue.churnPct = Array(12).fill(0).map((_, i) => (i === 0 ? 2 : 0));
  d = t.computeTargetDeltas(base, n);
  check(near(d[0].revenue, 5_100_000 * 0.98) && near(d[2].revenue, 5_300_000 * 0.98), 'new MRR: cumulative; churn: compounding on the result', `${d[0].revenue} ${d[2].revenue}`);

  const p = t.emptyTargets();
  p.payroll.deptPct = [{ dept: 'R&D', pct: 10, from: 4 }, { dept: t.ALL_DEPARTMENTS, pct: 2, from: 1 }];
  p.payroll.hires = [{ dept: 'Sales', monthlyCost: 15_000, start: 7, count: 2 }];
  d = t.computeTargetDeltas(base, p);
  check(near(d[0].dPayroll, 40_000) && near(d[3].dPayroll, 40_000 + 120_000) && near(d[6].dPayroll, 40_000 + 120_000 + 30_000),
    'payroll: department % from its month, "all" on the whole payroll, hires from their start month', `${d[0].dPayroll} ${d[3].dPayroll} ${d[6].dPayroll}`);

  const o = t.emptyTargets();
  o.opex.categoryPct = { Marketing: -25, 'Cloud Infrastructure & DevOps': 50 };
  o.server = { enabled: true, pctOfRevenue: 6, category: 'Cloud Infrastructure & DevOps' };
  d = t.computeTargetDeltas(base, o);
  check(near(d[0].dOpex, -100_000) && near(d[0].dServer, 300_000 - 400_000) && near(d[0].server, 300_000),
    'opex: category %; server = 6% of revenue replaces its category (its own % no longer applies)', `${d[0].dOpex} ${d[0].dServer}`);
  const og = JSON.parse(JSON.stringify(o));
  og.revenue.mode = 'growth';
  og.revenue.growthPct = Array(12).fill(0).map((_, i) => (i === 0 ? 10 : 0));
  d = t.computeTargetDeltas(base, og);
  check(near(d[0].server, 5_500_000 * 0.06), 'server follows the revenue after the revenue targets');
}

async function testApply(t) {
  console.log('\nPAGES: the changes on the cash and P&L years');
  const base = baseOf(t);
  const tg = t.emptyTargets();
  tg.revenue.mode = 'newMrr';
  tg.revenue.newMrr = Array(12).fill(0).map((_, i) => (i === 0 ? 100_000 : 0));
  tg.payroll.hires = [{ dept: 'Sales', monthlyCost: 10_000, start: 1, count: 1 }];
  const deltas = t.computeTargetDeltas(base, tg);

  const cash = cashYear();
  const same = t.applyCashTargets(cash, base, t.computeTargetDeltas(base, t.emptyTargets()));
  check(JSON.stringify(same.rows.map((r) => r.eur)) === JSON.stringify(cash.rows.map((r) => r.eur)), 'cash: empty targets give the Plan, to the cent');
  const c = t.applyCashTargets(cash, base, deltas);
  // Jan–Jun collect 100%, Jul–Dec 90%: Δnet = 100K×pct − 10K.
  const expectDec = cash.rows[11].eur.closing + 6 * 90_000 + 6 * 80_000;
  check(near(c.rows[0].eur.collections, 4_100_000) && near(c.rows[6].eur.collections, 4_090_000) && near(c.rows[0].eur.salary, 2_010_000),
    'cash: collections move by revenue × the month\'s collection %, salary by the hires');
  check(near(c.rows[0].eur.opening, cash.rows[0].eur.opening) && near(c.rows[1].eur.opening, cash.rows[1].eur.opening + 90_000) && near(c.rows[11].eur.closing, expectDec),
    'cash: January opens unchanged; later balances carry the changes', c.rows[11].eur.closing - expectDec);
  check(near(c.rows[11].ils.closing, cash.rows[11].ils.closing + (expectDec - cash.rows[11].eur.closing) * R, 0.05), 'cash: ₪ at the month\'s rate');

  const pnl = pnlYear();
  const pz = t.applyPnlTargets(pnl, base, t.computeTargetDeltas(base, t.emptyTargets()));
  check(JSON.stringify(pz.rows.map((r) => r.eur)) === JSON.stringify(pnl.rows.map((r) => r.eur)), 'P&L: empty targets give the Plan, to the cent');
  const pp = t.applyPnlTargets(pnl, base, deltas);
  const r0 = pp.rows[0].eur;
  check(near(r0.revenue, 5_100_000) && near(r0.totalRevenue, 5_180_000) && near(r0.payroll, 2_010_000) && near(r0.totalCosts, 2_010_000)
    && near(r0.ebitda, 3_170_000) && near(r0.net, 2_228_000), 'P&L: revenue, payroll and every line summed from them');
  check(near(pp.rows[11].eur.accClosing, pnl.rows[11].eur.accClosing + 12 * 90_000) && near(pp.rows[0].eur.accOpening, pnl.rows[0].eur.accOpening),
    'P&L: accumulated profit carries the changes from January');
  const v = t.variantWithTargets({ years: [{ year: T - 1, kind: 'current', rows: [] }, pnl] }, base, tg, t.applyPnlTargets);
  check(v.years[0].rows.length === 0 && v.years[1].rows[0].eur.revenue === 5_100_000, 'only the projection year changes');
}

async function testValidation(t) {
  console.log('\nVALIDATION: what can be saved');
  check(t.validateTargets(t.emptyTargets()).ok, 'empty targets are valid');
  const bad = [
    [{ revenue: { mode: 'up' } }, 'unknown revenue mode'],
    [{ revenue: { growthPct: [1, 2] } }, 'not 12 months'],
    [{ revenue: { growthPct: Array(12).fill(80) } }, 'growth out of range'],
    [{ payroll: { hires: [{ dept: 'Sales', monthlyCost: 5_000, start: 13, count: 1 }] } }, 'start month 13'],
    [{ payroll: { deptPct: [{ dept: '', pct: 5, from: 1 }] } }, 'empty department'],
    [{ opex: { categoryPct: { Marketing: '10' } } }, 'a % as text'],
    [{ server: { enabled: true, pctOfRevenue: 8 } }, 'server without a category'],
    [{ evil: 1 }, 'unknown field'],
    [{ revenue: { mode: 'growth', __proto__x: 1 } }, 'unknown nested field'],
    [null, 'not an object'],
    [JSON.parse('{"opex":{"categoryPct":{"__proto__":5}}}'), 'a reserved name as a category'],
  ];
  const passed = bad.filter(([x]) => t.validateTargets(x).ok).map(([, why]) => why);
  check(!passed.length, `${bad.length} kinds of bad input are refused`, passed.join(', '));
  const ok = t.validateTargets({ revenue: { mode: 'growth', growthPct: Array(12).fill(1.5) }, server: { enabled: true, pctOfRevenue: 8, category: 'Cloud Infrastructure & DevOps' } });
  check(ok.ok && ok.targets.payroll.hires.length === 0 && ok.targets.revenue.churnPct.length === 12, 'partial targets are completed with no-change defaults');
}

function call(handler, { method = 'GET', body, headers = {}, user } = {}) {
  return new Promise((resolve) => {
    const raw = body === undefined ? '' : typeof body === 'string' ? body : JSON.stringify(body);
    const req = require('stream').Readable.from(raw ? [Buffer.from(raw)] : []);
    Object.assign(req, { method, url: '/api/projection-targets', headers: { host: 'finance.example', ...headers }, user });
    const res = {
      statusCode: 200, headers: {},
      setHeader(k, v) { this.headers[k.toLowerCase()] = v; },
      end(b) { resolve({ status: this.statusCode, headers: this.headers, body: b ? JSON.parse(b) : null }); },
    };
    handler(req, res);
  });
}

async function testStore(t) {
  console.log('\nSTORE: GET / PUT /api/projection-targets (server/projection-targets.cjs)');
  const { createProjectionTargetsHandler } = require(path.join(ROOT, 'server', 'projection-targets.cjs'));
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'projection-targets-'));
  const file = path.join(tmp, 'targets.json');
  const h = createProjectionTargetsHandler({ file, clock: () => new Date('2026-10-04T10:00:00Z').getTime(), referenceOf: () => ({ serverRatioYtd: 7.5 }) });
  let r = await call(h);
  check(r.status === 200 && r.body.ok && Object.keys(r.body.years).length === 0 && r.body.reference.serverRatioYtd === 7.5 && r.headers['cache-control'] === 'no-store',
    'GET with nothing saved: no targets, the reference ratio');
  const tg = t.emptyTargets();
  tg.revenue.mode = 'growth';
  tg.revenue.growthPct = Array(12).fill(1);
  const json = { 'content-type': 'application/json' };
  r = await call(h, { method: 'PUT', body: { year: T, targets: tg }, headers: { ...json, origin: 'https://finance.example' }, user: { email: 'Someone@Example.com' } });
  check(r.status === 200 && r.body.years[String(T)].revenue.mode === 'growth' && r.body.updatedBy === 'someone@example.com', 'PUT saves the targets with who saved them');
  const stored = JSON.parse(fs.readFileSync(file, 'utf8'));
  const history = fs.readFileSync(file.replace(/\.json$/, '-history.jsonl'), 'utf8').trim().split('\n');
  check(stored.years[String(T)].revenue.growthPct[0] === 1 && history.length === 1 && JSON.parse(history[0]).year === T, 'saved to the file, one history line per save');
  r = await call(h);
  check(r.body.years[String(T)].revenue.mode === 'growth', 'GET returns what was saved');

  const codes = {
    text: (await call(h, { method: 'PUT', body: { year: T, targets: tg }, headers: { 'content-type': 'text/plain' } })).status,
    cross: (await call(h, { method: 'PUT', body: { year: T, targets: tg }, headers: { ...json, origin: 'https://evil.example' } })).status,
    invalid: (await call(h, { method: 'PUT', body: { year: T, targets: { revenue: { mode: 'up' } } }, headers: json })).status,
    year: (await call(h, { method: 'PUT', body: { year: 2031, targets: tg }, headers: json })).status,
    notJson: (await call(h, { method: 'PUT', body: '{nope', headers: json })).status,
    big: (await call(h, { method: 'PUT', body: { year: T, targets: tg, pad: 'x'.repeat(40_000) }, headers: json })).status,
    post: (await call(h, { method: 'POST', body: {}, headers: json })).status,
  };
  check(codes.text === 415 && codes.cross === 403 && codes.invalid === 400 && codes.year === 400 && codes.notJson === 400 && codes.big === 413 && codes.post === 405,
    'refused: not JSON 415, another site 403, out of range 400, wrong year 400, bad JSON 400, too large 413, POST 405', JSON.stringify(codes));
  const after = JSON.parse(fs.readFileSync(file, 'utf8'));
  check(JSON.stringify(after) === JSON.stringify(stored), 'a refused PUT leaves the saved targets as they were');
  fs.rmSync(tmp, { recursive: true, force: true });
}

async function main() {
  console.log('=== projection targets checks (synthetic data) ===');
  const t = await import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'targets.mjs')).href);
  await testFormulas(t);
  await testApply(t);
  await testValidation(t);
  await testStore(t);
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
