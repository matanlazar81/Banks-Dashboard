#!/usr/bin/env node
// ============================================================================
// test-salary-projection.cjs — checks for the Salary Projection page.
// No dependencies, no NetSuite/Snowflake access: every input is SYNTHETIC (illustrative numbers, not
// LSports figures).
//
//   node scripts/test-salary-projection.cjs                  run all checks
//   node scripts/test-salary-projection.cjs --write-fixture  also (re)write the synthetic UI fixture
//                                                            scripts/fixtures/salary-projection-sample.json
//
// Covers: the payload of buildSalaryProjection / computeSalaryProjection (server/salary-projection.cjs) —
// NetSuite payroll rows, account classification, the last closed month and its guard, permanent run-rate,
// temporary same-month-last-year (into the next year too), actuals-only accounts, forecast euros — the
// cached handler, and the UI table model (src/salary-projection/model.ts, imported directly — Node strips
// the TypeScript types).
// Exit 0 = all checks passed; 1 = a failure.
// ============================================================================
const fs = require('fs');
const os = require('os');
const path = require('path');
const { pathToFileURL } = require('url');

const ROOT = path.resolve(__dirname, '..');
const cp = require(path.join(ROOT, 'server', 'cash-projection.cjs'));
const sp = require(path.join(ROOT, 'server', 'salary-projection.cjs'));

let failures = 0;
const fail = (msg) => { failures++; console.error('  ✗ ' + msg); };
const ok = (msg) => console.log('  ✓ ' + msg);
const check = (cond, msg, detail) => (cond ? ok(msg) : fail(detail ? `${msg} — ${detail}` : msg));
const near = (a, b, tol = 0.011) => Math.abs(a - b) <= tol;

async function quiet(fn) {
  const { log, warn, error } = console;
  console.log = () => {}; console.warn = () => {}; console.error = () => {};
  try { return await fn(); } finally { console.log = log; console.warn = warn; console.error = error; }
}

// ── synthetic data ──────────────────────────────────────────────────────────
const Y = 2026;
const T = 2027;
const NOW = new Date('2026-10-15T12:00:00'); // Jan–Sep actual, Oct current, Nov–Dec forecast
const mk = (y, m) => `${y}-${String(m).padStart(2, '0')}`;
const range = (a, b) => Array.from({ length: b - a + 1 }, (_, i) => a + i);
const RATE = { [Y - 1]: 4.0, [Y]: 3.7 }; // ₪ per € by year (different, so the forecast rate is visible)

// Synthetic payroll rows (costs positive): 2025 in full, 2026 through September, a token posting in the
// current month, and one payroll account that is not classified.
function syntheticRows({ lagSeptember = false, withPrevYear = true } = {}) {
  const rows = [];
  const add = (y, m, acct, ils, name) => rows.push({ month: mk(y, m), acct, name: name || `Account ${acct}`, eur: ils / RATE[y], ils });
  for (const y of withPrevYear ? [Y - 1, Y] : [Y]) {
    const last = y === Y ? 9 : 12;
    for (const m of range(1, last)) {
      const gross = (y === Y ? 5_200_000 : 5_000_000) + m * 1_000;
      const token = lagSeptember && y === Y && m === 9;
      add(y, m, '760001', token ? 500 : gross, 'Gross Salaries');
      if (token) continue;
      add(y, m, '760002', gross * 0.1);
      add(y, m, '760005', 100_000);
      add(y, m, '760006', -100_000);
      add(y, m, '760018', 20_000);
    }
  }
  if (withPrevYear) {
    add(Y - 1, 1, '760017', 900_000, 'One time payments (Bonus, grants)');
    add(Y - 1, 7, '760015', 300_000);
    add(Y - 1, 10, '760017', 200_000);
    add(Y - 1, 12, '760017', 150_000);
    add(Y - 1, 11, '760019', 40_000);
    add(Y - 1, 10, '760023', -50_000);
    add(Y - 1, 11, '760023', -80_000);
    add(Y - 1, 12, '760029', 70_000);
    add(Y - 1, 12, '760008', 356_000, 'option employess expemses');
    add(Y - 1, 12, '760014', -703_000, 'Vacation expenses');
  }
  add(Y, 1, '760017', 1_000_000);
  add(Y, 7, '760015', 320_000);
  add(Y, 3, '760023', -30_000);
  add(Y, 4, '760020', 5_000, 'Reducing salary expenses-option exercise');
  add(Y, 3, '760040', 1_000, 'Unclassified payroll');
  add(Y, 10, '760017', 2_000); // the current month: partial, never shown as actual
  return rows;
}

// The same rows as NetSuite's P&L actuals return them: every P&L account, profit-signed (costs negative).
function syntheticPnlActuals() {
  const byMonth = {};
  for (const r of syntheticRows()) {
    (byMonth[r.month] || (byMonth[r.month] = {}))[r.acct] = { acct: r.acct, name: r.name, type: 'Expense', eur: -r.eur, ils: -r.ils };
  }
  byMonth[mk(Y, 3)]['400001'] = { acct: '400001', name: 'Revenue', type: 'Income', eur: 900_000, ils: 3_330_000 };
  byMonth[mk(Y, 3)]['640001'] = { acct: '640001', name: 'Cloud', type: 'Expense', eur: -50_000, ils: -185_000 };
  return { basis: 'period', byMonth, accountIds: {} };
}

const blockOf = (payload, year) => payload.blocks.find((b) => b.year === year);
const rowOf = (payload, year, m) => blockOf(payload, year).rows[m - 1];
const ils = (payload, year, m, acct) => rowOf(payload, year, m).ils[acct];
const accountOf = (payload, acct) => payload.accounts.find((a) => a.acct === acct);

// ── 1. payload ──────────────────────────────────────────────────────────────
function testPayload() {
  console.log('PAYLOAD: buildSalaryProjection (server/salary-projection.cjs)');
  const { payload } = sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW });
  const accts = payload.accounts.map((a) => a.acct);
  const permanent = sp.SALARY_GL_GROUPS[0].accounts.map(([a]) => a);
  const temporary = sp.SALARY_GL_GROUPS[1].accounts.map(([a]) => a);
  check(JSON.stringify(accts) === JSON.stringify([...permanent, ...temporary, '760040']),
    'every classified account in finance\'s order (permanent, then temporary), then the unclassified one', accts.join(','));
  check(permanent.length === 10 && temporary.length === 12 && permanent.includes('760025')
    && ['760020', '76003', '760008', '760014'].every((a) => temporary.includes(a)),
    'classification: 10 permanent (incl. 760025), 12 temporary (incl. 760020, 76003, 760008, 760014)');
  check(accountOf(payload, '760001').method === 'run-rate' && accountOf(payload, '760017').method === 'last-year'
    && ['760023', '760029', '760008', '760014'].every((a) => accountOf(payload, a).method === 'none') && accountOf(payload, '760040').method === 'last-year',
    'methods: permanent run-rate, temporary same month last year, 760023, 760029, 760008 and 760014 not projected');
  check(accountOf(payload, '760001').name === 'Gross Salaries' && accountOf(payload, '760024').name === 'National insurance Damage fees',
    'names from the source, the classification\'s name for an account without postings');
  check(accountOf(payload, '760040').group === 'other' && payload.warnings.some((w) => w.includes('760040')) && !payload.warnings.some((w) => w.includes('760020')),
    'an unclassified payroll account is shown, grouped "other", with a warning (760020 is classified now)');

  const statuses = blockOf(payload, Y).rows.map((r) => r.status);
  check(statuses.slice(0, 9).every((s) => s === 'actual') && statuses[9] === 'current' && statuses.slice(10).every((s) => s === 'forecast'),
    'Jan–Sep actual, October current, Nov–Dec forecast', statuses.join(','));
  check(blockOf(payload, T).kind === 'projection' && blockOf(payload, T).rows.every((r) => r.status === 'forecast'), `${T}: every month forecast`);
  check(payload.actuals.through === mk(Y, 9) && payload.basis.runRateMonth === mk(Y, 9) && payload.actuals.source === 'netsuite',
    'actuals from NetSuite through September; September is the run-rate month');

  check(ils(payload, Y, 3, '760001') === 5_203_000 && near(rowOf(payload, Y, 3).eur['760001'], 5_203_000 / 3.7) && ils(payload, Y, 3, '760040') === 1_000,
    'actual months are the source exactly (₪ and €)');
  check(ils(payload, Y, 2, '760017') === 0 && ils(payload, Y, 1, '760024') === 0, 'an account without postings in an actual month is 0');

  const sep = (acct) => ils(payload, Y, 9, acct);
  const flat = [[Y, 10], [Y, 11], [Y, 12], ...range(1, 12).map((m) => [T, m])];
  const offFlat = [];
  for (const acct of ['760001', '760002', '760005', '760006', '760018', '760025']) {
    for (const [y, m] of flat) if (ils(payload, y, m, acct) !== sep(acct)) offFlat.push(`${acct} ${mk(y, m)}`);
  }
  check(!offFlat.length, 'permanent accounts: every forecast month (Oct 2026 – Dec 2027) carries September flat', offFlat.slice(0, 3).join('; '));

  check(ils(payload, Y, 12, '760017') === 150_000 && ils(payload, Y, 11, '760017') === 0 && ils(payload, Y, 11, '760019') === 40_000,
    'temporary: Oct–Dec 2026 repeat Oct–Dec 2025 (bonus, maternity)');
  check(ils(payload, Y, 10, '760017') === 200_000, 'the current month is projected: its partial posting (2,000) is replaced by October 2025');
  check(ils(payload, T, 1, '760017') === 1_000_000 && ils(payload, T, 7, '760015') === 320_000 && ils(payload, T, 4, '760020') === 5_000,
    `${T} repeats ${Y}'s actual months (January bonus, July recuperation, April option exercise)`);
  check(ils(payload, T, 10, '760017') === 200_000 && ils(payload, T, 11, '760019') === 40_000,
    `${T} repeats ${Y}'s forecast months, which repeat ${Y - 1} (the chain)`);

  const offNone = [];
  for (const acct of ['760023', '760029', '760008', '760014']) {
    for (const [y, m] of flat) if (ils(payload, y, m, acct) !== 0) offNone.push(`${acct} ${mk(y, m)}`);
  }
  check(ils(payload, Y, 3, '760023') === -30_000 && !offNone.length,
    '760023, 760029, 760008, 760014: actual months shown, every forecast month 0 (the 2025 refunds, notice pay and December accruals not repeated)', offNone.slice(0, 3).join('; '));
  check(!payload.warnings.some((w) => w.includes('760008') || w.includes('760014')), '760008 and 760014 are classified: no warning');
  check(ils(payload, T, 3, '760040') === 1_000 && ils(payload, Y, 11, '760040') === 0, 'the unclassified account is projected like a temporary one');

  check(near(payload.basis.ilsPerEur, 3.7, 0.0001), 'forecast rate: September\'s permanent payroll ₪/€', String(payload.basis.ilsPerEur));
  check(near(rowOf(payload, Y, 11).eur['760001'], sep('760001') / 3.7) && near(rowOf(payload, Y, 10).eur['760017'], 200_000 / 3.7),
    'forecast € = forecast ₪ ÷ that rate (also for amounts repeated from the previous year)');
  check(payload.status === 'ready' && payload.company === cp.COMPANY && payload.years[0] === Y && payload.years[1] === T,
    'payload envelope: ready, company, years');
  return payload;
}

function testEdges() {
  console.log('\nEDGES: guard, January, no previous year, no data');
  const lag = sp.buildSalaryProjection({ rows: syntheticRows({ lagSeptember: true }), now: NOW }).payload;
  check(lag.basis.runRateMonth === mk(Y, 8) && lag.actuals.through === mk(Y, 8), 'a token September (not posted yet) → August is the last closed month');
  check(rowOf(lag, Y, 9).status === 'forecast' && ils(lag, Y, 9, '760001') === ils(lag, Y, 8, '760001'), 'September is then projected from August');
  check(lag.warnings.some((w) => w.includes('September 2026') && w.includes('August 2026')), 'with a warning naming both months');

  const jan = sp.buildSalaryProjection({ rows: syntheticRows(), now: new Date('2026-01-20T12:00:00') }).payload;
  const janRows = blockOf(jan, Y).rows;
  check(janRows[0].status === 'current' && janRows.slice(1).every((r) => r.status === 'forecast') && jan.actuals.through === null,
    'in January: no actual months, January projected');
  check(jan.basis.runRateMonth === mk(Y - 1, 12) && ils(jan, Y, 1, '760001') === 5_012_000 && ils(jan, Y, 1, '760017') === 900_000,
    'in January: permanent from December of the previous year, temporary from January of the previous year');

  const noPrev = sp.buildSalaryProjection({ rows: syntheticRows({ withPrevYear: false }), now: NOW }).payload;
  check(noPrev.warnings.some((w) => w.includes(`no ${Y - 1} payroll`)) && ils(noPrev, Y, 12, '760017') === 0,
    `no ${Y - 1} rows: a warning, and temporary accounts project 0 in ${Y}`);

  let threw = null;
  try { sp.buildSalaryProjection({ rows: [], now: NOW }); } catch (e) { threw = e; }
  check(threw instanceof cp.ProjectionError, 'no payroll at all → a ProjectionError (its message is shown)');
}

async function testCompute() {
  console.log('\nCOMPUTE: computeSalaryProjection and its NetSuite client');
  const prevId = process.env.NETSUITE_ACCOUNT_ID;
  process.env.NETSUITE_ACCOUNT_ID = 'TEST';
  try {
    let asked = null;
    let queued = 0;
    const ns = { fetchPnlActuals: async (q) => { asked = q; return syntheticPnlActuals(); } };
    const { payload } = await sp.computeSalaryProjection({ now: NOW, getNsClient: () => ns, queueNsCall: (fn) => { queued++; return fn(); } });
    check(asked && asked.fromYear === Y - 1 && asked.toYear === Y && asked.basis === 'period' && queued === 1,
      `one NetSuite read, ${Y - 1}–${Y} by posting period, through the NetSuite queue`);
    const direct = sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW }).payload;
    check(JSON.stringify(payload.blocks) === JSON.stringify(direct.blocks) && JSON.stringify(payload.accounts) === JSON.stringify(direct.accounts),
      'NetSuite\'s profit-signed amounts become costs; only 76xxxx accounts (revenue and other costs left out)');

    const err = async (getNsClient) => { try { await quiet(() => sp.computeSalaryProjection({ now: NOW, getNsClient })); return null; } catch (e) { return e; } };
    const none = await err(() => null);
    check(none instanceof cp.ProjectionError && /unavailable/.test(none.message), 'no NetSuite client → "unavailable"');
    const down = await err(() => ({ fetchPnlActuals: async () => { throw new Error('secret connection detail'); } }));
    check(down instanceof cp.ProjectionError && !down.message.includes('secret'), 'a NetSuite failure → a generic message (the detail stays in the server log)');
    delete process.env.NETSUITE_ACCOUNT_ID;
    const unconfigured = await err(() => ns);
    check(unconfigured instanceof cp.ProjectionError && /not configured/.test(unconfigured.message), 'no NETSUITE_ACCOUNT_ID → "not configured"');
  } finally {
    if (prevId === undefined) delete process.env.NETSUITE_ACCOUNT_ID; else process.env.NETSUITE_ACCOUNT_ID = prevId;
  }
}

// ── 2. handler ──────────────────────────────────────────────────────────────
function call(handler, url = '/api/salary-projection') {
  return new Promise((resolve) => {
    const res = {
      statusCode: 200, headers: {}, setHeader(k, v) { this.headers[k.toLowerCase()] = v; },
      end(body) { resolve({ status: this.statusCode, headers: this.headers, body: body ? JSON.parse(body) : null }); },
    };
    Promise.resolve(handler({ method: 'GET', url }, res));
  });
}

async function testHandler(payload) {
  console.log('\nHANDLER: cached GET /api/salary-projection');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'salary-projection-test-'));
  const cacheFile = path.join(tmp, 'salary-cache.json');
  let t = NOW.getTime();
  let runs = 0;
  const h = sp.createSalaryProjectionHandler({ clock: () => t, cacheFile, compute: async () => { runs++; return { payload }; } });
  const first = await call(h);
  check(first.status === 202 && first.body.status === 'computing', 'first call: 202 computing');
  await h.idle();
  const second = await call(h);
  check(second.status === 200 && second.body.status === 'ready' && runs === 1 && second.body.blocks.length === 2, 'then 200 ready from one computation');
  const saved = JSON.parse(fs.readFileSync(cacheFile, 'utf-8'));
  check(saved.schemaVersion === sp.SCHEMA_VERSION && saved.entry.payload.basis && saved.entry.scenarioName === null, 'cache file written with the salary schema (no plan)');
  check(sp.DEFAULT_CACHE_FILE !== cp.DEFAULT_CACHE_FILE, 'its own cache file, next to the cash projection\'s');

  const old = JSON.parse(fs.readFileSync(cacheFile, 'utf-8'));
  old.schemaVersion = 1; old.entry.schemaVersion = 1;
  const oldFile = path.join(tmp, 'old-cache.json');
  fs.writeFileSync(oldFile, JSON.stringify(old));
  let oldRuns = 0;
  const hOld = sp.createSalaryProjectionHandler({ clock: () => t, cacheFile: oldFile, compute: async () => { oldRuns++; return { payload }; } });
  const fromOld = await call(hOld);
  await hOld.idle();
  check(fromOld.status === 202 && oldRuns === 1, 'a cache from the Snowflake version (schema 1) is ignored and recomputed');

  const prev = process.env.NET_CASH_SCENARIO_NAME;
  process.env.NET_CASH_SCENARIO_NAME = 'Another plan';
  const other = await call(h);
  if (prev === undefined) delete process.env.NET_CASH_SCENARIO_NAME; else process.env.NET_CASH_SCENARIO_NAME = prev;
  check(other.status === 200 && other.body.status === 'ready' && runs === 1, 'a different plan name does not invalidate the cache (the page has no plan)');

  const h2 = sp.createSalaryProjectionHandler({ clock: () => t, cacheFile, compute: async () => { runs++; return { payload }; } });
  const fromDisk = await call(h2);
  check(fromDisk.status === 200 && runs === 1, 'a new handler serves the cache file without recomputing');

  t += 31 * 60 * 1000;
  const stale = await call(h);
  check(stale.body.cache.stale && stale.body.cache.refreshing, 'after the TTL: served stale while it refreshes');
  await h.idle();
  check(runs === 2, 'one background recompute');
  const post = await new Promise((resolve) => {
    const res = { statusCode: 200, setHeader() {}, end() { resolve(this.statusCode); } };
    h({ method: 'POST', url: '/api/salary-projection' }, res);
  });
  check(post === 405, 'anything but GET → 405');
}

// ── 3. UI model ─────────────────────────────────────────────────────────────
async function testModel(payload) {
  const file = path.join(ROOT, 'src', 'salary-projection', 'model.ts');
  console.log('\nMODEL: table lines and KPIs (src/salary-projection/model.ts)');
  const model = await import(pathToFileURL(file).href);
  const table = model.buildSalaryTable(payload, 'ils', 'both');
  check(table.columns.length === 26, 'both years → 26 columns (12 months + FY, twice)');
  check(model.buildSalaryTable(payload, 'ils', 'current').columns.length === 13 && model.buildSalaryTable(payload, 'ils', 'next').columns[0].mKey === mk(T, 1),
    'one year → 13 columns');
  const keys = table.lines.map((l) => l.key);
  const expected = [
    ...sp.SALARY_GL_GROUPS[0].accounts.map(([a]) => a), 'total-permanent',
    ...sp.SALARY_GL_GROUPS[1].accounts.map(([a]) => a), 'total-temporary',
    '760040', 'total-other', 'total',
  ];
  check(JSON.stringify(keys) === JSON.stringify(expected), 'lines: permanent accounts + subtotal, temporary + subtotal, unclassified + subtotal, total', keys.join(','));
  check(table.lines[0].label === '760001 - Gross Salaries' && table.lines[0].kind === 'item' && table.lines.find((l) => l.key === 'total').kind === 'subtotal',
    'labels read "760001 - Gross Salaries"; totals are subtotals');
  const hint = (k) => table.lines.find((l) => l.key === k).hint;
  check(/NetSuite/.test(hint('760001')) && /September 2026 carried forward/.test(hint('760001')) && /same month of the previous year/.test(hint('760017')) && /not projected/.test(hint('760023')),
    'hints say how each account is projected (run-rate, same month last year, not projected)');

  const line = (k) => table.lines.find((l) => l.key === k);
  const groups = { permanent: [], temporary: [], other: [] };
  for (const a of payload.accounts) groups[a.group].push(a.acct);
  const off = [];
  table.columns.forEach((c, i) => {
    for (const [g, accts] of Object.entries(groups)) {
      const s = accts.reduce((x, a) => x + line(a).values[i], 0);
      if (!near(line(`total-${g}`).values[i], s, 0.05)) off.push(`${c.id} ${g}`);
    }
    const all = ['permanent', 'temporary', 'other'].reduce((x, g) => x + line(`total-${g}`).values[i], 0);
    if (!near(line('total').values[i], all, 0.05)) off.push(`${c.id} total`);
  });
  check(!off.length, `each subtotal = its accounts, total = the subtotals, in all ${table.columns.length} columns`, off.slice(0, 3).join('; '));
  const fyY = table.columns.findIndex((c) => c.id === `fy-${Y}`);
  const yRows = blockOf(payload, Y).rows;
  const monthTotal = (r, ccy) => Object.values(r[ccy]).reduce((s, v) => s + v, 0);
  check(near(line('total').values[fyY], yRows.reduce((s, r) => s + monthTotal(r, 'ils'), 0), 0.05), 'FY total = the sum of the months');
  const eurTable = model.buildSalaryTable(payload, 'eur', 'both');
  check(near(eurTable.lines.find((l) => l.key === '760001').values[2], rowOf(payload, Y, 3).eur['760001']), 'EUR shows the € figures');

  const kpis = model.computeSalaryKpis(payload, 'ils');
  const ytd = yRows.filter((r) => r.status === 'actual').reduce((s, r) => s + monthTotal(r, 'ils'), 0);
  const permSep = groups.permanent.reduce((s, a) => s + rowOf(payload, Y, 9).ils[a], 0);
  check(near(kpis.ytd.value, ytd, 0.05) && kpis.ytd.through === mk(Y, 9), 'KPI: payroll year to date = the actual months');
  check(kpis.byYear.length === 2 && near(kpis.byYear[1].value, blockOf(payload, T).rows.reduce((s, r) => s + monthTotal(r, 'ils'), 0), 0.05),
    'KPI: full-year payroll per year');
  check(near(kpis.runRate.value, permSep, 0.05) && kpis.runRate.mKey === mk(Y, 9), 'KPI: permanent run-rate = September\'s permanent payroll');
}

async function main() {
  console.log('=== salary-projection checks (synthetic data) ===');
  const payload = testPayload();
  testEdges();
  await testCompute();
  await testHandler(payload);
  await testModel(payload);
  if (process.argv.includes('--write-fixture')) {
    const file = path.join(__dirname, 'fixtures', 'salary-projection-sample.json');
    fs.writeFileSync(file, JSON.stringify({ ...payload, cache: { ageSec: 0, stale: false, staleReason: null, refreshing: false, lastError: null } }, null, 2) + '\n');
    console.log(`\nwrote ${path.relative(ROOT, file)}`);
  }
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
