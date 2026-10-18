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
const { Readable } = require('stream');
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
async function testPayload() {
  console.log('PAYLOAD: buildSalaryProjection (server/salary-projection.cjs)');
  const { payload } = await sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW });
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
  check(accountOf(payload, '760040').group === 'other' && accountOf(payload, '760040').defaultMethod === null && accountOf(payload, '760020').group === 'temporary',
    'an unclassified payroll account is shown, grouped "other" (no default); 760020 is temporary');
  check(accountOf(payload, '760001').defaultMethod === 'run-rate' && accountOf(payload, '760017').defaultMethod === 'last-year' && accountOf(payload, '760023').defaultMethod === 'none',
    'each account carries finance\'s default method');

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
  const temporaryAccts = payload.accounts.filter((a) => a.group !== 'permanent').map((a) => a.acct);
  const offNext = [];
  for (const acct of temporaryAccts) for (const m of range(1, 12)) if (ils(payload, T, m, acct) !== 0) offNext.push(`${acct} ${mk(T, m)}`);
  check(!offNext.length && temporaryAccts.length === 13,
    `${T}: every temporary (and not classified) account is 0 in every month (no bonus, recuperation or option exercise repeated)`, offNext.slice(0, 3).join('; '));
  check(rowOf(payload, T, 1).ils['760017'] === 0 && ils(payload, Y, 1, '760017') === 1_000_000 && ils(payload, Y, 12, '760017') === 150_000,
    `${Y} keeps its actual bonus and the repeated December; ${T} starts at 0`);

  const offNone = [];
  for (const acct of ['760023', '760029', '760008', '760014']) {
    for (const [y, m] of flat) if (ils(payload, y, m, acct) !== 0) offNone.push(`${acct} ${mk(y, m)}`);
  }
  check(ils(payload, Y, 3, '760023') === -30_000 && !offNone.length,
    '760023, 760029, 760008, 760014: actual months shown, every forecast month 0 (the 2025 refunds, notice pay and December accruals not repeated)', offNone.slice(0, 3).join('; '));
  check(!payload.warnings.some((w) => w.includes('760008') || w.includes('760014')), '760008 and 760014 are classified: no warning');
  check(ils(payload, Y, 11, '760040') === 0 && ils(payload, T, 3, '760040') === 0 && ils(payload, Y, 3, '760040') === 1_000, 'the unclassified account follows the temporary rule (0 next year)');

  check(near(payload.basis.ilsPerEur, 3.7, 0.0001), 'forecast rate: September\'s permanent payroll ₪/€', String(payload.basis.ilsPerEur));
  check(near(rowOf(payload, Y, 11).eur['760001'], sep('760001') / 3.7) && near(rowOf(payload, Y, 10).eur['760017'], 200_000 / 3.7),
    'forecast € = forecast ₪ ÷ that rate (also for amounts repeated from the previous year)');
  check(payload.status === 'ready' && payload.company === cp.COMPANY && payload.years[0] === Y && payload.years[1] === T,
    'payload envelope: ready, company, years');
  return payload;
}

async function testEdges() {
  console.log('\nEDGES: guard, January, no previous year, no data');
  const lag = (await sp.buildSalaryProjection({ rows: syntheticRows({ lagSeptember: true }), now: NOW })).payload;
  check(lag.basis.runRateMonth === mk(Y, 8) && lag.actuals.through === mk(Y, 8), 'a token September (not posted yet) → August is the last closed month');
  check(rowOf(lag, Y, 9).status === 'forecast' && ils(lag, Y, 9, '760001') === ils(lag, Y, 8, '760001'), 'September is then projected from August');
  check(lag.warnings.some((w) => w.includes('September 2026') && w.includes('August 2026')), 'with a warning naming both months');

  const jan = (await sp.buildSalaryProjection({ rows: syntheticRows(), now: new Date('2026-01-20T12:00:00') })).payload;
  const janRows = blockOf(jan, Y).rows;
  check(janRows[0].status === 'current' && janRows.slice(1).every((r) => r.status === 'forecast') && jan.actuals.through === null,
    'in January: no actual months, January projected');
  check(jan.basis.runRateMonth === mk(Y - 1, 12) && ils(jan, Y, 1, '760001') === 5_012_000 && ils(jan, Y, 1, '760017') === 900_000,
    'in January: permanent from December of the previous year, temporary from January of the previous year');

  const noPrev = (await sp.buildSalaryProjection({ rows: syntheticRows({ withPrevYear: false }), now: NOW })).payload;
  check(noPrev.warnings.some((w) => w.includes(`no ${Y - 1} payroll`)) && ils(noPrev, Y, 12, '760017') === 0,
    `no ${Y - 1} rows: a warning, and temporary accounts project 0 in ${Y}`);

  let threw = null;
  try { await sp.buildSalaryProjection({ rows: [], now: NOW }); } catch (e) { threw = e; }
  check(threw instanceof cp.ProjectionError, 'no payroll at all → a ProjectionError (its message is shown)');
}

// ── 2. classification ───────────────────────────────────────────────────────
function callJson(handler, { method = 'GET', url = '/api/salary-projection/classification', body, headers = {} } = {}) {
  return new Promise((resolve) => {
    const raw = body === undefined ? '' : JSON.stringify(body);
    const req = Readable.from(raw ? [Buffer.from(raw)] : []);
    Object.assign(req, { method, url, headers: { host: 'finance.example', ...headers } });
    const res = {
      statusCode: 200, headers: {},
      setHeader(k, v) { this.headers[k.toLowerCase()] = v; },
      end(b) { resolve({ status: this.statusCode, body: b ? JSON.parse(b) : null }); },
    };
    handler(req, res);
  });
}

async function testClassification(payload) {
  console.log('\nCLASSIFICATION: saved choices, the page re-projecting, GET/PUT /api/salary-projection/classification');
  const months = Object.keys(payload.actual);
  check(months.includes(mk(Y - 1, 12)) && months.includes(mk(Y, 9)) && !months.includes(mk(Y, 10)) && payload.actual[mk(Y, 1)]['760017'].ils === 1_000_000,
    'the payload carries the actual months (the previous year, this year to September; never the current month)');

  // Saved choices change the projection: 760017 actuals only, 760002 temporary, 760040 permanent.
  const saved = { '760017': 'none', '760002': 'last-year', '760040': 'run-rate' };
  const withSaved = (await sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW, saved })).payload;
  check(accountOf(withSaved, '760017').method === 'none' && ils(withSaved, Y, 12, '760017') === 0 && ils(withSaved, T, 1, '760017') === 0 && ils(withSaved, Y, 1, '760017') === 1_000_000,
    'saved "actuals only" for 760017: the January bonus stays, no bonus in forecast months');
  check(accountOf(withSaved, '760002').group === 'temporary' && ils(withSaved, Y, 11, '760002') === 501_100,
    'saved "temporary" for 760002 (default permanent): November repeats November of last year');
  check(accountOf(withSaved, '760040').group === 'permanent' && ils(withSaved, T, 3, '760040') === 0,
    'saved "permanent" for the unclassified 760040: it moves to the permanent group and carries September (0)');
  check(withSaved.basis.runRateMonth === payload.basis.runRateMonth && withSaved.basis.ilsPerEur === payload.basis.ilsPerEur,
    'reclassifying never moves the run-rate month or the rate');

  // The page applies the same choices to the payload and must show exactly what the server computes.
  const model = await import(pathToFileURL(path.join(ROOT, 'src', 'salary-projection', 'model.ts')).href);
  const onPage = model.withClassification(payload, saved);
  check(JSON.stringify(onPage.blocks) === JSON.stringify(withSaved.blocks) && JSON.stringify(onPage.accounts) === JSON.stringify(withSaved.accounts),
    'the page, re-projecting with the same choices, equals the server\'s projection');
  check(JSON.stringify(model.withClassification(payload, {}).blocks) === JSON.stringify(payload.blocks), 'no choices: the page shows the server\'s projection unchanged');
  const older = { ...payload, actual: undefined };
  check(model.withClassification(older, saved) === older, 'an older payload without the actual months is shown as it is');

  const v = sp.validateClassification;
  check(v({ value: { methods: { '760001': 'last-year', '76003': 'none' } } }).ok, 'a valid classification');
  const bad = [{ value: { methods: { '640001': 'none' } } }, { value: { methods: { '760001': 'sometimes' } } }, { value: { methods: [] } }, { value: { methods: {}, extra: 1 } }, { methods: {} }];
  check(bad.every((b) => !v(b).ok), `${bad.length} kinds of bad classification are refused (not payroll, unknown method, not an object, unknown field, no value)`);

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'salary-classification-test-'));
  const file = path.join(tmp, 'salary-classification.json');
  const h = sp.createSalaryClassificationHandler({ file, clock: () => NOW.getTime() });
  let r = await callJson(h);
  check(r.status === 200 && r.body.ok && JSON.stringify(r.body.value) === '{"methods":{}}' && r.body.updatedAt === null, 'GET before any save: no choices');
  const json = { 'content-type': 'application/json' };
  r = await callJson(h, { method: 'PUT', body: { value: { methods: saved } }, headers: json });
  const r2 = await callJson(h, { method: 'PUT', body: { value: { methods: { '760001': 'x' } } }, headers: json });
  const r3 = await callJson(h, { method: 'PUT', body: { value: { methods: {} } }, headers: { ...json, origin: 'https://evil.example' } });
  const r4 = await callJson(h, { method: 'PUT', body: { value: { methods: {} } } });
  check(r.status === 200 && r.body.value.methods['760017'] === 'none' && r2.status === 400 && r3.status === 403 && r4.status === 415,
    'PUT: saved; a bad method 400; another site 403; not JSON 415');
  check(JSON.stringify(sp.readSavedMethods(file)) === JSON.stringify(saved) && fs.existsSync(file.replace(/\.json$/, '-history.jsonl')),
    'the saved choices read back, with a history line');

  const prevId = process.env.NETSUITE_ACCOUNT_ID;
  process.env.NETSUITE_ACCOUNT_ID = 'TEST';
  try {
    const ns = { fetchPnlActuals: async () => syntheticPnlActuals() };
    const computed = (await sp.computeSalaryProjection({ now: NOW, getNsClient: () => ns, classificationFile: file })).payload;
    check(accountOf(computed, '760017').method === 'none' && ils(computed, T, 1, '760017') === 0, 'the server\'s projection uses the saved classification');
  } finally {
    if (prevId === undefined) delete process.env.NETSUITE_ACCOUNT_ID; else process.env.NETSUITE_ACCOUNT_ID = prevId;
  }
  fs.rmSync(tmp, { recursive: true, force: true });
}

// ── the override file's payroll rows ───────────────────────────────────────
async function testOverrides(payload) {
  console.log('\nOVERRIDES: the override file\'s payroll rows under the table (Snowflake)');
  const prevId = process.env.NETSUITE_ACCOUNT_ID;
  process.env.NETSUITE_ACCOUNT_ID = 'TEST';
  try {
    const ns = { fetchPnlActuals: async () => syntheticPnlActuals() };
    let asked = null;
    let askedAccounts = null;
    let askedChanges = null;
    const sf = {
      fetchPayrollOverrides: async (from, accounts) => {
        asked = from;
        askedAccounts = accounts;
        return [
          { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', department: 'R&D', mode: 'Increment', eur: 20_000, ils: 0, memo: 'Two hires' },
          { month: mk(Y - 1, 12), acct: '760001', name: 'Gross Salaries', department: 'R&D', mode: 'Increment', eur: 5_000, ils: 18_500, memo: 'last year' },
          { month: mk(Y, 10), acct: '760017', name: 'Bonus', department: '', mode: 'Override', eur: 100_000, ils: 368_000, memo: '' },
        ];
      },
      fetchPayrollChanges: async (from, accounts) => {
        askedChanges = { from, accounts };
        return [
          { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', source: 'Terminated', actual: true, eur: 0, ils: -150_000 },
          { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', source: 'Open position', actual: true, eur: 20_000, ils: 74_000 },
          { month: mk(Y, 10), acct: '760001', name: 'Gross Salaries', source: 'Terminated', actual: false, eur: -30_000, ils: -110_000 },
          { month: mk(Y - 1, 12), acct: '760001', name: 'Gross Salaries', source: 'Leave end', actual: true, eur: 5_000, ils: 18_500 },
          { month: mk(Y, 12), acct: '760001', name: 'Gross Salaries', source: 'payroll future baseline', eur: 1_000_000, ils: 3_700_000 },
        ];
      },
    };
    const run = (getSfClient) => quiet(() => sp.computeSalaryProjection({ now: NOW, getNsClient: () => ns, getSfClient }));
    const { payload: p } = await run(() => sf);
    const rows = p.overrides.rows;
    check(asked === Y && p.overrides.error === null && rows.length === 2 && rows[0].month === mk(Y, 10) && rows[1].month === mk(Y, 11),
      `read from January ${Y}; earlier rows dropped; sorted by month`);
    check(askedAccounts === undefined && !('accounts' in p.overrides),
      'every payroll account\'s rows are read (as the old dashboard), not an account list');
    check(rows[0].mode === 'Override' && rows[0].ils === 368_000 && !rows[0].ilsEstimated
      && rows[1].mode === 'Increment' && near(rows[1].ils, 20_000 * p.basis.ilsPerEur) && rows[1].ilsEstimated && rows[1].memo === 'Two hires',
      'type, amounts and comment kept; ₪ at the forecast rate when the file has € only');
    check(JSON.stringify(p.blocks) === JSON.stringify(payload.blocks), 'the account projection (blocks) is unchanged: the page adds the planned changes');

    const ch = p.changes.rows;
    check(askedChanges && askedChanges.from === Y && askedChanges.accounts === undefined && p.changes.error === null,
      `planned hires and leavers: read for every payroll account from January ${Y}`);
    check(JSON.stringify(ch.map((r) => `${r.month} ${r.source}`)) === JSON.stringify([`${mk(Y, 10)} Terminated`, `${mk(Y, 11)} Open position`, `${mk(Y, 11)} Terminated`]),
      'earlier months and other sources (the baseline) dropped; sorted by month, then hires, leavers, returns');
    check(near(ch[2].eur, -150_000 / p.basis.ilsPerEur) && ch[2].ils === -150_000 && ch[1].eur === 20_000 && ch[1].ils === 74_000,
      'amounts kept; the missing currency at the forecast rate');
    check(ch[0].actual === false && ch[1].actual === true && ch[2].actual === true, 'each row keeps its TRUE / FALSE mark (IS_SOURCE_ACTUAL)');
    check(!p.warnings.some((w) => /hires and leavers/.test(w)), 'read fine: no warning');

    const failed = (await run(() => ({
      fetchPayrollOverrides: async () => { throw new Error('secret detail'); },
      fetchPayrollChanges: async () => { throw new Error('secret detail'); },
    }))).payload;
    check(failed.overrides.rows.length === 0 && /could not be read/.test(failed.overrides.error) && !failed.overrides.error.includes('secret') && failed.status === 'ready',
      'a Snowflake failure: the page still loads, with a note instead of the rows');
    check(failed.changes.rows.length === 0 && failed.warnings.some((w) => /hires and leavers could not be read/.test(w)) && !failed.warnings.join(' ').includes('secret'),
      'planned changes failing: the page still loads, with a warning that Total payroll leaves them out');
    const none = (await run(() => null)).payload;
    check(none.overrides.rows.length === 0 && /not configured/.test(none.overrides.error) && none.warnings.some((w) => /not configured/.test(w)),
      'no Snowflake on the server: a note and a warning');
  } finally {
    if (prevId === undefined) delete process.env.NETSUITE_ACCOUNT_ID; else process.env.NETSUITE_ACCOUNT_ID = prevId;
  }
  check(/not read/.test(payload.overrides.error) && payload.overrides.rows.length === 0 && payload.changes.rows.length === 0
    && !payload.warnings.some((w) => /hires and leavers/.test(w)), 'built without reading Snowflake: says so, with no warning');
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
    const direct = (await sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW })).payload;
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

// ── 3. handler ──────────────────────────────────────────────────────────────
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

// ── 4. UI model ─────────────────────────────────────────────────────────────
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
    'header-permanent', ...sp.SALARY_GL_GROUPS[0].accounts.map(([a]) => a), 'total-permanent',
    'header-temporary', ...sp.SALARY_GL_GROUPS[1].accounts.map(([a]) => a), 'total-temporary',
    'header-other', '760040', 'total-other', 'total',
  ];
  check(JSON.stringify(keys) === JSON.stringify(expected), 'lines: each group\'s header, accounts and subtotal (permanent, temporary, unclassified), then the total', keys.join(','));
  const acct1 = table.lines.find((l) => l.key === '760001');
  check(acct1.label === '760001 - Gross Salaries' && acct1.kind === 'item' && table.lines.find((l) => l.key === 'total').kind === 'subtotal',
    'labels read "760001 - Gross Salaries"; totals are subtotals');
  const headers = table.lines.filter((l) => l.kind === 'header');
  check(JSON.stringify(headers.map((l) => l.label)) === JSON.stringify(['Permanent', 'Temporary', 'Not classified']) && headers.every((l) => l.values.every((v) => v === 0))
    && /September 2026 carried into every month/.test(headers[0].hint) && /rest of 2026, 0 in 2027/.test(headers[1].hint),
    'section headers: Permanent, Temporary, Not classified; no amounts; hints say how each group is projected');
  const hint = (k) => table.lines.find((l) => l.key === k).hint;
  check(/NetSuite/.test(hint('760001')) && /September 2026 carried forward/.test(hint('760001')) && /same month of the previous year for the rest of 2026; 0 in 2027/.test(hint('760017')) && /not projected/.test(hint('760023')),
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

  console.log('\nMODEL: planned changes between permanent and temporary; hires, leavers and increments in Total payroll');
  const rate = payload.basis.ilsPerEur;
  const overrides = sp.overridesOf({ error: null, rows: [
    { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', department: 'R&D', mode: 'Increment', eur: 20_000, ils: 0, memo: 'Two hires' },
    { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', department: 'Sales', mode: 'Increment', eur: 10_000, ils: 37_000, memo: 'One hire' },
    { month: mk(Y, 10), acct: '760001', name: 'Gross Salaries', department: 'R&D', mode: 'Increment', eur: 500, ils: 1_850, memo: '' },
    { month: mk(Y, 3), acct: '760001', name: 'Gross Salaries', department: 'R&D', mode: 'Increment', eur: 2_000, ils: 7_400, memo: 'Closed month' },
    { month: mk(Y, 12), acct: '760001', name: 'Gross Salaries', department: 'Sales', mode: 'Override', eur: 50_000, ils: 185_000, memo: '' },
    { month: mk(T, 2), acct: '760002', name: 'National Insurance expenses', department: 'R&D', mode: 'Increment', eur: 1_300, ils: 5_000, memo: '' },
    { month: mk(T + 1, 1), acct: '760001', name: 'Gross Salaries', department: 'R&D', mode: 'Override', eur: 9_000, ils: 33_000, memo: '' },
  ] }, Y, rate);
  const gross = { acct: '760001', name: 'Gross Salaries' };
  const changes = sp.changesOf({ error: null, rows: [
    { ...gross, month: mk(Y, 10), source: 'Terminated', department: 'R&D', actual: true, eur: -22_000, ils: -80_000 },
    { ...gross, month: mk(Y, 10), source: 'Terminated', department: 'Sales', actual: true, eur: -8_000, ils: -30_000 },
    { acct: '760002', name: 'National Insurance expenses', month: mk(Y, 10), source: 'Terminated', department: 'R&D', actual: true, eur: -2_000, ils: -7_000 },
    { ...gross, month: mk(Y, 12), source: 'Open position', department: 'Sales', actual: true, eur: 23_000, ils: 85_000 },
    { ...gross, month: mk(Y, 12), source: 'Leave end', department: 'HR', actual: true, eur: 5_000, ils: 19_000 },
    { ...gross, month: mk(T, 1), source: 'Open position', department: 'Sales', actual: true, eur: 23_000, ils: 86_000 },
    { ...gross, month: mk(T, 1), source: 'Terminated', department: 'Finance', actual: true, eur: -87_000, ils: -323_000 },
    { ...gross, month: mk(Y, 2), source: 'Terminated', department: 'R&D', actual: true, eur: -10_000, ils: -37_000 },
    { ...gross, month: mk(Y, 11), source: 'Terminated', department: 'R&D', actual: false, eur: -50_000, ils: -185_000 },
  ] }, Y, rate);
  const withOv = { ...payload, overrides, changes };

  const sm = await sp.loadSalaryModule();
  const counted = sm.countedIncrementRows(payload.blocks, overrides.rows);
  check(JSON.stringify(counted.map((r) => r.month)) === JSON.stringify([mk(Y, 10), mk(Y, 11), mk(Y, 11), mk(T, 2)]) && counted[0] === overrides.rows.find((r) => r.month === mk(Y, 10)),
    'countedIncrementRows: increments of the current and forecast months (the same row objects); not closed months, overrides, or months after the page');
  const countedCh = sm.countedChangeRows(payload.blocks, changes.rows);
  check(countedCh.length === 7 && !countedCh.some((r) => r.month === mk(Y, 2)) && countedCh[0] === changes.rows.find((r) => r.month === mk(Y, 10)),
    'countedChangeRows: hires, leavers and returns of the current and forecast months; not closed months');
  check(changes.rows.some((r) => r.month === mk(Y, 11) && r.actual === false) && countedCh.every((r) => r.actual === true)
    && sm.countedChangeRows(payload.blocks, [{ month: mk(Y, 11), actual: false }, { month: mk(Y, 11) }]).length === 0,
    'a change marked FALSE (or without a mark) never counts, even in a forecast month');

  const ovTable = model.buildSalaryTable(withOv, 'ils', 'both');
  const ovKeys = ovTable.lines.map((l) => l.key);
  const ovExpected = [
    'header-permanent', ...sp.SALARY_GL_GROUPS[0].accounts.map(([a]) => a), 'total-permanent',
    'header-changes', 'hires', 'leavers', 'returns', 'total-changes',
    'header-overrides', 'increment|760001', 'increment|760002', 'total-increments', 'override|760001', 'total-overrides',
    'header-temporary', ...sp.SALARY_GL_GROUPS[1].accounts.map(([a]) => a), 'total-temporary',
    'header-other', '760040', 'total-other', 'total',
  ];
  check(JSON.stringify(ovKeys) === JSON.stringify(ovExpected),
    'between Total permanent and the temporary accounts: hires, leavers, returns + Total headcount changes, then increments + Total increments, then overrides + Total overrides', ovKeys.join(','));
  const ovLine = (k, t = ovTable) => t.lines.find((l) => l.key === k);
  const col = (id, t = ovTable) => t.columns.findIndex((c) => c.id === id);
  check(ovLine('hires').label === 'Planned hires' && ovLine('leavers').label === 'Leavers' && ovLine('returns').label === 'Returns from leave'
    && ovLine('total-changes').label === 'Total headcount changes' && ovLine('hires').kind === 'item' && ovLine('total-changes').kind === 'subtotal'
    && /Snowflake/.test(ovLine('leavers').hint) && /Added to Total payroll/.test(ovLine('leavers').hint)
    && /Accounts: 760001 - Gross Salaries; 760002 - National Insurance expenses/.test(ovLine('leavers').hint),
    'one line per type (planned hires, leavers, returns from leave) with a subtotal; hints name the accounts and say they are in Total payroll');
  const lv = ovLine('leavers').values;
  check(near(lv[col(mk(Y, 10))], -117_000, 0.05) && lv[col(mk(Y, 2))] === 0 && near(lv[col(mk(T, 1))], -323_000, 0.05) && near(ovLine('total-changes').values[col(mk(Y, 12))], 104_000, 0.05),
    'planned changes summed across the payroll accounts in their months (leavers negative); 0 in closed months');
  check(lv[col(mk(Y, 11))] === 0, 'November: its only leaver row is marked FALSE, so 0');
  check(ovLine('increment|760001').kind === 'item' && ovLine('total-increments').kind === 'subtotal' && ovLine('override|760001').kind === 'note'
    && ovLine('total-overrides').kind === 'subtotal' && /not in total payroll/.test(ovLine('total-overrides').label)
    && ovLine('increment|760001').label === '760001 - Gross Salaries (increment)' && ovLine('override|760001').label === '760001 - Gross Salaries (override)',
    'increments read as lines, overrides as italic notes; Total overrides says it is not in total payroll');
  const inc1 = ovLine('increment|760001').values;
  check(near(inc1[col(mk(Y, 11))], 20_000 * rate + 37_000, 0.05) && near(inc1[col(mk(Y, 10))], 1_850, 0.05) && inc1[col(mk(Y, 3))] === 0 && inc1[col(mk(Y, 9))] === 0,
    'increments: summed across departments in the current and forecast months (€-only rows at the forecast rate); 0 in closed months');
  const ovHint = ovLine('increment|760001').hint;
  check(/Departments: R&D; Sales/.test(ovHint) && /Comments: Two hires; One hire\./.test(ovHint) && !/Closed month/.test(ovHint) && /added to Total payroll/.test(ovHint)
    && /replaces the budget/.test(ovLine('override|760001').hint) && /not in Total payroll/.test(ovLine('override|760001').hint),
    'increment and override hints: type, departments and comments of the rows shown');

  const base = (k) => table.lines.find((l) => l.key === k).values;
  const changed = table.lines.filter((l) => l.key !== 'total' && JSON.stringify(ovLine(l.key).values) !== JSON.stringify(l.values)).map((l) => l.key);
  check(!changed.length, 'every account and group subtotal is unchanged', changed.join(','));
  const totalOff = [];
  const subOff = [];
  ovTable.columns.forEach((c, i) => {
    const v = (k) => ovLine(k).values[i];
    if (!near(v('total'), base('total')[i] + v('total-changes') + v('total-increments'), 0.05)) totalOff.push(c.id);
    if (!near(v('total-changes'), v('hires') + v('leavers') + v('returns'), 0.05)) subOff.push(`${c.id} changes`);
    if (!near(v('total-increments'), v('increment|760001') + v('increment|760002'), 0.05)) subOff.push(`${c.id} increments`);
    if (!near(v('total-overrides'), v('override|760001'), 0.05)) subOff.push(`${c.id} overrides`);
  });
  check(!totalOff.length, 'Total payroll = the accounts + Total headcount changes + Total increments, in every column (overrides not in it)', totalOff.slice(0, 3).join('; '));
  check(!subOff.length, 'each subtotal = its lines, in every column', subOff.slice(0, 3).join('; '));
  const incY = 20_000 * rate + 37_000 + 1_850;
  const chY = -110_000 - 7_000 + 85_000 + 19_000;
  const chT = 86_000 - 323_000;
  check(near(ovLine('total-increments').values[col(`fy-${Y}`)], incY, 0.05) && near(ovLine('total-increments').values[col(`fy-${T}`)], 5_000, 0.05)
    && near(ovLine('total-changes').values[col(`fy-${Y}`)], chY, 0.05) && near(ovLine('total-changes').values[col(`fy-${T}`)], chT, 0.05),
    'FY columns sum each year\'s months');
  check(near(ovLine('override|760001').values[col(mk(Y, 12))], 185_000, 0.05) && ovLine('override|760001').values[col(`fy-${T}`)] === 0,
    `overrides show in their month; a row after December ${T} is left out (the rows list counts it)`);
  const ovEur = model.buildSalaryTable(withOv, 'eur', 'current');
  check(ovEur.columns.length === 13 && near(ovLine('increment|760001', ovEur).values[col(mk(Y, 11), ovEur)], 30_000, 0.05)
    && near(ovLine('leavers', ovEur).values[col(mk(Y, 10), ovEur)], -32_000, 0.05),
    'follows the currency (€ from Snowflake and the file) and the year view');

  const ovKpis = model.computeSalaryKpis(withOv, 'ils');
  check(near(ovKpis.byYear[0].value, kpis.byYear[0].value + incY + chY, 0.05) && near(ovKpis.byYear[0].changes, incY + chY, 0.05)
    && near(ovKpis.byYear[1].value, kpis.byYear[1].value + 5_000 + chT, 0.05) && near(ovKpis.byYear[1].changes, 5_000 + chT, 0.05),
    'KPIs: full-year payroll adds the planned changes');
  check(near(ovKpis.byYear[0].value, ovLine('total').values[col(`fy-${Y}`)], 0.05) && near(ovKpis.byYear[1].value, ovLine('total').values[col(`fy-${T}`)], 0.05),
    'KPI full-year payroll = the table\'s FY Total payroll');
  check(ovKpis.ytd.value === kpis.ytd.value && ovKpis.runRate.value === kpis.runRate.value && kpis.byYear[0].changes === 0,
    'KPIs: year to date and the run-rate are the accounts\' own (planned changes never count in closed months)');

  const allTemporary = model.withClassification(withOv, Object.fromEntries(payload.accounts.map((a) => [a.acct, 'last-year'])));
  const firstKeys = model.buildSalaryTable(allTemporary, 'ils', 'both').lines.slice(0, 2).map((l) => l.key);
  check(JSON.stringify(firstKeys) === JSON.stringify(['header-changes', 'hires']), 'no permanent account: the planned changes come first, under their header');
  check(ovLine('header-changes').label === 'Headcount changes' && ovLine('header-overrides').label === 'Override file' && ovLine('header-changes').kind === 'header'
    && ovLine('header-overrides').values.every((v) => v === 0) && /Snowflake/.test(ovLine('header-changes').hint),
    'planned changes: a "Headcount changes" and an "Override file" header, no amounts');
  const onlyChanges = model.buildSalaryTable({ ...payload, changes }, 'ils', 'both').lines.map((l) => l.key);
  check(onlyChanges.includes('total-changes') && !onlyChanges.some((k) => /increment|override/.test(k)), 'planned changes without the override file');
  const accentOf = (k) => ovLine(k).accent;
  const offAccent = ovTable.lines.filter((l) => {
    const want = /^(header-permanent|total-permanent)$/.test(l.key) || sp.SALARY_GL_GROUPS[0].accounts.some(([a]) => a === l.key) ? 'sky'
      : /^(header-changes|hires|leavers|returns|total-changes)$/.test(l.key) ? 'amber'
        : /^(header-overrides|total-increments|total-overrides)$|^(increment|override)\|/.test(l.key) ? 'violet'
          : /^(header-temporary|total-temporary)$/.test(l.key) || sp.SALARY_GL_GROUPS[1].accounts.some(([a]) => a === l.key) ? 'emerald'
            : /^(header-other|total-other|760040)$/.test(l.key) ? 'slate' : undefined;
    return l.accent !== want;
  }).map((l) => l.key);
  check(!offAccent.length && accentOf('total') === undefined && JSON.stringify([...model.SECTION_ORDER].sort()) === JSON.stringify(Object.keys(model.SECTION_NAMES).sort())
    && model.SECTION_NAMES.sky === 'Permanent' && model.SECTION_NAMES.amber === 'Headcount changes' && model.SECTION_NAMES.emerald === 'Temporary' && model.SECTION_NAMES.pink === 'Deel',
    'colours: Permanent blue, headcount changes amber, the override file violet, Temporary green, not classified grey; Total payroll none', offAccent.join(','));
  const plain = (p) => JSON.stringify(model.buildSalaryTable(p, 'ils', 'both').lines.map((l) => l.key)) === JSON.stringify(expected);
  check(plain({ ...payload, overrides: { error: null, rows: [] }, changes: { error: null, rows: [] } }) && plain({ ...payload, overrides: undefined, changes: undefined })
    && plain({ ...payload, overrides: { error: null, rows: overrides.rows.filter((r) => r.month > mk(T, 12)) } })
    && plain({ ...payload, overrides: { error: null, rows: overrides.rows.filter((r) => r.memo === 'Closed month') }, changes: { error: null, rows: changes.rows.filter((r) => r.month === mk(Y, 2)) } }),
    'no rows, rows after the page\'s years only, or closed months only: no planned-change lines');

  console.log('\nBREAKDOWN: clicking a planned change (src/salary-projection/breakdown.ts)');
  const bd = await import(pathToFileURL(path.join(ROOT, 'src', 'salary-projection', 'breakdown.ts')).href);
  const ask = (line, period, more = {}) => bd.salaryBreakdown(withOv, { line, period, variant: 'base', ccy: 'ils', ...more });
  check(['hires', 'leavers', 'returns', 'total-changes', 'increment|760001', 'override|760001', 'total-increments', 'total-overrides'].every(bd.isSalaryBreakdownLine)
    && !['760001', 'total', 'total-permanent', 'header-changes'].some(bd.isSalaryBreakdownLine),
    'the planned changes and their totals open a breakdown; accounts, headers and Total payroll do not');
  const bdOff = [];
  ovTable.lines.filter((l) => bd.isSalaryBreakdownLine(l.key)).forEach((l) => ovTable.columns.forEach((c, i) => {
    if (Math.abs(l.values[i]) < 1) return;
    const r = ask(l.key, c.kind === 'fy' ? `FY-${c.year}` : c.mKey);
    if (r.status !== 'ready' || !near(r.cell, l.values[i], 0.05) || !near(r.sections[0].total, r.cell, 0.05)) bdOff.push(`${l.key} ${c.id}`);
  }));
  check(!bdOff.length, 'every clickable cell: the breakdown\'s cell = the table cell, and its first section adds up to it', bdOff.slice(0, 3).join('; '));

  const lvOct = ask('leavers', mk(Y, 10));
  const sec = (r, id) => r.sections.find((x) => x.id === id);
  check(lvOct.lineLabel === 'Leavers' && lvOct.periodStatus === 'current' && lvOct.periodLabel === 'October 2026' && lvOct.cell === -117_000 && lvOct.variant === undefined
    && JSON.stringify(sec(lvOct, 'accounts').rows.map((r) => [r.ref, r.amount, !!r.drillable])) === JSON.stringify([['760001', -110_000, true], ['760002', -7_000, true]])
    && JSON.stringify(sec(lvOct, 'departments').rows.map((r) => [r.label, r.amount])) === JSON.stringify([['R&D', -87_000], ['Sales', -30_000]])
    && sec(lvOct, 'departments').total === -117_000,
    'leavers, October: by account (each opens by department), and by department; both add up to the cell; no plan label');
  const lvDrill = ask('leavers', mk(Y, 10), { row: '760001' });
  check(lvDrill.account && lvDrill.account.acct === '760001' && lvDrill.cell === -110_000 && /Leavers · October 2026/.test(lvDrill.account.of)
    && JSON.stringify(lvDrill.sections[0].rows.map((r) => [r.label, r.amount])) === JSON.stringify([['R&D', -80_000], ['Sales', -30_000]]),
    'an account by department: the second window adds up to the account row');
  const chFy = ask('total-changes', `FY-${Y}`);
  check(chFy.cell === -13_000 && JSON.stringify([...new Set(chFy.sections[0].rows.map((r) => r.group))]) === JSON.stringify(['Planned hires', 'Leavers', 'Returns from leave'])
    && chFy.sections[0].rows.some((r) => r.key === 'leavers|760001' && r.amount === -110_000),
    'Total headcount changes for the year: by type and account (closed months left out, as in the table)');
  const chDrill = ask('total-changes', `FY-${Y}`, { row: 'leavers|760001' });
  check(chDrill.cell === -110_000 && chDrill.sections[0].rows.length === 2, 'and each of its accounts opens by department');
  const incNov = ask('increment|760001', mk(Y, 11));
  const incRows = incNov.sections[0].rows;
  check(near(incNov.cell, 20_000 * rate + 37_000, 0.05) && incNov.lineLabel === '760001 - Gross Salaries (increment)'
    && incRows.find((r) => r.label === 'R&D').hint === 'Two hires' && incRows.find((r) => r.label === 'Sales').hint === 'One hire' && incNov.notes.some((n) => /forecast rate/.test(n)),
    'an override-file line: by department, with the comments from the file');
  const incFy = ask('increment|760001', `FY-${Y}`);
  check(incFy.sections.some((x) => x.id === 'months') && near(sec(incFy, 'months').total, incFy.cell, 0.05), 'a year: also by month');
  const ovTot = ask('total-overrides', mk(Y, 12));
  check(ovTot.cell === 185_000 && ovTot.sections[0].rows[0].group === '760001 - Gross Salaries' && /not in Total payroll/.test(ovTot.notes[0]),
    'Total overrides: by account and department, marked as not in Total payroll');
  check(ask('leavers', mk(Y, 10), { ccy: 'eur' }).cell === -32_000 && ask('leavers', '2031-01').status === 'error' && ask('760001', mk(Y, 10)).status === 'error',
    'follows the currency; unknown periods and lines answer with an error');
}

// ── 5. Deel and the headcount (HiBob) ───────────────────────────────────────
// Deel: two vendors on 660004, no bill in September (booked late), one posting on a payroll account.
const DEEL = {
  vendors: [{ id: 11, name: 'Deel Inc. (synthetic)' }, { id: 12, name: 'Deel Ltd (synthetic)' }],
  rows: [
    { month: mk(Y - 2, 12), acct: '660004', name: 'Contractors', vendor: 11, eur: 1_000, ils: 4_000 },
    { month: mk(Y - 1, 12), acct: '660004', name: 'Contractors', vendor: 11, eur: 50_000, ils: 200_000 },
    ...range(1, 8).map((m) => ({ month: mk(Y, m), acct: '660004', name: 'Contractors', vendor: 11, eur: 60_000, ils: 222_000 })),
    { month: mk(Y, 8), acct: '660004', name: 'Contractors', vendor: 12, eur: 10_000, ils: 37_000 },
    { month: mk(Y, 8), acct: '760001', name: 'Gross Salaries', vendor: 11, eur: 2_000, ils: 7_400 },
  ],
};
// HiBob: employees with their dates (no names), and planned events by month, type, sub-type, employee and status.
const SPANS = [
  { start: '2019-01-01', end: null, type: 'Full-time' },
  { start: '2025-11-15', end: null, type: 'Full-time' },
  { start: '2024-02-01', end: mk(Y, 3) + '-31', type: 'Full-time' }, // employed at the end of March, gone in April
  { start: mk(Y, 5) + '-10', end: null, type: 'Student' },
  { start: '2023-01-01', end: mk(Y, 10) + '-05', type: 'Full-time' }, // left in the current month
  { start: mk(Y, 10) + '-01', end: null, type: 'Full-time' }, // joined in the current month
  { start: mk(Y, 12) + '-01', end: null, type: 'Full-time' }, // signed, starts in December
  { start: '2022-01-01', end: mk(T, 1) + '-31', type: 'Full-time' }, // leaving at the end of January next year
];
const EVENTS = [
  { month: mk(Y, 4), type: 'decrease', subType: 'Terminated', employee: true, status: '', count: 1 },
  { month: mk(Y, 6), type: 'increase', subType: 'Open position', employee: false, status: 'Open', count: 1 },
  { month: mk(Y, 6), type: 'increase', subType: 'Open position', employee: false, status: 'Closed', count: 2 },
  { month: mk(Y, 8), type: 'increase', subType: 'Leave end', employee: true, status: '', count: 1 },
  { month: mk(Y, 10), type: 'decrease', subType: 'Terminated', employee: true, status: '', count: 1 },
  { month: mk(Y, 11), type: 'increase', subType: 'Open position', employee: false, status: 'Approved', count: 2 },
  { month: mk(Y, 12), type: 'increase', subType: 'Hired', employee: true, status: 'Filled', count: 1 },
  { month: mk(Y, 12), type: 'increase', subType: 'Leave end', employee: true, status: '', count: 1 },
  { month: mk(T, 2), type: 'decrease', subType: 'Leave start', employee: true, status: '', count: 1 },
  { month: mk(T, 3), type: 'decrease', subType: 'Position cut', employee: false, status: '', count: 1 },
  { month: mk(T, 4), type: 'transfer', subType: 'Moved', employee: false, status: '', count: 4 },
];
const HEADCOUNT = { company: 'LSports', spans: SPANS, events: EVENTS, error: null };

async function testDeelAndHeadcount(payload) {
  const sm = await sp.loadSalaryModule();
  console.log('\nDEEL: run-rate and months (src/forecast/salary-projection.mjs)');
  const a = (month, acct, eur) => ({ month, acct, eur, ils: eur * 4 });
  const six = sm.deelRunRate([a(mk(Y, 3), '660004', 999), ...[400, 395, 338, 97, 393, 435].map((v, i) => a(mk(Y, 4 + i), '660004', v))], mk(Y, 9));
  check(sm.DEEL_RUN_RATE_MONTHS === 6 && JSON.stringify(six.months) === JSON.stringify(range(4, 9).map((m) => mk(Y, m))) && six.byAcct['660004'].eur === 343 && six.byAcct['660004'].ils === 1_372,
    'the average of the last six months up to the run-rate month (April–September); earlier months do not count');
  const late = sm.deelRunRate([a(mk(Y, 1), '660004', 100), a(mk(Y, 4), '660004', 100), a(mk(Y, 6), '660004', 200), ...range(7, 9).map((m) => a(mk(Y, m), '660004', 100))], mk(Y, 9));
  check(late.months.length === 6 && late.byAcct['660004'].eur === 100,
    'a month without a bill counts as 0, so a bill booked a month late and the month that holds two average out');
  const moved = sm.deelRunRate([...range(4, 7).map((m) => a(mk(Y, m), '660004', 100)), a(mk(Y, 8), '660010', 100), a(mk(Y, 9), '660010', 100)], mk(Y, 9));
  check(near(moved.byAcct['660004'].eur, 66.67) && near(moved.byAcct['660010'].eur, 33.33) && near(moved.byAcct['660004'].eur + moved.byAcct['660010'].eur, 100, 0.02),
    'a move to another account: each account its share, the total is right');
  const recent = sm.deelRunRate([a(mk(Y, 8), '660004', 80), a(mk(Y, 9), '660004', 120), a(mk(Y, 10), '660004', 999)], mk(Y, 9));
  check(JSON.stringify(recent.months) === JSON.stringify([mk(Y, 8), mk(Y, 9)]) && recent.byAcct['660004'].eur === 100,
    'Deel started in August: the months since its first bill; months after the run-rate month never count');
  const none = sm.deelRunRate([a(mk(Y - 2, 1), '660004', 80)], mk(Y, 9));
  const never = sm.deelRunRate([], mk(Y, 9));
  check(none.months.length === 6 && JSON.stringify(none.byAcct) === '{}' && never.months.length === 0 && JSON.stringify(never.byAcct) === '{}',
    'no bill in the last six months, or none ever: no run-rate (0)');
  const byMonth = sm.deelByMonth(payload.blocks, [a(mk(Y, 8), '660004', 80), a(mk(Y, 9), '660004', 120), a(mk(Y, 10), '660004', 999)], payload.basis.runRateMonth);
  check(byMonth.get(mk(Y, 9)).eur['660004'] === 120 && byMonth.get(mk(Y, 10)).eur['660004'] === 100 && byMonth.get(mk(T, 12)).eur['660004'] === 100 && byMonth.size === 24,
    'deelByMonth: actual months as booked, the current month (its partial postings ignored) and forecast months at the run-rate');

  console.log('\nHEADCOUNT: HiBob\'s events and the series (src/forecast/salary-projection.mjs)');
  check(sm.headcountKind('increase', 'Open position', false) === 'openings' && sm.headcountKind('decrease', 'Position cut', false) === 'cuts'
    && sm.headcountKind('decrease', 'Terminated', true) === 'employee' && sm.headcountKind('increase', 'Hired', true) === 'employee'
    && sm.headcountKind('increase', 'Leave end', true) === 'returns' && sm.headcountKind('Decrease', 'Maternity leave', true) === 'leaveStarts' && sm.headcountKind('transfer', 'x', false) === null,
    'events: with no employee, an increase is an open position and a decrease a planned leaver; a known employee\'s event is already in HiBob\'s dates; a sub-type about leave is a return or a leave start; an unknown type counts nowhere');

  console.log('\nDEEL AND HEADCOUNT: the payload (server/salary-projection.cjs)');
  const { payload: p } = await sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW, deel: DEEL, headcount: HEADCOUNT });
  const d = p.deel;
  check(d.error === null && JSON.stringify(d.vendors) === JSON.stringify(['Deel Inc. (synthetic)', 'Deel Ltd (synthetic)'])
    && d.rows.length === 10 && d.rows[0].month === mk(Y - 1, 12) && d.rows.every((r) => r.acct === '660004') && d.rows.find((r) => r.month === mk(Y, 8) && r.vendor === 'Deel Ltd (synthetic)'),
    `Deel: rows from January ${Y - 1} with their vendor's name; payroll accounts left out`);
  check(JSON.stringify(d.onPayroll) === JSON.stringify([{ acct: '760001', name: 'Gross Salaries' }]), 'a Deel posting on a payroll account is listed (already in Total payroll)');
  check(JSON.stringify(p.blocks) === JSON.stringify(payload.blocks), 'the account projection (blocks) is unchanged by Deel and the headcount');
  const h = p.headcount;
  const m = (mKey) => h.months.find((x) => x.mKey === mKey);
  check(h.company === 'LSports' && h.asOf === '2026-10-15' && h.today === 5 && h.error === null && h.months.length === 24 && h.events.length === EVENTS.length
    && h.events.every((e) => 'employee' in e && 'status' in e),
    'headcount: the company, today (who joined and left this month counted as of today) and every month of both years');
  check(m(mk(Y, 1)).employees === 5 && m(mk(Y, 3)).employees === 5 && m(mk(Y, 4)).employees === 4 && m(mk(Y, 4)).leavers === 1 && m(mk(Y, 5)).hires === 1 && m(mk(Y, 9)).employees === 5
    && m(mk(Y, 9)).status === 'actual' && m(mk(Y, 9)).openings === 0,
    'months before the current one: HiBob\'s employees at month-end, who joined and who left (leaving on the 31st counts in March)');
  const idOff = h.months.filter((x, i) => i > 0 && x.employees !== h.months[i - 1].employees + x.hires - x.leavers).map((x) => x.mKey);
  check(!idOff.length, 'every month: employees = the month before + hires − leavers', idOff.join(','));
  check(m(mk(Y, 10)).status === 'current' && m(mk(Y, 10)).employees === 5 && m(mk(Y, 10)).hires === 1 && m(mk(Y, 10)).leavers === 1 && m(mk(Y, 10)).openings === 1,
    'the current month: HiBob\'s employees by their dates (who joined and left this month; terminations of known employees not counted again); June\'s opening still open shows, not counted, a closed one not at all');
  check(m(mk(Y, 11)).employees === 5 && m(mk(Y, 11)).openings === 2 && m(mk(Y, 12)).employees === 6 && m(mk(Y, 12)).hires === 1
    && m(mk(T, 2)).employees === 5 && m(mk(T, 2)).leavers === 1 && m(mk(T, 3)).employees === 5 && m(mk(T, 3)).leavers === 0 && m(mk(T, 3)).cuts === 1 && m(mk(T, 12)).employees === 5,
    'forecast months: HiBob\'s employees only, signed hires and people leaving by their dates (the "Hired" event not counted again); open positions and a planned cut with no employee show, not counted');
  check(m(mk(Y, 8)).returns === 1 && m(mk(Y, 12)).returns === 1 && m(mk(T, 2)).leaveStarts === 1 && m(mk(T, 4)).hires === 0 && m(mk(T, 4)).employees === 5,
    'returns from leave and leave starts are shown in their month but do not move the count; an unknown event type is left out');
  const json = JSON.stringify(p.headcount);
  check(!('spans' in p.headcount) && !/2019-01-01|2025-11-15|Full-time|Student/.test(json), 'no employee detail in the payload: counts only');

  const wErr = (await sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW, deel: { vendors: [], rows: [], error: 'Deel could not be read.' }, headcount: { company: 'LSports', spans: null, events: [], error: 'HiBob failed.' } })).payload;
  check(wErr.warnings.includes('Deel could not be read.') && wErr.warnings.includes('HiBob failed.') && wErr.headcount.today === null && wErr.headcount.months.every((x) => x.employees === null),
    'read failures: a warning each; no employees means no headcount');
  const wNone = (await sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW, deel: { vendors: [], rows: [], error: null } })).payload;
  check(wNone.warnings.some((w) => /no vendor named "deel"/.test(w)), 'no Deel vendor in NetSuite: a warning');

  // The reads, through computeSalaryProjection. HiBob's employees are read as the Metrics page reads them
  // (server/metrics.cjs), with snowflake-api.cjs's table names: a checkout without node_modules (no
  // snowflake-sdk) gets the names alone.
  const sfFile = require.resolve(path.join(ROOT, 'snowflake-api.cjs'));
  try { require(sfFile); } catch {
    require.cache[sfFile] = { id: sfFile, filename: sfFile, loaded: true, exports: { SF_TABLES: { DIM_EMPLOYEE: 'DL_PRODUCTION.CONSUMER_HUB__FINANCE.DIM_EMPLOYEE__FINANCE' } } };
  }
  const prevId = process.env.NETSUITE_ACCOUNT_ID;
  const prevCo = process.env.METRICS_HEADCOUNT_COMPANY;
  process.env.NETSUITE_ACCOUNT_ID = 'TEST';
  delete process.env.METRICS_HEADCOUNT_COMPANY;
  try {
    let askedDeel = null;
    let queued = 0;
    const ns = {
      fetchPnlActuals: async () => syntheticPnlActuals(),
      fetchVendorSpend: async (q) => { askedDeel = q; return DEEL; },
    };
    let askedPlan = null;
    let spansSql = null;
    const sf = {
      fetchPayrollOverrides: async () => [],
      fetchPayrollChanges: async () => [],
      fetchHeadcountPlan: async (from, company) => { askedPlan = { from, company }; return EVENTS; },
      query: async (sql, binds) => { spansSql = { sql, binds }; return SPANS.map((s) => ({ S: s.start, E: s.end, T: s.type })); },
    };
    const run = (nsClient, sfClient) => quiet(() => sp.computeSalaryProjection({ now: NOW, getNsClient: () => nsClient, getSfClient: () => sfClient, queueNsCall: (fn) => { queued++; return fn(); } }));
    const { payload: c } = await run(ns, sf);
    check(askedDeel && askedDeel.name === 'deel' && askedDeel.fromYear === Y - 1 && askedDeel.toYear === Y && queued === 2,
      `Deel: one NetSuite read of the vendors named "deel", ${Y - 1}–${Y}, through the NetSuite queue`);
    check(askedPlan && askedPlan.from === Y && askedPlan.company === 'LSports' && /DIM_EMPLOYEE/.test(spansSql.sql) && JSON.stringify(spansSql.binds) === '["LSports"]',
      'HiBob: employees and planned events of LSports (a bound parameter)');
    check(c.headcount.today === 5 && c.deel.rows.length === 10 && !c.warnings.some((w) => /Deel|HiBob|headcount/.test(w)), 'read fine: Deel and the headcount, no warning');
    const broken = (await run(
      { fetchPnlActuals: async () => syntheticPnlActuals(), fetchVendorSpend: async () => { throw new Error('secret detail'); } },
      { ...sf, fetchHeadcountPlan: async () => { throw new Error('secret detail'); } },
    )).payload;
    check(broken.status === 'ready' && broken.deel.rows.length === 0 && broken.warnings.some((w) => /Deel could not be read/.test(w))
      && broken.headcount.today === 5 && broken.headcount.events.length === 0 && broken.warnings.some((w) => /HiBob's planned events could not be read/.test(w))
      && !broken.warnings.join(' ').includes('secret'),
      'a Deel or HiBob failure: the page still loads, with a note (the detail stays in the server log)');
    const old = (await run({ fetchPnlActuals: async () => syntheticPnlActuals() }, null)).payload;
    check(old.status === 'ready' && old.warnings.some((w) => /cannot read Deel yet/.test(w)) && old.warnings.some((w) => /headcount \(HiBob\) is not shown/.test(w)),
      'an older NetSuite client or no Snowflake: notes, the payroll still shows');
  } finally {
    if (prevId === undefined) delete process.env.NETSUITE_ACCOUNT_ID; else process.env.NETSUITE_ACCOUNT_ID = prevId;
    if (prevCo !== undefined) process.env.METRICS_HEADCOUNT_COMPANY = prevCo;
  }

  console.log('\nDEEL AND HEADCOUNT: the page (src/salary-projection/model.ts, breakdown.ts)');
  const model = await import(pathToFileURL(path.join(ROOT, 'src', 'salary-projection', 'model.ts')).href);
  const bd = await import(pathToFileURL(path.join(ROOT, 'src', 'salary-projection', 'breakdown.ts')).href);
  const table = model.buildSalaryTable(p, 'ils', 'both');
  const keys = table.lines.map((l) => l.key);
  check(JSON.stringify(keys.slice(keys.indexOf('total'))) === JSON.stringify(['total', 'header-deel', 'deel|660004', 'total-deel', 'payroll-deel']),
    'below Total payroll: a Deel header, its accounts, Total Deel, then Payroll + Deel', keys.slice(-6).join(','));
  const line = (k, t = table) => t.lines.find((l) => l.key === k);
  const col = (id, t = table) => t.columns.findIndex((c) => c.id === id);
  const base = model.buildSalaryTable(payload, 'ils', 'both');
  check(JSON.stringify(line('total').values) === JSON.stringify(base.lines.find((l) => l.key === 'total').values), 'Total payroll is unchanged by Deel');
  const sumOff = [];
  table.columns.forEach((c, i) => {
    if (!near(line('payroll-deel').values[i], line('total').values[i] + line('total-deel').values[i], 0.05)) sumOff.push(c.id);
    if (!near(line('total-deel').values[i], line('deel|660004').values[i], 0.05)) sumOff.push(`${c.id} deel`);
  });
  check(!sumOff.length, 'Payroll + Deel = Total payroll + Total Deel, Total Deel = its accounts, in every column', sumOff.slice(0, 3).join('; '));
  const dl = line('deel|660004').values;
  const rateIls = (222_000 * 4 + 259_000 + 0) / 6;
  check(dl[col(mk(Y, 8))] === 259_000 && dl[col(mk(Y, 9))] === 0 && near(dl[col(mk(Y, 10))], rateIls, 0.01) && near(dl[col(mk(T, 6))], rateIls, 0.01),
    'Deel: NetSuite in actual months (none booked in September), the average of April–September (September as 0) in the current and forecast months');
  check(line('header-deel').kind === 'header' && /Deel Inc\. \(synthetic\), Deel Ltd \(synthetic\)|Deel Inc\. \(synthetic\); Deel Ltd \(synthetic\)/.test(line('header-deel').hint)
    && /760001 - Gross Salaries\) is already in Total payroll/.test(line('header-deel').hint)
    && /average of April 2026 to September 2026 \(6 months\)/.test(line('deel|660004').hint) && line('payroll-deel').kind === 'subtotal',
    'hints name the vendors, a payroll account already in Total payroll, and the months the forecast averages');
  check(!model.buildSalaryTable(payload, 'ils', 'both').lines.some((l) => /deel/.test(l.key)) && !model.buildSalaryTable({ ...p, deel: undefined }, 'ils', 'both').lines.some((l) => /deel/.test(l.key)),
    'no Deel rows (or an older payload): no Deel lines');
  check(['header-deel', 'deel|660004', 'total-deel'].every((k) => line(k).accent === 'pink') && line('payroll-deel').accent === undefined,
    'Deel\'s lines in pink; Payroll + Deel in none');

  const deelOff = [];
  table.lines.filter((l) => bd.isSalaryBreakdownLine(l.key) && /deel/.test(l.key)).forEach((l) => table.columns.forEach((c, i) => {
    if (Math.abs(l.values[i]) < 1) return;
    const r = bd.salaryBreakdown(p, { line: l.key, period: c.kind === 'fy' ? `FY-${c.year}` : c.mKey, variant: 'base', ccy: 'ils' });
    if (r.status !== 'ready' || !near(r.cell, l.values[i], 0.05) || !near(r.sections[0].total, r.cell, 0.05)) deelOff.push(`${l.key} ${c.id}`);
  }));
  check(bd.isSalaryBreakdownLine('deel|660004') && bd.isSalaryBreakdownLine('total-deel') && !bd.isSalaryBreakdownLine('payroll-deel') && !deelOff.length,
    'every Deel cell opens a breakdown that equals the cell, its first section adding up to it', deelOff.slice(0, 3).join('; '));
  const deelAsk = (period, lineKey = 'deel|660004') => bd.salaryBreakdown(p, { line: lineKey, period, variant: 'base', ccy: 'ils' });
  const aug = deelAsk(mk(Y, 8));
  check(JSON.stringify(aug.sections[0].rows.map((r) => [r.label, r.amount])) === JSON.stringify([['Deel Inc. (synthetic)', 222_000], ['Deel Ltd (synthetic)', 37_000]]),
    'an actual month: by vendor');
  const nov = deelAsk(mk(Y, 11));
  check(nov.sections[0].id === 'run-rate' && JSON.stringify(nov.sections[0].rows.map((r) => r.label)) === JSON.stringify(['April', 'May', 'June', 'July', 'August'].map((mo) => `${mo} 2026 ÷ 6`))
    && /without a Deel bill counts as 0/.test(nov.sections[0].note),
    'a forecast month: the months its run-rate averages, each ÷ 6 (September, with no bill, as 0)');
  const fyT = deelAsk(`FY-${T}`, 'total-deel');
  check(fyT.sections.map((s) => s.id).join(',') === 'accounts,months' && fyT.sections[1].rows.length === 12 && /Not in Total payroll/.test(fyT.notes.join(' ')),
    'Total Deel for a year: by account and by month');

  const hcTable = model.buildHeadcountTable(p, 'both');
  const hl = (k) => hcTable.lines.find((l) => l.key === k);
  check(JSON.stringify(hcTable.lines.map((l) => l.key)) === JSON.stringify(['hc-employees', 'hc-hires', 'hc-leavers', 'hc-openings', 'hc-cuts', 'hc-returns', 'hc-leave-starts'])
    && hcTable.lines.every((l) => l.unit === 'count') && hl('hc-hires').signed && hl('hc-leavers').signed && hl('hc-openings').kind === 'note' && hl('hc-cuts').kind === 'note'
    && /not counted/.test(hl('hc-openings').label),
    'headcount table: HiBob\'s employees, hires and leavers; then, not counted, open positions, planned leavers with no employee, returns from leave, leave starts');
  check(hcTable.columns.length === 26 && hl('hc-employees').values[col(`fy-${Y}`, hcTable)] === 6 && hl('hc-employees').values[col(mk(Y, 4), hcTable)] === 4
    && hl('hc-leavers').values[col(mk(Y, 4), hcTable)] === -1 && hl('hc-leavers').values[col(`fy-${Y}`, hcTable)] === -2
    && hl('hc-hires').values[col(`fy-${Y}`, hcTable)] === 3 && hl('hc-openings').values[col(`fy-${Y}`, hcTable)] === 3,
    'the payroll table\'s columns; full year: employees at December, hires, leavers and open positions summed (leavers negative)');
  check(hcTable.columns.find((c) => c.mKey === mk(Y, 9)).status === 'actual' && hcTable.columns.find((c) => c.mKey === mk(Y, 10)).status === 'current',
    'its own statuses: HiBob\'s months before the current one are actual');
  const noLeave = model.buildHeadcountTable({ ...p, headcount: { ...p.headcount, months: p.headcount.months.map((x) => ({ ...x, openings: 0, cuts: 0, returns: 0, leaveStarts: 0 })) } }, 'next');
  check(noLeave.lines.map((l) => l.key).join(',') === 'hc-employees,hc-hires,hc-leavers' && noLeave.columns.length === 13 && model.buildHeadcountTable(wErr, 'both') === null
    && model.buildHeadcountTable(payload, 'both') === null,
    'no open positions or leave events: no such lines; follows the year view; no employees (or not read): no table');
  const kpis = model.computeSalaryKpis(p, 'ils');
  check(kpis.headcount && kpis.headcount.today === 5 && kpis.headcount.asOf === '2026-10-15'
    && JSON.stringify(kpis.headcount.yearEnd) === JSON.stringify([{ year: Y, value: 6 }, { year: T, value: 5 }]) && model.computeSalaryKpis(payload, 'ils').headcount === null,
    'KPI: headcount today and at the end of each year; none without HiBob');
}

async function main() {
  console.log('=== salary-projection checks (synthetic data) ===');
  const payload = await testPayload();
  await testEdges();
  await testClassification(payload);
  await testCompute();
  await testOverrides(payload);
  await testHandler(payload);
  await testModel(payload);
  await testDeelAndHeadcount(payload);
  if (process.argv.includes('--write-fixture')) {
    const file = path.join(__dirname, 'fixtures', 'salary-projection-sample.json');
    const overrides = sp.overridesOf({ error: null, rows: [
      { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', department: 'R&D', mode: 'Increment', eur: 20_000, ils: 0, memo: 'Two hires (synthetic)' },
      { month: mk(Y, 12), acct: '760001', name: 'Gross Salaries', department: 'Sales', mode: 'Override', eur: 50_000, ils: 185_000, memo: '' },
    ] }, Y, payload.basis.ilsPerEur);
    const changes = sp.changesOf({ error: null, rows: [
      { month: mk(Y, 10), acct: '760001', name: 'Gross Salaries', source: 'Terminated', department: 'R&D', actual: true, eur: -30_000, ils: -110_000 },
      { month: mk(Y, 12), acct: '760001', name: 'Gross Salaries', source: 'Open position', department: 'Sales', actual: true, eur: 23_000, ils: 85_000 },
      { month: mk(T, 1), acct: '760001', name: 'Gross Salaries', source: 'Terminated', department: 'Finance', actual: true, eur: -87_000, ils: -323_000 },
      { month: mk(T, 2), acct: '760001', name: 'Gross Salaries', source: 'Terminated', department: 'Sales', actual: false, eur: -20_000, ils: -74_000 },
    ] }, Y, payload.basis.ilsPerEur);
    const { deel, headcount } = (await sp.buildSalaryProjection({ rows: syntheticRows(), now: NOW, deel: DEEL, headcount: HEADCOUNT })).payload;
    fs.writeFileSync(file, JSON.stringify({ ...payload, overrides, changes, deel, headcount, cache: { ageSec: 0, stale: false, staleReason: null, refreshing: false, lastError: null } }, null, 2) + '\n');
    console.log(`\nwrote ${path.relative(ROOT, file)}`);
  }
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
