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
          { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', source: 'Terminated', eur: 0, ils: -150_000 },
          { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', source: 'Open position', eur: 20_000, ils: 74_000 },
          { month: mk(Y, 10), acct: '760001', name: 'Gross Salaries', source: 'Terminated', eur: -30_000, ils: -110_000 },
          { month: mk(Y - 1, 12), acct: '760001', name: 'Gross Salaries', source: 'Leave end', eur: 5_000, ils: 18_500 },
          { month: mk(Y, 12), acct: '760001', name: 'Gross Salaries', source: 'payroll future baseline', eur: 1_000_000, ils: 3_700_000 },
        ];
      },
    };
    const run = (getSfClient) => quiet(() => sp.computeSalaryProjection({ now: NOW, getNsClient: () => ns, getSfClient }));
    const { payload: p } = await run(() => sf);
    const rows = p.overrides.rows;
    check(asked === Y && p.overrides.error === null && rows.length === 2 && rows[0].month === mk(Y, 10) && rows[1].month === mk(Y, 11),
      `read from January ${Y}; earlier rows dropped; sorted by month`);
    check(JSON.stringify(askedAccounts) === JSON.stringify(['760001']) && JSON.stringify(sp.OVERRIDE_ACCOUNTS) === JSON.stringify(['760001'])
      && JSON.stringify(p.overrides.accounts) === JSON.stringify(['760001']),
      'only account 760001\'s rows are read (finance\'s payroll rows), and the payload says so');
    check(rows[0].mode === 'Override' && rows[0].ils === 368_000 && !rows[0].ilsEstimated
      && rows[1].mode === 'Increment' && near(rows[1].ils, 20_000 * p.basis.ilsPerEur) && rows[1].ilsEstimated && rows[1].memo === 'Two hires',
      'type, amounts and comment kept; ₪ at the forecast rate when the file has € only');
    check(JSON.stringify(p.blocks) === JSON.stringify(payload.blocks), 'the account projection (blocks) is unchanged: the page adds the planned changes');

    const ch = p.changes.rows;
    check(askedChanges && askedChanges.from === Y && JSON.stringify(askedChanges.accounts) === JSON.stringify(['760001']) && p.changes.error === null
      && JSON.stringify(p.changes.accounts) === JSON.stringify(['760001']),
      `planned hires and leavers: read for 760001 from January ${Y}`);
    check(JSON.stringify(ch.map((r) => `${r.month} ${r.source}`)) === JSON.stringify([`${mk(Y, 10)} Terminated`, `${mk(Y, 11)} Open position`, `${mk(Y, 11)} Terminated`]),
      'earlier months and other sources (the baseline) dropped; sorted by month, then hires, leavers, returns');
    check(near(ch[2].eur, -150_000 / p.basis.ilsPerEur) && ch[2].ils === -150_000 && ch[1].eur === 20_000 && ch[1].ils === 74_000,
      'amounts kept; the missing currency at the forecast rate');
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
    { ...gross, month: mk(Y, 10), source: 'Terminated', eur: -30_000, ils: -110_000 },
    { ...gross, month: mk(Y, 12), source: 'Open position', eur: 23_000, ils: 85_000 },
    { ...gross, month: mk(Y, 12), source: 'Leave end', eur: 5_000, ils: 19_000 },
    { ...gross, month: mk(T, 1), source: 'Open position', eur: 23_000, ils: 86_000 },
    { ...gross, month: mk(T, 1), source: 'Terminated', eur: -87_000, ils: -323_000 },
    { ...gross, month: mk(Y, 2), source: 'Terminated', eur: -10_000, ils: -37_000 },
  ] }, Y, rate);
  const withOv = { ...payload, overrides, changes };

  const sm = await sp.loadSalaryModule();
  const counted = sm.countedIncrementRows(payload.blocks, overrides.rows);
  check(JSON.stringify(counted.map((r) => r.month)) === JSON.stringify([mk(Y, 10), mk(Y, 11), mk(Y, 11), mk(T, 2)]) && counted[0] === overrides.rows.find((r) => r.month === mk(Y, 10)),
    'countedIncrementRows: increments of the current and forecast months (the same row objects); not closed months, overrides, or months after the page');
  const countedCh = sm.countedChangeRows(payload.blocks, changes.rows);
  check(countedCh.length === 5 && !countedCh.some((r) => r.month === mk(Y, 2)) && countedCh[0] === changes.rows.find((r) => r.month === mk(Y, 10)),
    'countedChangeRows: hires, leavers and returns of the current and forecast months; not closed months');

  const ovTable = model.buildSalaryTable(withOv, 'ils', 'both');
  const ovKeys = ovTable.lines.map((l) => l.key);
  const ovExpected = [
    'header-permanent', ...sp.SALARY_GL_GROUPS[0].accounts.map(([a]) => a), 'total-permanent',
    'header-changes', 'hires|760001', 'leavers|760001', 'returns|760001', 'total-changes',
    'header-overrides', 'increment|760001', 'increment|760002', 'total-increments', 'override|760001', 'total-overrides',
    'header-temporary', ...sp.SALARY_GL_GROUPS[1].accounts.map(([a]) => a), 'total-temporary',
    'header-other', '760040', 'total-other', 'total',
  ];
  check(JSON.stringify(ovKeys) === JSON.stringify(ovExpected),
    'between Total permanent and the temporary accounts: hires, leavers, returns + Total headcount changes, then increments + Total increments, then overrides + Total overrides', ovKeys.join(','));
  const ovLine = (k, t = ovTable) => t.lines.find((l) => l.key === k);
  const col = (id, t = ovTable) => t.columns.findIndex((c) => c.id === id);
  check(ovLine('hires|760001').label === '760001 - Gross Salaries (planned hires)' && ovLine('leavers|760001').label === '760001 - Gross Salaries (leavers)'
    && ovLine('returns|760001').label === '760001 - Gross Salaries (returns from leave)' && ovLine('total-changes').label === 'Total headcount changes'
    && ovLine('hires|760001').kind === 'item' && ovLine('total-changes').kind === 'subtotal' && /Snowflake/.test(ovLine('leavers|760001').hint) && /Added to Total payroll/.test(ovLine('leavers|760001').hint),
    'hires, leavers and returns read as lines with a subtotal; hints say where they come from and that they are in Total payroll');
  const lv = ovLine('leavers|760001').values;
  check(near(lv[col(mk(Y, 10))], -110_000, 0.05) && lv[col(mk(Y, 2))] === 0 && near(lv[col(mk(T, 1))], -323_000, 0.05) && near(ovLine('total-changes').values[col(mk(Y, 12))], 104_000, 0.05),
    'planned changes in their months (leavers negative); 0 in closed months');
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
    if (!near(v('total-changes'), v('hires|760001') + v('leavers|760001') + v('returns|760001'), 0.05)) subOff.push(`${c.id} changes`);
    if (!near(v('total-increments'), v('increment|760001') + v('increment|760002'), 0.05)) subOff.push(`${c.id} increments`);
    if (!near(v('total-overrides'), v('override|760001'), 0.05)) subOff.push(`${c.id} overrides`);
  });
  check(!totalOff.length, 'Total payroll = the accounts + Total headcount changes + Total increments, in every column (overrides not in it)', totalOff.slice(0, 3).join('; '));
  check(!subOff.length, 'each subtotal = its lines, in every column', subOff.slice(0, 3).join('; '));
  const incY = 20_000 * rate + 37_000 + 1_850;
  const chY = -110_000 + 85_000 + 19_000;
  const chT = 86_000 - 323_000;
  check(near(ovLine('total-increments').values[col(`fy-${Y}`)], incY, 0.05) && near(ovLine('total-increments').values[col(`fy-${T}`)], 5_000, 0.05)
    && near(ovLine('total-changes').values[col(`fy-${Y}`)], chY, 0.05) && near(ovLine('total-changes').values[col(`fy-${T}`)], chT, 0.05),
    'FY columns sum each year\'s months');
  check(near(ovLine('override|760001').values[col(mk(Y, 12))], 185_000, 0.05) && ovLine('override|760001').values[col(`fy-${T}`)] === 0,
    `overrides show in their month; a row after December ${T} is left out (the rows list counts it)`);
  const ovEur = model.buildSalaryTable(withOv, 'eur', 'current');
  check(ovEur.columns.length === 13 && near(ovLine('increment|760001', ovEur).values[col(mk(Y, 11), ovEur)], 30_000, 0.05)
    && near(ovLine('leavers|760001', ovEur).values[col(mk(Y, 10), ovEur)], -30_000, 0.05),
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
  check(JSON.stringify(firstKeys) === JSON.stringify(['header-changes', 'hires|760001']), 'no permanent account: the planned changes come first, under their header');
  check(ovLine('header-changes').label === 'Headcount changes' && ovLine('header-overrides').label === 'Override file' && ovLine('header-changes').kind === 'header'
    && ovLine('header-overrides').values.every((v) => v === 0) && /Snowflake/.test(ovLine('header-changes').hint),
    'planned changes: a "Headcount changes" and an "Override file" header, no amounts');
  const onlyChanges = model.buildSalaryTable({ ...payload, changes }, 'ils', 'both').lines.map((l) => l.key);
  check(onlyChanges.includes('total-changes') && !onlyChanges.some((k) => /increment|override/.test(k)), 'planned changes without the override file');
  const plain = (p) => JSON.stringify(model.buildSalaryTable(p, 'ils', 'both').lines.map((l) => l.key)) === JSON.stringify(expected);
  check(plain({ ...payload, overrides: { error: null, rows: [] }, changes: { error: null, rows: [] } }) && plain({ ...payload, overrides: undefined, changes: undefined })
    && plain({ ...payload, overrides: { error: null, rows: overrides.rows.filter((r) => r.month > mk(T, 12)) } })
    && plain({ ...payload, overrides: { error: null, rows: overrides.rows.filter((r) => r.memo === 'Closed month') }, changes: { error: null, rows: changes.rows.filter((r) => r.month === mk(Y, 2)) } }),
    'no rows, rows after the page\'s years only, or closed months only: no planned-change lines');
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
  if (process.argv.includes('--write-fixture')) {
    const file = path.join(__dirname, 'fixtures', 'salary-projection-sample.json');
    const overrides = sp.overridesOf({ error: null, rows: [
      { month: mk(Y, 11), acct: '760001', name: 'Gross Salaries', department: 'R&D', mode: 'Increment', eur: 20_000, ils: 0, memo: 'Two hires (synthetic)' },
      { month: mk(Y, 12), acct: '760001', name: 'Gross Salaries', department: 'Sales', mode: 'Override', eur: 50_000, ils: 185_000, memo: '' },
    ] }, Y, payload.basis.ilsPerEur);
    const changes = sp.changesOf({ error: null, rows: [
      { month: mk(Y, 10), acct: '760001', name: 'Gross Salaries', source: 'Terminated', eur: -30_000, ils: -110_000 },
      { month: mk(Y, 12), acct: '760001', name: 'Gross Salaries', source: 'Open position', eur: 23_000, ils: 85_000 },
      { month: mk(T, 1), acct: '760001', name: 'Gross Salaries', source: 'Terminated', eur: -87_000, ils: -323_000 },
    ] }, Y, payload.basis.ilsPerEur);
    fs.writeFileSync(file, JSON.stringify({ ...payload, overrides, changes, cache: { ageSec: 0, stale: false, staleReason: null, refreshing: false, lastError: null } }, null, 2) + '\n');
    console.log(`\nwrote ${path.relative(ROOT, file)}`);
  }
  console.log(failures ? `\n❌ FAIL — ${failures} check(s) failed.` : '\n✅ PASS — all checks green.');
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
