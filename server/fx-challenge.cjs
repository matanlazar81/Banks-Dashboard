// ─────────────────────────────────────────────────────────────────────────────
// The FX Challenge page (Business Tools → FX Challenge, fx-challenge.html):
//   GET /api/fx-challenge          what the page simulates on; ?refresh=true reads NetSuite again
//   GET/PUT /api/fx-challenge/table  the conversion table, saved for everyone (none saved = the default)
//   GET/PUT /api/fx-challenge/mail   who gets the daily mail, saved for everyone (none saved = FX_MAIL_TO)
// createFxChallengeRouter serves all three behind the one prefix.
//
// The page simulates the monthly EUR→ILS conversions on the table (src/forecast/fx-challenge.mjs) and says how
// much EUR to keep for them. This module gathers what it needs; each read is cached, and a failed read becomes a
// warning while the rest still shows:
//   rates        EUR/ILS by day (NetSuite currencyrate, HSBC), back to the first past conversion and at least 400
//                days: today's rate, the volatility (the last 400 days) and each past day's rate
//   balances     the daily balances mail's figures, from the RESTlet next to it (NS_BALANCES_RESTLET =
//                script:deploy); without it, or when it fails, NetSuite's books (EUR accounts in the primary book:
//                exact in euros, but no deposit maturities)
//   flows        EUR in and out by month, from four months back: the typical net inflow
//   conversions  "Conversion" / המרה journals Hapoalim EUR ↔ ILS (583400), from January 2026 (HISTORY_FROM) or the
//                table's first month if earlier: what each past month booked, euros bought back included
//   payroll      what each month's payroll journal owes (net salaries, withholding tax, pension, social security),
//                four months back, and the Salary Projection's Total payroll by month (the figures its page saved)
//   ilsVendors   vendor payments out of the ILS accounts by month, four months back: the shekels kept for vendors
// The table is the saved one (data/fx-challenge-table.json) or the default. The daily mail
// (scripts/fx-challenge-mail.cjs) reads the same, uncached.
// ─────────────────────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');
const { pathToFileURL } = require('url');
const { readDoc, createDocHandler, send } = require('./json-store.cjs');
const { keysOnly, numIn, textIn } = require('./metrics-settings.cjs');

const ROOT = path.join(__dirname, '..');
const TABLE_FILE = path.join(ROOT, 'data', 'fx-challenge-table.json');
/** The daily mail's recipients as saved on the page, and when it last went (scripts/fx-challenge-mail.cjs). */
const MAIL_FILE = path.join(ROOT, 'data', 'fx-challenge-mail.json');
const MAIL_SENT_FILE = path.join(ROOT, 'data', 'fx-challenge-mail-last.json');
let moduleP = null;
function loadFxModule() {
  if (!moduleP) {
    moduleP = import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'fx-challenge.mjs')).href)
      .catch((e) => { moduleP = null; throw e; });
  }
  return moduleP;
}

const KINDS = new Set(['current', 'deposit', 'card', 'other']);
const isIso = (s) => /^\d{4}-\d{2}-\d{2}$/.test(String(s || ''));
const shiftMonth = (mKey, n) => {
  const [y, m] = mKey.split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
};
/** Today in Israel, YYYY-MM-DD (the fixings and the bank's dates are Israeli). */
const israelDay = (ms) => new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Jerusalem', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date(ms));
/** The Salary Projection's Total payroll by month (₪), from the figures its page saved, or null when there are none
 * (or they are from an older version of it). */
async function readSalaryProjection(file) {
  const { readCacheEntry } = require('./cash-projection.cjs');
  const sp = require('./salary-projection.cjs');
  const entry = readCacheEntry(file || sp.DEFAULT_CACHE_FILE, sp.SCHEMA_VERSION);
  if (!entry || !entry.payload) return null;
  const mod = await import(pathToFileURL(path.join(ROOT, 'src', 'forecast', 'salary-projection.mjs')).href);
  const byMonth = mod.totalPayrollByMonth(entry.payload);
  return {
    asOf: entry.payload.generatedAt || (entry.generatedAtMs ? new Date(entry.generatedAtMs).toISOString() : null),
    months: Object.entries(byMonth).sort(([a], [b]) => a.localeCompare(b)).map(([month, v]) => ({ month, ils: Math.round(v.ils), status: v.status })),
  };
}

const daysBetween = (from, to) => Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000);
const isoDaysBefore = (iso, n) => new Date(Date.parse(`${iso}T00:00:00Z`) - n * 86400000).toISOString().slice(0, 10);

/** NS_BALANCES_RESTLET = "script:deploy" → { script, deploy }, or null when unset or malformed. */
function restletIds(value) {
  const m = /^\s*([A-Za-z0-9_]{1,80})\s*:\s*([A-Za-z0-9_]{1,80})\s*$/.exec(String(value || ''));
  return m ? { script: m[1], deploy: m[2] } : null;
}

/** A books row (fetchEurBalancesBooks) as a balance row: the bank and kind told by the account's name. */
function bookRow(a) {
  const name = String(a.name || '');
  const kind = /deposit/i.test(name) ? 'deposit'
    : /amex|mastercard|isracard|visa|diners/i.test(name) ? 'card'
      : /netting|clearing/i.test(name) ? 'other' : 'current';
  const bank = /poalim/i.test(name) ? 'Bank Hapoalim' : /pko/i.test(name) ? 'PKO' : /leumi/i.test(name) ? 'Bank Leumi'
    : /discount/i.test(name) ? 'Bank Discount' : /payhound/i.test(name) ? 'Payhound' : /paypal/i.test(name) ? 'PayPal'
      : /moneynet/i.test(name) ? 'MoneyNet' : name.replace(/^\d+\s*/, '');
  return { company: 'LSports', bank, kind, currency: 'EUR', amount: Number(a.eur) || 0, account: name, asOf: null, valueDate: null };
}

/** The RESTlet's answer as balance rows; rows that don't fit the contract are dropped and counted. */
function mailRows(body) {
  if (!body || body.ok !== true || !Array.isArray(body.rows)) throw new Error((body && body.error) || 'the balances RESTlet answered without rows');
  let dropped = 0;
  const rows = [];
  for (const r of body.rows) {
    const amount = Number(r && r.amount);
    if (!r || typeof r.bank !== 'string' || !KINDS.has(r.kind) || !/^[A-Z]{3}$/.test(String(r.currency)) || !Number.isFinite(amount)) { dropped += 1; continue; }
    rows.push({
      company: typeof r.company === 'string' ? r.company : 'LSports',
      bank: r.bank,
      kind: r.kind,
      currency: r.currency,
      amount,
      asOf: typeof r.asOf === 'string' ? r.asOf : null,
      valueDate: isIso(r.valueDate) ? r.valueDate : null,
      label: typeof r.label === 'string' ? r.label : undefined,
      blocked: r.blocked === true,
    });
  }
  return { rows, dropped, builtAt: typeof body.builtAt === 'string' ? body.builtAt : null, notes: Array.isArray(body.notes) ? body.notes.filter((n) => typeof n === 'string').slice(0, 10) : [] };
}

// ── the saved table ─────────────────────────────────────────────────────────
// The same rules as tableProblems in src/forecast/fx-challenge.mjs (the page checks a pasted table with it before
// saving).
const LIMITS = { rows: 60, bounds: 30, eurK: 100000, rate: 20 };
function tableIn(t, errors) {
  const where = 'table';
  if (!keysOnly(t, ['pair', 'fixingDay', 'bounds', 'rows'], where, errors)) return null;
  const pair = textIn(t.pair, 20, `${where}.pair`, errors);
  let fixingDay = t.fixingDay;
  if (!(Number.isInteger(fixingDay) && fixingDay >= 1 && fixingDay <= 28)) { errors.push(`${where}.fixingDay: a day from 1 to 28`); fixingDay = 7; }
  if (!Array.isArray(t.bounds) || !t.bounds.length || t.bounds.length > LIMITS.bounds) { errors.push(`${where}.bounds: 1–${LIMITS.bounds} rates`); return null; }
  const bounds = t.bounds.map((b, i) => {
    const v = numIn(b, 0.0001, LIMITS.rate, `${where}.bounds[${i + 1}]`, errors);
    if (v !== null && i && !(v > t.bounds[i - 1])) errors.push(`${where}.bounds[${i + 1}]: above the bound before`);
    return v;
  });
  if (!Array.isArray(t.rows) || !t.rows.length || t.rows.length > LIMITS.rows) { errors.push(`${where}.rows: 1–${LIMITS.rows} months`); return null; }
  const months = new Set();
  const rows = t.rows.map((r, n) => {
    const at = `${where}.rows[${n + 1}]`;
    if (!keysOnly(r, ['month', 'cells'], at, errors)) return null;
    const month = typeof r.month === 'string' && /^\d{4}-(0[1-9]|1[0-2])$/.test(r.month) ? r.month : (errors.push(`${at}.month: YYYY-MM`), '');
    if (month && months.has(month)) errors.push(`${at}.month: ${month} twice`);
    months.add(month);
    if (!Array.isArray(r.cells) || r.cells.length !== bounds.length + 1) { errors.push(`${at}.cells: ${bounds.length + 1} (one per band)`); return null; }
    const cells = r.cells.map((c, i) => (keysOnly(c, ['eurK', 'rate'], `${at}.cells[${i + 1}]`, errors) ? {
      eurK: numIn(c.eurK, -LIMITS.eurK, LIMITS.eurK, `${at}.cells[${i + 1}].eurK`, errors),
      rate: numIn(c.rate, 0.0001, LIMITS.rate, `${at}.cells[${i + 1}].rate`, errors),
    } : null));
    return { month, cells };
  });
  return { pair, fixingDay, bounds, rows: rows.filter(Boolean).sort((a, b) => a.month.localeCompare(b.month)) };
}

/** PUT body { value: table } → { ok, value, errors }; { value: null } puts the default table back. */
function validateTable(body) {
  const errors = [];
  if (!keysOnly(body, ['value'], 'body', errors)) return { ok: false, value: null, errors };
  if (body.value === null) return { ok: true, value: null, errors: [] };
  const value = tableIn(body.value, errors);
  return errors.length || !value ? { ok: false, value: null, errors } : { ok: true, value, errors: [] };
}

// ── the daily mail's recipients ─────────────────────────────────────────────
// Kept on the page and saved for everyone; until a list is saved, the checkout's FX_MAIL_TO. lsports.eu addresses only:
// the mail carries LSports' balances. An empty list sends to nobody.
const LSPORTS_ADDRESS = /^[^\s@<>"',;]+@lsports\.eu$/i;
const MAX_RECIPIENTS = 30;

/** A list of addresses as written in FX_MAIL_TO (commas, semicolons or spaces), each once. */
const splitAddresses = (value) => [...new Set(String(value || '').split(/[,;\s]+/).map((s) => s.trim()).filter(Boolean))];

/** FX_MAIL_HOUR (Israel time) as an hour, 10 when unset or not an hour. */
const mailHourOf = (value) => (/^\d{1,2}$/.test(String(value || '').trim()) && +value <= 23 ? +value : 10);

/** PUT body { value: { to: [addresses] } } → { ok, value, errors }; { value: null } goes back to FX_MAIL_TO. */
function validateMailSettings(body) {
  const errors = [];
  if (!keysOnly(body, ['value'], 'body', errors) || errors.length) return { ok: false, value: null, errors };
  if (body.value === null) return { ok: true, value: null, errors: [] };
  if (!keysOnly(body.value, ['to'], 'value', errors) || errors.length) return { ok: false, value: null, errors };
  const { to } = body.value;
  if (!Array.isArray(to) || to.length > MAX_RECIPIENTS) return { ok: false, value: null, errors: [`value.to: a list of up to ${MAX_RECIPIENTS} addresses`] };
  const out = [];
  to.forEach((a, i) => {
    const s = typeof a === 'string' ? a.trim().toLowerCase() : '';
    if (!LSPORTS_ADDRESS.test(s)) errors.push(`value.to[${i + 1}]: ${s ? `${s} is not an lsports.eu address` : 'not an address'}`);
    else if (!out.includes(s)) out.push(s);
  });
  return errors.length ? { ok: false, value: null, errors } : { ok: true, value: { to: out }, errors: [] };
}

/** The saved list (an array, maybe empty) or null when none is saved: the mail then uses FX_MAIL_TO. */
function savedRecipients(file = MAIL_FILE) {
  const doc = readDoc(file);
  return doc && doc.value && Array.isArray(doc.value.to) ? doc.value.to.filter((a) => typeof a === 'string') : null;
}

/** The checkout's .env as the mail script reads it (finance-it's backend runs with its own environment). */
function checkoutEnv(file = path.join(ROOT, '.env')) {
  try {
    return require('dotenv').parse(fs.readFileSync(file));
  } catch {
    return {};
  }
}

/**
 * GET/PUT /api/fx-challenge/mail: who gets the daily mail, saved for everyone (with history). GET adds what the mail
 * uses while no list is saved (the checkout's FX_MAIL_TO), its sender and hour, and when it last went.
 *   deps: file, sentFile, env() (default: the checkout's .env), clock()
 */
function createFxMailHandler(deps = {}) {
  const file = deps.file || MAIL_FILE;
  const sentFile = deps.sentFile || MAIL_SENT_FILE;
  const env = deps.env || (() => checkoutEnv());
  const save = createDocHandler({ file, label: 'fx-challenge-mail', maxBytes: 8 * 1024, validate: validateMailSettings, empty: () => null, clock: deps.clock });
  return async function fxMailHandler(req, res) {
    if ((req.method || 'GET').toUpperCase() !== 'GET') return save(req, res);
    const doc = readDoc(file);
    const e = env();
    const sent = readDoc(sentFile);
    send(res, 200, {
      ok: true,
      value: doc && doc.value && Array.isArray(doc.value.to) ? { to: doc.value.to } : null,
      updatedAt: (doc && doc.updatedAt) || null,
      updatedBy: (doc && doc.updatedBy) || null,
      envTo: splitAddresses(e.FX_MAIL_TO),
      from: e.FX_MAIL_FROM || null,
      hour: mailHourOf(e.FX_MAIL_HOUR),
      lastSent: sent && typeof sent.at === 'string' ? { day: sent.day || null, at: sent.at, recipients: Number.isInteger(sent.recipients) ? sent.recipients : null } : null,
    });
  };
}

/**
 * The FX Challenge payload.
 *   reads     { rates(days), books(), mail (null when the RESTlet is not configured), flows(fromMonth), conversions(fromDay),
 *               payroll(fromMonth), ilsVendors(fromMonth), salary() } (the last three optional)
 *   cached    (key, fn) → promise, the handler's memo
 *   tableDoc  the saved table document ({ value: table | null, updatedAt, updatedBy }) or null
 */
async function readFxChallenge({ reads, cached = (k, fn) => fn(), tableDoc = null, clock = Date.now }) {
  const fx = await loadFxModule();
  const mail = reads.mail;
  const nowMs = clock();
  const today = israelDay(nowMs);
  const saved = tableDoc && tableDoc.value;
  const table = saved || fx.cloneTable(fx.DEFAULT_TABLE);
  const firstMonth = [fx.HISTORY_FROM, ...table.rows.map((r) => r.month)].sort()[0];
  const flowsFrom = shiftMonth(today.slice(0, 7), -4);
  const convFrom = `${shiftMonth(firstMonth, -1)}-20`;
  // The rates reach back to the first past conversion, and 400 days at least for the volatility.
  const rateDays = Math.max(400, daysBetween(convFrom, today) + 1);
  const warnings = [];

  const settled = await Promise.allSettled([
    cached(`fx:rates:${rateDays}`, () => reads.rates(rateDays)),
    mail ? cached('fx:mail', () => mail()) : Promise.resolve(null),
    cached('fx:books', () => reads.books()),
    cached(`fx:flows:${flowsFrom}`, () => reads.flows(flowsFrom)),
    cached(`fx:conversions:${convFrom}`, () => reads.conversions(convFrom)),
    reads.payroll ? cached(`fx:payroll:${flowsFrom}`, () => reads.payroll(flowsFrom)) : Promise.resolve(null),
    reads.ilsVendors ? cached(`fx:ils-vendors:${flowsFrom}`, () => reads.ilsVendors(flowsFrom)) : Promise.resolve(null),
    reads.salary ? Promise.resolve().then(() => reads.salary()) : Promise.resolve(null),
  ]);
  const val = (i, what) => {
    if (settled[i].status === 'fulfilled') return settled[i].value;
    console.warn(`[fx-challenge] ${what} unavailable: ${settled[i].reason && settled[i].reason.message}`);
    return undefined;
  };

  const series = val(0, 'rates') || [];
  if (!series.length) warnings.push('EUR/ILS could not be read from NetSuite, so the simulation starts from the rate you type.');
  const spot = fx.rateOn(series, today);
  const sigma = fx.annualVol(series.filter((p) => p.date >= isoDaysBefore(today, 400)));

  let balances = null;
  const mailBody = val(1, 'mail balances');
  if (mail && mailBody !== undefined) {
    try {
      const m = mailRows(mailBody);
      balances = { source: 'mail', asOf: m.builtAt, rows: m.rows, notes: m.notes };
      if (m.dropped) warnings.push(`${m.dropped} balance row(s) from the mail's sources did not fit and were left out.`);
    } catch (e) {
      console.warn(`[fx-challenge] mail balances unusable: ${e && e.message}`);
    }
  }
  if (!balances) {
    const books = val(2, 'books balances');
    if (books) {
      balances = { source: 'books', asOf: null, rows: books.map(bookRow).filter((r) => Math.abs(r.amount) >= 0.5), notes: [] };
      warnings.push(mail
        ? "The daily mail's balances could not be read, so the balances are NetSuite's books (EUR only, no deposit maturities)."
        : "Balances are NetSuite's books (EUR only, no deposit maturities) until the balances RESTlet is set up (NS_BALANCES_RESTLET).");
    } else warnings.push('No balances could be read, so the recommendation is not shown.');
  }

  const flows = val(3, 'flows');
  if (!flows) warnings.push('The monthly EUR flows could not be read, so the typical inflow is left out.');
  const conversions = val(4, 'conversions');
  if (!conversions) warnings.push('The booked conversions could not be read, so past conversions are not checked.');
  const journals = val(5, 'payroll journals');
  const ilsVendors = val(6, 'ILS vendor payments');
  const projection = val(7, 'the Salary Projection');
  if ((reads.payroll || reads.salary) && !(journals && journals.length) && !projection) {
    warnings.push("Neither the Salary Projection's figures nor NetSuite's payroll journals could be read, so salaries and vendors are left out of the plan.");
  }

  return {
    ok: true,
    status: 'ready',
    computedAt: new Date(nowMs).toISOString(),
    today,
    table,
    tableSaved: saved ? { at: tableDoc.updatedAt || null, by: tableDoc.updatedBy || null } : null,
    rates: { provider: 'HSBC', series, spot, sigma },
    balances,
    flows: flows || [],
    inflowMonths: [-3, -2, -1].map((n) => shiftMonth(today.slice(0, 7), n)),
    conversions: conversions || [],
    payroll: (journals && journals.length) || projection ? { journals: journals || [], projection: projection || null } : null,
    ilsVendors: Array.isArray(ilsVendors) ? ilsVendors : null,
    warnings,
  };
}

/** The NetSuite reads, through the dashboard's NetSuite client (ns()) and its call queue. env() gives the
 * environment once the checkout's .env is loaded, so the RESTlet setting is read when the page is. */
function nsReads({ ns, queue, env = () => process.env }) {
  return {
    rates: (days = 400) => queue(() => ns().fetchEurIlsRates({ days, provider: 'HSBC' })),
    books: () => queue(() => ns().fetchEurBalancesBooks()),
    get mail() {
      const ids = restletIds(env().NS_BALANCES_RESTLET);
      return ids ? () => queue(() => ns().restletGet(ids.script, ids.deploy)) : null;
    },
    flows: (from) => queue(() => ns().fetchEurFlows({ from })),
    conversions: (from) => queue(() => ns().fetchHapoalimConversions({ from })),
    payroll: (from) => queue(() => ns().fetchPayrollLiabilities({ from })),
    ilsVendors: (from) => queue(() => ns().fetchIlsVendorPayments({ from })),
    salary: () => readSalaryProjection(),
  };
}

// ── handlers ────────────────────────────────────────────────────────────────
function envMinutes(name, fallback) {
  const v = parseFloat(process.env[name] || '');
  return Number.isFinite(v) && v > 0 ? v : fallback;
}

/** The reads through the dashboard's NetSuite client: deps.getNsClient(sub) and deps.queueNsCall(fn), by default
 * the cash projection's. The checkout's .env is loaded on the first read. */
function defaultNsReads(deps = {}) {
  const cmp = () => require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs')); // loads the checkout's .env
  const ns = () => {
    const sub = cmp().SUBSIDIARY;
    const c = deps.getNsClient ? deps.getNsClient(sub) : require('./cash-projection.cjs').defaultGetNsClient(sub);
    if (!c) throw new Error('NetSuite is not configured');
    return c;
  };
  const queue = deps.queueNsCall || require('./cash-projection.cjs').defaultQueueNsCall;
  return nsReads({ ns, queue, env: () => { cmp(); return process.env; } });
}

/**
 * Express/connect handler for GET /api/fx-challenge. deps (all optional):
 *   getNsClient(sub), queueNsCall(fn)  the dashboard's NetSuite client and call queue; default: the cash
 *                                      projection's (finance-it's own route file passes none)
 *   reads      override every NetSuite read (tests): { rates(days), books, mail, flows(from), conversions(from) }
 *   tableFile, clock(), ttlMs (default FX_CHALLENGE_TTL_MIN, 30 minutes)
 */
function createFxChallengeHandler(deps = {}) {
  const clock = deps.clock || Date.now;
  const ttlMs = deps.ttlMs ?? envMinutes('FX_CHALLENGE_TTL_MIN', 30) * 60 * 1000;
  const tableFile = deps.tableFile || TABLE_FILE;
  const reads = deps.reads || defaultNsReads(deps);

  const memo = new Map();
  const cached = (key, fn) => {
    const hit = memo.get(key);
    if (hit && clock() - hit.at < ttlMs) return hit.p;
    const p = Promise.resolve().then(fn);
    memo.set(key, { at: clock(), p });
    p.catch(() => { if (memo.get(key) && memo.get(key).p === p) memo.delete(key); });
    return p;
  };

  return async function fxChallengeHandler(req, res) {
    if ((req.method || 'GET').toUpperCase() !== 'GET') {
      send(res, 405, { ok: false, status: 'error', error: 'Method not allowed' }, { Allow: 'GET' });
      return;
    }
    let refresh = false;
    try {
      refresh = /^(1|true)$/.test(new URL(req.url || '', 'http://localhost').searchParams.get('refresh') || '');
    } catch { /* plain read */ }
    if (refresh) memo.clear();
    try {
      send(res, 200, await readFxChallenge({ reads, cached, tableDoc: readDoc(tableFile), clock }));
    } catch (e) {
      console.error(`[fx-challenge] failed: ${e && e.stack ? e.stack : e}`);
      send(res, 200, { ok: false, status: 'error', error: 'The FX Challenge could not be read. The server log has the details.' });
    }
  };
}

/** GET/PUT /api/fx-challenge/table: the table everyone sees, with who saved it; each save is kept in the history. */
function createFxTableHandler(deps = {}) {
  // 64KB: fifteen months of the table are about 6KB, the 60 allowed about 25KB.
  return createDocHandler({ file: deps.file || TABLE_FILE, label: 'fx-challenge-table', maxBytes: 64 * 1024, validate: validateTable, empty: () => null, clock: deps.clock });
}

/**
 * Every FX Challenge route behind one handler, by the path after /api/fx-challenge: '' the page's data, /table the
 * table, /mail the daily mail's recipients; anything else 404. finance-it mounts the prefix once
 * (app.use('/api/fx-challenge', role, …)), so a route added here needs no change there. The path is read from
 * req.originalUrl when the mount strips it from req.url.
 *   deps: as createFxChallengeHandler, plus tableFile, mail (createFxMailHandler's deps)
 */
function createFxChallengeRouter(deps = {}) {
  const routes = {
    '': createFxChallengeHandler(deps),
    '/table': createFxTableHandler({ file: deps.tableFile, clock: deps.clock }),
    '/mail': createFxMailHandler({ clock: deps.clock, ...deps.mail }),
  };
  return function fxChallengeRouter(req, res) {
    let p = '';
    try { p = new URL(req.originalUrl || req.url || '/', 'http://localhost').pathname; } catch { /* the page's data */ }
    const route = routes[p.replace(/^\/api\/fx-challenge/, '').replace(/\/+$/, '')];
    if (!route) { send(res, 404, { ok: false, error: 'Not found' }); return undefined; }
    return route(req, res);
  };
}

module.exports = {
  createFxChallengeRouter, createFxChallengeHandler, createFxTableHandler, createFxMailHandler, validateTable, validateMailSettings,
  readFxChallenge, readSalaryProjection, nsReads, defaultNsReads, restletIds, bookRow, mailRows, israelDay, loadFxModule,
  splitAddresses, mailHourOf, savedRecipients, checkoutEnv, LSPORTS_ADDRESS, TABLE_FILE, MAIL_FILE, MAIL_SENT_FILE,
};
