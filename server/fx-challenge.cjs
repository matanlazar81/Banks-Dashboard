// ─────────────────────────────────────────────────────────────────────────────
// The FX Challenge page (Business Tools → FX Challenge, fx-challenge.html):
//   GET /api/fx-challenge          what the page simulates on; ?refresh=true reads NetSuite again
//   GET/PUT /api/fx-challenge/table  the conversion table, saved for everyone (none saved = the default)
//
// The page simulates the monthly EUR→ILS conversions on the table (src/forecast/fx-challenge.mjs) and says how
// much EUR to keep for them. This module gathers what it needs; each read is cached, and a failed read becomes a
// warning while the rest still shows:
//   rates        EUR/ILS by day (NetSuite currencyrate, HSBC), the last 400 days: today's rate and the volatility
//   balances     the daily balances mail's figures, from the RESTlet next to it (NS_BALANCES_RESTLET =
//                script:deploy); without it, or when it fails, NetSuite's books (EUR accounts in the primary book:
//                exact in euros, but no deposit maturities)
//   flows        EUR in and out by month, from four months back: the typical net inflow
//   conversions  "Conversion" / המרה journals Hapoalim EUR → ILS (583400), from just before the table's first
//                fixing: what each past fixing booked
// The table is the saved one (data/fx-challenge-table.json) or the default.
// ─────────────────────────────────────────────────────────────────────────────

const path = require('path');
const { pathToFileURL } = require('url');
const { readDoc, createDocHandler, send } = require('./json-store.cjs');
const { keysOnly, numIn, textIn } = require('./metrics-settings.cjs');

const ROOT = path.join(__dirname, '..');
const TABLE_FILE = path.join(ROOT, 'data', 'fx-challenge-table.json');
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

/**
 * The FX Challenge payload.
 *   reads     { rates(), books(), mail (null when the RESTlet is not configured), flows(fromMonth), conversions(fromDay) }
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
  const firstMonth = [...table.rows].map((r) => r.month).sort()[0];
  const flowsFrom = shiftMonth(today.slice(0, 7), -4);
  const convFrom = `${shiftMonth(firstMonth, -1)}-20`;
  const warnings = [];

  const settled = await Promise.allSettled([
    cached('fx:rates', () => reads.rates()),
    mail ? cached('fx:mail', () => mail()) : Promise.resolve(null),
    cached('fx:books', () => reads.books()),
    cached(`fx:flows:${flowsFrom}`, () => reads.flows(flowsFrom)),
    cached(`fx:conversions:${convFrom}`, () => reads.conversions(convFrom)),
  ]);
  const val = (i, what) => {
    if (settled[i].status === 'fulfilled') return settled[i].value;
    console.warn(`[fx-challenge] ${what} unavailable: ${settled[i].reason && settled[i].reason.message}`);
    return undefined;
  };

  const series = val(0, 'rates') || [];
  if (!series.length) warnings.push('EUR/ILS could not be read from NetSuite, so the simulation starts from the rate you type.');
  const spot = fx.rateOn(series, today);
  const sigma = fx.annualVol(series);

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
    warnings,
  };
}

/** The NetSuite reads, through the dashboard's NetSuite client (ns()) and its call queue. env() gives the
 * environment once the checkout's .env is loaded, so the RESTlet setting is read when the page is. */
function nsReads({ ns, queue, env = () => process.env }) {
  return {
    rates: () => queue(() => ns().fetchEurIlsRates({ days: 400, provider: 'HSBC' })),
    books: () => queue(() => ns().fetchEurBalancesBooks()),
    get mail() {
      const ids = restletIds(env().NS_BALANCES_RESTLET);
      return ids ? () => queue(() => ns().restletGet(ids.script, ids.deploy)) : null;
    },
    flows: (from) => queue(() => ns().fetchEurFlows({ from })),
    conversions: (from) => queue(() => ns().fetchHapoalimConversions({ from })),
  };
}

// ── handlers ────────────────────────────────────────────────────────────────
function envMinutes(name, fallback) {
  const v = parseFloat(process.env[name] || '');
  return Number.isFinite(v) && v > 0 ? v : fallback;
}

/**
 * Express/connect handler for GET /api/fx-challenge. deps (all optional):
 *   getNsClient(sub), queueNsCall(fn)  the dashboard's NetSuite client and call queue; default: the cash
 *                                      projection's (finance-it's own route file passes none)
 *   reads      override every NetSuite read (tests): { rates, books, mail, flows(from), conversions(from) }
 *   tableFile, clock(), ttlMs (default FX_CHALLENGE_TTL_MIN, 30 minutes)
 */
function createFxChallengeHandler(deps = {}) {
  const clock = deps.clock || Date.now;
  const ttlMs = deps.ttlMs ?? envMinutes('FX_CHALLENGE_TTL_MIN', 30) * 60 * 1000;
  const tableFile = deps.tableFile || TABLE_FILE;
  const reads = deps.reads || (() => {
    const cmp = () => require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs')); // loads the checkout's .env
    const ns = () => {
      const sub = cmp().SUBSIDIARY;
      const c = deps.getNsClient ? deps.getNsClient(sub) : require('./cash-projection.cjs').defaultGetNsClient(sub);
      if (!c) throw new Error('NetSuite is not configured');
      return c;
    };
    const queue = deps.queueNsCall || require('./cash-projection.cjs').defaultQueueNsCall;
    return nsReads({ ns, queue, env: () => { cmp(); return process.env; } });
  })();

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

module.exports = {
  createFxChallengeHandler, createFxTableHandler, validateTable, readFxChallenge, nsReads, restletIds, bookRow, mailRows, israelDay,
  TABLE_FILE,
};
