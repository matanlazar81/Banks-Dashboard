// ─────────────────────────────────────────────────────────────────────────────
// GET /api/metrics?view=fx — the Metrics page's FX Challenge tab (server/metrics.cjs routes it here).
//
// The page simulates the monthly EUR→ILS conversions on the table (src/forecast/fx-challenge.mjs) and says how
// much EUR to keep for them. This module gathers what it needs; each read is cached by the Metrics handler, and a
// failed read becomes a warning while the rest still shows:
//   rates        EUR/ILS by day (NetSuite currencyrate, HSBC), the last 400 days: today's rate and the volatility
//   balances     the daily balances mail's figures, from the RESTlet next to it (NS_BALANCES_RESTLET =
//                script:deploy); without it, or when it fails, NetSuite's books (EUR accounts in the primary book:
//                exact in euros, but no deposit maturities)
//   flows        EUR in and out by month, from four months back: the typical net inflow
//   conversions  "Conversion" / המרה journals Hapoalim EUR → ILS (583400), from just before the table's first
//                fixing: what each past fixing booked
// The table is the saved one (Metrics settings, fxChallenge) or the default.
// ─────────────────────────────────────────────────────────────────────────────

const path = require('path');
const { pathToFileURL } = require('url');

const ROOT = path.join(__dirname, '..');
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

/**
 * The FX Challenge payload.
 *   reads     { rates(), books(), mail (null when the RESTlet is not configured), flows(fromMonth), conversions(fromDay) }
 *   cached    (key, fn) → promise, the Metrics handler's memo
 *   settingsDoc  the saved Metrics settings document ({ value, updatedAt, updatedBy }) or null
 */
async function readFxChallenge({ reads, cached = (k, fn) => fn(), settingsDoc = null, clock = Date.now }) {
  const fx = await loadFxModule();
  const mail = reads.mail;
  const nowMs = clock();
  const today = israelDay(nowMs);
  const saved = settingsDoc && settingsDoc.value && settingsDoc.value.fxChallenge;
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
    console.warn(`[metrics] fx ${what} unavailable: ${settled[i].reason && settled[i].reason.message}`);
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
      console.warn(`[metrics] fx mail balances unusable: ${e && e.message}`);
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
  if (!conversions) warnings.push('The booked conversions could not be read, so past fixings are not checked.');

  return {
    ok: true,
    status: 'ready',
    view: 'fx',
    computedAt: new Date(nowMs).toISOString(),
    today,
    table,
    tableSaved: saved ? { at: settingsDoc.updatedAt || null, by: settingsDoc.updatedBy || null } : null,
    rates: { provider: 'HSBC', series, spot, sigma },
    balances,
    flows: flows || [],
    inflowMonths: [-3, -2, -1].map((n) => shiftMonth(today.slice(0, 7), n)),
    conversions: conversions || [],
    warnings,
  };
}

/** The NetSuite reads, through the dashboard's NetSuite client (ns()) and its call queue. env() gives the
 * environment once the checkout's .env is loaded, so the RESTlet setting is read when the tab is. */
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

module.exports = { readFxChallenge, nsReads, restletIds, bookRow, mailRows, israelDay };
