// ─────────────────────────────────────────────────────────────────────────────
// P&L Projection: which line every NetSuite income-statement account belongs to.
//
// The lines follow NetSuite's "EBITDA_Profit and Loss" report (subsidiary 3):
//   Sales (all 4xxxxx Income)  −  Overheads (6xxxxx–7xxxxx incl. payroll, 800011, 950000)
//   = Operating Profit (EBITDA)
// and continue below EBITDA with the accounts that report leaves out (FX revaluation, the other
// 800xxx finance accounts, depreciation, tax / IFRS16 / equity, FA gain/loss), so Net profit is the
// sum of every P&L account. Every account maps to exactly one line, so nothing is dropped.
//
// Amounts handled with these lines are profit-signed (credit − debit): revenue positive, costs
// negative. Display signs are the frontend's job.
// ─────────────────────────────────────────────────────────────────────────────

// Customer revenue (what Snowflake's monthly revenue forecasts). Every other Income account (I/C with
// Statscore / Bringits, sub-lease, FA sales, employees' interest, …) is "Other & intercompany revenue".
const CUSTOMER_REVENUE = new Set(['4000', '400001', '400002', '400008', '400009', '400010', '400011', '400018', '400026', '4050']);
const FX_ACCOUNTS = new Set(['800028', '800029', '800030', '800031']);
// Listed under Overheads by the EBITDA report although it is an 800xxx account.
const OVERHEAD_800 = new Set(['800011']);
// Finance accounts outside the 80xxxx range: interest payable, IFRS 16 financing.
const FINANCE_OTHER = new Set(['190002', '900002', '900003']);

/** Line keys in table order (profit-signed groups). */
const LINE_KEYS = ['revenue', 'otherRevenue', 'payroll', 'capex', 'opex', 'fx', 'finance', 'depreciation', 'taxOther'];

const LINE_LABELS = {
  revenue: 'Customer revenue',
  pipeline: 'Pipeline',
  churn: 'Churn',
  otherRevenue: 'Other & intercompany revenue',
  payroll: 'Payroll',
  capex: 'Salaries CAPEX',
  opex: 'Operating expenses',
  fx: 'FX revaluation',
  finance: 'Finance, net',
  depreciation: 'Depreciation',
  taxOther: 'Tax & other',
};

/** The line of one account. type = NetSuite accttype (Income, COGS, Expense, OthIncome, OthExpense). */
function classifyAccount(acct, type) {
  const a = String(acct || '');
  const t = String(type || '');
  if (a === '950000') return 'capex';
  if (t === 'Income') return CUSTOMER_REVENUE.has(a) || a.startsWith('410') ? 'revenue' : 'otherRevenue';
  if (a.startsWith('76')) return 'payroll';
  if (a.startsWith('7805')) return 'depreciation';
  if (FX_ACCOUNTS.has(a)) return 'fx';
  if (OVERHEAD_800.has(a)) return 'opex';
  if (a.startsWith('80') || FINANCE_OTHER.has(a)) return 'finance';
  if (t === 'COGS' || a.startsWith('5')) return 'opex';
  if (t === 'Expense' && (a.startsWith('6') || a.startsWith('7'))) return 'opex';
  return 'taxOther'; // 9xxxxx tax / IFRS16 / equity share, 780030 FA gain/loss, 180018 WHT clearing, anything new
}

/** True for the lines inside EBITDA (Operating Profit of the EBITDA report). */
const ABOVE_EBITDA = new Set(['revenue', 'otherRevenue', 'payroll', 'capex', 'opex']);

const r2 = (n) => Math.round((Number(n) || 0) * 100) / 100;

/**
 * Sums one month of NetSuite accounts ({ [acct]: { acct, name, type, eur, ils } }) into lines.
 * Returns { totals: { [line]: { eur, ils } }, accounts: { [line]: [{ acct, name, eur, ils }] } }, profit-signed.
 */
function sumByLine(monthAccounts) {
  const totals = Object.fromEntries(LINE_KEYS.map((k) => [k, { eur: 0, ils: 0 }]));
  const accounts = Object.fromEntries(LINE_KEYS.map((k) => [k, []]));
  for (const a of Object.values(monthAccounts || {})) {
    const line = classifyAccount(a.acct, a.type);
    totals[line].eur += Number(a.eur) || 0;
    totals[line].ils += Number(a.ils) || 0;
    if (Math.abs(a.eur) >= 0.005 || Math.abs(a.ils) >= 0.005) accounts[line].push({ acct: a.acct, name: a.name, eur: r2(a.eur), ils: r2(a.ils) });
  }
  for (const k of LINE_KEYS) {
    totals[k] = { eur: r2(totals[k].eur), ils: r2(totals[k].ils) };
    accounts[k].sort((x, y) => Math.abs(y.eur) - Math.abs(x.eur));
  }
  return { totals, accounts };
}

module.exports = { classifyAccount, sumByLine, LINE_KEYS, LINE_LABELS, ABOVE_EBITDA, CUSTOMER_REVENUE, FX_ACCOUNTS };
