// ─────────────────────────────────────────────────────────────────────────────
// Links from a breakdown row to the NetSuite account's register, shared by the New Bank Dashboard and
// the P&L Projection breakdowns. The register opens for a date range (NetSuite's m/d/yyyy), on
// subsidiary LSports Data. No account id (projection cached before the ids) or no NETSUITE_ACCOUNT_ID
// → no link.
// ─────────────────────────────────────────────────────────────────────────────

const lastDay = (y, m) => new Date(y, m, 0).getDate();
const usDate = (y, m, d) => `${m}/${d}/${y}`;

/** [from, to] of a 'YYYY-MM' month as NetSuite report dates (m/d/yyyy). */
function monthSpan(mKey) {
  const [y, m] = String(mKey).split('-').map(Number);
  return [usDate(y, m, 1), usDate(y, m, lastDay(y, m))];
}

/** [from, to] covering a list of 'YYYY-MM' months, or null for none. */
function rangeSpan(months) {
  const list = [...new Set((months || []).filter((m) => /^\d{4}-\d{2}$/.test(m)))].sort();
  return list.length ? [monthSpan(list[0])[0], monthSpan(list[list.length - 1])[1]] : null;
}

/** The account register in NetSuite for a date range, or null without an account id or NetSuite host. */
function registerLink(accountIds, acct, span) {
  const host = String(process.env.NETSUITE_ACCOUNT_ID || '').replace(/_/g, '-').toLowerCase();
  const id = accountIds && accountIds[acct];
  if (!/^[a-z0-9-]+$/.test(host) || !id || !span || !/^\d{4,6}$/.test(String(acct))) return null;
  return `https://${host}.app.netsuite.com/app/reporting/reportrunner.nl?acctid=${id}&reporttype=REGISTER&subsidiary=3&combinebalance=T&startdate=${span[0]}&enddate=${span[1]}`;
}

module.exports = { monthSpan, rangeSpan, registerLink };
