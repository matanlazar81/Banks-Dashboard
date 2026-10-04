// ─────────────────────────────────────────────────────────────────────────────
// One account of a breakdown by department: the second window of the New Bank Dashboard and the
// P&L Projection. A breakdown row that is an account carries `drill`, the parts its amount was built
// from: { source, month, sign } = that source's amount for the account in that month × sign. The
// departments come from the same source and months, so they add up to the row; a "Difference to the
// account row" line closes any gap.
//   source 'ns'        NetSuite GL, profit-signed, on the P&L actuals' month basis
//   source 'sfExpense' Snowflake FCT_EXPENSE (booked costs, debit-positive), as the cash page's actuals
//   source 'sfBudget'  Snowflake FCT_BUDGET (budget, costs positive), as the forecast rows
// ─────────────────────────────────────────────────────────────────────────────

const SOURCE_LABELS = { ns: 'NetSuite', sfExpense: 'Snowflake booked expenses', sfBudget: 'Snowflake budget' };
const ACTUAL_SOURCES = new Set(['ns', 'sfExpense']);
const MONTHS_LONG = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const MIN = 60 * 1000;

const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const cents = (n) => Math.round(num(n) * 100) / 100;
const monthLong = (mKey) => {
  const [y, m] = String(mKey).split('-');
  return `${MONTHS_LONG[Number(m) - 1] || m} ${y}`;
};

/** The drill of a row built from `source` amounts of `months`, each × sign. */
function drillOf(source, months, sign) {
  return { parts: months.map((month) => ({ source, month, sign })) };
}

/** Drills of one row in two months (full years). A row that cannot be split in one of them cannot be split. */
function mergeDrill(a, b) {
  return a && b ? { parts: [...a.parts, ...b.parts] } : null;
}

/** Months whose amounts are booked (NetSuite or Snowflake actuals): the dates a link should open. */
function actualMonths(drill) {
  return drill ? [...new Set(drill.parts.filter((p) => ACTUAL_SOURCES.has(p.source)).map((p) => p.month))] : [];
}

/** Breakdown row key of a drillable account row: 'acct:640001', 'sf:640001', 'mirror:acct:640001'… */
const ROW_KEY = /^[a-z]+(:[a-z]+)*:(\d{4,6})$/;
const acctOfKey = (key) => {
  const m = ROW_KEY.exec(String(key || ''));
  return m ? m[2] : null;
};

/** { [dept]: € } of a row from its drill. dx: { ns(acct, months, basis), sfExpense(acct, months), sfBudget(acct, months) }. */
async function departmentsOf(row, acct, dx, nsBasis) {
  const weights = new Map(); // 'source|month' → Σ sign (a forecast can repeat one month several times)
  const monthsBySource = new Map();
  for (const p of row.drill.parts) {
    const k = `${p.source}|${p.month}`;
    weights.set(k, (weights.get(k) || 0) + p.sign);
    if (!monthsBySource.has(p.source)) monthsBySource.set(p.source, new Set());
    monthsBySource.get(p.source).add(p.month);
  }
  const out = new Map();
  for (const [source, months] of monthsBySource) {
    const rows = await dx[source](acct, [...months].sort(), nsBasis);
    for (const r of rows) {
      const w = weights.get(`${source}|${r.month}`) || 0;
      if (!w) continue;
      out.set(r.dept, (out.get(r.dept) || 0) + w * num(r.eur));
    }
  }
  return out;
}

function sourceText(drill) {
  const sources = [...new Set(drill.parts.map((p) => p.source))];
  const months = [...new Set(drill.parts.map((p) => p.month))].sort();
  const span = months.length === 1 ? monthLong(months[0]) : `${monthLong(months[0])} – ${monthLong(months[months.length - 1])}`;
  const repeats = drill.parts.length > months.length && sources.length === 1;
  return {
    period: `${span} · ${sources.map((s) => SOURCE_LABELS[s]).join(' + ')}`,
    note: repeats
      ? `The forecast repeats ${months.length === 1 ? 'this month' : 'these months'}: each month's departments count as often as the row uses them.`
      : null,
  };
}

/**
 * The department window's response, in the breakdown shape the panel already renders.
 *   meta: the cell's breakdown (line, lineLabel, period, periodLabel, periodStatus, variant, ccy)
 *   row:  the account row (both currencies, with drill); depts: departmentsOf(); link: its register
 * ₪ follow the row's own ₪/€ ratio, so both currencies add up to the row.
 */
function departmentBreakdown({ meta, row, acct, depts, link }) {
  const ratio = Math.abs(row.eur) >= 0.5 ? row.ils / row.eur : 0;
  const items = [...depts.entries()]
    .map(([dept, eur]) => ({ key: `dept:${dept}`, label: dept, eur, ils: eur * ratio, kind: 'item' }))
    .sort((a, b) => Math.abs(b.eur) - Math.abs(a.eur));
  const diff = row.eur - items.reduce((s, r) => s + r.eur, 0);
  if (Math.abs(diff) >= 0.5) {
    items.push({
      key: 'diff', label: 'Difference to the account row', eur: diff, ils: diff * ratio, kind: 'adjust',
      hint: 'Amounts of this account that the source holds without a department, or postings it has not loaded yet.',
    });
  }
  const ccy = meta.ccy;
  const rows = items
    .map((r) => ({ key: r.key, label: r.label, ref: null, link: null, group: null, kind: r.kind, hint: r.hint || null, amount: cents(r[ccy]) }))
    .filter((r) => Math.abs(r.amount) >= 0.5);
  const src = sourceText(row.drill);
  return {
    ok: true, status: 'ready', line: meta.line, lineLabel: `${acct} ${row.label}`, period: meta.period,
    periodLabel: src.period, periodStatus: meta.periodStatus, variant: meta.variant, ccy,
    cell: cents(row[ccy]),
    account: { acct, name: row.label, link, of: `${meta.lineLabel} · ${meta.periodLabel}` },
    sections: [{
      id: 'departments', title: 'By department', note: src.note, collapsed: false, informational: false,
      rows, total: cents(rows.reduce((t, r) => t + r.amount, 0)),
    }],
    notes: [],
  };
}

/** The account row a department request is about, or null. Rows of the main section come first. */
function findDrillRow(sections, key) {
  for (const s of sections) {
    const hit = s.rows.find((r) => r.kind === 'item' && r.key === key && r.drill && r.drill.parts.length);
    if (hit) return hit;
  }
  return null;
}

/**
 * Department reads shared by all requests, cached per (source, account, months) for ttlMs; a failure is
 * not cached. NetSuite calls go through queueNsCall (one NetSuite queue for the whole server).
 */
function departmentReads({ getNs, queueNsCall, getSf, clock = Date.now, ttlMs = 30 * MIN, BreakdownError = Error }) {
  const memo = new Map();
  const cached = (key, fn) => {
    const hit = memo.get(key);
    if (hit && clock() - hit.at < ttlMs) return hit.p;
    const p = Promise.resolve().then(fn);
    memo.set(key, { at: clock(), p });
    p.catch(() => { if (memo.get(key) && memo.get(key).p === p) memo.delete(key); });
    return p;
  };
  const sfReads = () => require('./cash-projection-breakdown.cjs');
  return {
    ns(acct, months, basis = 'period') {
      return cached(`ns|${basis}|${acct}|${months.join(',')}`, () => {
        const ns = getNs ? getNs() : null;
        if (!ns || !ns.fetchAccountByDepartment) throw new BreakdownError('NetSuite is not configured on this server.');
        return (queueNsCall || ((fn) => fn()))(() => ns.fetchAccountByDepartment({ acct, months, basis }));
      });
    },
    sfExpense(acct, months) {
      return cached(`sfExpense|${acct}|${months.join(',')}`, () => {
        const sf = getSf();
        if (!sf) throw new BreakdownError('Snowflake is not configured on this server.');
        return sfReads().sfExpenseByAccountDept(sf, acct, months);
      });
    },
    sfBudget(acct, months) {
      return cached(`sfBudget|${acct}|${months.join(',')}`, () => {
        const sf = getSf();
        if (!sf) throw new BreakdownError('Snowflake is not configured on this server.');
        return sfReads().sfBudgetByAccountDept(sf, acct, months);
      });
    },
  };
}

module.exports = {
  drillOf, mergeDrill, actualMonths, acctOfKey, departmentsOf, departmentBreakdown, findDrillRow, departmentReads,
  ROW_KEY, SOURCE_LABELS,
};
