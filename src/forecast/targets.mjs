// ─────────────────────────────────────────────────────────────────────────────
// Projection-year targets ("2027 targets"): drivers set on top of the Plan, for the projection year only.
// Pure — no I/O — shared by the browser (live preview, both projection pages) and the server
// (validation of what is saved). The current year and its December closing never change.
//
// The pages carry a small baseline per projection month (payload.targetsBase, built server-side):
//   { mKey, revenue, collPct, payroll, payrollByDept: { dept: € }, opex, opexByCategory: { cat: € }, ilsRate }
// computeTargetDeltas(base, targets) turns the drivers into € changes per month; applyCashTargets /
// applyPnlTargets add them to a year of the page's rows (₪ at each month's rate).
//
// Drivers (each a whole-year value, with optional values per month):
//   revenue  mode 'growth': revenue × Π(1 + growth%) to that month   | mode 'newMrr': revenue + Σ new MRR
//            then × Π(1 − churn%)                                    | mode 'none': no revenue change
//   payroll  department % from a month ('*' = all of payroll); hires: monthly cost × count from a month
//   opex     % by vendor category
//   server   one category (Cloud Infrastructure & DevOps by default) = X% of the revenue after targets;
//            it replaces that category's baseline, and the category % above no longer applies to it
// ─────────────────────────────────────────────────────────────────────────────

export const TARGETS_VERSION = 1;
export const ALL_DEPARTMENTS = '*';
const N = 12;

const zeros = () => Array(N).fill(0);
const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;

/** Targets that change nothing. */
export function emptyTargets() {
  return {
    revenue: { mode: 'none', growthPct: zeros(), newMrr: zeros(), churnPct: zeros() },
    payroll: { deptPct: [], hires: [] },
    opex: { categoryPct: {} },
    server: { enabled: false, pctOfRevenue: 8, category: '' },
  };
}

// ── validation (server: what may be saved; browser: before saving) ──────────
const LIMITS = {
  growthPct: [-50, 50], newMrr: [-10_000_000, 10_000_000], churnPct: [0, 50],
  deptPct: [-50, 100], monthlyCost: [0, 1_000_000], count: [1, 500], categoryPct: [-100, 300], pctOfRevenue: [0, 100],
};
const TEXT_MAX = 120;
const RESERVED = new Set(['__proto__', 'constructor', 'prototype']); // never object keys

function onlyKeys(obj, allowed, where, errors) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) { errors.push(`${where}: expected an object`); return false; }
  for (const k of Object.keys(obj)) if (!allowed.includes(k)) errors.push(`${where}: unknown field "${k}"`);
  return true;
}
function numberIn(v, [lo, hi], where, errors) {
  const n = Number(v);
  if (typeof v !== 'number' || !Number.isFinite(n) || n < lo || n > hi) { errors.push(`${where}: a number from ${lo} to ${hi}`); return 0; }
  return n;
}
function monthIn(v, where, errors) {
  if (!Number.isInteger(v) || v < 1 || v > 12) { errors.push(`${where}: a month from 1 to 12`); return 1; }
  return v;
}
function text(v, where, errors) {
  if (typeof v !== 'string' || !v.trim() || v.length > TEXT_MAX || /[\u0000-\u001f]/.test(v) || RESERVED.has(v.trim())) {
    errors.push(`${where}: a name of 1–${TEXT_MAX} characters`);
    return '';
  }
  return v.trim();
}
function months12(v, limits, where, errors) {
  if (!Array.isArray(v) || v.length !== N) { errors.push(`${where}: 12 monthly values`); return zeros(); }
  return v.map((x, i) => numberIn(x, limits, `${where}[${i + 1}]`, errors));
}

/** { ok, targets, errors }: the targets normalised, or why they cannot be saved. Unknown fields are refused. */
export function validateTargets(input) {
  const errors = [];
  const out = emptyTargets();
  if (!onlyKeys(input, ['revenue', 'payroll', 'opex', 'server'], 'targets', errors)) return { ok: false, targets: null, errors };
  const { revenue = {}, payroll = {}, opex = {}, server = {} } = input;

  if (onlyKeys(revenue, ['mode', 'growthPct', 'newMrr', 'churnPct'], 'revenue', errors)) {
    if (revenue.mode !== undefined && !['none', 'growth', 'newMrr'].includes(revenue.mode)) errors.push('revenue.mode: none, growth or newMrr');
    else if (revenue.mode !== undefined) out.revenue.mode = revenue.mode;
    if (revenue.growthPct !== undefined) out.revenue.growthPct = months12(revenue.growthPct, LIMITS.growthPct, 'revenue.growthPct', errors);
    if (revenue.newMrr !== undefined) out.revenue.newMrr = months12(revenue.newMrr, LIMITS.newMrr, 'revenue.newMrr', errors);
    if (revenue.churnPct !== undefined) out.revenue.churnPct = months12(revenue.churnPct, LIMITS.churnPct, 'revenue.churnPct', errors);
  }
  if (onlyKeys(payroll, ['deptPct', 'hires'], 'payroll', errors)) {
    const dp = payroll.deptPct === undefined ? [] : payroll.deptPct;
    if (!Array.isArray(dp) || dp.length > 50) errors.push('payroll.deptPct: up to 50 entries');
    else {
      out.payroll.deptPct = dp.map((d, i) => {
        const where = `payroll.deptPct[${i + 1}]`;
        if (!onlyKeys(d, ['dept', 'pct', 'from'], where, errors)) return null;
        return { dept: d.dept === ALL_DEPARTMENTS ? ALL_DEPARTMENTS : text(d.dept, `${where}.dept`, errors), pct: numberIn(d.pct, LIMITS.deptPct, `${where}.pct`, errors), from: monthIn(d.from, `${where}.from`, errors) };
      }).filter(Boolean);
    }
    const hires = payroll.hires === undefined ? [] : payroll.hires;
    if (!Array.isArray(hires) || hires.length > 200) errors.push('payroll.hires: up to 200 entries');
    else {
      out.payroll.hires = hires.map((h, i) => {
        const where = `payroll.hires[${i + 1}]`;
        if (!onlyKeys(h, ['dept', 'monthlyCost', 'start', 'count'], where, errors)) return null;
        return {
          dept: text(h.dept, `${where}.dept`, errors), monthlyCost: numberIn(h.monthlyCost, LIMITS.monthlyCost, `${where}.monthlyCost`, errors),
          start: monthIn(h.start, `${where}.start`, errors), count: numberIn(h.count, LIMITS.count, `${where}.count`, errors),
        };
      }).filter(Boolean);
    }
  }
  if (onlyKeys(opex, ['categoryPct'], 'opex', errors)) {
    const cp = opex.categoryPct === undefined ? {} : opex.categoryPct;
    if (!cp || typeof cp !== 'object' || Array.isArray(cp) || Object.keys(cp).length > 60) errors.push('opex.categoryPct: up to 60 categories');
    else {
      for (const [cat, pct] of Object.entries(cp)) {
        const name = text(cat, 'opex.categoryPct (category)', errors);
        if (name) out.opex.categoryPct[name] = numberIn(pct, LIMITS.categoryPct, `opex.categoryPct.${name}`, errors);
      }
    }
  }
  if (onlyKeys(server, ['enabled', 'pctOfRevenue', 'category'], 'server', errors)) {
    if (server.enabled !== undefined && typeof server.enabled !== 'boolean') errors.push('server.enabled: true or false');
    else if (server.enabled !== undefined) out.server.enabled = server.enabled;
    if (server.pctOfRevenue !== undefined) out.server.pctOfRevenue = numberIn(server.pctOfRevenue, LIMITS.pctOfRevenue, 'server.pctOfRevenue', errors);
    if (server.category !== undefined && server.category !== '') out.server.category = text(server.category, 'server.category', errors);
    if (out.server.enabled && !out.server.category) errors.push('server.category: choose the category the % replaces');
  }
  return errors.length ? { ok: false, targets: null, errors } : { ok: true, targets: out, errors: [] };
}

/** True when the targets change nothing (the Targets view then equals the Plan). */
export function isEmptyTargets(t) {
  if (!t) return true;
  const any = (a) => a.some((x) => x !== 0);
  const r = t.revenue;
  return (r.mode === 'none' || (r.mode === 'growth' ? !any(r.growthPct) : !any(r.newMrr))) && !any(r.churnPct)
    && !t.payroll.deptPct.some((d) => d.pct !== 0) && !t.payroll.hires.some((h) => h.monthlyCost !== 0)
    && !Object.values(t.opex.categoryPct).some((p) => p !== 0) && !t.server.enabled;
}

// ── in plain words (a page that applies the saved targets without editing them) ──
const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const groupThousands = (n) => String(Math.round(Math.abs(n))).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
const pctText = (n) => `${Number(n.toFixed(2))}%`;
const signedPct = (n) => `${n > 0 ? '+' : ''}${pctText(n)}`;
const eurText = (n) => `${n < 0 ? '-' : ''}€${groupThousands(n)}`;

// Runs of equal months, zeros left out: 'Jan–Jun 3%, Sep 1%'.
function monthRuns(values, fmt) {
  const out = [];
  for (let i = 0; i < values.length;) {
    let j = i;
    while (j + 1 < values.length && values[j + 1] === values[i]) j++;
    if (values[i] !== 0) out.push(`${MONTH_NAMES[i]}${j > i ? `–${MONTH_NAMES[j]}` : ''} ${fmt(values[i])}`);
    i = j + 1;
  }
  return out.join(', ');
}

/** One line per driver that changes something, e.g. 'Revenue growth 3% a month, compounding'. */
export function describeTargets(t) {
  if (!t) return [];
  const lines = [];
  const same = (a) => a.every((v) => v === a[0]);
  const any = (a) => a.some((v) => v !== 0);
  const r = t.revenue;
  if (r.mode === 'growth' && any(r.growthPct)) {
    lines.push(same(r.growthPct) ? `Revenue growth ${pctText(r.growthPct[0])} a month, compounding`
      : `Revenue growth a month, compounding: ${monthRuns(r.growthPct, pctText)}`);
  }
  if (r.mode === 'newMrr' && any(r.newMrr)) {
    lines.push(same(r.newMrr) ? `New MRR ${eurText(r.newMrr[0])} a month, cumulative`
      : `New MRR a month, cumulative: ${monthRuns(r.newMrr, eurText)}`);
  }
  if (any(r.churnPct)) {
    lines.push(same(r.churnPct) ? `Churn ${pctText(r.churnPct[0])} of revenue a month`
      : `Churn a month (% of revenue): ${monthRuns(r.churnPct, pctText)}`);
  }
  for (const d of t.payroll.deptPct) {
    if (d.pct !== 0) lines.push(`Salaries ${signedPct(d.pct)} for ${d.dept === ALL_DEPARTMENTS ? 'all departments' : d.dept} from ${MONTH_NAMES[d.from - 1]}`);
  }
  for (const h of t.payroll.hires) {
    if (h.monthlyCost !== 0) lines.push(`${h.count} ${h.count === 1 ? 'hire' : 'hires'} in ${h.dept} at ${eurText(h.monthlyCost)} a month each from ${MONTH_NAMES[h.start - 1]}`);
  }
  const serverCat = t.server.enabled ? t.server.category : '';
  for (const [cat, pct] of Object.entries(t.opex.categoryPct).sort(([a], [b]) => a.localeCompare(b))) {
    if (pct !== 0 && cat !== serverCat) lines.push(`Operating expenses, ${cat}: ${signedPct(pct)}`);
  }
  if (serverCat) lines.push(`Server costs (${serverCat}) at ${pctText(t.server.pctOfRevenue)} of revenue`);
  return lines;
}

// ── baseline (server) ───────────────────────────────────────────────────────
const shares = (byKey) => {
  const entries = Object.entries(byKey || {}).map(([k, v]) => [k, Number(v) || 0]);
  const total = entries.reduce((s, [, v]) => s + v, 0);
  return Math.abs(total) >= 0.5 ? Object.fromEntries(entries.map(([k, v]) => [k, v / total])) : {};
};
const spread = (amount, sh) => Object.fromEntries(Object.entries(sh).map(([k, s]) => [k, round2(amount * s)]));

/**
 * The baseline the pages carry for one projection year.
 *   months: [{ mKey, revenue, collPct, payroll, opex, ilsRate }]   (the Plan's projection months, €)
 *   deptAmounts: { dept: € } — payroll by department of the basis month (its shares split each month)
 *   categoryAmountsByMonth: { 'MM': { category: € } } — the vendor budget by category of the same month
 *     of the current year (its shares split each month's operating expenses)
 *   serverCategory: the default category of the server driver; serverRatioYtd: reference (or null)
 */
export function buildTargetsBase({ year, months, deptAmounts, categoryAmountsByMonth, serverCategory = '', serverRatioYtd = null }) {
  const deptShares = shares(deptAmounts);
  const cats = new Set();
  const out = months.map((m) => {
    const catShares = shares((categoryAmountsByMonth || {})[String(m.mKey).slice(5)]);
    Object.keys(catShares).forEach((c) => cats.add(c));
    return {
      mKey: m.mKey, revenue: round2(m.revenue), collPct: Number.isFinite(m.collPct) ? m.collPct : 100,
      payroll: round2(m.payroll), payrollByDept: spread(m.payroll, deptShares),
      opex: round2(m.opex), opexByCategory: spread(m.opex, catShares),
      ilsRate: Number.isFinite(m.ilsRate) && m.ilsRate > 0 ? m.ilsRate : 0,
    };
  });
  const categories = [...cats].sort();
  const server = serverCategory && categories.includes(serverCategory) ? serverCategory : (categories.find((c) => /cloud/i.test(c)) || '');
  return {
    version: TARGETS_VERSION, year, months: out, departments: Object.keys(deptShares).sort(), categories,
    serverCategory: server, serverRatioYtd: Number.isFinite(serverRatioYtd) ? serverRatioYtd : null,
  };
}

// ── deltas and their effect on a page's year ─────────────────────────────────
/** € changes per projection month: { mKey, dRevenue, dPayroll, dOpex, dServer, revenue, server }. */
export function computeTargetDeltas(base, targets) {
  const t = targets || emptyTargets();
  const r = t.revenue;
  let growth = 1;
  let added = 0;
  let kept = 1;
  const serverCat = t.server.enabled ? t.server.category : '';
  return base.months.map((m, i) => {
    growth *= 1 + (r.growthPct[i] || 0) / 100;
    added += r.newMrr[i] || 0;
    kept *= 1 - (r.churnPct[i] || 0) / 100;
    const lifted = r.mode === 'growth' ? m.revenue * growth : r.mode === 'newMrr' ? m.revenue + added : m.revenue;
    const revenue = lifted * kept;
    let dPayroll = 0;
    for (const d of t.payroll.deptPct) {
      if (i + 1 < d.from) continue;
      const amount = d.dept === ALL_DEPARTMENTS ? m.payroll : (m.payrollByDept[d.dept] || 0);
      dPayroll += amount * d.pct / 100;
    }
    for (const h of t.payroll.hires) if (i + 1 >= h.start) dPayroll += h.monthlyCost * h.count;
    let dOpex = 0;
    for (const [cat, pct] of Object.entries(t.opex.categoryPct)) {
      if (cat !== serverCat) dOpex += (m.opexByCategory[cat] || 0) * pct / 100;
    }
    const serverBase = serverCat ? (m.opexByCategory[serverCat] || 0) : 0;
    const server = serverCat ? revenue * t.server.pctOfRevenue / 100 : serverBase;
    return {
      mKey: m.mKey, dRevenue: round2(revenue - m.revenue), dPayroll: round2(dPayroll), dOpex: round2(dOpex),
      dServer: round2(server - serverBase), revenue: round2(revenue), server: round2(server), serverBase: round2(serverBase),
    };
  });
}

const addTo = (fig, k, v) => { fig[k] = round2((fig[k] || 0) + v); };

/** The cash page's year with the deltas: collections × the month's collection %, salary, vendors, balances carried. */
export function applyCashTargets(block, base, deltas) {
  const byKey = new Map(deltas.map((d) => [d.mKey, d]));
  const baseByKey = new Map(base.months.map((m) => [m.mKey, m]));
  const cum = { eur: 0, ils: 0 };
  const rows = block.rows.map((row) => {
    const d = byKey.get(row.mKey);
    const b = baseByKey.get(row.mKey);
    if (!d || !b) return row;
    const eur = { ...row.eur };
    const ils = { ...row.ils };
    const dColl = d.dRevenue * b.collPct / 100;
    const dCost = d.dOpex + d.dServer;
    const dNet = dColl - d.dPayroll - dCost;
    for (const [fig, k, rate] of [[eur, 'eur', 1], [ils, 'ils', b.ilsRate]]) {
      addTo(fig, 'collections', dColl * rate);
      addTo(fig, 'salary', d.dPayroll * rate);
      addTo(fig, 'vendors', dCost * rate);
      addTo(fig, 'net', dNet * rate);
      addTo(fig, 'opening', cum[k]);
      cum[k] += dNet * rate;
      addTo(fig, 'closing', cum[k]);
    }
    return { ...row, eur, ils };
  });
  return { ...block, rows };
}

/** The P&L page's year with the deltas: revenue, payroll, opex and everything summed from them. */
export function applyPnlTargets(block, base, deltas) {
  const byKey = new Map(deltas.map((d) => [d.mKey, d]));
  const baseByKey = new Map(base.months.map((m) => [m.mKey, m]));
  const cum = { eur: 0, ils: 0 };
  const rows = block.rows.map((row) => {
    const d = byKey.get(row.mKey);
    const b = baseByKey.get(row.mKey);
    if (!d || !b) return row;
    const eur = { ...row.eur };
    const ils = { ...row.ils };
    const dCost = d.dPayroll + d.dOpex + d.dServer;
    const dProfit = d.dRevenue - dCost;
    for (const [fig, k, rate] of [[eur, 'eur', 1], [ils, 'ils', b.ilsRate]]) {
      addTo(fig, 'revenue', d.dRevenue * rate);
      addTo(fig, 'totalRevenue', d.dRevenue * rate);
      addTo(fig, 'payroll', d.dPayroll * rate);
      addTo(fig, 'opex', (d.dOpex + d.dServer) * rate);
      addTo(fig, 'totalCosts', dCost * rate);
      addTo(fig, 'ebitda', dProfit * rate);
      addTo(fig, 'net', dProfit * rate);
      addTo(fig, 'accOpening', cum[k]);
      cum[k] += dProfit * rate;
      addTo(fig, 'accClosing', cum[k]);
    }
    return { ...row, eur, ils };
  });
  return { ...block, rows };
}

/** A variant ({ years, … }) with the targets applied to its projection year. */
export function variantWithTargets(variant, base, targets, apply) {
  if (!variant || !base) return variant;
  const deltas = computeTargetDeltas(base, targets);
  return { ...variant, years: variant.years.map((y) => (y.year === base.year ? apply(y, base, deltas) : y)) };
}
