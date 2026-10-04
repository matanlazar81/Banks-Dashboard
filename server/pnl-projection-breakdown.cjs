// ─────────────────────────────────────────────────────────────────────────────
// GET /api/pnl-projection/breakdown — what makes up one cell of the P&L Projection.
//
//   ?line=revenue|pipeline|churn|otherRevenue|payroll|capex|opex|fx|finance|depreciation|taxOther
//   &period=YYYY-MM | FY-YYYY   &variant=plan|base   &ccy=eur|ils
//
// Same contract as /api/cash-projection/breakdown (the page reuses its window). Every answer adds up
// to the table cell:
//   • actual months   — the NetSuite accounts of the line (the cell is their sum), plus a collapsed
//                       Snowflake FCT_EXPENSE check of the same accounts for cost lines
//   • forecast months — the components the projection used (expected revenue and deals, pipeline
//                       cohorts, payroll basis by department, budget by category, run-rate months, …)
//                       and labelled adjustment rows for what they do not explain
// Facts come from the server-only `details` saved with the cached projection; the Snowflake check is
// read on demand and cached for PNL_PROJECTION_TTL_MIN.
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');
const { classifyAccount, LINE_LABELS } = require('./pnl-lines.cjs');

const ROOT = path.resolve(__dirname, '..');
const MIN = 60 * 1000;

const LINES = ['revenue', 'pipeline', 'churn', 'otherRevenue', 'payroll', 'capex', 'opex', 'fx', 'finance', 'depreciation', 'taxOther'];
// Display sign of a profit-signed NetSuite amount on each line (costs are shown positive).
const SIGN = { revenue: 1, otherRevenue: 1, payroll: -1, capex: -1, opex: -1, fx: 1, finance: 1, depreciation: 1, taxOther: 1 };
const COST_LINES = new Set(['payroll', 'capex', 'opex', 'fx', 'finance', 'depreciation', 'taxOther']);
const MONTHS_LONG = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

const num = (n) => (Number.isFinite(Number(n)) ? Number(n) : 0);
const cents = (n) => Math.round(num(n) * 100) / 100;
const monthLong = (mKey) => {
  const [y, m] = String(mKey).split('-');
  return `${MONTHS_LONG[Number(m) - 1] || m} ${y}`;
};
const fmtEur = (n) => `€${Math.round(n).toLocaleString('en-US')}`;

class BreakdownError extends Error {}

// ── building blocks (both currencies per row, so one build serves € and ₪ and years sum by key) ──
const item = (key, label, eur, ils, extra = {}) => ({ key, label, eur: num(eur), ils: num(ils), kind: 'item', ...extra });
const adjust = (key, label, eur, ils, hint) => ({ key, label, eur: num(eur), ils: num(ils), kind: 'adjust', hint });
const sum = (rows, ccy) => rows.reduce((s, r) => s + r[ccy], 0);
// ₪ for an engine amount the projection only holds in €: the month's ₪/€ ratio of the cell.
const ilsAt = (ctx) => (eur) => (Math.abs(ctx.cell.eur) >= 0.5 ? eur * (ctx.cell.ils / ctx.cell.eur) : 0);

function cellOf(line, row) {
  const v = (ccy) => (line === 'churn' ? -row[ccy].churn : row[ccy][line]);
  return { eur: num(v('eur')), ils: num(v('ils')) };
}

function tieOut(section, target, tie) {
  const de = target.eur - sum(section.rows, 'eur');
  const di = target.ils - sum(section.rows, 'ils');
  if (Math.abs(de) >= 0.5 || Math.abs(di) >= 0.5) section.rows.push(adjust(tie.key, tie.label, de, di, tie.hint));
}

const FX_TIE = { key: 'fx-diff', label: 'FX conversion and rounding', hint: 'Forecast amounts are converted to ₪ at the month\'s rate; components are rounded separately.' };
const BASIS_NOTE = {
  trandate: 'NetSuite GL, subsidiary LSports Data, primary book (€) and ILS book (₪), by transaction date.',
  period: 'NetSuite GL, subsidiary LSports Data, primary book (€) and ILS book (₪), by posting period.',
};

// ── actual months: NetSuite accounts (+ Snowflake check for cost lines) ─────
const placedCache = new WeakMap();
function nsLineOf(details) {
  let m = placedCache.get(details);
  if (!m) {
    m = new Map();
    for (const byLine of Object.values(details.accounts || {})) {
      for (const [line, accts] of Object.entries(byLine)) for (const a of accts) m.set(a.acct, line);
    }
    placedCache.set(details, m);
  }
  return m;
}

async function buildActual(ctx) {
  const { line, mKey, details, sfx, year } = ctx;
  if (line === 'pipeline' || line === 'churn') throw new BreakdownError('Actual months have no pipeline or churn: their revenue is in the NetSuite revenue lines.');
  const sign = SIGN[line];
  const accts = ((details.accounts[mKey] || {})[line]) || [];
  const main = {
    id: 'accounts', title: 'NetSuite P&L by account', note: BASIS_NOTE[details.basis] || BASIS_NOTE.trandate,
    rows: accts.map((a) => item(`acct:${a.acct}`, a.name || a.acct, sign * a.eur, sign * a.ils, { ref: a.acct })),
    tie: { key: 'rounding', label: 'Rounding' },
  };
  const sections = [main];
  const notes = [];
  if (COST_LINES.has(line) && sfx) {
    try {
      // Snowflake has no NetSuite account type: an account NetSuite already placed keeps its line.
      const placed = nsLineOf(details);
      const sfRows = (await sfx.expense(year)).filter((x) => x.month === mKey && (placed.get(x.acct) || classifyAccount(x.acct, 'Expense')) === line);
      // FCT_EXPENSE is debit-positive: costs positive, credits negative.
      const sfSign = sign === -1 ? 1 : -1;
      sections.push({
        id: 'snowflake', title: 'Snowflake check (FCT_EXPENSE, same accounts)', collapsed: true, informational: true,
        note: 'The same accounts as Snowflake holds them. The table uses NetSuite; the last row is the difference.',
        rows: sfRows.map((x) => item(`sf:${x.acct}`, x.name || x.acct, sfSign * x.eur, sfSign * x.ils, { ref: x.acct })),
        tie: { key: 'sf-diff', label: 'Difference to NetSuite', hint: 'NetSuite − Snowflake. Usually postings Snowflake has not loaded yet.' },
      });
    } catch {
      notes.push('The Snowflake check is unavailable right now.');
    }
  }
  return { sections, notes };
}

// ── forecast months: the components the projection used ─────────────────────
function lastMonthsRows(ctx, line, months) {
  const totals = ctx.details.rules.totals || {};
  const n = months.length || 1;
  return months.map((k) => {
    const t = (totals[k] && totals[k][line]) || { eur: 0, ils: 0 };
    return item(`m:${k}`, `${monthLong(k)} ÷ ${n}`, SIGN[line] * t.eur / n, SIGN[line] * t.ils / n);
  });
}

const FORECAST = {
  revenue(ctx) {
    const { M, yearKind, ilsOf, year } = ctx;
    const rows = yearKind === 'projection'
      ? [item('runrate', `Oct–Dec ${year - 1} revenue run-rate (incl. pipeline and churn)`, M.mr, ilsOf(M.mr), { hint: 'The previous year\'s last quarter, flat across the year.' })]
      : [item('mr', 'Expected revenue (Snowflake monthly revenue)', M.mr, ilsOf(M.mr), { hint: 'Revenue of signed customers for the month. The cash page applies a collection rate to it; the P&L does not.' })];
    if (Math.abs(M.deals) >= 0.5) rows.push(item('deals', 'Open deals at or above the minimum probability', M.deals, ilsOf(M.deals)));
    return { id: 'components', title: 'Expected customer revenue', rows };
  },
  pipeline(ctx) {
    const { M, ilsOf, mKey } = ctx;
    const cohorts = ctx.yearDetails.pipeline || {};
    const rows = Object.entries(cohorts)
      .filter(([k, v]) => k <= mKey && Math.abs(v) >= 0.5)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => item(`cohort:${k}`, `New business from ${monthLong(k)}`, v, ilsOf(v), { group: 'Projected MRR × calibration factor, cumulative' }));
    const base = sum(rows, 'eur');
    if (Math.abs(ctx.cell.eur - base) >= 0.5) rows.push(adjust('pct', `Pipeline % in the plan (${M.pipelinePct}%)`, ctx.cell.eur - base, ilsOf(ctx.cell.eur - base)));
    return { id: 'components', title: 'New business from the open pipeline', rows };
  },
  churn(ctx) {
    const { M, cell } = ctx;
    const label = M.churnOverride ? 'Churn set in the plan for this month' : `Churn run-rate × ${M.churnIndex} forecast month${M.churnIndex === 1 ? '' : 's'}`;
    return { id: 'components', title: 'Revenue lost to churn (cumulative)', rows: [item('churn', label, cell.eur, cell.ils)] };
  },
  otherRevenue(ctx) {
    return { id: 'components', title: 'Average of the last 3 closed months (NetSuite)', rows: lastMonthsRows(ctx, 'otherRevenue', ctx.details.rules.last3) };
  },
  finance(ctx) {
    return { id: 'components', title: 'Average of the last 3 closed months (NetSuite)', rows: lastMonthsRows(ctx, 'finance', ctx.details.rules.last3) };
  },
  payroll(ctx) {
    const { M, ilsOf, cell, yearDetails } = ctx;
    const basis = yearDetails.salaryBasis;
    const rows = [];
    const usesBasis = basis && (ctx.mKey > basis.month);
    if (usesBasis) {
      const group = ctx.yearKind === 'projection' ? `Oct–Dec ${ctx.year - 1} payroll run-rate by department` : `Payroll of ${monthLong(basis.month)} by department (Snowflake)`;
      for (const [d, v] of Object.entries(basis.byDept).sort((a, b) => b[1].eur - a[1].eur)) rows.push(item(`dept:${d}`, d, v.eur, v.ils, { group }));
      const extra = M.salaryBase - sum(rows, 'eur');
      if (Math.abs(extra) >= 0.5) rows.push(adjust('hc', 'Hires, leavers and budget overrides', extra, ilsOf(extra)));
    } else {
      rows.push(item('budget', 'Payroll budget (Snowflake)', M.salaryBase, ilsOf(M.salaryBase)));
    }
    const plan = cell.eur - M.salaryBase;
    if (Math.abs(plan) >= 0.5) rows.push(adjust('plan', 'Plan changes (salary % and departments)', plan, ilsOf(plan)));
    return { id: 'components', title: 'Projected payroll', rows };
  },
  opex(ctx) {
    const { M, ilsOf, cell, yearKind, mKey, year } = ctx;
    const rows = [];
    if (yearKind === 'projection') {
      const same = `${year - 1}${mKey.slice(4)}`;
      rows.push(item('mirror', `Same month of ${year - 1} (${monthLong(same)})`, M.vendorsBase, ilsOf(M.vendorsBase)));
    } else if (M.categories) {
      for (const [c, v] of Object.entries(M.categories).sort((a, b) => b[1] - a[1])) {
        if (Math.abs(num(v)) >= 0.5) rows.push(item(`cat:${c}`, c, num(v), ilsOf(num(v)), { group: 'Vendor budget by category (Snowflake)' }));
      }
      const rest = M.vendorsBase - sum(rows, 'eur');
      if (Math.abs(rest) >= 0.5) rows.push(adjust('overrides', 'Budget overrides', rest, ilsOf(rest)));
    } else {
      rows.push(item('budget', 'Vendor budget (Snowflake)', M.vendorsBase, ilsOf(M.vendorsBase)));
    }
    const plan = cell.eur - M.vendorsBase;
    if (Math.abs(plan) >= 0.5) rows.push(adjust('plan', 'Plan changes (vendor categories and accounts)', plan, ilsOf(plan)));
    return { id: 'components', title: 'Projected operating expenses', rows };
  },
  capex(ctx) {
    const from = ctx.details.rules.capexFrom;
    const label = from ? `Salaries CAPEX of ${monthLong(from)}, carried flat` : 'No Salaries CAPEX in the last 12 closed months';
    return { id: 'components', title: 'Last closed month (NetSuite account 950000)', rows: [item('capex', label, ctx.cell.eur, ctx.cell.ils, { ref: '950000' })] };
  },
  fx(ctx) {
    const { M, cell } = ctx;
    const ref = `${fmtEur(M.defense.budget)} × ${M.defense.pct}%`;
    return { id: 'components', title: 'Currency-defense budget × defense %', rows: [item('defense', 'Expected FX result', cell.eur, cell.ils, { ref })] };
  },
  depreciation(ctx) {
    const { M, cell } = ctx;
    if (M.depreciation && M.depreciation.method === 'budget') {
      return { id: 'components', title: 'Depreciation budget by account (Snowflake)', rows: M.depreciation.accounts.map((a) => item(`acct:${a.acct}`, a.name || a.acct, a.eur, a.ils, { ref: a.acct })) };
    }
    return { id: 'components', title: 'No budget: average of the last 3 closed months (NetSuite)', rows: lastMonthsRows(ctx, 'depreciation', ctx.details.rules.last3) };
  },
  taxOther(ctx) {
    const { M } = ctx;
    const accts = (M.taxOther && M.taxOther.accounts) || [];
    return { id: 'components', title: 'Budget by account (Snowflake)', rows: accts.map((a) => item(`acct:${a.acct}`, a.name || a.acct, a.eur, a.ils, { ref: a.acct })) };
  },
};

async function buildMonth(ctx) {
  if (ctx.status === 'actual') {
    const out = await buildActual(ctx);
    for (const s of out.sections) tieOut(s, ctx.cell, s.tie);
    return out;
  }
  if (!ctx.M) throw new BreakdownError('Nothing to break down for this cell.');
  const main = FORECAST[ctx.line](ctx);
  tieOut(main, ctx.cell, FX_TIE);
  return { sections: [main], notes: [] };
}

// Full year: the months merged by row key; the main sections add up to the FY cell.
async function buildYear(ctx) {
  const main = { id: 'main', title: '', rows: [] };
  const secondary = [];
  const ids = new Set();
  const notes = new Set();
  let months = 0;
  const merge = (target, rows) => {
    for (const r of rows) {
      const hit = target.rows.find((x) => x.key === r.key);
      if (hit) { hit.eur += r.eur; hit.ils += r.ils; } else target.rows.push({ ...r });
    }
  };
  for (const row of ctx.block.rows) {
    if (Math.abs(cellOf(ctx.line, row)[ctx.ccy]) < 0.5) continue;
    const one = await buildMonth(monthCtx(ctx, row));
    months++;
    one.notes.forEach((n) => notes.add(n));
    const [first, ...rest] = one.sections;
    if (!ids.size) main.title = first.title;
    ids.add(first.id);
    merge(main, first.rows);
    for (const s of rest) {
      let target = secondary.find((x) => x.id === s.id);
      if (!target) { target = { ...s, rows: [] }; secondary.push(target); }
      merge(target, s.rows);
    }
  }
  if (!months) throw new BreakdownError('Nothing to break down for this year.');
  if (ids.size > 1) main.title = 'NetSuite accounts in actual months, projection components in forecast months';
  const sections = [main, ...secondary];
  for (const s of sections) s.note = `Sum of the ${months} month${months === 1 ? '' : 's'} with an amount.`;
  return { sections, notes: [...notes] };
}

function monthCtx(yearCtx, row) {
  const M = yearCtx.yearDetails.months[row.mKey] || null;
  const ctx = { ...yearCtx, mKey: row.mKey, row, status: row.status, M, cell: cellOf(yearCtx.line, row) };
  ctx.ilsOf = ilsAt(ctx);
  return ctx;
}

/** One breakdown from a cache entry. sfx: { expense(year) } (Snowflake check; optional). */
async function buildBreakdown({ entry, line, period, variant, ccy, sfx }) {
  const fy = /^FY-(\d{4})$/.exec(period);
  const year = fy ? Number(fy[1]) : Number(String(period).slice(0, 4));
  const v = entry.payload.variants[variant];
  const block = v && v.years.find((y) => y.year === year);
  const yearDetails = entry.details.variants[variant] && entry.details.variants[variant][year];
  if (!block || !yearDetails) return null;
  const base = { entry, details: entry.details, line, variant, ccy, sfx, year, yearKind: block.kind, block, yearDetails };
  let built;
  let cell;
  let status;
  if (fy) {
    cell = block.rows.reduce((s, r) => { const c = cellOf(line, r); return { eur: s.eur + c.eur, ils: s.ils + c.ils }; }, { eur: 0, ils: 0 });
    built = await buildYear(base);
    status = 'fy';
  } else {
    const row = block.rows.find((r) => r.mKey === period);
    if (!row) return null;
    const ctx = monthCtx(base, row);
    cell = ctx.cell;
    built = await buildMonth(ctx);
    status = row.status;
  }
  const sections = built.sections.map((s) => {
    const rows = s.rows
      .map((r) => ({ key: r.key, label: r.label, ref: r.ref || null, group: r.group || null, kind: r.kind, hint: r.hint || null, amount: cents(r[ccy]) }))
      .filter((r) => Math.abs(r.amount) >= 0.5);
    return {
      id: s.id, title: s.title, note: s.note || null, collapsed: !!s.collapsed, informational: !!s.informational,
      rows, total: cents(rows.reduce((t, r) => t + r.amount, 0)),
    };
  });
  return {
    ok: true, status: 'ready', line, lineLabel: LINE_LABELS[line] || line, period,
    periodLabel: fy ? `FY ${year}` : monthLong(period), periodStatus: status, variant, ccy,
    cell: cents(cell[ccy]), sections, notes: built.notes,
  };
}

// ── handler ─────────────────────────────────────────────────────────────────
function envMinutes(name, fallback) {
  const v = parseFloat(process.env[name] || '');
  return Number.isFinite(v) && v > 0 ? v : fallback;
}

function snowflakeExpense(getSf, clock, ttlMs) {
  const { sfExpenseByAccount } = require('./cash-projection-breakdown.cjs');
  const memo = new Map();
  return {
    expense(year) {
      const hit = memo.get(year);
      if (hit && clock() - hit.at < ttlMs) return hit.p;
      const sf = getSf();
      if (!sf) return Promise.reject(new BreakdownError('Snowflake is not configured on this server.'));
      const p = sfExpenseByAccount(sf, year);
      memo.set(year, { at: clock(), p });
      p.catch(() => { if (memo.get(year) && memo.get(year).p === p) memo.delete(year); });
      return p;
    },
  };
}

/**
 * Express/connect handler for GET /api/pnl-projection/breakdown. deps (all optional):
 *   getSfClient(), sfx ({ expense(year) } — tests), cacheFile, clock(), ttlMs
 */
function createPnlProjectionBreakdownHandler(deps = {}) {
  const cp = require('./cash-projection.cjs');
  const pnl = require('./pnl-projection.cjs');
  const clock = deps.clock || Date.now;
  const ttlMs = deps.ttlMs ?? envMinutes('PNL_PROJECTION_TTL_MIN', envMinutes('CASH_PROJECTION_TTL_MIN', 30)) * MIN;
  const cacheFile = deps.cacheFile === undefined ? pnl.DEFAULT_CACHE_FILE : deps.cacheFile;
  const getSf = deps.getSfClient || (() => {
    require(path.join(ROOT, 'scripts', 'net-cash-forecast-compute.cjs')); // loads the checkout's .env
    return cp.defaultGetSfClient();
  });
  const sfx = deps.sfx || snowflakeExpense(getSf, clock, ttlMs);
  const state = { entry: null, mtimeMs: 0 };

  function currentEntry() {
    let mtimeMs;
    try { mtimeMs = fs.statSync(cacheFile).mtimeMs; } catch { return state.entry; }
    if (mtimeMs > state.mtimeMs) {
      state.mtimeMs = mtimeMs;
      const e = cp.readCacheEntry(cacheFile, pnl.SCHEMA_VERSION);
      if (e) state.entry = e;
    }
    return state.entry;
  }

  function send(res, status, body) {
    res.statusCode = status;
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'no-store');
    res.end(JSON.stringify(body));
  }

  return async function pnlBreakdownHandler(req, res) {
    if ((req.method || 'GET').toUpperCase() !== 'GET') {
      res.setHeader('Allow', 'GET');
      send(res, 405, { ok: false, status: 'error', error: 'Method not allowed' });
      return;
    }
    let q;
    try { q = new URL(req.url || '', 'http://localhost').searchParams; } catch { q = new URLSearchParams(); }
    const line = q.get('line') || '';
    const period = q.get('period') || '';
    const variant = q.get('variant') || 'plan';
    const ccy = q.get('ccy') || 'eur';
    if (!LINES.includes(line) || !/^(\d{4}-(0[1-9]|1[0-2])|FY-\d{4})$/.test(period) || !['plan', 'base'].includes(variant) || !['eur', 'ils'].includes(ccy)) {
      send(res, 400, { ok: false, status: 'error', error: 'Unknown line, period, variant or currency.' });
      return;
    }
    const entry = currentEntry();
    if (!entry || !entry.details || entry.details.version !== pnl.DETAILS_VERSION) {
      send(res, 202, { ok: true, status: 'computing' });
      return;
    }
    try {
      const out = await buildBreakdown({ entry, line, period, variant, ccy, sfx });
      if (!out) { send(res, 404, { ok: false, status: 'error', error: 'This period is not part of the projection.' }); return; }
      send(res, 200, { ...out, generatedAt: entry.payload.generatedAt });
    } catch (e) {
      const safe = e instanceof BreakdownError;
      if (!safe) console.error(`[pnl-projection] breakdown ${line} ${period} failed: ${e && e.stack ? e.stack : e}`);
      send(res, 200, { ok: false, status: 'error', error: safe ? e.message : 'The breakdown could not be loaded. The server log has the details.' });
    }
  };
}

module.exports = { createPnlProjectionBreakdownHandler, buildBreakdown, BreakdownError, LINES };
