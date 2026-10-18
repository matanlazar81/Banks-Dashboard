// The breakdown window of a planned-change cell on the Salary Projection: what makes up the figure, built on
// the page from the payload (no server call), in the shared window's shape (src/new-dashboard/breakdown.ts).
// Pure, no React: unit-tested by scripts/test-salary-projection.cjs.
//   hires / leavers / returns         by account (each opens by department), and by department
//   total-changes                     by type and account (each opens by department), and by department
//   increment|ACCT / override|ACCT    the override file's rows by department, with their comments; a year also by month
//   total-increments / total-overrides by account and department
//   deel|ACCT / total-deel            an actual month by vendor (the total also by account); a forecast month by
//                                     the months its run-rate averages; a year by month (the total also by account)
import { countedChangeRows, countedIncrementRows, deelByMonth, deelRunRate } from '../forecast/salary-projection.mjs';
import type { BreakdownReady, BreakdownRequest, BreakdownResponse, BreakdownRow, BreakdownSection } from '../new-dashboard/breakdown.ts';
import { monthLongLabel } from '../new-dashboard/model.ts';
import { SOURCE_KEY, SOURCE_LINE, SOURCES } from './model.ts';
import type { PayrollChange, PayrollOverride, SalaryPayload } from './types.ts';

type Source = PayrollChange['source'];
type Mode = PayrollOverride['mode'];

const SOURCE_OF_KEY = new Map<string, Source>(SOURCES.map((s) => [SOURCE_KEY[s], s]));
const MODE_OF_KEY: Record<string, Mode> = { increment: 'Increment', override: 'Override' };
const TOTAL_MODE: Record<string, Mode> = { 'total-increments': 'Increment', 'total-overrides': 'Override' };

/** Table lines whose cells open a breakdown: the planned changes, Deel, and their totals. */
export function isSalaryBreakdownLine(key: string): boolean {
  return SOURCE_OF_KEY.has(key) || key === 'total-changes' || key in TOTAL_MODE || /^(increment|override|deel)\|/.test(key) || key === 'total-deel';
}

const round = (n: number) => Math.round(n * 100) / 100;
const deptLabel = (d: string) => d || 'No department';
const accountLabel = (acct: string, name: string) => `${acct}${name ? ` - ${name}` : ''}`;

/** The months of a period ('YYYY-MM' or 'FY-YYYY') in the payload, with its label and status. */
function periodOf(payload: SalaryPayload, period: string): { months: Set<string>; label: string; status: BreakdownReady['periodStatus'] } | null {
  const fy = /^FY-(\d{4})$/.exec(period);
  if (fy) {
    const block = payload.blocks.find((b) => b.year === Number(fy[1]));
    return block ? { months: new Set(block.rows.map((r) => r.mKey)), label: `FY ${block.year}`, status: 'fy' } : null;
  }
  for (const b of payload.blocks) {
    const row = b.rows.find((r) => r.mKey === period);
    if (row) return { months: new Set([period]), label: monthLongLabel(period), status: row.status };
  }
  return null;
}

/** Rows summed by a key, largest amount first. */
function sumBy<T>(rows: T[], keyOf: (r: T) => string, labelOf: (r: T) => string, amountOf: (r: T) => number) {
  const groups = new Map<string, { key: string; label: string; amount: number; rows: T[] }>();
  for (const r of rows) {
    const key = keyOf(r);
    const g = groups.get(key) ?? { key, label: labelOf(r), amount: 0, rows: [] };
    g.amount += amountOf(r);
    g.rows.push(r);
    groups.set(key, g);
  }
  return [...groups.values()]
    .map((g) => ({ ...g, amount: round(g.amount) }))
    .sort((a, b) => Math.abs(b.amount) - Math.abs(a.amount) || a.label.localeCompare(b.label));
}

const item = (key: string, label: string, amount: number, more: Partial<BreakdownRow> = {}): BreakdownRow => ({
  key, label, ref: null, group: null, kind: 'item', hint: null, amount, ...more,
});

function section(id: string, title: string, note: string | null, rows: BreakdownRow[]): BreakdownSection {
  return { id, title, note, collapsed: false, informational: false, rows, total: round(rows.reduce((s, r) => s + r.amount, 0)) };
}

const comments = (rows: PayrollOverride[]) => {
  const unique = [...new Set(rows.map((r) => r.memo).filter(Boolean))];
  return unique.length ? unique.join('; ') : null;
};

/**
 * The breakdown of one cell of the payload's table, or with req.row of one of its account rows by department.
 * The first section always adds up to the table cell (or the account row).
 */
export function salaryBreakdown(payload: SalaryPayload, req: BreakdownRequest): BreakdownResponse {
  const fail = (error: string): BreakdownResponse => ({ ok: false, status: 'error', error });
  const period = periodOf(payload, req.period);
  if (!period) return fail('This period is not in the projection.');
  const { ccy } = req;
  const amount = (r: { eur: number; ils: number }) => (ccy === 'eur' ? r.eur : r.ils);
  const total = (rows: { eur: number; ils: number }[]) => round(rows.reduce((s, r) => s + amount(r), 0));
  const inPeriod = <R extends { month: string }>(rows: R[]) => rows.filter((r) => period.months.has(r.month));
  const base = {
    ok: true as const, status: 'ready' as const, line: req.line, period: req.period, periodLabel: period.label,
    periodStatus: period.status, ccy, generatedAt: payload.generatedAt,
  };

  // Snowflake's planned hires, leavers and returns from leave.
  const source = SOURCE_OF_KEY.get(req.line);
  if (source || req.line === 'total-changes') {
    const all = inPeriod(countedChangeRows(payload.blocks, payload.changes ? payload.changes.rows : []));
    const rows = source ? all.filter((r) => r.source === source) : all;
    const lineLabel = source ? SOURCE_LINE[source] : 'Total headcount changes';
    const notes = [
      `HiBob's forecast in Snowflake's payroll forecast (${source ? `source "${source}"` : 'sources "Open position", "Terminated" and "Leave end"'}), LSports, every payroll account: monthly totals by account and department, no employee detail.`,
      'Counted in Total payroll from the current month on, when marked TRUE (IS_SOURCE_ACTUAL); FALSE ones are left out. Closed months are NetSuite\'s actuals, which already include what happened.',
    ];

    // One account row by department (the second window).
    if (req.row) {
      const [typeKey, acct] = source ? [SOURCE_KEY[source], req.row] : req.row.split('|');
      const rowSource = SOURCE_OF_KEY.get(typeKey);
      const of = rows.filter((r) => r.source === rowSource && r.acct === acct);
      if (!rowSource || !of.length) return fail('Nothing to show for this account.');
      const depts = sumBy(of, (r) => r.department, (r) => deptLabel(r.department), amount).map((d) => item(`dept|${d.key}`, d.label, d.amount));
      return {
        ...base, lineLabel, cell: total(of), notes,
        sections: [section('departments', 'By department', null, depts)],
        account: { acct, name: of[0].name || acct, link: null, of: `${SOURCE_LINE[rowSource]} · ${period.label}` },
      };
    }

    const accounts: BreakdownRow[] = [];
    for (const s of source ? [source] : SOURCES) {
      for (const a of sumBy(rows.filter((r) => r.source === s), (r) => r.acct, (r) => r.name || r.acct, amount)) {
        accounts.push(item(source ? a.key : `${SOURCE_KEY[s]}|${a.key}`, a.label, a.amount, { ref: a.key, drillable: true, group: source ? null : SOURCE_LINE[s] }));
      }
    }
    const depts = sumBy(rows, (r) => r.department, (r) => deptLabel(r.department), amount).map((d) => item(`dept|${d.key}`, d.label, d.amount));
    return {
      ...base, lineLabel, cell: total(rows), notes,
      sections: [
        section('accounts', source ? 'By account' : 'By type and account', 'Click an account for its departments.', accounts),
        section('departments', 'By department', 'The same amount, by department across the accounts.', depts),
      ],
    };
  }

  // The override file.
  const line = /^(increment|override)\|(.+)$/.exec(req.line);
  const mode = line ? MODE_OF_KEY[line[1]] : TOTAL_MODE[req.line];
  if (mode) {
    const fileRows = payload.overrides ? payload.overrides.rows : [];
    // As the table: increments where they count in Total payroll, overrides in every month (for reference).
    const shown = mode === 'Increment' ? countedIncrementRows(payload.blocks, fileRows) : fileRows.filter((r) => r.mode === 'Override');
    const rows = inPeriod(shown).filter((r) => !line || r.acct === line[2]);
    if (!rows.length) return fail('Nothing to show for this cell.');
    const lineLabel = line ? `${accountLabel(line[2], rows[0].name)} (${line[1]})` : mode === 'Increment' ? 'Total increments' : 'Total overrides';
    const notes = [
      mode === 'Increment'
        ? 'The override file (Snowflake): an increment adds to the budget of its account and month. Counted in Total payroll from the current month on.'
        : 'The override file (Snowflake): an override replaces the budget of its account and month. For reference: not in Total payroll.',
      ...(ccy === 'ils' && rows.some((r) => r.ilsEstimated) ? ['Where the file has euros only, shekels are at the forecast rate.'] : []),
    ];
    const byDept = (of: PayrollOverride[], keyPrefix: string, group: string | null) => sumBy(of, (r) => r.department, (r) => deptLabel(r.department), amount)
      .map((d) => item(`${keyPrefix}${d.key}`, d.label, d.amount, { group, hint: comments(d.rows) }));
    const first = line
      ? section('departments', 'By department', 'Hover a department for the comments in the file.', byDept(rows, 'dept|', null))
      : section('accounts', 'By account and department', 'Hover a department for the comments in the file.',
        sumBy(rows, (r) => r.acct, (r) => accountLabel(r.acct, r.name), amount).flatMap((a) => byDept(a.rows, `${a.key}|`, a.label)));
    const months = period.status === 'fy'
      ? [section('months', 'By month', null, sumBy(rows, (r) => r.month, (r) => monthLongLabel(r.month), amount)
        .sort((a, b) => a.key.localeCompare(b.key)).map((m) => item(`month|${m.key}`, m.label, m.amount)))]
      : [];
    return { ...base, lineLabel, cell: total(rows), notes, sections: [first, ...months] };
  }

  // Deel (NetSuite).
  const deelLine = /^deel\|(.+)$/.exec(req.line);
  if (deelLine || req.line === 'total-deel') {
    const deel = payload.deel;
    if (!deel || !deel.rows.length) return fail('Nothing to show for this cell.');
    const acct = deelLine ? deelLine[1] : null;
    const runRateMonth = payload.basis.runRateMonth;
    const byMonth = deelByMonth(payload.blocks, deel.rows, runRateMonth);
    const rate = deelRunRate(deel.rows, runRateMonth);
    const names = new Map(deel.rows.map((r) => [r.acct, r.name]));
    // The table's figures: one account's, or every account's, in a month.
    const valueOf = (mKey: string, a: string | null) => {
      const f = byMonth.get(mKey)?.[ccy] ?? {};
      return a ? f[a] ?? 0 : Object.values(f).reduce((s, v) => s + v, 0);
    };
    const months = [...period.months].sort();
    const cell = round(months.reduce((s, m) => s + valueOf(m, acct), 0));
    if (Math.abs(cell) < 0.5) return fail('Nothing to show for this cell.');
    const lineLabel = acct ? `Deel · ${accountLabel(acct, names.get(acct) ?? '')}` : 'Total Deel';
    const rateText = rate.months.length
      ? `the average of ${rate.months.map(monthLongLabel).join(', ')}, the last ${rate.months.length > 1 ? `${rate.months.length} months` : 'month'} with Deel postings up to ${monthLongLabel(runRateMonth)}`
      : `0, no Deel postings in the 12 months up to ${monthLongLabel(runRateMonth)}`;
    const notes = [
      `NetSuite: the costs of ${deel.vendors.join(', ') || 'Deel'} by posting period, on ${acct ? 'this account' : 'every account but payroll ones'}. Forecast months: ${rateText}.`,
      'Not in Total payroll: Payroll + Deel adds it.',
    ];
    const accountRows = (amountOf: (a: string) => number) => [...names.keys()]
      .map((a) => item(`acct|${a}`, accountLabel(a, names.get(a) ?? ''), round(amountOf(a))))
      .filter((r) => Math.abs(r.amount) >= 0.5)
      .sort((x, y) => Math.abs(y.amount) - Math.abs(x.amount) || x.label.localeCompare(y.label));
    const sections: BreakdownSection[] = [];
    if (period.status === 'fy') {
      if (!acct) sections.push(section('accounts', 'By account', null, accountRows((a) => months.reduce((s, m) => s + valueOf(m, a), 0))));
      sections.push(section('months', 'By month', 'Actual months as NetSuite has them; forecast months at the run-rate.',
        months.map((m) => item(`month|${m}`, monthLongLabel(m), round(valueOf(m, acct)))).filter((r) => Math.abs(r.amount) >= 0.5)));
    } else if (period.status === 'actual') {
      if (!acct) sections.push(section('accounts', 'By account', null, accountRows((a) => valueOf(months[0], a))));
      const rows = inPeriod(deel.rows).filter((r) => !acct || r.acct === acct);
      sections.push(section('vendors', 'By vendor', null, sumBy(rows, (r) => r.vendor, (r) => r.vendor, amount).map((v) => item(`vendor|${v.key}`, v.label, v.amount))));
    } else {
      // A forecast month: each month of the run-rate, divided by their number, so the rows add up to the cell.
      const n = rate.months.length;
      const rows = deel.rows.filter((r) => rate.months.includes(r.month) && (!acct || r.acct === acct));
      sections.push(section('run-rate', 'Run-rate: the months it averages', `Each month ÷ ${n}.`,
        sumBy(rows, (r) => r.month, (r) => `${monthLongLabel(r.month)} ÷ ${n}`, (r) => amount(r) / n)
          .sort((a, b) => a.key.localeCompare(b.key)).map((m) => item(`month|${m.key}`, m.label, m.amount))));
      if (!acct) sections.push(section('accounts', 'By account', null, accountRows((a) => valueOf(months[0], a))));
    }
    return { ...base, lineLabel, cell, notes, sections };
  }

  return fail('This line has no breakdown.');
}
