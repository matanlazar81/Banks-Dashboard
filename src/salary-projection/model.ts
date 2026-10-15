// Pure table model for the Salary Projection: one line per payroll account, a subtotal per group (permanent,
// temporary, not classified) and the total, after applying the page's classification (saved choices plus
// unsaved edits). Columns, full-year values and formatting are the New Bank Dashboard's
// (src/new-dashboard/model.ts). No React, no DOM — unit-tested by scripts/test-salary-projection.cjs.
import { groupOfMethod, projectSalary } from '../forecast/salary-projection.mjs';
import { buildYearTable, monthLongLabel, type LineDefOf, type ProjectionTable } from '../new-dashboard/model.ts';
import type { YearView } from '../new-dashboard/types.ts';
import type { Ccy, PayrollOverride, SalaryAccount, SalaryFigures, SalaryGroup, SalaryMethod, SalaryPayload } from './types.ts';

/**
 * The payload with a classification applied: each account's method and group (choice, else finance's
 * default, else "not classified" and projected like a temporary account), re-projected from the actual
 * months. A payload without them (an older cache) is returned as it is.
 */
export function withClassification(payload: SalaryPayload, methods: Record<string, SalaryMethod>): SalaryPayload {
  if (!payload.actual) return payload;
  const accounts: SalaryAccount[] = payload.accounts.map((a) => {
    const chosen = methods[a.acct] || a.defaultMethod;
    return { ...a, method: chosen || 'last-year', group: chosen ? groupOfMethod(chosen) : 'other' };
  });
  const byAcct = new Map(accounts.map((a) => [a.acct, a.method]));
  const blocks = projectSalary({
    years: payload.years,
    currentMonth: payload.computedMonth,
    runRateMonth: payload.basis.runRateMonth,
    ilsPerEur: payload.basis.ilsPerEur,
    accounts: accounts.map((a) => a.acct),
    actual: payload.actual,
    methodOf: (acct) => byAcct.get(acct) || 'last-year',
  });
  return { ...payload, accounts, blocks };
}

type SalaryLineDef = LineDefOf<SalaryFigures, string>;

const GROUPS: SalaryGroup[] = ['permanent', 'temporary', 'other'];

const SUBTOTAL_LABEL: Record<SalaryGroup, string> = {
  permanent: 'Total permanent',
  temporary: 'Total temporary',
  other: 'Total not classified',
};

const GROUP_HINT: Record<SalaryGroup, string> = {
  permanent: 'Permanent.',
  temporary: 'Temporary.',
  other: 'Not classified as permanent or temporary.',
};

function itemHint(group: SalaryGroup, method: SalaryMethod, runRate: string, years: [number, number]): string {
  const forecast = method === 'run-rate' ? `${runRate} carried forward every month`
    : method === 'none' ? 'not projected (0)'
      : `the same month of the previous year for the rest of ${years[0]}; 0 in ${years[1]}`;
  return `${GROUP_HINT[group]} Actual months: NetSuite. Forecast: ${forecast}.`;
}

const sumOf = (accts: string[]) => (f: SalaryFigures) => accts.reduce((s, a) => s + (f[a] ?? 0), 0);

/** The table lines for the payload's accounts: each group's accounts, then its subtotal, then the total. */
export function salaryLineDefs(accounts: SalaryAccount[], runRateMonth: string, years: [number, number]): SalaryLineDef[] {
  const runRate = monthLongLabel(runRateMonth);
  const defs: SalaryLineDef[] = [];
  for (const group of GROUPS) {
    const list = accounts.filter((a) => a.group === group);
    if (!list.length) continue;
    for (const a of list) {
      defs.push({ key: a.acct, label: `${a.acct} - ${a.name}`, kind: 'item', fy: 'sum', value: (f) => f[a.acct] ?? 0, hint: itemHint(group, a.method, runRate, years) });
    }
    defs.push({ key: `total-${group}`, label: SUBTOTAL_LABEL[group], kind: 'subtotal', fy: 'sum', value: sumOf(list.map((a) => a.acct)) });
  }
  defs.push({
    key: 'total', label: 'Total payroll', kind: 'subtotal', fy: 'sum', value: sumOf(accounts.map((a) => a.acct)),
    hint: 'Every payroll account (76xxxx): permanent + temporary.',
  });
  return defs;
}

/** Columns + lines for the currency and year view. */
export function buildSalaryTable(payload: SalaryPayload, ccy: Ccy, view: YearView): ProjectionTable<string> {
  return buildYearTable(payload.blocks, ccy, view, salaryLineDefs(payload.accounts, payload.basis.runRateMonth, payload.years));
}

type Mode = PayrollOverride['mode'];

const MODES: Mode[] = ['Increment', 'Override'];
const MODE_KEY: Record<Mode, string> = { Increment: 'increment', Override: 'override' };
const MODE_TOTAL: Record<Mode, string> = { Increment: 'Total increments', Override: 'Total overrides' };
const MODE_HINT: Record<Mode, string> = {
  Increment: 'Increment: adds to the budget of this account and month.',
  Override: 'Override: replaces the budget of this account and month.',
};

const listOf = (values: string[], max: number) => {
  const unique = [...new Set(values.filter(Boolean))];
  return unique.length > max ? `${unique.slice(0, max).join('; ')}; and ${unique.length - max} more` : unique.join('; ');
};

export interface OverrideTable {
  /** Null when every row is outside the page's years. */
  table: ProjectionTable<string> | null;
  /** Rows whose month is after the page's last year: listed with the rows, not in the months. */
  outside: number;
}

/**
 * The override file's payroll rows by month, in the projection's columns: one line per type and account (the
 * rows of the same month summed across departments), a subtotal per type, and the total when both types are
 * there. Null when there are no rows.
 */
export function buildOverrideTable(payload: SalaryPayload, ccy: Ccy, view: YearView): OverrideTable | null {
  const rows = payload.overrides ? payload.overrides.rows : [];
  if (!rows.length) return null;
  const months = new Set(payload.blocks.flatMap((b) => b.rows.map((r) => r.mKey)));
  const sums = new Map<string, { eur: SalaryFigures; ils: SalaryFigures }>();
  const lines = new Map<string, { mode: Mode; acct: string; name: string; rows: PayrollOverride[] }>();
  let outside = 0;
  for (const r of rows) {
    if (!months.has(r.month)) { outside++; continue; }
    const key = `${MODE_KEY[r.mode]}|${r.acct}`;
    const line = lines.get(key) ?? { mode: r.mode, acct: r.acct, name: r.name, rows: [] };
    line.rows.push(r);
    lines.set(key, line);
    const month = sums.get(r.month) ?? { eur: {}, ils: {} };
    month.eur[key] = (month.eur[key] ?? 0) + r.eur;
    month.ils[key] = (month.ils[key] ?? 0) + r.ils;
    sums.set(r.month, month);
  }
  if (!lines.size) return { table: null, outside };

  const blocks = payload.blocks.map((b) => ({
    year: b.year,
    kind: b.kind,
    rows: b.rows.map((r) => ({ mKey: r.mKey, status: r.status, eur: sums.get(r.mKey)?.eur ?? {}, ils: sums.get(r.mKey)?.ils ?? {} })),
  }));
  const defs: SalaryLineDef[] = [];
  const present = MODES.filter((m) => [...lines.values()].some((l) => l.mode === m));
  for (const mode of present) {
    const list = [...lines.entries()].filter(([, l]) => l.mode === mode).sort(([, a], [, b]) => a.acct.localeCompare(b.acct));
    for (const [key, l] of list) {
      const departments = listOf(l.rows.map((r) => r.department), 6);
      const comments = listOf(l.rows.map((r) => r.memo), 4);
      const estimated = l.rows.some((r) => r.ilsEstimated) ? ' ≈ The file has euros only for some months: shekels at the forecast rate.' : '';
      defs.push({
        key, label: `${l.acct}${l.name ? ` - ${l.name}` : ''}`, kind: 'item', fy: 'sum', value: (f) => f[key] ?? 0,
        hint: `${MODE_HINT[mode]}${departments ? ` Departments: ${departments}.` : ''}${comments ? ` Comments: ${comments}.` : ''}${estimated}`,
      });
    }
    defs.push({ key: `total-${MODE_KEY[mode]}`, label: MODE_TOTAL[mode], kind: 'subtotal', fy: 'sum', value: sumOf(list.map(([k]) => k)) });
  }
  if (present.length > 1) {
    defs.push({
      key: 'total', label: 'Total in the file', kind: 'subtotal', fy: 'sum', value: sumOf([...lines.keys()]),
      hint: 'Increments + overrides: every payroll amount in the override file.',
    });
  }
  return { table: buildYearTable(blocks, ccy, view, defs), outside };
}

export interface SalaryKpis {
  /** Payroll of the actual months of the current year. */
  ytd: { value: number; through: string | null };
  /** Full-year payroll per year (actuals + forecast). */
  byYear: { year: number; value: number }[];
  /** The permanent accounts' monthly run-rate (the last closed month). */
  runRate: { value: number; mKey: string };
}

export function computeSalaryKpis(payload: SalaryPayload, ccy: Ccy): SalaryKpis {
  const all = sumOf(payload.accounts.map((a) => a.acct));
  const permanent = sumOf(payload.accounts.filter((a) => a.group === 'permanent').map((a) => a.acct));
  const current = payload.blocks.find((b) => b.kind === 'current') ?? payload.blocks[0];
  const actual = current.rows.filter((r) => r.status === 'actual');
  // The run-rate month's own figures when it is in the table; else (January) the first projected month,
  // which carries it.
  const rateRow = current.rows.find((r) => r.mKey === payload.basis.runRateMonth) ?? current.rows.find((r) => r.status !== 'actual') ?? current.rows[0];
  return {
    ytd: { value: actual.reduce((s, r) => s + all(r[ccy]), 0), through: payload.actuals.through },
    byYear: payload.blocks.map((b) => ({ year: b.year, value: b.rows.reduce((s, r) => s + all(r[ccy]), 0) })),
    runRate: { value: permanent(rateRow[ccy]), mKey: payload.basis.runRateMonth },
  };
}
