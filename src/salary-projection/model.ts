// Pure table model for the Salary Projection: one line per payroll account, a subtotal per group (permanent,
// temporary, not classified) and the total, after applying the page's classification (saved choices plus
// unsaved edits). Columns, full-year values and formatting are the New Bank Dashboard's
// (src/new-dashboard/model.ts). No React, no DOM — unit-tested by scripts/test-salary-projection.cjs.
import { groupOfMethod, projectSalary } from '../forecast/salary-projection.mjs';
import { buildYearTable, monthLongLabel, type LineDefOf, type ProjectionTable } from '../new-dashboard/model.ts';
import type { YearView } from '../new-dashboard/types.ts';
import type { Ccy, SalaryAccount, SalaryFigures, SalaryGroup, SalaryMethod, SalaryPayload } from './types.ts';

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
