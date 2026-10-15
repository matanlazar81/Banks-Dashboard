// Pure table model for the Salary Projection: one line per payroll account, a subtotal per group (permanent,
// temporary, not classified) and the total, after applying the page's classification (saved choices plus
// unsaved edits). Between permanent and temporary come the override file's payroll lines: its increments, which
// count in Total payroll from the current month on, and its overrides, for reference only.
// Columns, full-year values and formatting are the New Bank Dashboard's (src/new-dashboard/model.ts). No React,
// no DOM — unit-tested by scripts/test-salary-projection.cjs.
import { countedIncrementRows, groupOfMethod, projectSalary } from '../forecast/salary-projection.mjs';
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

/** The override file's table lines, and the ones Total payroll adds (the increments). */
interface OverrideDefs {
  defs: SalaryLineDef[];
  totalKeys: string[];
}

/**
 * The table lines: each group's accounts, then its subtotal, then the total. The override file's lines (when
 * given) come right after the permanent group; Total payroll adds those in totalKeys (the increments).
 */
export function salaryLineDefs(
  accounts: SalaryAccount[], runRateMonth: string, years: [number, number], overrides: OverrideDefs = { defs: [], totalKeys: [] },
): SalaryLineDef[] {
  const runRate = monthLongLabel(runRateMonth);
  const defs: SalaryLineDef[] = [];
  for (const group of GROUPS) {
    const list = accounts.filter((a) => a.group === group);
    if (list.length) {
      for (const a of list) {
        defs.push({ key: a.acct, label: `${a.acct} - ${a.name}`, kind: 'item', fy: 'sum', value: (f) => f[a.acct] ?? 0, hint: itemHint(group, a.method, runRate, years) });
      }
      defs.push({ key: `total-${group}`, label: SUBTOTAL_LABEL[group], kind: 'subtotal', fy: 'sum', value: sumOf(list.map((a) => a.acct)) });
    }
    if (group === 'permanent') defs.push(...overrides.defs);
  }
  const added = overrides.totalKeys.length ? ', plus the override file\'s increments from the current month on' : '';
  const left = overrides.defs.some((d) => d.key === 'total-overrides') ? ' The override file\'s overrides are not in it.' : '';
  defs.push({
    key: 'total', label: 'Total payroll', kind: 'subtotal', fy: 'sum', value: sumOf([...accounts.map((a) => a.acct), ...overrides.totalKeys]),
    hint: `Every payroll account (76xxxx): permanent + temporary${added}.${left}`,
  });
  return defs;
}

/** Columns + lines for the currency and year view, with the override file's lines between permanent and temporary. */
export function buildSalaryTable(payload: SalaryPayload, ccy: Ccy, view: YearView): ProjectionTable<string> {
  const ov = overrideLines(payload);
  const blocks = ov.defs.length
    ? payload.blocks.map((b) => ({
      ...b,
      rows: b.rows.map((r) => {
        const s = ov.sums.get(r.mKey);
        return s ? { ...r, eur: { ...r.eur, ...s.eur }, ils: { ...r.ils, ...s.ils } } : r;
      }),
    }))
    : payload.blocks;
  return buildYearTable(blocks, ccy, view, salaryLineDefs(payload.accounts, payload.basis.runRateMonth, payload.years, ov));
}

// ── The override file (OVERIDE_TEMP in Snowflake), shown between permanent and temporary ──
type Mode = PayrollOverride['mode'];
type MonthSums = { eur: SalaryFigures; ils: SalaryFigures };

const MODES: Mode[] = ['Increment', 'Override'];
const MODE_KEY: Record<Mode, string> = { Increment: 'increment', Override: 'override' };
const MODE_HINT: Record<Mode, string> = {
  Increment: 'Increment from the override file: added to Total payroll from the current month on (closed months are NetSuite\'s actuals, which already include it).',
  Override: 'Override from the override file: replaces the budget of this account and month. For reference: not in Total payroll (the projection is by account, and an override replaces one department\'s budget).',
};

const listOf = (values: string[], max: number) => {
  const unique = [...new Set(values.filter(Boolean))];
  return unique.length > max ? `${unique.slice(0, max).join('; ')}; and ${unique.length - max} more` : unique.join('; ');
};

/**
 * The override file's payroll rows as table lines: one per account and type, summing an account's rows of a
 * month across departments. Increments (normal lines, then "Total increments") show only where they count in
 * Total payroll: the projected months (countedIncrementRows). Overrides (italic notes, then "Total overrides")
 * show in every month of the table, for reference. Each month's amounts are keyed by line ('increment|760001'),
 * so they never mix with the accounts' own figures. Rows outside the page's months are left out (the rows list
 * under the table counts them).
 */
function overrideLines(payload: SalaryPayload): OverrideDefs & { sums: Map<string, MonthSums> } {
  const rows = payload.overrides ? payload.overrides.rows : [];
  const months = new Set(payload.blocks.flatMap((b) => b.rows.map((r) => r.mKey)));
  const counted = new Set(countedIncrementRows(payload.blocks, rows));
  const sums = new Map<string, MonthSums>();
  const lines = new Map<string, { mode: Mode; acct: string; name: string; rows: PayrollOverride[] }>();
  for (const r of rows) {
    if (r.mode === 'Increment' ? !counted.has(r) : !months.has(r.month)) continue;
    const key = `${MODE_KEY[r.mode]}|${r.acct}`;
    const line = lines.get(key) ?? { mode: r.mode, acct: r.acct, name: r.name, rows: [] };
    line.rows.push(r);
    lines.set(key, line);
    const month = sums.get(r.month) ?? { eur: {}, ils: {} };
    month.eur[key] = (month.eur[key] ?? 0) + r.eur;
    month.ils[key] = (month.ils[key] ?? 0) + r.ils;
    sums.set(r.month, month);
  }
  const list = [...lines.entries()].sort(([, a], [, b]) => MODES.indexOf(a.mode) - MODES.indexOf(b.mode) || a.acct.localeCompare(b.acct));
  const defs: SalaryLineDef[] = [];
  const totalKeys: string[] = [];
  for (const mode of MODES) {
    const ofMode = list.filter(([, l]) => l.mode === mode);
    if (!ofMode.length) continue;
    for (const [key, l] of ofMode) {
      const departments = listOf(l.rows.map((r) => r.department), 6);
      const comments = listOf(l.rows.map((r) => r.memo), 4);
      const estimated = l.rows.some((r) => r.ilsEstimated) ? ' ≈ The file has euros only for some months: shekels at the forecast rate.' : '';
      defs.push({
        key, label: `${l.acct}${l.name ? ` - ${l.name}` : ''} (${MODE_KEY[mode]})`, kind: mode === 'Increment' ? 'item' : 'note', fy: 'sum',
        value: (f) => f[key] ?? 0,
        hint: `${MODE_HINT[mode]}${departments ? ` Departments: ${departments}.` : ''}${comments ? ` Comments: ${comments}.` : ''}${estimated}`,
      });
    }
    const keys = ofMode.map(([k]) => k);
    if (mode === 'Increment') {
      totalKeys.push(...keys);
      defs.push({
        key: 'total-increments', label: 'Total increments', kind: 'subtotal', fy: 'sum', value: sumOf(keys),
        hint: 'The override file\'s increments from the current month on: in Total payroll.',
      });
    } else {
      defs.push({
        key: 'total-overrides', label: 'Total overrides (not in total payroll)', kind: 'subtotal', fy: 'sum', value: sumOf(keys),
        hint: 'The override file\'s overrides, for reference: not in Total payroll.',
      });
    }
  }
  return { sums, defs, totalKeys };
}

export interface SalaryKpis {
  /** Payroll of the actual months of the current year. */
  ytd: { value: number; through: string | null };
  /** Full-year payroll per year (actuals + forecast + the override file's increments), and the increments in it. */
  byYear: { year: number; value: number; increments: number }[];
  /** The permanent accounts' monthly run-rate (the last closed month). */
  runRate: { value: number; mKey: string };
}

export function computeSalaryKpis(payload: SalaryPayload, ccy: Ccy): SalaryKpis {
  const all = sumOf(payload.accounts.map((a) => a.acct));
  const permanent = sumOf(payload.accounts.filter((a) => a.group === 'permanent').map((a) => a.acct));
  const ov = overrideLines(payload);
  const increments = sumOf(ov.totalKeys);
  const incrementsOf = (mKey: string) => {
    const s = ov.sums.get(mKey);
    return s ? increments(s[ccy]) : 0;
  };
  const current = payload.blocks.find((b) => b.kind === 'current') ?? payload.blocks[0];
  const actual = current.rows.filter((r) => r.status === 'actual');
  // The run-rate month's own figures when it is in the table; else (January) the first projected month,
  // which carries it.
  const rateRow = current.rows.find((r) => r.mKey === payload.basis.runRateMonth) ?? current.rows.find((r) => r.status !== 'actual') ?? current.rows[0];
  return {
    // Increments never count in actual months, so year to date is the accounts' actuals.
    ytd: { value: actual.reduce((s, r) => s + all(r[ccy]), 0), through: payload.actuals.through },
    byYear: payload.blocks.map((b) => {
      const inc = b.rows.reduce((s, r) => s + incrementsOf(r.mKey), 0);
      return { year: b.year, value: b.rows.reduce((s, r) => s + all(r[ccy]), 0) + inc, increments: inc };
    }),
    runRate: { value: permanent(rateRow[ccy]), mKey: payload.basis.runRateMonth },
  };
}
