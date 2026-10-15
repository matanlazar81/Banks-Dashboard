// Pure table model for the Salary Projection: one line per payroll account, a subtotal per group (permanent,
// temporary, not classified) and the total, after applying the page's classification (saved choices plus
// unsaved edits). Between permanent and temporary come the planned changes: Snowflake's hires, leavers and
// returns from leave and the override file's increments, which count in Total payroll from the current month
// on, and the override file's overrides, for reference only.
// Columns, full-year values and formatting are the New Bank Dashboard's (src/new-dashboard/model.ts). No React,
// no DOM — unit-tested by scripts/test-salary-projection.cjs.
import { countedChangeRows, countedIncrementRows, groupOfMethod, projectSalary } from '../forecast/salary-projection.mjs';
import { buildYearTable, monthLongLabel, type LineDefOf, type ProjectionTable } from '../new-dashboard/model.ts';
import type { YearView } from '../new-dashboard/types.ts';
import type { Ccy, PayrollChange, PayrollOverride, SalaryAccount, SalaryFigures, SalaryGroup, SalaryMethod, SalaryPayload } from './types.ts';

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

/** A section title row over a group of lines (no amounts). */
const header = (key: string, label: string, hint: string): SalaryLineDef => ({ key, label, kind: 'header', fy: 'sum', value: () => 0, hint });

const GROUP_HEADER: Record<SalaryGroup, string> = { permanent: 'Permanent', temporary: 'Temporary', other: 'Not classified' };

function groupHeaderHint(group: SalaryGroup, runRate: string, years: [number, number]): string {
  if (group === 'permanent') return `Recurring pay and its burden. Forecast: ${runRate} carried into every month.`;
  if (group === 'temporary') {
    return `One-offs and seasonal payments. Forecast: the same month of the previous year for the rest of ${years[0]}, 0 in ${years[1]}; actuals-only accounts are not projected.`;
  }
  return 'Accounts without a classification yet, projected like temporary ones. Use Permanent / temporary to pick.';
}

/** The planned changes' table lines, and the ones Total payroll adds (hires, leavers, returns, increments). */
interface PlannedDefs {
  defs: SalaryLineDef[];
  totalKeys: string[];
}

/**
 * The table lines: each group's header, accounts and subtotal, then the total. The planned changes' lines (when
 * given, with their own headers) come right after the permanent group; Total payroll adds those in totalKeys.
 */
export function salaryLineDefs(
  accounts: SalaryAccount[], runRateMonth: string, years: [number, number], planned: PlannedDefs = { defs: [], totalKeys: [] },
): SalaryLineDef[] {
  const runRate = monthLongLabel(runRateMonth);
  const defs: SalaryLineDef[] = [];
  for (const group of GROUPS) {
    const list = accounts.filter((a) => a.group === group);
    if (list.length) {
      defs.push(header(`header-${group}`, GROUP_HEADER[group], groupHeaderHint(group, runRate, years)));
      for (const a of list) {
        defs.push({ key: a.acct, label: `${a.acct} - ${a.name}`, kind: 'item', fy: 'sum', value: (f) => f[a.acct] ?? 0, hint: itemHint(group, a.method, runRate, years) });
      }
      defs.push({ key: `total-${group}`, label: SUBTOTAL_LABEL[group], kind: 'subtotal', fy: 'sum', value: sumOf(list.map((a) => a.acct)) });
    }
    if (group === 'permanent') defs.push(...planned.defs);
  }
  const has = (key: string) => planned.defs.some((d) => d.key === key);
  const added = [
    has('total-changes') && 'planned hires, leavers and returns from leave',
    has('total-increments') && 'the override file\'s increments',
  ].filter(Boolean).join(' and ');
  defs.push({
    key: 'total', label: 'Total payroll', kind: 'subtotal', fy: 'sum', value: sumOf([...accounts.map((a) => a.acct), ...planned.totalKeys]),
    hint: `Every payroll account (76xxxx): permanent + temporary${added ? `, plus ${added} from the current month on` : ''}.${has('total-overrides') ? ' The override file\'s overrides are not in it.' : ''}`,
  });
  return defs;
}

/** Columns + lines for the currency and year view, with the planned changes between permanent and temporary. */
export function buildSalaryTable(payload: SalaryPayload, ccy: Ccy, view: YearView): ProjectionTable<string> {
  const planned = plannedLines(payload);
  const blocks = planned.defs.length
    ? payload.blocks.map((b) => ({
      ...b,
      rows: b.rows.map((r) => {
        const s = planned.sums.get(r.mKey);
        return s ? { ...r, eur: { ...r.eur, ...s.eur }, ils: { ...r.ils, ...s.ils } } : r;
      }),
    }))
    : payload.blocks;
  return buildYearTable(blocks, ccy, view, salaryLineDefs(payload.accounts, payload.basis.runRateMonth, payload.years, planned));
}

// ── Planned changes, between permanent and temporary: Snowflake's hires, leavers and returns from leave, then
// the override file (OVERIDE_TEMP in Snowflake) ──
type Source = PayrollChange['source'];
type Mode = PayrollOverride['mode'];
type MonthSums = { eur: SalaryFigures; ils: SalaryFigures };

export const SOURCES: Source[] = ['Open position', 'Terminated', 'Leave end'];
export const SOURCE_KEY: Record<Source, string> = { 'Open position': 'hires', Terminated: 'leavers', 'Leave end': 'returns' };
export const SOURCE_LINE: Record<Source, string> = { 'Open position': 'Planned hires', Terminated: 'Leavers', 'Leave end': 'Returns from leave' };
const SOURCE_HINT: Record<Source, string> = {
  'Open position': 'Planned hires: open positions in Snowflake\'s payroll forecast.',
  Terminated: 'Leavers (negative): employees leaving, from Snowflake\'s payroll forecast.',
  'Leave end': 'Returns from leave, from Snowflake\'s payroll forecast.',
};
const COUNTED = ' Added to Total payroll from the current month on (closed months are NetSuite\'s actuals, which already include what happened).';

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
const accountLabel = (acct: string, name: string) => `${acct}${name ? ` - ${name}` : ''}`;

/**
 * The planned changes as table lines, each source under its header. Each month's amounts are keyed by line
 * ('hires', 'increment|760001'), so they never mix with the accounts' own figures.
 *   - Snowflake's planned hires, leavers and returns from leave: one line per source summed across the payroll
 *     accounts, then "Total headcount changes". They show only where they count in Total payroll: the projected
 *     months (countedChangeRows).
 *   - The override file: one line per account and type, summing an account's rows of a month across
 *     departments. Increments (then "Total increments") count the same way (countedIncrementRows); overrides
 *     (italic notes, then "Total overrides") show in every month of the table, for reference.
 * Rows outside the page's months are left out (the rows list under the table counts the override file's).
 */
function plannedLines(payload: SalaryPayload): PlannedDefs & { sums: Map<string, MonthSums> } {
  const months = new Set(payload.blocks.flatMap((b) => b.rows.map((r) => r.mKey)));
  const sums = new Map<string, MonthSums>();
  const addTo = (key: string, month: string, eur: number, ils: number) => {
    const m = sums.get(month) ?? { eur: {}, ils: {} };
    m.eur[key] = (m.eur[key] ?? 0) + eur;
    m.ils[key] = (m.ils[key] ?? 0) + ils;
    sums.set(month, m);
  };
  const defs: SalaryLineDef[] = [];
  const totalKeys: string[] = [];

  // Snowflake's planned hires, leavers and returns from leave: one line per source, summed across the payroll
  // accounts (the hint names them).
  const changes = new Map<Source, Map<string, string>>();
  for (const r of countedChangeRows(payload.blocks, payload.changes ? payload.changes.rows : [])) {
    const accts = changes.get(r.source) ?? new Map<string, string>();
    if (!accts.has(r.acct)) accts.set(r.acct, r.name);
    changes.set(r.source, accts);
    addTo(SOURCE_KEY[r.source], r.month, r.eur, r.ils);
  }
  const changeSources = SOURCES.filter((source) => changes.has(source));
  if (changeSources.length) {
    defs.push(header('header-changes', 'Headcount changes', `Planned hires, leavers and returns from leave on the payroll accounts, from Snowflake's payroll forecast.${COUNTED}`));
  }
  for (const source of changeSources) {
    const key = SOURCE_KEY[source];
    const accts = [...(changes.get(source) ?? new Map<string, string>()).entries()].sort(([a], [b]) => a.localeCompare(b)).map(([a, n]) => accountLabel(a, n));
    defs.push({
      key, label: SOURCE_LINE[source], kind: 'item', fy: 'sum', value: (f) => f[key] ?? 0,
      hint: `${SOURCE_HINT[source]} Accounts: ${listOf(accts, 8)}.${COUNTED}`,
    });
  }
  if (changeSources.length) {
    const keys = changeSources.map((source) => SOURCE_KEY[source]);
    totalKeys.push(...keys);
    defs.push({
      key: 'total-changes', label: 'Total headcount changes', kind: 'subtotal', fy: 'sum', value: sumOf(keys),
      hint: `Planned hires, leavers and returns from leave (Snowflake).${COUNTED}`,
    });
  }

  // The override file.
  const rows = payload.overrides ? payload.overrides.rows : [];
  const counted = new Set(countedIncrementRows(payload.blocks, rows));
  const lines = new Map<string, { mode: Mode; acct: string; name: string; rows: PayrollOverride[] }>();
  for (const r of rows) {
    if (r.mode === 'Increment' ? !counted.has(r) : !months.has(r.month)) continue;
    const key = `${MODE_KEY[r.mode]}|${r.acct}`;
    const line = lines.get(key) ?? { mode: r.mode, acct: r.acct, name: r.name, rows: [] };
    line.rows.push(r);
    lines.set(key, line);
    addTo(key, r.month, r.eur, r.ils);
  }
  const list = [...lines.entries()].sort(([, a], [, b]) => a.acct.localeCompare(b.acct));
  if (list.length) {
    defs.push(header('header-overrides', 'Override file', 'The override sheet\'s rows (Snowflake): increments are added to Total payroll from the current month on; overrides are for reference.'));
  }
  for (const mode of MODES) {
    const ofMode = list.filter(([, l]) => l.mode === mode);
    if (!ofMode.length) continue;
    for (const [key, l] of ofMode) {
      const departments = listOf(l.rows.map((r) => r.department), 6);
      const comments = listOf(l.rows.map((r) => r.memo), 4);
      const estimated = l.rows.some((r) => r.ilsEstimated) ? ' ≈ The file has euros only for some months: shekels at the forecast rate.' : '';
      defs.push({
        key, label: `${accountLabel(l.acct, l.name)} (${MODE_KEY[mode]})`, kind: mode === 'Increment' ? 'item' : 'note', fy: 'sum',
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
  /** Full-year payroll per year (actuals + forecast + the planned changes), and the planned changes in it. */
  byYear: { year: number; value: number; changes: number }[];
  /** The permanent accounts' monthly run-rate (the last closed month). */
  runRate: { value: number; mKey: string };
}

export function computeSalaryKpis(payload: SalaryPayload, ccy: Ccy): SalaryKpis {
  const all = sumOf(payload.accounts.map((a) => a.acct));
  const permanent = sumOf(payload.accounts.filter((a) => a.group === 'permanent').map((a) => a.acct));
  const planned = plannedLines(payload);
  const counted = sumOf(planned.totalKeys);
  const changesIn = (mKey: string) => {
    const s = planned.sums.get(mKey);
    return s ? counted(s[ccy]) : 0;
  };
  const current = payload.blocks.find((b) => b.kind === 'current') ?? payload.blocks[0];
  const actual = current.rows.filter((r) => r.status === 'actual');
  // The run-rate month's own figures when it is in the table; else (January) the first projected month,
  // which carries it.
  const rateRow = current.rows.find((r) => r.mKey === payload.basis.runRateMonth) ?? current.rows.find((r) => r.status !== 'actual') ?? current.rows[0];
  return {
    // Planned changes never count in actual months, so year to date is the accounts' actuals.
    ytd: { value: actual.reduce((s, r) => s + all(r[ccy]), 0), through: payload.actuals.through },
    byYear: payload.blocks.map((b) => {
      const changes = b.rows.reduce((s, r) => s + changesIn(r.mKey), 0);
      return { year: b.year, value: b.rows.reduce((s, r) => s + all(r[ccy]), 0) + changes, changes };
    }),
    runRate: { value: permanent(rateRow[ccy]), mKey: payload.basis.runRateMonth },
  };
}
