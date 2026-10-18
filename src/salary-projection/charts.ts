// Pure data for the Salary Projection's Charts view (SalaryCharts.tsx): the payroll by month, stacked by the
// payroll table's sections, and HiBob's headcount by month. Both are read off the tables the Table view shows
// (buildSalaryTable, buildHeadcountTable), so the two views always agree and follow the same currency and year
// view. No React, no DOM — unit-tested by scripts/test-salary-projection.cjs.
import { CCY_SYMBOL, type Accent, type ProjectionTable } from '../new-dashboard/model.ts';
import type { Ccy, MonthStatus, SalaryPayload } from './types.ts';

export type PayrollSeriesKey = 'permanent' | 'changes' | 'increments' | 'temporary' | 'other' | 'deel';

/** One stacked series of the payroll chart: a section of the table, by its subtotal line, in its colour. */
export interface PayrollSeries {
  key: PayrollSeriesKey;
  /** The table line it is read from. */
  line: string;
  label: string;
  accent: Accent;
}

/**
 * The stack, bottom first: the table's sections in its order, Deel on top, so a month's column is Total payroll,
 * plus Deel when the table has it. The override file's overrides are for reference (not in Total payroll), so
 * they are not drawn.
 */
export const PAYROLL_SERIES: PayrollSeries[] = [
  { key: 'permanent', line: 'total-permanent', label: 'Permanent', accent: 'sky' },
  { key: 'changes', line: 'total-changes', label: 'Headcount changes', accent: 'amber' },
  { key: 'increments', line: 'total-increments', label: 'Override increments', accent: 'violet' },
  { key: 'temporary', line: 'total-temporary', label: 'Temporary', accent: 'emerald' },
  { key: 'other', line: 'total-other', label: 'Not classified', accent: 'slate' },
  { key: 'deel', line: 'total-deel', label: 'Deel', accent: 'pink' },
];

/** One month of the payroll chart: each series' amount (0 when not drawn), Total payroll, and the column. */
export type PayrollPoint = Record<PayrollSeriesKey, number> & {
  mKey: string;
  /** 'Oct 26' */
  label: string;
  status: MonthStatus;
  /** The current month or a forecast one: drawn faded. */
  projected: boolean;
  /** Total payroll. */
  payroll: number;
  /** The whole column: Payroll + Deel when the table has Deel, else Total payroll. */
  total: number;
};

/** A value axis: its extent and its ticks. */
export interface Axis {
  domain: [number, number];
  ticks: number[];
}

/** The step between ticks: the first of 1, 2, 2.5 and 5 × 10ⁿ that is at least raw (no 2.5 for whole numbers). */
function niceStep(raw: number, whole: boolean): number {
  const pow = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].filter((m) => !(whole && m === 2.5)).map((m) => m * pow).find((s) => s >= raw * (1 - 1e-9)) ?? 10 * pow;
  return whole ? Math.max(1, Math.round(step)) : step;
}

/**
 * A column chart's axis, from zero: about four steps over the larger side, ending on a tick. Below zero it goes
 * only as far as the lowest column needs, so a small dip does not take a whole step.
 */
export function valueAxis(min: number, max: number, whole = false): Axis {
  const lo = Math.min(0, min);
  const hi = Math.max(0, max);
  const step = niceStep(Math.max(hi, -lo, whole ? 1 : 1e-6) / 4, whole);
  const down = Math.ceil(-lo / step - 1e-9);
  const up = Math.ceil(hi / step - 1e-9) || (down ? 0 : 1);
  const bottom = lo < 0 ? Math.max(-down * step, lo * 1.15) : 0;
  const ticks: number[] = [];
  for (let k = -down; k <= up; k++) if (k * step >= bottom - 1e-9) ticks.push(k * step);
  return { domain: [bottom, up * step], ticks };
}

/** A line's axis, not from zero (whole numbers): ticks around the line, a tick of room above and below it. */
export function rangeAxis(low: number, high: number): Axis {
  const step = niceStep(Math.max(high - low, 4) / 4, true);
  let bottom = Math.floor(low / step) * step;
  let top = Math.ceil(high / step) * step;
  if (bottom === low) bottom -= step;
  if (top === high) top += step;
  bottom = Math.max(0, bottom);
  const ticks: number[] = [];
  for (let t = bottom; t <= top; t += step) ticks.push(t);
  return { domain: [bottom, top], ticks };
}

/**
 * The payroll chart, from the payroll table as shown: one point per month column (no full-year columns) and the
 * series with an amount in those months, in stack order. A month's series add up to its column (total). The
 * axis covers the stacks: amounts above zero stack up, below zero down.
 */
export function payrollChart(table: ProjectionTable<string>): { series: PayrollSeries[]; points: PayrollPoint[]; axis: Axis } {
  const lines = new Map(table.lines.map((l) => [l.key, l]));
  const months = table.columns.flatMap((c, i) => (c.kind === 'month' && c.mKey && c.status ? [{ c, i, mKey: c.mKey, status: c.status }] : []));
  const valueOf = (line: string, i: number) => lines.get(line)?.values[i] ?? 0;
  const series = PAYROLL_SERIES.filter((s) => months.some(({ i }) => Math.abs(valueOf(s.line, i)) >= 0.5));
  const drawn = new Set(series.map((s) => s.key));
  const points = months.map(({ c, i, mKey, status }): PayrollPoint => {
    const point = {
      mKey, label: c.label, status, projected: status !== 'actual',
      payroll: valueOf('total', i), total: valueOf(lines.has('payroll-deel') ? 'payroll-deel' : 'total', i),
    } as PayrollPoint;
    for (const s of PAYROLL_SERIES) point[s.key] = drawn.has(s.key) ? valueOf(s.line, i) : 0;
    return point;
  });
  const stack = (pt: PayrollPoint, sign: 1 | -1) => series.reduce((sum, s) => sum + (pt[s.key] * sign > 0 ? pt[s.key] : 0), 0);
  const axis = valueAxis(Math.min(0, ...points.map((pt) => stack(pt, -1))), Math.max(0, ...points.map((pt) => stack(pt, 1))));
  return { series, points, axis };
}

/** One month of the headcount charts. */
export interface HeadcountPoint {
  mKey: string;
  label: string;
  /** HiBob's status of the month: before the current month, actual. */
  status: MonthStatus;
  projected: boolean;
  /** HiBob's employees at month-end; null when HiBob has no figure for the month. */
  employees: number | null;
  /**
   * The line in two parts: actual months solid, projected ones dashed. The last actual month is in both, so
   * the dashed part starts where the solid one ends.
   */
  employeesActual: number | null;
  employeesProjected: number | null;
  hires: number;
  /** Negative, as in the table: drawn below zero. */
  leavers: number;
  /** People who moved to another department: already employees, not drawn (the count does not move). */
  transfers: number;
  /** HiBob's open positions: shown, not counted. */
  openings: number;
}

/**
 * The headcount charts, from the headcount table as shown (its month columns) and HiBob's months, with the
 * employees line's axis (null when HiBob has no figure in those months) and the hires and leavers' (at least one
 * step each side of zero).
 */
export function headcountChart(
  table: ProjectionTable<string>, headcount: NonNullable<SalaryPayload['headcount']>,
): { points: HeadcountPoint[]; employeesAxis: Axis | null; movesAxis: Axis } {
  const byKey = new Map(headcount.months.map((m) => [m.mKey, m]));
  const months = table.columns.flatMap((c) => (c.kind === 'month' && c.mKey && c.status ? [{ label: c.label, mKey: c.mKey, status: c.status }] : []));
  const lastActual = months.reduce((last, m, i) => (m.status === 'actual' ? i : last), -1);
  const points = months.map(({ label, mKey, status }, i): HeadcountPoint => {
    const m = byKey.get(mKey);
    const employees = m ? m.employees : null;
    const projected = status !== 'actual';
    return {
      mKey, label, status, projected, employees,
      employeesActual: projected ? null : employees,
      employeesProjected: projected || (i === lastActual && i < months.length - 1) ? employees : null,
      hires: m?.hires ?? 0,
      leavers: -(m?.leavers ?? 0),
      transfers: m?.transfers ?? 0,
      openings: m?.openings ?? 0,
    };
  });
  const counts = points.flatMap((pt) => (pt.employees === null ? [] : [pt.employees]));
  return {
    points,
    employeesAxis: counts.length ? rangeAxis(Math.min(...counts), Math.max(...counts)) : null,
    movesAxis: valueAxis(Math.min(-1, ...points.map((pt) => pt.leavers)), Math.max(1, ...points.map((pt) => pt.hires)), true),
  };
}

/** An axis tick in the currency, compact: '₪4.25M', '₪850K', '−₪120K', '₪0'. */
export function formatAxisMoney(n: number, ccy: Ccy): string {
  const a = Math.abs(n);
  const sign = n < 0 ? '−' : '';
  const short = (v: number) => String(Number(v.toFixed(2)));
  if (a >= 1_000_000) return `${sign}${CCY_SYMBOL[ccy]}${short(a / 1_000_000)}M`;
  if (a >= 1_000) return `${sign}${CCY_SYMBOL[ccy]}${short(a / 1_000)}K`;
  return `${a ? sign : ''}${CCY_SYMBOL[ccy]}${Math.round(a)}`;
}
