// Pure helpers for the Metrics page: formatting and the rows of the pack as exported. No React, no DOM —
// unit-tested by scripts/test-metrics.cjs.
import type { Cell, FigureStatus, Metric, MetricsPayload } from './types.ts';

const int = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });

/** € in millions (2 decimals) from €1M, else in thousands; '–' for nothing. */
export function formatEur(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return '–';
  const sign = n < 0 ? '-' : '';
  const a = Math.abs(n);
  if (a >= 1_000_000) return `${sign}€${(a / 1_000_000).toFixed(2)}M`;
  if (a >= 1_000) return `${sign}€${int.format(Math.round(a / 1_000))}K`;
  return a < 0.5 ? '–' : `${sign}€${int.format(Math.round(a))}`;
}

/** Whole euros: '€1,234,567'. */
export function formatEurFull(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return '–';
  return `${n < 0 ? '-' : ''}€${int.format(Math.round(Math.abs(n)))}`;
}

export function formatPct(n: number | null | undefined, digits = 1): string {
  return n === null || n === undefined || !Number.isFinite(n) ? '–' : `${n.toFixed(digits)}%`;
}

export function formatCell(cell: Cell | null, unit: Metric['unit']): string {
  if (!cell || cell.value === null) return '–';
  return unit === 'pct' ? formatPct(cell.value) : formatEur(cell.value);
}

export const STATUS_SHORT: Record<FigureStatus, string> = { actual: 'A', forecast: 'F', 'actual+forecast': 'A+F' };
export const STATUS_LONG: Record<FigureStatus, string> = {
  actual: 'Actual', forecast: 'Forecast', 'actual+forecast': 'Actual months + forecast months',
};

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
export const monthName = (mKey: string | null) => (mKey ? `${MONTHS[Number(mKey.slice(5)) - 1]} ${mKey.slice(0, 4)}` : '–');
export const MONTH_NAMES = MONTHS;

const tagged = (cell: Cell | null, unit: Metric['unit']) => (cell && cell.value !== null
  ? `${unit === 'pct' ? formatPct(cell.value) : formatEurFull(cell.value)} (${STATUS_SHORT[cell.status]})`
  : '–');

/** The pack as rows (Excel export): the same order and wording every month. */
export function packRows(p: MetricsPayload): (string | number)[][] {
  const [y0, y1] = p.years;
  const rows: (string | number)[][] = [
    [`LSports metrics pack, as of ${monthName(p.asOf.lastClosed)}`],
    [`Generated ${new Date(p.generatedAt).toLocaleString('en-GB')} · Plan: ${p.asOf.plan || '–'} · A = actual, F = forecast, A+F = actual months + forecast months`],
    ...(p.targets && p.targets.active
      ? [[`FY ${p.targets.year} includes the ${p.targets.year} targets set on the New Bank Dashboard: ${p.targets.assumptions.join('; ')}`]]
      : []),
    [],
    ['Metric', 'Last month', 'Year to date', `FY ${y0}`, `FY ${y1}`, 'Basis'],
    ...p.metrics.map((m) => [m.label, tagged(m.lastMonth, m.unit), tagged(m.ytd, m.unit), tagged(m.fy[0], m.unit), tagged(m.fy[1], m.unit), m.note]),
    [],
    ...(p.people && p.people.ytd ? [
      [`Payroll / revenue and revenue per employee, through ${monthName(p.people.through)} (the last payroll JE posted)`],
      ['Month', 'Payroll (76xxxx, gross)', 'Revenue', 'Payroll / revenue', 'Employees (month-end)', 'Revenue per employee'],
      ...p.people.months.map((m) => [monthName(m.mKey), m.payroll, m.revenue, formatPct(m.payrollPct, 1), m.headcount ?? '–', m.revenuePerEmployee ?? '–']),
      [p.people.ytd.label, p.people.ytd.payroll, p.people.ytd.revenue, formatPct(p.people.ytd.payrollPct, 1),
        p.people.ytd.avgHeadcount === null ? '–' : `${p.people.ytd.avgHeadcount} average`, p.people.ytd.revenuePerEmployee ?? '–'],
      [],
    ] : []),
    [`Cloud (${p.cloud.category || 'cloud'}, NetSuite ${p.cloud.accounts}) against ${formatPct(p.cloud.capPct, 1)} of projected revenue`],
    ['Year', 'Cloud', 'Cap', 'Headroom', '% of revenue', 'Status'],
    ...p.cloud.years.map((c) => [
      `FY ${c.year} (${STATUS_SHORT[c.status]})`, formatEurFull(c.total), formatEurFull(c.cap), formatEurFull(c.headroom), formatPct(c.pctOfRevenue, 2),
      c.within ? 'Within the cap' : 'Over the cap',
      ...(c.targets ? [c.targets.kind === 'server' ? `Server costs ${formatPct(c.targets.pct, 1)} of customer revenue (targets)` : `Category ${c.targets.pct > 0 ? '+' : ''}${formatPct(c.targets.pct, 1)} (targets)`] : []),
    ]),
  ];
  return rows;
}

/** A breakdown figure as text: € in full, % to one decimal, counts as they are. */
export function formatExplain(v: number | string | null | undefined, unit: 'eur' | 'pct' | 'int' | 'text', sign = false): string {
  if (v === null || v === undefined || v === '') return '–';
  if (unit === 'text' || typeof v === 'string') return String(v);
  const plus = sign && v > 0 ? '+' : '';
  if (unit === 'pct') return `${plus}${formatPct(v, 1)}`;
  if (unit === 'int') return `${plus}${int.format(v)}`;
  return v === 0 ? '€0' : `${plus}${formatEurFull(v)}`;
}
