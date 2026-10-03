// Pure table model for the New Bank Dashboard: turns the API payload into columns, line items and
// KPI values. No React, no DOM — unit-tested directly by scripts/test-cash-projection.cjs.
import type { Ccy, Figures, MonthRow, MonthStatus, ProjectionPayload, Variant, VariantKey, YearBlock, YearView } from './types.ts';

export type LineKey =
  | 'opening' | 'collections' | 'pipeline' | 'churn' | 'inflows'
  | 'salary' | 'vendors' | 'other' | 'outflows'
  | 'reval' | 'net' | 'dividend' | 'closing' | 'reanchor' | 'gap';

export type LineKind = 'balance' | 'item' | 'subtotal' | 'note';

interface LineDef {
  key: LineKey;
  label: string;
  kind: LineKind;
  hint?: string;
  /** Displayed value for one month. Outflow lines show positive amounts; churn shows as a deduction. */
  value: (f: Figures) => number;
  /** Full-year column: sum of the months, or the first/last month for balances. */
  fy: 'sum' | 'first' | 'last';
  /** Shown with an explicit + / − and green / red instead of parentheses. */
  signed?: boolean;
}

const inflows = (f: Figures) => f.collections + f.pipeline - f.churn;
const outflows = (f: Figures) => f.salary + f.vendors + f.other;

export const LINES: LineDef[] = [
  { key: 'opening', label: 'Opening balance', kind: 'balance', fy: 'first', value: (f) => f.opening },
  { key: 'collections', label: 'Collections (AR)', kind: 'item', fy: 'sum', value: (f) => f.collections,
    hint: 'Actual months: cash received from customers. Forecast: expected revenue × collection rate.' },
  { key: 'pipeline', label: 'Pipeline', kind: 'item', fy: 'sum', value: (f) => f.pipeline,
    hint: 'New business from the open pipeline (forecast months of the current year only).' },
  { key: 'churn', label: 'Churn', kind: 'item', fy: 'sum', value: (f) => -f.churn,
    hint: 'Revenue lost to churned customers, cumulative across forecast months.' },
  { key: 'inflows', label: 'Total inflows', kind: 'subtotal', fy: 'sum', value: inflows },
  { key: 'salary', label: 'Salary', kind: 'item', fy: 'sum', value: (f) => f.salary,
    hint: 'Actual months: NetSuite payroll. Forecast: last closed payroll month by department, plus planned changes.' },
  { key: 'vendors', label: 'Vendors', kind: 'item', fy: 'sum', value: (f) => f.vendors,
    hint: 'Actual months: vendor payments. Forecast: vendor budget, plus planned changes.' },
  { key: 'other', label: 'Other (tax, I/C, fees)', kind: 'item', fy: 'sum', value: (f) => f.other,
    hint: 'Bank movements outside the other lines (taxes, intercompany, fees, transfers). Shown in parentheses when it is a net inflow.' },
  { key: 'outflows', label: 'Total outflows', kind: 'subtotal', fy: 'sum', value: outflows },
  { key: 'reval', label: 'Reval (FX)', kind: 'item', fy: 'sum', value: (f) => f.reval,
    hint: 'Actual months: booked FX revaluation. Forecast: currency-defense budget × defense %.' },
  { key: 'net', label: 'Net change', kind: 'subtotal', fy: 'sum', value: (f) => f.net,
    hint: 'Total inflows − total outflows + reval. Dividends are shown separately below.' },
  { key: 'dividend', label: 'Dividend paid', kind: 'item', fy: 'sum', value: (f) => (f.dividend ? -f.dividend : 0),
    hint: 'Dividend distributions and their withholding tax paid from the bank (NetSuite). Kept out of Vendors and Other. Future dividends are not forecast.' },
  { key: 'closing', label: 'Closing balance', kind: 'balance', fy: 'last', value: (f) => f.closing,
    hint: 'Opening + net change − dividend paid: the cash in the bank at month-end.' },
  { key: 'reanchor', label: 'incl. bank re-anchor', kind: 'note', fy: 'sum', value: (f) => f.reanchor,
    hint: 'The current month opens at the actual NetSuite bank balance of the previous month-end. This is the difference to the model\'s previous closing; it is already inside the opening balance.' },
  { key: 'gap', label: 'Monthly gap', kind: 'subtotal', fy: 'sum', signed: true, value: (f) => f.closing - f.opening,
    hint: 'Closing − opening: how much the bank balance rose (+) or fell (−) in the month, after dividends (net change − dividend paid). FY: the sum of the months.' },
];

export interface Column {
  id: string;
  kind: 'month' | 'fy';
  year: number;
  yearKind: YearBlock['kind'];
  /** 'Oct 26' or 'FY 2026' */
  label: string;
  /** 'Actual' | 'Current' | 'Forecast' | '' */
  statusLabel: string;
  status: MonthStatus | null;
  mKey: string | null;
  /** First column of a year group (draws the divider). */
  firstOfYear: boolean;
  /** January of the projection year: its opening is the previous December's closing. */
  rollForward: boolean;
}

export interface TableLine {
  key: LineKey;
  label: string;
  kind: LineKind;
  hint?: string;
  signed?: boolean;
  /** One value per column, in display sign. */
  values: number[];
}

export interface ProjectionTable {
  columns: Column[];
  lines: TableLine[];
  /** Year groups for the top header row. */
  groups: { year: number; kind: YearBlock['kind']; span: number; label: string }[];
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const STATUS_LABEL: Record<MonthStatus, string> = { actual: 'Actual', current: 'Current', forecast: 'Forecast' };

export function monthLabel(mKey: string): string {
  const [y, m] = mKey.split('-');
  return `${MONTHS[Number(m) - 1] ?? m} ${y.slice(2)}`;
}

/** Long form for tooltips: 'October 2026'. */
export function monthLongLabel(mKey: string): string {
  const [y, m] = mKey.split('-');
  const d = new Date(Number(y), Number(m) - 1, 1);
  return d.toLocaleString('en-GB', { month: 'long', year: 'numeric' });
}

function yearsForView(variant: Variant, view: YearView): YearBlock[] {
  if (view === 'current') return variant.years.filter((y) => y.kind === 'current');
  if (view === 'next') return variant.years.filter((y) => y.kind === 'projection');
  return variant.years;
}

function fyValue(line: LineDef, rows: MonthRow[], ccy: Ccy): number {
  if (line.fy === 'first') return line.value(rows[0][ccy]);
  if (line.fy === 'last') return line.value(rows[rows.length - 1][ccy]);
  return rows.reduce((s, r) => s + line.value(r[ccy]), 0);
}

/** Columns + line values for the selected variant, currency and year view. */
export function buildTable(variant: Variant, ccy: Ccy, view: YearView): ProjectionTable {
  const blocks = yearsForView(variant, view);
  const columns: Column[] = [];
  const groups: ProjectionTable['groups'] = [];
  for (const block of blocks) {
    block.rows.forEach((r, i) => {
      columns.push({
        id: r.mKey,
        kind: 'month',
        year: block.year,
        yearKind: block.kind,
        label: monthLabel(r.mKey),
        statusLabel: STATUS_LABEL[r.status],
        status: r.status,
        mKey: r.mKey,
        firstOfYear: i === 0,
        rollForward: block.kind === 'projection' && i === 0,
      });
    });
    columns.push({
      id: `fy-${block.year}`, kind: 'fy', year: block.year, yearKind: block.kind,
      label: `FY ${block.year}`, statusLabel: '', status: null, mKey: null, firstOfYear: false, rollForward: false,
    });
    groups.push({
      year: block.year,
      kind: block.kind,
      span: block.rows.length + 1,
      label: block.kind === 'current' ? `${block.year} · actuals + forecast` : `${block.year} · projection, rolled forward from Dec ${block.year - 1}`,
    });
  }

  const lines: TableLine[] = LINES.map((line) => {
    const values: number[] = [];
    for (const block of blocks) {
      for (const r of block.rows) values.push(line.value(r[ccy]));
      values.push(fyValue(line, block.rows, ccy));
    }
    return { key: line.key, label: line.label, kind: line.kind, hint: line.hint, signed: line.signed, values };
  });

  // The re-anchor note only appears when it is material (≥ 1 currency unit somewhere).
  const visible = lines.filter((l) => l.key !== 'reanchor' || l.values.some((v) => Math.abs(v) >= 1));
  return { columns, lines: visible, groups };
}

export interface Kpis {
  bankToday: { value: number; asOf: string } | null;
  closings: { year: number; value: number }[];
  lowest: { value: number; mKey: string } | null;
}

export function computeKpis(payload: ProjectionPayload, variantKey: VariantKey, ccy: Ccy): Kpis {
  const variant = payload.variants[variantKey];
  const closings = variant.years.map((y) => ({ year: y.year, value: y.rows[y.rows.length - 1][ccy].closing }));
  let lowest: Kpis['lowest'] = null;
  for (const y of variant.years) {
    for (const r of y.rows) {
      if (r.status === 'actual') continue;
      const v = r[ccy].closing;
      if (!lowest || v < lowest.value) lowest = { value: v, mKey: r.mKey };
    }
  }
  return {
    bankToday: payload.bankToday ? { value: payload.bankToday[ccy], asOf: payload.bankToday.asOf } : null,
    closings,
    lowest,
  };
}

// ── formatting ──────────────────────────────────────────────────────────────
const intFmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });

/** Thousands with negatives in parentheses; '–' when it rounds to zero. */
export function formatThousands(n: number): string {
  const k = Math.round(n / 1000);
  if (k === 0 || Object.is(k, -0)) return '–';
  return k < 0 ? `(${intFmt.format(-k)})` : intFmt.format(k);
}

/** Thousands with an explicit sign: '+1,235' / '−1,235' (true minus); '–' when it rounds to zero. */
export function formatSignedThousands(n: number): string {
  const k = Math.round(n / 1000);
  if (k === 0 || Object.is(k, -0)) return '–';
  return k < 0 ? `−${intFmt.format(-k)}` : `+${intFmt.format(k)}`;
}

export const CCY_SYMBOL: Record<Ccy, string> = { eur: '€', ils: '₪' };

/** Whole units with the currency symbol: '€7,050,123' / '-€1,200' . */
export function formatFull(n: number, ccy: Ccy): string {
  const v = Math.round(n);
  return `${v < 0 ? '-' : ''}${CCY_SYMBOL[ccy]}${intFmt.format(Math.abs(v))}`;
}

/** Millions with one decimal for KPI cards: '€7.1M'. */
export function formatMillions(n: number, ccy: Ccy): string {
  const m = Math.round(n / 100_000) / 10;
  return `${m < 0 ? '-' : ''}${CCY_SYMBOL[ccy]}${Math.abs(m).toFixed(1)}M`;
}
