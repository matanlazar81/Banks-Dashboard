// Pure table model for the P&L Projection: line items and KPI values from the API payload.
// Columns, full-year values and formatting are the New Bank Dashboard's (src/new-dashboard/model.ts).
// No React, no DOM — unit-tested directly by scripts/test-pnl-projection.cjs.
import { buildYearTable, type LineDefOf, type ProjectionTable } from '../new-dashboard/model.ts';
import type { Ccy, PnlFigures, PnlPayload, PnlVariant, VariantKey } from './types.ts';
import type { YearView } from '../new-dashboard/types.ts';

export type PnlLineKey =
  | 'accOpening' | 'revenue' | 'pipeline' | 'churn' | 'otherRevenue' | 'totalRevenue'
  | 'payroll' | 'capex' | 'opex' | 'totalCosts' | 'ebitda'
  | 'fx' | 'finance' | 'depreciation' | 'taxOther' | 'net' | 'accClosing';

export const PNL_LINES: LineDefOf<PnlFigures, PnlLineKey>[] = [
  { key: 'accOpening', label: 'Accumulated profit, opening', kind: 'balance', fy: 'first', value: (f) => f.accOpening,
    hint: 'Net profit accumulated since 1 January of the first year shown. The next year continues from December (no reset).' },
  { key: 'revenue', label: 'Customer revenue', kind: 'item', fy: 'sum', value: (f) => f.revenue,
    hint: 'Actual months: NetSuite revenue accounts. Forecast: expected revenue of signed customers (Snowflake) plus open deals at the plan\'s minimum probability. No collection rate: revenue is recognised, not collected.' },
  { key: 'pipeline', label: 'Pipeline', kind: 'item', fy: 'sum', value: (f) => f.pipeline,
    hint: 'New business from the open pipeline, cumulative, from the current month (current year only).' },
  { key: 'churn', label: 'Churn', kind: 'item', fy: 'sum', value: (f) => -f.churn,
    hint: 'Revenue lost to churned customers, cumulative across forecast months.' },
  { key: 'otherRevenue', label: 'Other & intercompany revenue', kind: 'item', fy: 'sum', value: (f) => f.otherRevenue,
    hint: 'Intercompany (Statscore, Bringits), sub-lease, asset sales and other income. Forecast: average of the last 3 closed months.' },
  { key: 'totalRevenue', label: 'Total revenue', kind: 'subtotal', fy: 'sum', value: (f) => f.totalRevenue },
  { key: 'payroll', label: 'Payroll', kind: 'item', fy: 'sum', value: (f) => f.payroll,
    hint: 'Actual months: NetSuite payroll accounts (76xxxx). Forecast: last closed payroll month by department, plus hires, leavers and planned changes.' },
  { key: 'capex', label: 'Salaries CAPEX', kind: 'item', fy: 'sum', value: (f) => f.capex,
    hint: 'Payroll capitalised as CAPEX (NetSuite account 950000), a credit that lowers costs. Forecast: the last closed month, carried flat.' },
  { key: 'opex', label: 'Operating expenses', kind: 'item', fy: 'sum', value: (f) => f.opex,
    hint: 'All other overheads of the EBITDA report. Forecast: vendor budget plus planned changes; the next year mirrors this year month by month.' },
  { key: 'totalCosts', label: 'Total operating costs', kind: 'subtotal', fy: 'sum', value: (f) => f.totalCosts },
  { key: 'ebitda', label: 'Operating profit (EBITDA)', kind: 'subtotal', fy: 'sum', value: (f) => f.ebitda,
    hint: 'Total revenue − total operating costs. Actual months equal the Operating Profit of NetSuite\'s EBITDA P&L.' },
  { key: 'fx', label: 'FX revaluation', kind: 'item', fy: 'sum', value: (f) => f.fx,
    hint: 'NetSuite 800028–800031. Forecast: currency-defense budget × defense %. None projected for the next year.' },
  { key: 'finance', label: 'Finance, net', kind: 'item', fy: 'sum', value: (f) => f.finance,
    hint: 'Bank fees, interest and other finance accounts. Forecast: average of the last 3 closed months.' },
  { key: 'depreciation', label: 'Depreciation', kind: 'item', fy: 'sum', value: (f) => f.depreciation,
    hint: 'Forecast: the depreciation budget (Snowflake) when the year has one, else the average of the last 3 closed months.' },
  { key: 'taxOther', label: 'Tax & other', kind: 'item', fy: 'sum', value: (f) => f.taxOther,
    hint: 'Income tax, IFRS 16 and other accounts outside EBITDA. Forecast: the budget (Snowflake) when there is one.' },
  { key: 'net', label: 'Net profit', kind: 'subtotal', fy: 'sum', value: (f) => f.net,
    hint: 'EBITDA + the lines below it. Actual months equal the sum of every NetSuite P&L account.' },
  { key: 'accClosing', label: 'Accumulated profit, closing', kind: 'balance', fy: 'last', value: (f) => f.accClosing,
    hint: 'Accumulated profit, opening + net profit.' },
];

/** Lines whose cells open a breakdown. */
export const PNL_BREAKDOWN_LINES: ReadonlySet<string> = new Set<PnlLineKey>([
  'revenue', 'pipeline', 'churn', 'otherRevenue', 'payroll', 'capex', 'opex', 'fx', 'finance', 'depreciation', 'taxOther',
]);

/** Columns + P&L lines for the selected variant, currency and year view. */
export function buildPnlTable(variant: PnlVariant, ccy: Ccy, view: YearView): ProjectionTable<PnlLineKey> {
  return buildYearTable(variant.years, ccy, view, PNL_LINES);
}

export interface PnlKpis {
  /** EBITDA of the NetSuite months of the current year. */
  ebitdaYtd: { value: number; through: string | null };
  netByYear: { year: number; value: number }[];
  accumulatedEnd: { value: number; mKey: string };
}

export function computePnlKpis(payload: PnlPayload, variantKey: VariantKey, ccy: Ccy): PnlKpis {
  const variant = payload.variants[variantKey];
  const current = variant.years.find((y) => y.kind === 'current') ?? variant.years[0];
  const actual = current.rows.filter((r) => r.status === 'actual');
  const lastYear = variant.years[variant.years.length - 1];
  const last = lastYear.rows[lastYear.rows.length - 1];
  return {
    ebitdaYtd: { value: actual.reduce((s, r) => s + r[ccy].ebitda, 0), through: payload.actuals.through },
    netByYear: variant.years.map((y) => ({ year: y.year, value: y.rows.reduce((s, r) => s + r[ccy].net, 0) })),
    accumulatedEnd: { value: last[ccy].accClosing, mKey: last.mKey },
  };
}
