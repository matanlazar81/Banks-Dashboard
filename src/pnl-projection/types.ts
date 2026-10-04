// Response shapes of GET /api/pnl-projection (server/pnl-projection.cjs).
import type { Ccy, ComputingResponse, ErrorResponse, MonthStatus, VariantKey } from '../new-dashboard/types.ts';

export type { Ccy, MonthStatus, VariantKey };

/**
 * One month's P&L in one currency.
 *   revenue, pipeline, otherRevenue, totalRevenue  income positive; churn = revenue lost (shown as a deduction)
 *   payroll, capex, opex, totalCosts               costs positive; the Salaries CAPEX credit is negative
 *   fx, finance, depreciation, taxOther            profit-signed (gain positive, cost negative)
 */
export interface PnlFigures {
  accOpening: number;
  revenue: number;
  pipeline: number;
  churn: number;
  otherRevenue: number;
  totalRevenue: number;
  payroll: number;
  capex: number;
  opex: number;
  totalCosts: number;
  /** Operating profit, as NetSuite's "EBITDA_Profit and Loss" report. */
  ebitda: number;
  fx: number;
  finance: number;
  depreciation: number;
  taxOther: number;
  /** Net profit: the sum of every P&L account. */
  net: number;
  accClosing: number;
}

export interface PnlMonthRow {
  mKey: string;
  status: MonthStatus;
  eur: PnlFigures;
  ils: PnlFigures;
}

export interface PnlYearBlock {
  year: number;
  kind: 'current' | 'projection';
  rows: PnlMonthRow[];
}

export interface PnlVariant {
  years: PnlYearBlock[];
  rollForward: {
    from: string;
    to: string;
    /** Accumulated net profit at the end of December, where the next year starts. */
    accumulated: { eur: number; ils: number };
    salaryBasis: { method: 'run-rate' | 'flat-budget'; monthsWithData: number; scale: { eur: number; ils: number } | null };
  };
}

export interface PnlPayload {
  ok: true;
  status: 'ready';
  schemaVersion: number;
  generatedAt: string;
  computedMonth: string;
  company: string;
  years: [number, number];
  plan: { name: string; loaded: boolean; source: 'postgres' | 'file' | 'none' };
  /** Actual months come from NetSuite, by transaction date or posting period, through `through`. */
  actuals: { source: 'netsuite'; basis: 'trandate' | 'period'; through: string | null };
  degraded: boolean;
  failedFeeds: string[];
  warnings: string[];
  variants: Record<VariantKey, PnlVariant>;
  cache?: { ageSec: number; stale: boolean; staleReason: string | null; refreshing: boolean; lastError: string | null };
}

export type PnlResponse = PnlPayload | ComputingResponse | ErrorResponse;
