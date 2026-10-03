// Response shapes of GET /api/cash-projection (server/cash-projection.cjs).

export type Ccy = 'eur' | 'ils';
export type VariantKey = 'plan' | 'base';
/** Which years the table shows: both, the current year only, or the projection year only. */
export type YearView = 'both' | 'current' | 'next';
export type MonthStatus = 'actual' | 'current' | 'forecast';

/** One month's figures in one currency. other > 0 is an outflow; churn > 0 reduces inflows. */
export interface Figures {
  opening: number;
  collections: number;
  pipeline: number;
  churn: number;
  salary: number;
  vendors: number;
  other: number;
  reval: number;
  /** Net change incl. reval = inflows − outflows + reval (dividends not included). */
  net: number;
  /** Dividend distributions + withholding tax paid this month (positive = cash out). */
  dividend: number;
  /** Cash in the bank: opening + net − dividend. */
  closing: number;
  /** opening − previous closing: the current month's re-anchor to the NetSuite bank balance. */
  reanchor: number;
}

export interface MonthRow {
  mKey: string;
  status: MonthStatus;
  dividendExcluded: number;
  eur: Figures;
  ils: Figures;
}

export interface YearBlock {
  year: number;
  kind: 'current' | 'projection';
  rows: MonthRow[];
}

export interface Variant {
  years: YearBlock[];
  rollForward: {
    from: string;
    to: string;
    closing: { eur: number; ils: number };
    opening: { eur: number; ils: number };
    source: 'live' | 'snapshot-file';
    salaryBasis: { method: 'run-rate' | 'flat-budget'; monthsWithData: number; scale: { eur: number; ils: number } | null };
  };
}

export interface ProjectionPayload {
  ok: true;
  status: 'ready';
  schemaVersion: number;
  generatedAt: string;
  computedMonth: string;
  company: string;
  years: [number, number];
  plan: { name: string; loaded: boolean; source: 'postgres' | 'file' | 'none' };
  bankToday: { eur: number; ils: number; asOf: string } | null;
  degraded: boolean;
  failedFeeds: string[];
  warnings: string[];
  variants: Record<VariantKey, Variant>;
  cache?: { ageSec: number; stale: boolean; staleReason: string | null; refreshing: boolean; lastError: string | null };
}

export interface ComputingResponse {
  ok: true;
  status: 'computing';
  startedAt: string;
  elapsedSec: number;
}

export interface ErrorResponse {
  ok: false;
  status: 'error';
  error: string;
  retryAfterSec?: number;
}

export type ProjectionResponse = ProjectionPayload | ComputingResponse | ErrorResponse;
