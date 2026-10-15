// Response shapes of GET /api/salary-projection (server/salary-projection.cjs).
import type { Ccy, ComputingResponse, ErrorResponse, MonthStatus } from '../new-dashboard/types.ts';

export type { Ccy, MonthStatus };

/** Permanent, temporary, or not classified (projected like temporary). */
export type SalaryGroup = 'permanent' | 'temporary' | 'other';

/** How forecast months are made: the last closed month carried flat, the same month last year, or not at all (0). */
export type SalaryMethod = 'run-rate' | 'last-year' | 'none';

export interface SalaryAccount {
  /** NetSuite account number, e.g. '760001'. */
  acct: string;
  name: string;
  group: SalaryGroup;
  method: SalaryMethod;
}

/** One month's payroll in one currency: amount per account number (costs positive). */
export type SalaryFigures = Record<string, number>;

export interface SalaryMonthRow {
  mKey: string;
  status: MonthStatus;
  eur: SalaryFigures;
  ils: SalaryFigures;
}

export interface SalaryYearBlock {
  year: number;
  kind: 'current' | 'projection';
  rows: SalaryMonthRow[];
}

export interface SalaryPayload {
  ok: true;
  status: 'ready';
  schemaVersion: number;
  generatedAt: string;
  computedMonth: string;
  company: string;
  years: [number, number];
  /** Permanent accounts first, then temporary, then any not classified. */
  accounts: SalaryAccount[];
  /** Actual months come from NetSuite by posting period, through `through` (null: none closed this year). */
  actuals: { source: 'netsuite'; basis: 'period'; through: string | null };
  /** The month the permanent accounts are carried from, and the ₪ per € of forecast months. */
  basis: { runRateMonth: string; ilsPerEur: number };
  degraded: boolean;
  failedFeeds: string[];
  warnings: string[];
  blocks: SalaryYearBlock[];
  cache?: { ageSec: number; stale: boolean; staleReason: string | null; refreshing: boolean; lastError: string | null };
}

export type SalaryResponse = SalaryPayload | ComputingResponse | ErrorResponse;
