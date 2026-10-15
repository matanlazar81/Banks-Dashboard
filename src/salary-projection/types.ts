// Response shapes of GET /api/salary-projection (server/salary-projection.cjs).
import type { Ccy, ComputingResponse, ErrorResponse, MonthStatus } from '../new-dashboard/types.ts';

export type { Ccy, MonthStatus };

/** Permanent: carried flat from the last closed month. Temporary (and not classified): same month last year. */
export type SalaryGroup = 'permanent' | 'temporary' | 'other';

export interface SalaryAccount {
  /** NetSuite account number, e.g. '760001'. */
  acct: string;
  name: string;
  group: SalaryGroup;
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
  /** Actual months come from Snowflake (FCT_EXPENSE), through `through` (null: none closed this year). */
  actuals: { source: 'snowflake'; through: string | null };
  /** The month the permanent accounts are carried from, and the ₪ per € of forecast months. */
  basis: { runRateMonth: string; ilsPerEur: number };
  degraded: boolean;
  failedFeeds: string[];
  warnings: string[];
  blocks: SalaryYearBlock[];
  cache?: { ageSec: number; stale: boolean; staleReason: string | null; refreshing: boolean; lastError: string | null };
}

export type SalaryResponse = SalaryPayload | ComputingResponse | ErrorResponse;
