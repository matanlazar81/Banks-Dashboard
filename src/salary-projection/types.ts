// Response shapes of GET /api/salary-projection (server/salary-projection.cjs).
import type { Ccy, ComputingResponse, ErrorResponse, MonthStatus } from '../new-dashboard/types.ts';
import type { AccountAmounts, SalaryMethod } from '../forecast/salary-projection.mjs';

export type { Ccy, MonthStatus, SalaryMethod };

/** Permanent, temporary, or not classified (projected like temporary until someone picks). */
export type SalaryGroup = 'permanent' | 'temporary' | 'other';

export interface SalaryAccount {
  /** NetSuite account number, e.g. '760001'. */
  acct: string;
  name: string;
  /** Finance's default; null when the account is not in it. */
  defaultMethod: SalaryMethod | null;
  /** How forecast months are made: the last closed month carried flat, the same month last year, or 0. */
  method: SalaryMethod;
  group: SalaryGroup;
}

/** One month's payroll in one currency: amount per account number (costs positive). */
export type SalaryFigures = Record<string, number>;

/** One payroll row of the override file (OVERIDE_TEMP, loaded into Snowflake). */
export interface PayrollOverride {
  month: string;
  acct: string;
  name: string;
  department: string;
  /** Override replaces the budget of that account and month; Increment adds to it. */
  mode: 'Override' | 'Increment';
  eur: number;
  ils: number;
  /** The file has € only: ₪ is at the forecast rate. */
  ilsEstimated: boolean;
  memo: string;
}

/** One month of Snowflake's planned payroll changes on an account and department (its HR-driven payroll forecast). */
export interface PayrollChange {
  month: string;
  acct: string;
  name: string;
  /** Planned hires, leavers (negative) or returns from leave. */
  source: 'Open position' | 'Terminated' | 'Leave end';
  /** Snowflake's department name ('' when none). */
  department: string;
  eur: number;
  ils: number;
}

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
  /** The actual months by account (the previous year and this year's closed months), to re-project on the page. */
  actual?: Record<string, AccountAmounts>;
  /**
   * The override file's rows on every payroll account (Snowflake). The account projection (blocks) leaves them
   * out; the page adds the increments to Total payroll from the current month on.
   */
  overrides?: { rows: PayrollOverride[]; error: string | null };
  /**
   * Snowflake's planned hires, leavers and returns from leave on every payroll account, by month. The page adds
   * them to Total payroll from the current month on; a read failure is also in `warnings`.
   */
  changes?: { rows: PayrollChange[]; error: string | null };
  degraded: boolean;
  failedFeeds: string[];
  warnings: string[];
  blocks: SalaryYearBlock[];
  cache?: { ageSec: number; stale: boolean; staleReason: string | null; refreshing: boolean; lastError: string | null };
}

export type SalaryResponse = SalaryPayload | ComputingResponse | ErrorResponse;
