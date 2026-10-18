// Type declarations for salary-projection.mjs (payroll by GL account, projected by account).

export type SalaryMethod = 'run-rate' | 'last-year' | 'none';

export declare const SALARY_METHODS: SalaryMethod[];
export declare const METHOD_LABEL: Record<SalaryMethod, string>;
export declare function groupOfMethod(method: SalaryMethod): 'permanent' | 'temporary';
export declare function isSalaryMethod(m: unknown): m is SalaryMethod;

export type AccountAmounts = Record<string, { eur: number; ils: number }>;

export interface SalaryRow {
  mKey: string;
  status: 'actual' | 'current' | 'forecast';
  eur: Record<string, number>;
  ils: Record<string, number>;
}

export interface SalaryBlock {
  year: number;
  kind: 'current' | 'projection';
  rows: SalaryRow[];
}

export declare function projectSalary(input: {
  years: [number, number];
  currentMonth: string;
  runRateMonth: string;
  ilsPerEur: number;
  accounts: string[];
  actual: Record<string, AccountAmounts>;
  methodOf: (acct: string) => SalaryMethod;
}): SalaryBlock[];

export declare function countedIncrementRows<R extends { month: string; mode: 'Override' | 'Increment' }>(
  blocks: { rows: { mKey: string; status: SalaryRow['status'] }[] }[],
  rows: R[],
): R[];

export declare function countedChangeRows<R extends { month: string }>(
  blocks: { rows: { mKey: string; status: SalaryRow['status'] }[] }[],
  rows: R[],
): R[];

export declare function projectedMonths(blocks: { rows: { mKey: string; status: SalaryRow['status'] }[] }[]): Set<string>;

/** Deel's costs by account (NetSuite, costs positive). */
export interface DeelAmountRow {
  month: string;
  acct: string;
  eur: number;
  ils: number;
}

export declare function deelRunRate(rows: DeelAmountRow[], runRateMonth: string): { months: string[]; byAcct: AccountAmounts };

export declare function deelByMonth(
  blocks: { rows: { mKey: string; status: SalaryRow['status'] }[] }[],
  rows: DeelAmountRow[],
  runRateMonth: string,
): Map<string, { eur: Record<string, number>; ils: Record<string, number> }>;

export type HeadcountKind = 'hires' | 'leavers' | 'returns' | 'leaveStarts';

/** HiBob's planned events (FCT_HEADCOUNT_EVENT), counted by month, type and sub-type. */
export interface HeadcountEventCount {
  month: string;
  type: string;
  subType: string;
  /** An opening with no employee yet, still open. */
  open: boolean;
  count: number;
}

export interface HeadcountMonth {
  mKey: string;
  status: SalaryRow['status'];
  /** Employees at month-end (null when HiBob's employees could not be read). */
  employees: number | null;
  hires: number | null;
  leavers: number | null;
  returns: number;
  leaveStarts: number;
}

export declare function headcountKind(type: string, subType: string): HeadcountKind | null;

export declare function headcountSeries(input: {
  months: string[];
  currentMonth: string;
  actual: Record<string, { employees: number; hires: number; leavers: number }> | null;
  events: HeadcountEventCount[];
}): HeadcountMonth[];
