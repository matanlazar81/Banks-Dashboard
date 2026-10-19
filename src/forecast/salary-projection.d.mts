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

export declare function countedChangeRows<R extends { month: string; actual: boolean }>(
  blocks: { rows: { mKey: string; status: SalaryRow['status'] }[] }[],
  rows: R[],
): R[];

export declare function uncountedChangeRows<R extends { month: string; actual: boolean }>(
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

export declare const DEEL_RUN_RATE_MONTHS: number;

export declare function deelRunRate(rows: DeelAmountRow[], runRateMonth: string): { months: string[]; byAcct: AccountAmounts };

export declare function deelByMonth(
  blocks: { rows: { mKey: string; status: SalaryRow['status'] }[] }[],
  rows: DeelAmountRow[],
  runRateMonth: string,
): Map<string, { eur: Record<string, number>; ils: Record<string, number> }>;

export type HeadcountKind = 'returns' | 'leaveStarts' | 'employee' | 'openings' | 'cuts';

/** HiBob's planned events (FCT_HEADCOUNT_EVENT), counted by month, type, sub-type, employee, opening status and group. */
export interface HeadcountEventCount {
  month: string;
  type: string;
  subType: string;
  /** The event is a known employee's (HiBob's employees hold it), not an opening. */
  employee: boolean;
  /** The opening's status ('' when none). */
  status: string;
  /** HiBob's group (department) ('' when none). */
  department?: string;
  /** An opening's position (a role; '' for a known employee's event). */
  position?: string;
  count: number;
}

/** Employees at month-end, hires, leavers and transfers in, of one department or employment type. */
export interface HeadcountCounts {
  employees: number;
  hires: number;
  leavers: number;
  /** People who moved here from another department (absent in an older payload). */
  transfers?: number;
}

export interface HeadcountMonth {
  mKey: string;
  status: SalaryRow['status'];
  /** HiBob's employees at month-end (null when they could not be read). */
  employees: number | null;
  /** Who joined, or is signed to join (HiBob's start dates). */
  hires: number | null;
  /** Who left, or is leaving (HiBob's leave dates). */
  leavers: number | null;
  /** Who moved to another department: already employees, so the count does not move (absent in an older payload). */
  transfers?: number | null;
  /** HiBob's open positions planned for the month (shown, not counted; 0 before the current month). */
  openings: number;
  /** HiBob's planned leavers with no employee (shown, not counted; 0 before the current month). */
  cuts: number;
  returns: number;
  leaveStarts: number;
  /** The same counts by HiBob group (department) and by employment type (absent in an older payload). */
  byDept?: Record<string, HeadcountCounts>;
  byType?: Record<string, HeadcountCounts>;
}

export declare function headcountKind(type: string, subType: string, employee: boolean): HeadcountKind | null;

export declare function headcountSeries(input: {
  months: string[];
  currentMonth: string;
  known: Record<string, HeadcountCounts & { byDept?: Record<string, HeadcountCounts>; byType?: Record<string, HeadcountCounts> }> | null;
  events: HeadcountEventCount[];
}): HeadcountMonth[];

export declare function placedHeadcountEvents(events: HeadcountEventCount[], currentMonth: string): {
  month: string;
  kind: Exclude<HeadcountKind, 'employee'>;
  department: string;
  position: string;
  count: number;
  carried: boolean;
}[];
