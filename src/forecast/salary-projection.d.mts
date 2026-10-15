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
