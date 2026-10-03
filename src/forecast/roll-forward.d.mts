// Type declarations for roll-forward.mjs (projection-year inputs from the current year's forecast).
import type { EurIls, ForecastInputs, ForecastRow } from './forecast-core.d.mts';

type MonthIndexMap = Record<number, number>;

/** Scenario data as stored by the dashboard (only the fields the roll-forward reads). */
export interface ScenarioDataLike {
  salaryAdjPctByMonth?: MonthIndexMap;
  collPctByMonth?: MonthIndexMap;
  pipelineAdjPctByMonth?: MonthIndexMap;
  currencyDefensePctByMonth?: MonthIndexMap;
  adjustmentsByYear?: Record<string, {
    salaryAdjPctByMonth?: MonthIndexMap;
    collPctByMonth?: MonthIndexMap;
    pipelineAdjPctByMonth?: MonthIndexMap;
    currencyDefensePctByMonth?: MonthIndexMap;
  }>;
  [key: string]: unknown;
}

export interface ProjectionMaps {
  salaryAdjPctByMonth: MonthIndexMap;
  collPctByMonth: MonthIndexMap;
  currencyDefensePctByMonth: MonthIndexMap;
  pipelineAdjPctByMonth: MonthIndexMap;
}

/** One row of Snowflake fetchSalaryBudgetBreakdown(month). */
export interface SalaryBudgetBreakdownRow {
  department?: string;
  account?: string;
  accountId?: number;
  name?: string;
  amountEUR?: number;
  amountILS?: number;
}

export interface SalaryBasis {
  salaryActualsByDept: Record<string, Record<string, EurIls>>;
  /** '<sourceYear>-AVG' */
  lastActualSalaryMonth: string;
  /** Flat monthly salary (sum of the department basis). */
  flat: EurIls;
  monthsWithData: number;
  scale: { eur: number; ils: number };
}

/** What the "→ <year>" snapshot file holds that a projection year reads. */
export interface SnapshotFields {
  sfBudgetByMonth: Record<string, Record<string, number>>;
  sfSalaryBudget: Record<string, { eur: number; ils?: number }>;
  sfRevenue: { budget?: Record<string, { eur: number }>; targets?: Record<string, unknown> };
  sfActualsSplit: Record<string, { salary?: number; vendors?: number; salaryILS?: number }>;
  nsBudget: { byMonth: Record<string, unknown> };
  sfPipeline: { probability: number; closeDate: string; amount: number }[];
  sfConversion: { yearly: { year: number; winRate: number; avgWonDays?: number }[]; [key: string]: unknown };
  salary: { month: string; amountEUR: number; amountILS?: number }[];
  vendorHistory: { paidDate: string; amountEUR: number }[];
  collections: Record<string, number>;
}

export type MonthStatus = 'actual' | 'current' | 'forecast';

/** Displayed figures for one month in one currency. Signs: other > 0 is an outflow; churn > 0 reduces inflows. */
export interface ProjectionFigures {
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
  /** Dividend distributions + withholding tax paid this month (positive = cash out); 0 for most months. */
  dividend: number;
  /** Cash in the bank: opening + net − dividend. */
  closing: number;
  /** opening − previous closing; non-zero only in the live current month (bank re-anchor). */
  reanchor: number;
}

export interface ProjectionRow {
  mKey: string;
  status: MonthStatus;
  /** EUR dividend kept out of Vendors/Other (same as eur.dividend); 0 for most months. */
  dividendExcluded: number;
  eur: ProjectionFigures;
  ils: ProjectionFigures;
}

export interface ProjectionYear {
  year: number;
  kind: 'current' | 'projection';
  rows: ProjectionRow[];
}

export function remapMonthKeys<T>(obj: Record<string, T> | null | undefined, targetYear: number): Record<string, T>;
export function getByMonthIdx<T>(obj: Record<string, T> | null | undefined, monthIndex: number): T | undefined;
export function inheritProjectionMaps(sd: ScenarioDataLike | null | undefined, targetYear: number, sourceYear: number): ProjectionMaps;
export function synthesizeSalaryBasis(
  breakdowns: (SalaryBudgetBreakdownRow[] | null | undefined)[],
  srcRows: ForecastRow[],
  sourceYear: number,
): SalaryBasis | null;
export function snapshotFieldsFromLive(args: {
  sourceYear: number;
  targetYear: number;
  srcInputs: ForecastInputs;
  rawSfBudget: { byMonth?: Record<string, Record<string, number>> } | null;
  rawSfSalaryBudget: Record<string, { eur: number; ils?: number }> | null;
  now: Date;
}): SnapshotFields;
export function snapshotFieldsFromFile(snap: Record<string, unknown> | null | undefined): SnapshotFields;
export function buildNextYearInputs(args: {
  sourceYear: number;
  targetYear: number;
  srcInputs: ForecastInputs;
  srcRows: ForecastRow[];
  snapshot: SnapshotFields;
  salaryBasis: SalaryBasis | null;
  knobs: Partial<ForecastInputs>;
  now: Date;
  ilsRevalRate: number;
}): ForecastInputs;
export function shapeYear(
  rows: ForecastRow[],
  opts: { year: number; kind: 'current' | 'projection'; prevClosing?: EurIls | null; dividendCarry?: EurIls | null },
): ProjectionYear;
