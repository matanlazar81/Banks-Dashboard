// Type declarations for fx-challenge.mjs (the monthly EUR→ILS conversion program and what to keep for it).

export interface FxCell { eurK: number; rate: number }
export interface FxRow { month: string; cells: FxCell[] }
export interface FxTable { pair: string; fixingDay: number; bounds: number[]; rows: FxRow[] }
export interface RatePoint { date: string; rate: number }

export declare const DEFAULT_BOUNDS: number[];
export declare const DEFAULT_TABLE: Readonly<FxTable>;
export declare function cloneTable(t: FxTable): FxTable;

export declare function bandLabels(bounds: number[]): string[];
export declare function bandIndex(bounds: number[], rate: number): number;

export interface Conversion { band: number; eur: number; rate: number; ils: number; vsSpotIls: number }
export declare function conversionAt(row: FxRow, bounds: number[], rate: number): Conversion;

export interface Fixing {
  month: string;
  date: string;
  weekday: string;
  weekend: boolean;
  status: 'past' | 'next' | 'future';
  days: number;
  row: FxRow;
}
export declare function fixingDate(month: string, day: number): string;
export declare function fixings(table: FxTable, today: string): Fixing[];

export declare function annualVol(series: RatePoint[], opts?: { minReturns?: number }): number | null;
export declare function rateOn(series: RatePoint[], date: string): RatePoint | null;
export declare function bandProbabilities(bounds: number[], spot: number, sigma: number, years: number): number[];

export interface FixingRisk { probs: number[]; expectedEur: number; expectedIls: number; p95Eur: number; worstEur: number }
export declare function fixingRisk(row: FxRow, bounds: number[], spot: number, sigma: number, years: number, quantile?: number): FixingRisk;

/** EUR/ILS from lo (included) to hi; null where open-ended. Neighbouring bands that sell the same are merged. */
export interface OutcomeRange { lo: number | null; hi: number | null; eur: number; rate: number; ils: number; p: number; bands: number[] }
export declare function outcomeRanges(row: FxRow, bounds: number[], probs: number[]): OutcomeRange[];

export type Policy = 'today' | 'p95' | 'worst';
export declare const POLICIES: Policy[];
export declare const POLICY_LABEL: Record<Policy, string>;
export declare function requirementOf(policy: Policy, conv: Conversion, risk: FixingRisk): number;

export interface BalanceRow {
  company?: string;
  bank: string;
  kind: 'current' | 'deposit' | 'card' | 'other';
  currency: string;
  amount: number;
  asOf?: string | null;
  valueDate?: string | null;
  label?: string;
  blocked?: boolean;
  account?: string;
}
export interface EurDeposit { amount: number; valueDate: string | null; blocked: boolean; label: string }
export interface EurPosition { hapoalim: number; other: number; deposits: EurDeposit[] }
export declare function eurPosition(rows: BalanceRow[]): EurPosition;

export interface FlowRow { month: string; kind: 'customer' | 'vendor' | 'conversion' | 'deposit' | 'transfer' | 'refund' | 'other'; eur: number }
export declare function typicalInflow(flows: FlowRow[], months: string[]): { perMonth: number; perDay: number; months: { month: string; net: number }[] } | null;

export interface Upcoming { month: string; date: string; days: number; conv: Conversion; risk: FixingRisk }
export interface NextFixingPlan {
  month: string;
  date: string;
  required: number;
  todayAmount: number;
  hapoalim: number;
  other: number;
  liquid: number;
  maturing: number;
  backup: number;
  needLiquid: number;
  needLiquidWithBackup: number;
  freeNow: number;
  freeNowWithBackup: number;
  shortfall: number;
  inflowUntil: number | null;
  freeByFixing: number | null;
  moveToHapoalim: number;
}
/** A deposit maturing on valueDate: the bank's value then, interest included. */
export interface DepositRelease { amount: number; valueDate: string; label: string }
export interface PathPoint {
  month: string;
  date: string;
  required: number;
  expected: number;
  /** EUR deposits maturing after the previous fixing (after today, for the first) and by this one. */
  released: number;
  releasedDeposits: DepositRelease[];
  available: number;
  surplus: number;
  /** What can be converted before this fixing, in total, and still cover it and every later one. */
  convertibleBefore: number;
}
export interface ReservePlan {
  policy: Policy;
  next: NextFixingPlan;
  path: PathPoint[];
  /** Deposits maturing after the last fixing in the path. */
  later: DepositRelease[];
}
export declare function planReserve(input: {
  upcoming: Upcoming[];
  policy?: Policy;
  eur: EurPosition;
  inflowPerDay?: number | null;
  today: string;
  horizon?: number;
}): ReservePlan | null;

export interface BookedConversion { date: string; eur: number; ils: number }
export interface PastCheck {
  month: string;
  date: string;
  spot: RatePoint | null;
  conv: Conversion | null;
  booked: (BookedConversion & { n: number; rate: number | null }) | null;
  match: boolean;
  gainVsSpotIls: number | null;
}
export declare function checkPast(input: { fixing: Fixing; bounds: number[]; series: RatePoint[]; conversions: BookedConversion[]; windowDays?: number }): PastCheck;

export declare function tableProblems(t: FxTable): string[];
export declare function tableToText(t: FxTable): string;
export declare function parseTable(text: string, opts?: { fixingDay?: number; pair?: string }): { ok: boolean; table: FxTable | null; errors: string[] };
