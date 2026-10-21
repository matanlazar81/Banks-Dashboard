// Type declarations for fx-challenge.mjs (the monthly EUR→ILS conversion program and what to keep for it).

export interface FxCell { eurK: number; rate: number }
export interface FxRow { month: string; cells: FxCell[] }
export interface FxTable { pair: string; fixingDay: number; bounds: number[]; rows: FxRow[] }
export interface RatePoint { date: string; rate: number }

export declare const DEFAULT_BOUNDS: number[];
export declare const DEFAULT_TABLE: Readonly<FxTable>;
export declare function cloneTable(t: FxTable): FxTable;
/** The volatility used when NetSuite's history is too short. */
export declare const FALLBACK_SIGMA: number;
/** The past conversions are listed from this month (YYYY-MM). */
export declare const HISTORY_FROM: string;

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

/** Today's rate against the `days` days before it: share = the part of those business days with a lower rate. */
export interface RateCheck { date: string; rate: number; days: number; n: number; average: number; low: number; high: number; share: number; level: 'high' | 'middle' | 'low' }
export declare function rateCheck(series: RatePoint[], today: string, opts?: { days?: number; minDays?: number }): RateCheck | null;
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
  /** The parts of available: typical inflow up to the day before (inflowDays days), deposits matured by the day,
   * and what the earlier conversions take. available = today's liquid euros + inflow + depositsBy − earlier. */
  inflow: number;
  inflowDays: number;
  depositsBy: number;
  earlier: number;
  available: number;
  surplus: number;
  /** What can be converted before this fixing, in total, and still cover it and every later one. */
  convertibleBefore: number;
  /** The month whose spare sets convertibleBefore: this one, unless a later one is tighter. */
  limitedBy: string;
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

/** What GET /api/fx-challenge answers that the recommendation needs. */
export interface PlanInput {
  today: string;
  table: FxTable;
  rates: { series: RatePoint[]; spot: RatePoint | null; sigma: number | null };
  balances: { rows: BalanceRow[] } | null;
  flows: FlowRow[];
  inflowMonths: string[];
}
export interface ScheduleItem {
  fixing: Fixing;
  atSpot: Conversion | null;
  risk: FixingRisk | null;
  /** What it can sell, by EUR/ILS range, with each range's chance (from today's rate). */
  outcomes: OutcomeRange[] | null;
}
export interface PlanView {
  all: Fixing[];
  spot: number | null;
  spotDate: string | null;
  spotBand: number | null;
  sigma: number;
  sigmaFromHistory: boolean;
  rateCheck: RateCheck | null;
  schedule: ScheduleItem[];
  position: EurPosition | null;
  inflow: ReturnType<typeof typicalInflow>;
  plan: ReservePlan | null;
}
export declare function planFromPayload(data: PlanInput, policy?: Policy): PlanView;

/** A conversion journal: euros sold (or bought back: negative, with the shekels paid negative too). */
export interface BookedConversion { date: string; eur: number; ils: number }
export interface PastCheck {
  month: string;
  date: string;
  spot: RatePoint | null;
  /** The table at that day's rate; null for a month the table doesn't have. */
  conv: Conversion | null;
  booked: (BookedConversion & { n: number; rate: number | null }) | null;
  match: boolean;
  gainVsSpotIls: number | null;
}
export declare function checkPast(input: { fixing: { month: string; date: string; row: FxRow | null }; bounds: number[]; series: RatePoint[]; conversions: BookedConversion[]; windowDays?: number }): PastCheck;

/** A day's other conversions in one direction: euros sold, or bought back (negative). */
export interface OtherConversion { date: string; eur: number; ils: number; n: number; rate: number; vsRateIls: number | null }
export interface PastMonth extends PastCheck { inTable: boolean; others: OtherConversion[] }
export interface PastTotals {
  /** What was booked near the conversion days. */
  booked: { eur: number; ils: number; vsRateIls: number; months: number };
  /** Everything else: sold and bought back, net (eur, ils), and against each day's rate. */
  others: { sold: number; bought: number; eur: number; ils: number; vsRateIls: number };
}
export declare function pastConversions(input: { table: FxTable; today: string; series: RatePoint[]; conversions: BookedConversion[]; from?: string; windowDays?: number }): { from: string; months: PastMonth[]; total: PastTotals };

export declare function tableProblems(t: FxTable): string[];
export declare function tableToText(t: FxTable): string;
export declare function parseTable(text: string, opts?: { fixingDay?: number; pair?: string }): { ok: boolean; table: FxTable | null; errors: string[] };
