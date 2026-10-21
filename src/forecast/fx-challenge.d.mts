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

// ── shekels for the payroll and the vendors ──
/** A payroll journal's credit to a payable or current account, in shekels (fetchPayrollLiabilities). */
export interface PayrollCredit { id: number; month: string; account: string; name: string; ils: number }
/** A month's payroll journal: net salaries (by the 5th of the next month) and the rest (by the 14th). */
export interface PayrollMonthSplit { month: string; net: number; later: number; total: number }
export declare function payrollSplit(rows: PayrollCredit[]): PayrollMonthSplit[];
/** The Salary Projection's Total payroll by month (₪), as its page saved it. */
export interface PayrollProjection { asOf: string | null; months: { month: string; ils: number; status?: string }[] }
export interface PayrollToPay { month: string; total: number; net: number; later: number; source: 'projection' | 'journal' }
export declare function payrollToPay(input: { projection: PayrollProjection | null; split: PayrollMonthSplit[]; months: string[]; from: string; count: number }): { share: number | null; rows: PayrollToPay[] };
/** ILS vendor payments in a month: the credit cards' (billed on the 10th of the next month) and the rest (bank
 * transfers and Masav batches); ils is both. */
export interface IlsVendorMonth { month: string; ils: number; cards?: number; other?: number }
/** Average monthly vendor payments: EUR (kept at all times) and ILS, split into the cards and the rest (null when not
 * read). */
export interface VendorAverages { eur: number; ils: number | null; cards: number | null; other: number | null }
export declare function vendorAverages(flows: FlowRow[], ilsVendors: IlsVendorMonth[] | null | undefined, months: string[]): VendorAverages;
/** A credit card bill: on the 10th, the month before's card spending. */
export interface CardBill { date: string; ils: number; spendMonth: string }
export declare function cardBills(input: { ilsVendors: IlsVendorMonth[] | null | undefined; average: number | null; today: string; until: string }): CardBill[];
/** LSports' shekels: the current accounts and the ILS deposits (released the day a payment goes out). */
export interface IlsOnHand { current: number; deposits: number; total: number }
export declare function ilsOnHand(rows: BalanceRow[]): IlsOnHand | null;
export type ShekelNeedWhy = 'vendors' | 'net' | 'later' | 'cards';
/** Shekels to convert by `date` (eur: at today's rate), and what they pay. */
export interface ShekelNeed { date: string; ils: number; eur: number; why: ShekelNeedWhy[]; month: string }
/** One payment on a payment day. */
export interface ShekelDueItem { why: ShekelNeedWhy; month: string; payrollMonth: string | null; spendMonth: string | null; rest: boolean; ils: number }
/** A day something is paid in shekels: what (items), when it has to be ready (by: the business day on or before), the
 * shekels on hand just before, what to convert for it (0 when covered) and the table's conversion since the payment
 * day before (after). */
export interface ShekelDue {
  date: string;
  by: string;
  month: string;
  ils: number;
  before: number;
  items: ShekelDueItem[];
  after: string | null;
  convertIls: number;
  eur: number;
}
/** A month paid in: the previous month's payroll (net by the 5th, the rest by the 14th), the credit cards (the 10th),
 * the vendors, the table's shekels that month (at today's rate), and what has to be converted for it. */
export interface ShekelMonth { month: string; payrollMonth: string | null; net: number; later: number; cards: number; vendors: number; tableIls: number; convertIls: number; convertEur: number }
export interface ShekelNeeds { needs: ShekelNeed[]; dues: ShekelDue[]; months: ShekelMonth[]; ilsNow: number | null; ilsAfter: number }
export declare function shekelNeeds(input: {
  today: string;
  ilsNow: number | null;
  payroll: { month: string; net: number; later: number }[];
  conversions: { date: string; ils: number }[];
  vendorsIls: number;
  cardBills?: CardBill[];
  spot: number;
  until: string;
}): ShekelNeeds;
/** The shekels side of the plan (planFromPayload). */
export interface CashPlan extends ShekelNeeds {
  /** The shekels on hand today, by kind; null when the balances have none. */
  onHand: IlsOnHand | null;
  payroll: PayrollToPay[];
  /** The net salaries' share of the payroll journals of the inflow months; null without them (all by the 5th). */
  share: number | null;
  split: PayrollMonthSplit[];
  vendors: VendorAverages;
  cardBills: CardBill[];
  until: string;
  projectionAsOf: string | null;
  notes: string[];
}
export declare const PAYROLL_DUE: Readonly<{ net: number; later: number }>;
export declare const CARD_BILL_DAY: number;
export declare function businessDayOnOrBefore(iso: string): string;
export declare function businessDaysBetween(from: string, to: string): number;
export declare function daysUntil(from: string, to: string): number;
export declare function duesAhead(cash: CashPlan | null): { dues: ShekelDue[]; eur: number };
export declare function dueWhat(due: Pick<ShekelDue, 'items'>): string;

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
  /** The conversions for shekels (payroll and vendors) due by the fixing, in euros at today's rate. */
  shekelsBefore: number;
  /** The euros kept for the EUR vendors at all times. */
  eurFloor: number;
  /** What can leave the euro accounts for good today (converted, say) with every due met as the typical inflow comes
   * in: the smallest spare of all the dues, never more than today's euros above the floor. */
  spare: number;
  /** The due with the smallest spare (euros held then, less what is due and the floor), and whether it is for shekels. */
  tightest: { date: string; spare: number; shekels: boolean } | null;
  /** When a due falls short: the largest gap (by how much, on which day, whether it is a conversion for shekels), and
   * the first day a gap opens. */
  short: { eur: number; date: string; shekels: boolean; first: { eur: number; date: string; shekels: boolean } } | null;
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
  /** The conversions for shekels due by this fixing, in total, and this row's (since the previous one). */
  forShekels: number;
  shekels: number;
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
  /** How much can go into a new deposit for 1, 3 and 6 months (until that day) with every due still met. */
  deposits: { months: number; until: string; eur: number }[];
}
export declare function planReserve(input: {
  upcoming: Upcoming[];
  policy?: Policy;
  eur: EurPosition;
  inflowPerDay?: number | null;
  today: string;
  horizon?: number;
  shekels?: { date: string; eur: number }[];
  eurFloor?: number;
}): ReservePlan | null;

/** What GET /api/fx-challenge answers that the recommendation needs. */
export interface PlanInput {
  today: string;
  table: FxTable;
  rates: { series: RatePoint[]; spot: RatePoint | null; sigma: number | null };
  balances: { rows: BalanceRow[] } | null;
  flows: FlowRow[];
  inflowMonths: string[];
  payroll?: { journals: PayrollCredit[]; projection: PayrollProjection | null } | null;
  ilsVendors?: IlsVendorMonth[] | null;
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
  /** Shekels for the payroll and the vendors; null without a payroll figure. */
  cash: CashPlan | null;
  plan: ReservePlan | null;
}
export declare function planFromPayload(data: PlanInput, policy?: Policy): PlanView;

/** Something to do today (todayActions). urgent: worth a mail whatever was sent before; key: the same on another day. */
export type TodayAction =
  | { kind: 'convert' | 'convert-early'; key: string; urgent: boolean; eur: number; by: string; left: number; due: ShekelDue }
  | { kind: 'short'; key: string; urgent: boolean; eur: number; date: string; by: string; left: number; shekels: boolean }
  | { kind: 'move'; key: string; urgent: boolean; eur: number; date: string; by: string; left: number }
  | { kind: 'convert-spare'; key: string; urgent: boolean; eur: number }
  | { kind: 'deposit'; key: string; urgent: boolean; eur: number; options: { months: number; until: string; eur: number }[] };
export declare function todayActions(view: PlanView, today: string, opts?: { lead?: number; moveLead?: number; minEur?: number }): { actions: TodayAction[]; ahead: { dues: ShekelDue[]; eur: number } };
/** Number and date formats for the texts: the page's or the mail's. */
export interface TextFormats { eur: (n: number) => string; ils: (n: number) => string; day: (iso: string) => string; rate: (r: number) => string }
export declare function describeAction(a: TodayAction, view: PlanView, today: string, fmt: TextFormats): { head: string; body: string };
export declare function describeDue(d: ShekelDue, today: string, fmt: Pick<TextFormats, 'eur' | 'ils' | 'day'>): string;
export declare function nothingToday(ahead: { dues: ShekelDue[] }, today: string, fmt: Pick<TextFormats, 'eur' | 'day'>, opts?: { minEur?: number }): string;

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
