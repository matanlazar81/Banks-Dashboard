// Type declarations for forecast-core.cjs (the extracted cashflow engine).
// The build skips tsc (Vite/esbuild only), so these types are for editor DX
// and to document the input/output contract of the shared engine.

/** A single classified bank bucket for a past month. */
export interface BankBucket { eur: number; ils: number }

/** Bank-classified truth for one past month (overrides forecast components). */
export interface BankClassifiedMonth {
  vendors: BankBucket;
  collections: BankBucket;
  salary: BankBucket;
  reval: BankBucket;
  other: BankBucket;
  total: BankBucket;
  details: { label: string; bucket: string; eur: number; ils: number }[];
}

/** A bank "book" (EUR or ILS side): opening/current balances. */
export interface Book {
  openingBalance?: number;
  currentBalance?: number;
  adjustedCurrentBalance?: number;
}

export interface EurIls { eur: number; ils: number }

/**
 * Every input the forecast engine reads. All optional except `activeYear`;
 * Node/tests should also pass `now` + `currentYear` for determinism (the
 * frontend passes them too). Records keyed by month use 'YYYY-MM'; records
 * keyed by month-index use 0..11.
 */
export interface ForecastInputs {
  // time / config
  activeYear: number;
  /** Injected wall-clock instant. Required for deterministic (Node/test) runs. */
  now?: Date;
  currentYear?: number;
  /** 'YYYY-MM-DD' to run as-of a historical date; null/undefined = live. */
  asOfDate?: string | null;
  /** ILS-per-EUR rate used only for the currency-defense reval ILS leg (was hardcoded 3.59). */
  ilsRevalRate?: number;

  // balances
  book?: Book | null;
  bookLocal?: Book | null;
  yearStartBalance?: EurIls | null;
  prevMonthEndBalance?: EurIls | null;
  liveFxRate?: number;
  fxRateByYear?: Record<number, number>;

  // salary
  salaryData?: { month: string; amountEUR: number; amountILS?: number }[];
  salaryProjectionMode?: 'lastActual' | 'budget' | string;
  lastActualSalaryMonth?: string;
  salaryActualsByDept?: Record<string, Record<string, { eur: number }>>;
  salaryDeptBudgets?: Record<string, Record<string, number>>;
  salaryDeptAdj?: Record<string, Record<string, number>>;
  salaryAdjPctByMonth?: Record<number, number>;
  sfSalaryOverrides?: { mKey: string; mode: string; newVal: number; oldVal: number; amountEUR: number }[];
  sfSalaryBudget?: Record<string, { eur: number }>;
  salaryManualILS?: Record<string, number>;
  monthlyHCImpact?: Record<string, { running: number }>;
  sfActualsSplit?: Record<string, { salary?: number; vendors?: number; salaryILS?: number }>;

  // vendors
  vendorBills?: { amountEUR: number }[];
  vendorActuals?: { month: string; amountEUR: number }[];
  nsPaidVendors?: { byMonth: Record<string, number> };
  vendorHistory?: { paidDate: string; amountEUR: number }[];
  sfBudget?: { totalByMonth?: Record<string, { eur: number }>; byMonth?: Record<string, Record<string, number>> };
  nsBudget?: { byMonth: Record<string, { salary?: number; vendors?: number; revenue?: number; categories?: Record<string, number> }> };
  expenseCategories?: { byMonth: Record<string, Record<string, number>> };
  vendorCatAdj?: Record<string, Record<string, number>>;
  vendorDetailAdj?: Record<string, Record<string, { pct: number; base: number }>>;

  // collections / revenue
  sfRevenuePaid?: Record<string, { revenue: number; customers?: number; paid?: number; unpaid?: number }>;
  actualCollections?: Record<string, number>;
  collPctByMonth?: Record<number, number>;
  sfRevenue?: { budget?: Record<string, { eur: number }> };
  revenueActuals?: { month: string; amountEUR: number }[];
  customerReceipts?: Record<string, number>;

  // pipeline
  sfPipeline?: { probability: number; closeDate: string; amount: number }[];
  pipelineMinProb?: number;
  sfConversion?: { yearly: { year: number; winRate: number; avgWonDays?: number }[] };
  pipelineAdjPctByMonth?: Record<number, number>;
  revenueMethodology?: 'legacy' | 'pipeline' | string;
  pipelineMethodology?: { byMonth?: Record<string, { monthlyContribution: number }> };

  // churn
  sfChurnQuarterly?: { partial: boolean; qs: string; amount: number }[];
  churnData?: { year: number; monthlyImpact: number }[];
  churnMonthlyAvg?: number;
  churnOverride?: Record<string, number>;

  // reval / currency defense
  monthlyReval?: { preYear?: EurIls; byMonth?: Record<string, { eur: number; ils: number; hasBothEnds?: boolean }> };
  nsBankClassified?: { byMonth: Record<string, BankClassifiedMonth> };
  currencyDefensePct?: number;
  currencyDefensePctByMonth?: Record<number, number>;
  sfFinanceBudget?: Record<string, { eur: number }>;
}

/** One monthly forecast row (12 per year, Jan..Dec of activeYear). */
export interface ForecastRow {
  month: string;
  mKey: string;
  openingBalance: number;
  openingBalanceILS: number;
  salary: number;
  salaryBase: number;
  salaryILS: number;
  vendors: number;
  vendorsBase: number;
  vendorsILS: number;
  other: number;
  otherILS: number;
  otherDetails: { label: string; bucket: string; eur: number; ils: number }[];
  totalOutflow: number;
  totalOutflowILS: number;
  collections: number;
  collectionsILS: number;
  collectionsActual: number;
  collectionsRemaining: number;
  collectionsForecast: number;
  collectionsRevenue: number;
  collectionsUnpaidCarry: number;
  collectionsUnpaidCarryMonth: string;
  collectionsPipeline: number;
  customers: number;
  pipelineWeighted: number;
  pipelineWeightedILS: number;
  pipelineTotal: number;
  pipelineCount: number;
  pipelineOpps: unknown[];
  pipelineHistWinRate: number;
  pipelineDelayMonths: number;
  churnDeduction: number;
  churnDeductionILS: number;
  net: number;
  netILS: number;
  revalImpact: number;
  revalImpactILS: number;
  revalHasBothEnds: boolean;
  closingBalance: number;
  closingBalanceILS: number;
  wcDelta: number;
  wcDeltaILS: number;
  isCurrent: boolean;
  isPast: boolean;
}

export function computeCashflowForecast(inputs: ForecastInputs): ForecastRow[];
