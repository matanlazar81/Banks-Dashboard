// Response shapes of GET /api/metrics (server/metrics.cjs) and what the page saves.

export type FigureStatus = 'actual' | 'forecast' | 'actual+forecast';

export interface Cell {
  value: number | null;
  status: FigureStatus;
  label: string;
  actual?: number;
  forecast?: number;
  grr?: number;
  customers?: number;
  /** Click to see what it is made of (GET /api/metrics?detail=…). */
  detail?: string;
}

export interface Metric {
  key: 'revenue' | 'ebitda' | 'netCash' | 'arr' | 'nrr' | 'churn';
  label: string;
  unit: 'eur' | 'pct';
  note: string;
  lastMonth: Cell | null;
  ytd: Cell | null;
  fy: (Cell | null)[];
}

export interface CloudYear {
  year: number;
  status: FigureStatus;
  actual: number;
  forecast: number;
  total: number;
  revenue: number;
  capPct: number;
  cap: number;
  headroom: number;
  pctOfRevenue: number | null;
  within: boolean;
  /** The projection year's targets set the cloud: server costs as a % of revenue, or the category's % change. */
  targets?: { kind: 'server' | 'category'; pct: number } | null;
}

export interface PeopleMonth {
  mKey: string;
  /** NetSuite 76xxxx, gross of capitalised salaries (€). */
  payroll: number;
  /** P&L total revenue (€). */
  revenue: number;
  payrollPct: number | null;
  /** Employees active at month-end (HiBob), by employment type; null when they could not be read. */
  headcount: number | null;
  byType: Record<string, number> | null;
  revenuePerEmployee: number | null;
}

export interface People {
  company: string | null;
  /** The last month whose payroll JE is posted (the series ends there). */
  through: string | null;
  /** Closed months after it whose payroll JE is not posted yet. */
  pending: string[];
  months: PeopleMonth[];
  ytd: {
    label: string; payroll: number; revenue: number; payrollPct: number | null; avgHeadcount: number | null;
    revenuePerEmployeeMonthly: number | null; revenuePerEmployee: number | null; revenuePerEmployeeAnnualised: number | null;
  } | null;
}

export interface MetricsSettings {
  cloudCapPct: number;
  cloudCategory: string;
}

export interface MetricsPayload {
  ok: true;
  status: 'ready';
  generatedAt: string;
  years: [number, number];
  asOf: { lastClosed: string | null; cash: string; pnl: string; plan: string | null };
  /** The projection-year targets saved on the New Bank Dashboard (applied when active). */
  targets?: { year: number; active: boolean; updatedAt: string | null; updatedBy: string | null; assumptions: string[] };
  metrics: Metric[];
  nrrTrend: { month: string; nrr: number; grr: number; customers: number }[];
  cloud: { category: string; categories: string[]; capPct: number; accounts: string; years: CloudYear[] };
  /** Payroll / revenue and revenue per employee (absent from servers before this page had them). */
  people?: People;
  settings: MetricsSettings;
  warnings: string[];
  refreshing?: boolean;
}

export type MetricsResponse =
  | MetricsPayload
  | { ok: true; status: 'computing'; startedAt: string | null; elapsedSec: number }
  | { ok: false; status: 'error'; error: string };

/** What one figure is made of (server/metrics.cjs buildDetail). */
export type ExplainUnit = 'eur' | 'pct' | 'int' | 'text';
export interface ExplainTable {
  title: string;
  note?: string | null;
  columns: { key: string; label: string; unit: ExplainUnit }[];
  rows: Record<string, string | number | null>[];
  /** Rows left out of the list (counted in the total). */
  more: number;
  total: Record<string, string | number | null> | null;
}
export interface Explain {
  item: string;
  title: string;
  subtitle: string;
  value: { value: number; unit: ExplainUnit };
  formula: string[];
  source: string[];
  summary: { label: string; value: number | null; unit: ExplainUnit; strong?: boolean; sign?: boolean }[];
  notes?: string[];
  tables: ExplainTable[];
}

export interface SavedDoc<T> { ok: true; value: T; updatedAt: string | null; updatedBy: string | null }
