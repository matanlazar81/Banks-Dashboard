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

export interface FxConversion {
  id?: number;
  tranid: string;
  date: string;
  from?: string;
  to?: string;
  fromCurrency: string;
  toCurrency: string;
  currency: string;
  amount: number;
  eur: number;
  rate: number | null;
}

export interface Deposit {
  id: string;
  bank: string;
  amount: number;
  currency: string;
  placedOn: string;
  maturity: string | null;
  confirmed: boolean;
  confirmedOn: string | null;
  note: string;
}

export interface MetricsSettings {
  usdEurPlanningRate: number | null;
  cloudCapPct: number;
  cloudCategory: string;
  innovation: { amountEur: number; year: number | null; startMonth: number; included: boolean };
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
  innovation: {
    amountEur: number; year: number; startMonth: number; included: boolean; monthly: number; months: number; applied: number;
    ebitda: { year: number; without: number; with: number } | null;
    netCash: { year: number; without: number; with: number } | null;
  };
  rates: { usdEurPlanning: number | null; usdEurLive: { rate: number; date: string; source: string } | null };
  fx: { month: string; items: FxConversion[]; totals: { pair: string; currency: string; count: number; amount: number; eur: number; rate: number | null }[] };
  deposits: { open: Deposit[]; openCount: number; total: number };
  settings: MetricsSettings;
  warnings: string[];
  refreshing?: boolean;
}

export type MetricsResponse =
  | MetricsPayload
  | { ok: true; status: 'computing'; startedAt: string | null; elapsedSec: number }
  | { ok: false; status: 'error'; error: string };

export interface SavedDoc<T> { ok: true; value: T; updatedAt: string | null; updatedBy: string | null }
