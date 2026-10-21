// Response shapes of GET /api/fx-challenge and GET/PUT /api/fx-challenge/table (server/fx-challenge.cjs).
import type { BalanceRow, BookedConversion, FlowRow, FxTable, RatePoint } from '../forecast/fx-challenge.mjs';

/** GET /api/fx-challenge: what the page simulates on. */
export interface FxPayload {
  ok: true;
  status: 'ready';
  computedAt: string;
  /** Today in Israel (YYYY-MM-DD). */
  today: string;
  table: FxTable;
  /** Who saved the table and when; null while it is the default. */
  tableSaved: { at: string | null; by: string | null } | null;
  rates: { provider: string; series: RatePoint[]; spot: RatePoint | null; sigma: number | null };
  /** 'mail': the daily balances mail's figures (the RESTlet); 'books': NetSuite's books, EUR only. */
  balances: { source: 'mail' | 'books'; asOf: string | null; rows: BalanceRow[]; notes: string[] } | null;
  flows: FlowRow[];
  /** The closed months the typical inflow averages. */
  inflowMonths: string[];
  conversions: (BookedConversion & { id: number; tranid: string })[];
  warnings: string[];
}
export type FxResponse = FxPayload | { ok: false; status: 'error'; error: string };

/** GET/PUT /api/fx-challenge/table: the saved table, null while it is the default. */
export interface SavedTable { ok: true; value: FxTable | null; updatedAt: string | null; updatedBy: string | null }
