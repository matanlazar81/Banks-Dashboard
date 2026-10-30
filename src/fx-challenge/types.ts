// Response shapes of GET /api/fx-challenge and GET/PUT /api/fx-challenge/table (server/fx-challenge.cjs).
import type { BalanceRow, BookedConversion, FlowRow, FxTable, IlsVendorMonth, PayrollCredit, PayrollProjection, Positions, RatePoint } from '../forecast/fx-challenge.mjs';

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
  /** The bank's deals (its position reports, read on the page) with who saved them; null while none are saved, and
   * absent from servers before them. */
  positions?: (Positions & { savedAt: string | null; savedBy: string | null }) | null;
  rates: { provider: string; series: RatePoint[]; spot: RatePoint | null; sigma: number | null };
  /** 'mail': the daily balances mail's figures (the RESTlet); 'books': NetSuite's books, EUR only. */
  balances: { source: 'mail' | 'books'; asOf: string | null; rows: BalanceRow[]; notes: string[] } | null;
  flows: FlowRow[];
  /** The closed months the typical inflow averages. */
  inflowMonths: string[];
  conversions: (BookedConversion & { id: number; tranid: string })[];
  /** What the payroll journals owe and the Salary Projection's Total payroll; null when neither was read. */
  payroll: { journals: PayrollCredit[]; projection: PayrollProjection | null } | null;
  /** Vendor payments out of the ILS accounts by month (the credit cards' apart); null when not read. */
  ilsVendors: IlsVendorMonth[] | null;
  warnings: string[];
}
export type FxResponse = FxPayload | { ok: false; status: 'error'; error: string };

/** GET/PUT /api/fx-challenge/table: the saved table, null while it is the default. */
export interface SavedTable { ok: true; value: FxTable | null; updatedAt: string | null; updatedBy: string | null }
/** GET/PUT /api/fx-challenge/positions: the saved deals, null while none are. */
export interface SavedPositions { ok: true; value: Positions | null; updatedAt: string | null; updatedBy: string | null }

/** GET /api/fx-challenge/mail: who gets the daily mail (scripts/fx-challenge-mail.cjs). */
export interface MailSettings {
  ok: true;
  /** The list saved on the page; null while none is saved and the server's FX_MAIL_TO is used. */
  value: { to: string[] } | null;
  updatedAt: string | null;
  updatedBy: string | null;
  /** The server's FX_MAIL_TO. */
  envTo: string[];
  /** FX_MAIL_FROM; null: the mail server's own account. */
  from: string | null;
  /** The hour it is sent, Israel time. */
  hour: number;
  /** The last mail that went; manual: sent with the page's Send now. */
  lastSent: { day: string | null; at: string; recipients: number | null; manual?: boolean } | null;
  /** The daily check, when no mail went (nothing to do, or a Friday or Saturday): when, and why. */
  lastCheck: { day: string; at: string; why: string } | null;
}
