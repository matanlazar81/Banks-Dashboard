// GET /api/cash-projection/breakdown (server/cash-projection-breakdown.cjs): what makes up one cell.
// The P&L Projection's /api/pnl-projection/breakdown answers in the same shape (pass its endpoint).
import type { Ccy, VariantKey } from './types.ts';

export type BreakdownLine = 'collections' | 'pipeline' | 'churn' | 'salary' | 'vendors' | 'other' | 'reval' | 'dividend';

/** Table lines whose cells open a breakdown. */
export const BREAKDOWN_LINES: ReadonlySet<string> = new Set<BreakdownLine>(['collections', 'pipeline', 'churn', 'salary', 'vendors', 'other', 'reval', 'dividend']);

export interface BreakdownRequest {
  /** A BreakdownLine on the cash page; the P&L page has its own lines. */
  line: string;
  /** 'YYYY-MM' or 'FY-YYYY' */
  period: string;
  variant: VariantKey;
  ccy: Ccy;
}

export interface BreakdownRow {
  key: string;
  label: string;
  /** Account number, or a short detail such as '60% · closes 2026-11'. */
  ref: string | null;
  /** Rows sharing a group are listed together under it (account category, customers, deals). */
  group: string | null;
  /** 'adjust' rows explain the difference between the listed rows and the cell. */
  kind: 'item' | 'adjust';
  hint: string | null;
  /** Same sign as the table cell. */
  amount: number;
}

export interface BreakdownSection {
  id: string;
  title: string;
  note: string | null;
  collapsed: boolean;
  /** Context only: its total is not meant to equal the cell. */
  informational: boolean;
  rows: BreakdownRow[];
  total: number;
}

export interface BreakdownReady {
  ok: true;
  status: 'ready';
  line: string;
  lineLabel: string;
  period: string;
  periodLabel: string;
  periodStatus: 'actual' | 'current' | 'forecast' | 'fy';
  variant: VariantKey;
  ccy: Ccy;
  /** The table cell; the first section always adds up to it. */
  cell: number;
  sections: BreakdownSection[];
  notes: string[];
  generatedAt: string;
}

export type BreakdownResponse = BreakdownReady | { ok: true; status: 'computing' } | { ok: false; status: 'error'; error: string };

export interface PanelPosition { x: number; y: number }

/** Width of the breakdown window (narrower on small screens). */
export const PANEL_W = 560;

/** Keeps at least the window's title bar on screen. */
export function clampPosition(p: PanelPosition, viewport = { w: window.innerWidth, h: window.innerHeight }): PanelPosition {
  const w = Math.min(PANEL_W, viewport.w - 16);
  return {
    x: Math.min(Math.max(8, p.x), Math.max(8, viewport.w - w - 8)),
    y: Math.min(Math.max(8, p.y), Math.max(8, viewport.h - 56)),
  };
}

export interface RowBlock {
  /** null: a single row shown on its own (adjustments, lines without a group). */
  group: string | null;
  rows: BreakdownRow[];
  total: number;
}

/** Consecutive item rows of the same group become one block (header + subtotal); the server orders them. */
export function arrangeRows(rows: BreakdownRow[]): RowBlock[] {
  const blocks: RowBlock[] = [];
  for (const r of rows) {
    const group = r.kind === 'item' ? r.group : null;
    const last = blocks[blocks.length - 1];
    if (group && last && last.group === group) {
      last.rows.push(r);
      last.total += r.amount;
    } else {
      blocks.push({ group, rows: [r], total: r.amount });
    }
  }
  return blocks;
}

export const CASH_BREAKDOWN_ENDPOINT = '/api/cash-projection/breakdown';

export async function fetchBreakdown(req: BreakdownRequest, signal?: AbortSignal, endpoint = CASH_BREAKDOWN_ENDPOINT): Promise<BreakdownResponse> {
  const q = new URLSearchParams({ line: req.line, period: req.period, variant: req.variant, ccy: req.ccy });
  const res = await fetch(`${endpoint}?${q}`, { credentials: 'include', headers: { Accept: 'application/json' }, signal });
  if (res.status === 401 || res.status === 403) {
    throw new Error('Your session has expired. Reload the page to sign in again.');
  }
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  if (!body || typeof body !== 'object' || !('status' in body)) {
    throw new Error(res.status === 404
      ? 'The breakdown service is not installed on this server yet.'
      : `The server returned an unexpected response (HTTP ${res.status}).`);
  }
  return body as BreakdownResponse;
}
