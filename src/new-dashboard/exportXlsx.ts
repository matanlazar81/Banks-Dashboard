// Excel export of the table as shown (variant, currency, years). Loaded only when the user clicks
// Export, so the spreadsheet library never weighs on the page load.
import type { Ccy, ProjectionPayload, VariantKey } from './types.ts';
import type { ProjectionTable } from './model.ts';

interface XlsxCell { v?: unknown; t?: string; z?: string; s?: Record<string, unknown> }
interface XlsxApi {
  utils: {
    aoa_to_sheet: (rows: unknown[][]) => Record<string, XlsxCell | unknown>;
    book_new: () => unknown;
    book_append_sheet: (wb: unknown, ws: unknown, name: string) => void;
    encode_cell: (c: { r: number; c: number }) => string;
  };
  writeFile: (wb: unknown, filename: string) => void;
}

const NUM_FMT = '#,##0;(#,##0);"-"';

/** What a projection page needs to export its table. */
export interface TableExport {
  /** 'cash projection' / 'P&L projection' (title and file name). */
  what: string;
  years: [number, number];
  planName: string;
  generatedAt: string;
  table: ProjectionTable<string>;
  variant: VariantKey;
  ccy: Ccy;
}

export async function exportProjectionXlsx(payload: ProjectionPayload, table: ProjectionTable, variant: VariantKey, ccy: Ccy): Promise<void> {
  await exportTableXlsx({ what: 'cash projection', years: payload.years, planName: payload.plan.name, generatedAt: payload.generatedAt, table, variant, ccy });
}

export async function exportTableXlsx({ what, years, planName, generatedAt, table, variant, ccy }: TableExport): Promise<void> {
  const mod = (await import('xlsx-js-style')) as unknown as { default?: XlsxApi } & XlsxApi;
  const XLSX: XlsxApi = mod.default ?? mod;

  const unit = ccy === 'eur' ? 'EUR' : 'ILS';
  const planLabel = variant === 'plan' ? `Plan: ${planName}` : 'Base forecast (no plan adjustments)';
  const generated = new Date(generatedAt).toLocaleString('en-GB');
  const header = ['Line item', ...table.columns.map((c) => c.label)];
  const status = ['', ...table.columns.map((c) => c.statusLabel || 'Full year')];
  const rows: unknown[][] = [
    [`LSports ${what} ${years[0]}–${years[1]}`],
    [`${planLabel} · ${unit} · generated ${generated}`],
    [],
    header,
    status,
    ...table.lines.map((l) => [l.label, ...l.values.map((v) => Math.round(v))]),
  ];
  const ws = XLSX.utils.aoa_to_sheet(rows) as Record<string, XlsxCell | unknown>;
  ws['!cols'] = [{ wch: 30 }, ...table.columns.map(() => ({ wch: 13 }))];
  ws['!freeze'] = { xSplit: 1, ySplit: 5 };

  const bold = { font: { bold: true } };
  for (let c = 0; c < header.length; c++) {
    const h = ws[XLSX.utils.encode_cell({ r: 3, c })] as XlsxCell | undefined;
    if (h) h.s = bold;
  }
  table.lines.forEach((l, i) => {
    const r = 5 + i;
    for (let c = 1; c < header.length; c++) {
      const cell = ws[XLSX.utils.encode_cell({ r, c })] as XlsxCell | undefined;
      if (!cell) continue;
      cell.z = NUM_FMT;
      if (l.kind === 'balance' || l.kind === 'subtotal') cell.s = bold;
    }
    const label = ws[XLSX.utils.encode_cell({ r, c: 0 })] as XlsxCell | undefined;
    if (label && (l.kind === 'balance' || l.kind === 'subtotal')) label.s = bold;
  });

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Projection');
  XLSX.writeFile(wb, `${what.replace(/[^A-Za-z0-9]+/g, '-').toLowerCase()}-${years[0]}-${years[1]}-${variant}-${unit}.xlsx`);
}
