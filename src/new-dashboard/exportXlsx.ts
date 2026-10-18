// Excel export of the table as shown (variant, currency, years). Loaded only when the user clicks
// Export, so the spreadsheet library never weighs on the page load.
import type { Ccy, ProjectionPayload, VariantKey } from './types.ts';
import type { Accent, ProjectionTable } from './model.ts';

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

// Section colours as on the page (ProjectionTable): a header row's band and text, and a line's label fill.
const ACCENT_RGB: Record<Accent, { band: string; text: string; label: string }> = {
  sky: { band: 'E0F2FE', text: '075985', label: 'F0F9FF' },
  amber: { band: 'FEF3C7', text: '92400E', label: 'FFFBEB' },
  violet: { band: 'EDE9FE', text: '5B21B6', label: 'F5F3FF' },
  emerald: { band: 'D1FAE5', text: '065F46', label: 'ECFDF5' },
  slate: { band: 'E2E8F0', text: '334155', label: 'F8FAFC' },
  pink: { band: 'FCE7F3', text: '9D174D', label: 'FDF2F8' },
};
const fill = (rgb: string) => ({ patternType: 'solid', fgColor: { rgb } });

/** What a projection page needs to export its table. */
export interface TableExport {
  /** 'cash projection' / 'P&L projection' (title and file name). */
  what: string;
  years: [number, number];
  planName?: string;
  generatedAt: string;
  table: ProjectionTable<string>;
  /** Plan or base (second title line and file name); pages without a plan pass a subtitle instead. */
  variant?: VariantKey;
  /** Replaces the plan label in the second title line. */
  subtitle?: string;
  ccy: Ccy;
}

export async function exportProjectionXlsx(payload: ProjectionPayload, table: ProjectionTable, variant: VariantKey, ccy: Ccy): Promise<void> {
  await exportTableXlsx({ what: 'cash projection', years: payload.years, planName: payload.plan.name, generatedAt: payload.generatedAt, table, variant, ccy });
}

export async function exportTableXlsx({ what, years, planName, generatedAt, table, variant, subtitle, ccy }: TableExport): Promise<void> {
  const mod = (await import('xlsx-js-style')) as unknown as { default?: XlsxApi } & XlsxApi;
  const XLSX: XlsxApi = mod.default ?? mod;

  const unit = ccy === 'eur' ? 'EUR' : 'ILS';
  const planLabel = subtitle ?? (variant === 'plan' ? `Plan: ${planName}` : 'Base forecast (no plan adjustments)');
  const generated = new Date(generatedAt).toLocaleString('en-GB');
  const header = ['Line item', ...table.columns.map((c) => c.label)];
  const status = ['', ...table.columns.map((c) => c.statusLabel || 'Full year')];
  const rows: unknown[][] = [
    [`LSports ${what} ${years[0]}–${years[1]}`],
    [`${planLabel} · ${unit} · generated ${generated}`],
    [],
    header,
    status,
    // Section headers carry their label only (and, in a section colour, empty cells that take its band).
    ...table.lines.map((l) => (l.kind === 'header' ? [l.label, ...(l.accent ? table.columns.map(() => '') : [])] : [l.label, ...l.values.map((v) => Math.round(v))])),
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
    const accent = l.accent ? ACCENT_RGB[l.accent] : null;
    for (let c = 1; c < header.length; c++) {
      const cell = ws[XLSX.utils.encode_cell({ r, c })] as XlsxCell | undefined;
      if (!cell) continue;
      if (l.kind === 'header') { if (accent) cell.s = { fill: fill(accent.band) }; continue; }
      cell.z = NUM_FMT;
      if (l.kind === 'balance' || l.kind === 'subtotal') cell.s = bold;
    }
    const label = ws[XLSX.utils.encode_cell({ r, c: 0 })] as XlsxCell | undefined;
    if (!label) return;
    const strong = l.kind === 'balance' || l.kind === 'subtotal' || l.kind === 'header';
    if (accent) {
      label.s = l.kind === 'header'
        ? { font: { bold: true, color: { rgb: accent.text } }, fill: fill(accent.band) }
        : { font: { bold: strong }, fill: fill(accent.label) };
    } else if (strong) {
      label.s = bold;
    }
  });

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Projection');
  XLSX.writeFile(wb, `${what.replace(/[^A-Za-z0-9]+/g, '-').toLowerCase()}-${years[0]}-${years[1]}${variant ? `-${variant}` : ''}-${unit}.xlsx`);
}
