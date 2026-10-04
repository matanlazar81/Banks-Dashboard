// Excel export of the Metrics pack: one sheet, the same rows every month (src/metrics/model.ts packRows).
// Loaded only when the user clicks Export.
import { packRows } from './model.ts';
import type { MetricsPayload } from './types.ts';

interface XlsxCell { v?: unknown; s?: Record<string, unknown> }
interface XlsxApi {
  utils: {
    aoa_to_sheet: (rows: unknown[][]) => Record<string, XlsxCell | unknown>;
    book_new: () => unknown;
    book_append_sheet: (wb: unknown, ws: unknown, name: string) => void;
    encode_cell: (c: { r: number; c: number }) => string;
  };
  writeFile: (wb: unknown, filename: string) => void;
}

export async function exportPackXlsx(p: MetricsPayload): Promise<void> {
  const mod = (await import('xlsx-js-style')) as unknown as { default?: XlsxApi } & XlsxApi;
  const XLSX: XlsxApi = mod.default ?? mod;
  const rows = packRows(p);
  const ws = XLSX.utils.aoa_to_sheet(rows) as Record<string, XlsxCell | unknown>;
  ws['!cols'] = [{ wch: 34 }, { wch: 24 }, { wch: 24 }, { wch: 24 }, { wch: 24 }, { wch: 46 }];
  // Section titles and table headers in bold.
  const bold = { font: { bold: true } };
  rows.forEach((r, i) => {
    const first = String(r[0] ?? '');
    const header = ['Metric', 'Year', 'Date', 'Bank'].includes(first);
    const title = i === 0 || (r.length === 1 && first && first !== 'None');
    if (!header && !title) return;
    for (let c = 0; c < (header ? r.length : 1); c++) {
      const cell = ws[XLSX.utils.encode_cell({ r: i, c })] as XlsxCell | undefined;
      if (cell) cell.s = bold;
    }
  });
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Metrics');
  XLSX.writeFile(wb, `lsports-metrics-${p.asOf.lastClosed || 'latest'}.xlsx`);
}
