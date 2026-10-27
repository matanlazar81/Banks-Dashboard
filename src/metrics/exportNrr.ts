// Excel of the NRR breakdown (nrrWorkbook.ts): every customer and the calculation as formulas, for review.
// Loaded only when the user clicks Excel in the NRR window.
import { nrrWorkbook } from './nrrWorkbook.ts';
import type { Explain } from './types.ts';

interface XlsxCell { v?: unknown; t?: string; f?: string; z?: string; s?: Record<string, unknown> }
interface XlsxApi {
  utils: {
    aoa_to_sheet: (rows: unknown[][]) => Record<string, XlsxCell | unknown>;
    book_new: () => unknown;
    book_append_sheet: (wb: unknown, ws: unknown, name: string) => void;
    encode_cell: (c: { r: number; c: number }) => string;
  };
  writeFile: (wb: unknown, filename: string) => void;
}

export async function exportNrrXlsx(d: Explain): Promise<void> {
  const mod = (await import('xlsx-js-style')) as unknown as { default?: XlsxApi } & XlsxApi;
  const XLSX: XlsxApi = mod.default ?? mod;
  const { sheets, fileName } = nrrWorkbook(d, new Date());
  const wb = XLSX.utils.book_new();
  for (const sheet of sheets) {
    // Values first; then each formatted cell gets its formula, number format and font.
    const ws = XLSX.utils.aoa_to_sheet(sheet.rows.map((row) => row.map((c) => (c !== null && typeof c === 'object' ? c.v : c))));
    sheet.rows.forEach((row, r) => row.forEach((c, col) => {
      if (c === null || typeof c !== 'object') return;
      const cell = ws[XLSX.utils.encode_cell({ r, c: col })] as XlsxCell | undefined;
      if (!cell) return;
      if (c.f) cell.f = c.f;
      const s: Record<string, unknown> = {};
      if (c.z) { cell.z = c.z; s.numFmt = c.z; }
      if (c.bold || c.size) s.font = { ...(c.bold ? { bold: true } : {}), ...(c.size ? { sz: c.size } : {}) };
      if (c.right) s.alignment = { horizontal: 'right' };
      if (Object.keys(s).length) cell.s = s;
    }));
    ws['!cols'] = sheet.cols.map((wch) => ({ wch }));
    if (sheet.autofilter) ws['!autofilter'] = { ref: sheet.autofilter };
    if (sheet.freezeRows) ws['!freeze'] = { xSplit: 0, ySplit: sheet.freezeRows };
    XLSX.utils.book_append_sheet(wb, ws, sheet.name);
  }
  XLSX.writeFile(wb, fileName);
}
