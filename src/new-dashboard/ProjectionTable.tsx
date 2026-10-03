import { useEffect, useRef } from 'react';
import { Info } from 'lucide-react';
import type { Ccy } from './types.ts';
import { CCY_SYMBOL, formatFull, formatSignedThousands, formatThousands, monthLongLabel, type Column, type LineKey, type ProjectionTable as Table, type TableLine } from './model.ts';
import { BREAKDOWN_LINES } from './breakdown.ts';

const LABEL_W = 'w-52 min-w-52';

function columnTone(col: Column): string {
  if (col.kind === 'fy') return 'bg-slate-100';
  if (col.status === 'actual') return 'bg-slate-50';
  if (col.status === 'current') return 'bg-sky-50';
  return 'bg-white';
}

function dividerClass(col: Column): string {
  return col.firstOfYear ? 'border-l-2 border-l-slate-300' : '';
}

function StatusChip({ col }: { col: Column }) {
  if (col.kind === 'fy') return <span className="text-[11px] font-normal text-slate-500">Full year</span>;
  const tone = col.status === 'current'
    ? 'bg-sky-600 text-white'
    : col.status === 'actual'
      ? 'bg-slate-200 text-slate-700'
      : 'border border-slate-300 bg-white text-slate-500';
  return <span className={`rounded px-1.5 py-px text-[10px] font-medium uppercase tracking-wide ${tone}`}>{col.statusLabel}</span>;
}

interface CellProps {
  line: TableLine;
  col: Column;
  value: number;
  ccy: Ccy;
  anchorDate: string | null;
  prevClosingLabel: string | null;
  /** Set when the cell opens a breakdown. */
  onOpen?: () => void;
  active?: boolean;
}

function Cell({ line, col, value, ccy, anchorDate, prevClosingLabel, onOpen, active }: CellProps) {
  const emphasize = line.kind === 'balance' || line.kind === 'subtotal';
  const k = Math.round(value / 1000);
  const negativeBad = (line.key === 'net' || line.kind === 'balance') && k < 0;
  const tone = line.signed
    ? (k > 0 ? 'text-emerald-700' : k < 0 ? 'text-rose-600' : '')
    : negativeBad ? 'text-rose-600' : '';
  const isAnchor = line.key === 'opening' && col.status === 'current';
  const isRollForward = line.key === 'opening' && col.rollForward;
  const where = col.kind === 'fy' ? `FY ${col.year}` : monthLongLabel(col.mKey!);
  let title = `${line.label} · ${where}: ${formatFull(value, ccy)}`;
  if (isAnchor && anchorDate) title += ` (re-anchored to the NetSuite bank balance of ${anchorDate})`;
  if (isRollForward && prevClosingLabel) title += ` (rolled forward: equals the ${prevClosingLabel} closing)`;
  const text = line.signed ? formatSignedThousands(value) : formatThousands(value);
  return (
    <td
      className={`whitespace-nowrap px-2 py-1.5 text-right tabular-nums ${columnTone(col)} ${dividerClass(col)} ${emphasize ? 'font-semibold' : ''} ${tone} ${line.kind === 'note' ? 'text-xs text-slate-500' : ''} ${active ? 'outline outline-2 -outline-offset-2 outline-sky-500' : ''}`}
      title={onOpen ? `${title} · click for the breakdown` : title}
    >
      {isAnchor && <span className="mr-1 text-sky-600" aria-label="re-anchored to bank">⚓</span>}
      {isRollForward && <span className="mr-1 text-emerald-600" aria-label="rolled forward">↩</span>}
      {onOpen ? (
        <button type="button" onClick={onOpen} aria-pressed={!!active}
          className="cursor-pointer tabular-nums underline decoration-slate-300 decoration-dotted underline-offset-2 hover:text-sky-700 hover:decoration-sky-500">
          {text}
        </button>
      ) : text}
    </td>
  );
}

interface Props {
  table: Table;
  ccy: Ccy;
  /** 'YYYY-MM-DD' of the bank balance the current month opens from. */
  anchorDate: string | null;
  /** Opens the breakdown of a cell (lines in BREAKDOWN_LINES with an amount). */
  onOpenCell?: (line: LineKey, col: Column) => void;
  /** The cell whose breakdown is open: `${line}|${columnId}`. */
  activeCell?: string | null;
}

export default function ProjectionTable({ table, ccy, anchorDate, onOpenCell, activeCell }: Props) {
  const scroller = useRef<HTMLDivElement>(null);
  const layoutKey = table.columns.map((c) => c.id).join('|');

  // Bring the current month into view (a few months of actuals stay visible on its left).
  useEffect(() => {
    const el = scroller.current;
    if (!el) return;
    const current = el.querySelector<HTMLElement>('th[data-current="true"]');
    const label = el.querySelector<HTMLElement>('th[data-label-col="true"]');
    if (!current) { el.scrollLeft = 0; return; }
    const offset = current.offsetLeft - (label ? label.offsetWidth : 0) - current.offsetWidth * 3;
    el.scrollLeft = Math.max(0, offset);
  }, [layoutKey]);

  const anchorLabel = anchorDate
    ? new Date(`${anchorDate}T12:00:00`).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
    : null;

  return (
    <div ref={scroller} className="overflow-x-auto">
      <table className="w-max min-w-full border-separate border-spacing-0 text-[13px]">
        <thead>
          <tr>
            <th
              rowSpan={2}
              data-label-col="true"
              className={`sticky left-0 z-20 border-b border-slate-200 bg-white px-3 py-2 text-left align-bottom text-xs font-medium text-slate-500 ${LABEL_W}`}
            >
              {CCY_SYMBOL[ccy]} thousands
            </th>
            {table.groups.map((g, i) => (
              <th
                key={g.year}
                colSpan={g.span}
                className={`border-b border-slate-200 px-3 py-2 text-left text-xs font-semibold text-slate-700 ${i > 0 ? 'border-l-2 border-l-slate-300' : ''} ${g.kind === 'projection' ? 'bg-emerald-50/60' : 'bg-white'}`}
              >
                {g.label}
              </th>
            ))}
          </tr>
          <tr>
            {table.columns.map((col) => (
              <th
                key={col.id}
                data-current={col.status === 'current' ? 'true' : undefined}
                className={`min-w-20 border-b border-slate-200 px-2 py-1.5 text-right align-bottom ${columnTone(col)} ${dividerClass(col)}`}
              >
                <div className="text-xs font-semibold text-slate-800">{col.label}</div>
                <div className="mt-0.5"><StatusChip col={col} /></div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.lines.map((line) => {
            const rowBorder = line.key === 'closing'
              ? '[&>*]:border-t-2 [&>*]:border-t-slate-300'
              : line.kind === 'subtotal'
                ? '[&>*]:border-t [&>*]:border-t-slate-200'
                : '';
            return (
              <tr key={line.key} className={rowBorder}>
                <th
                  scope="row"
                  className={`sticky left-0 z-10 bg-white px-3 py-1.5 text-left font-normal ${LABEL_W} ${line.kind === 'item' ? 'pl-6 text-slate-700' : ''} ${line.kind === 'balance' || line.kind === 'subtotal' ? 'font-semibold text-slate-900' : ''} ${line.kind === 'note' ? 'pl-6 text-xs italic text-slate-500' : ''}`}
                >
                  <span className="inline-flex items-center gap-1">
                    {line.label}
                    {line.key === 'reanchor' && <span className="not-italic text-sky-600">⚓</span>}
                    {line.hint && (
                      <span title={line.hint} className="cursor-help text-slate-400" aria-label={line.hint}>
                        <Info size={12} />
                      </span>
                    )}
                  </span>
                </th>
                {table.columns.map((col, i) => {
                  const prev = col.rollForward ? table.columns.find((c) => c.kind === 'month' && c.year === col.year - 1 && c.mKey?.endsWith('-12')) : null;
                  const value = line.values[i];
                  const opens = !!onOpenCell && BREAKDOWN_LINES.has(line.key) && Math.abs(value) >= 1;
                  return (
                    <Cell
                      key={col.id}
                      line={line}
                      col={col}
                      value={value}
                      ccy={ccy}
                      anchorDate={anchorLabel}
                      prevClosingLabel={col.rollForward ? (prev ? monthLongLabel(prev.mKey!) : `December ${col.year - 1}`) : null}
                      onOpen={opens ? () => onOpenCell?.(line.key, col) : undefined}
                      active={activeCell === `${line.key}|${col.id}`}
                    />
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
