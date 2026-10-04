// Movable window with the breakdown of one table cell. Drag it by its title bar; it stays where it was
// put when another cell is opened. Esc or × closes it. Not modal: the table stays usable behind it.
import { useEffect, useRef, useState, type PointerEvent as ReactPointerEvent } from 'react';
import { CheckCircle2, ChevronDown, ChevronRight, GripHorizontal, Info, Loader2, X } from 'lucide-react';
import {
  arrangeRows, CASH_BREAKDOWN_ENDPOINT, clampPosition, fetchBreakdown, PANEL_W,
  type BreakdownReady, type BreakdownRequest, type BreakdownRow, type BreakdownSection, type PanelPosition,
} from './breakdown.ts';
import { formatFull } from './model.ts';
import type { Ccy } from './types.ts';

const GROUP_PREVIEW = 12;

const STATUS_LABEL: Record<BreakdownReady['periodStatus'], string> = { actual: 'Actual', current: 'Current month', forecast: 'Forecast', fy: 'Full year' };

function Amount({ value, ccy, strong }: { value: number; ccy: Ccy; strong?: boolean }) {
  return <span className={`tabular-nums ${strong ? 'font-semibold' : ''} ${value < 0 ? 'text-slate-700' : ''}`}>{formatFull(value, ccy)}</span>;
}

function RowLine({ row, ccy, indent }: { row: BreakdownRow; ccy: Ccy; indent: boolean }) {
  const adjust = row.kind === 'adjust';
  return (
    <tr className={adjust ? 'text-slate-600' : ''}>
      <td className={`py-1 pr-2 align-top ${indent ? 'pl-4' : ''}`}>
        <span className={`inline-flex items-center gap-1 ${adjust ? 'italic' : ''}`}>
          {row.label}
          {row.hint && (
            <span title={row.hint} aria-label={row.hint} className="cursor-help not-italic text-slate-400"><Info size={12} /></span>
          )}
        </span>
        {row.ref && <span className="ml-1.5 font-mono text-[11px] text-slate-400">{row.ref}</span>}
      </td>
      <td className="whitespace-nowrap py-1 text-right align-top"><Amount value={row.amount} ccy={ccy} /></td>
    </tr>
  );
}

function SectionTable({ section, ccy }: { section: BreakdownSection; ccy: Ccy }) {
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const blocks = arrangeRows(section.rows);
  return (
    <table className="w-full text-[13px]">
      <tbody>
        {blocks.map((b, i) => {
          if (!b.group) return <RowLine key={b.rows[0].key} row={b.rows[0]} ccy={ccy} indent={false} />;
          const expanded = open[b.group] || b.rows.length <= GROUP_PREVIEW;
          const shown = expanded ? b.rows : b.rows.slice(0, GROUP_PREVIEW);
          return [
            <tr key={`g-${b.group}-${i}`} className="border-t border-slate-100">
              <td className="pt-2 pb-1 pr-2 font-medium text-slate-800">{b.group}</td>
              <td className="whitespace-nowrap pt-2 pb-1 text-right font-medium"><Amount value={b.total} ccy={ccy} /></td>
            </tr>,
            ...shown.map((r) => <RowLine key={r.key} row={r} ccy={ccy} indent />),
            !expanded && (
              <tr key={`more-${b.group}-${i}`}>
                <td colSpan={2} className="pb-1 pl-4">
                  <button type="button" className="text-xs font-medium text-sky-700 hover:underline" onClick={() => setOpen((o) => ({ ...o, [b.group as string]: true }))}>
                    Show all {b.rows.length}
                  </button>
                </td>
              </tr>
            ),
          ];
        })}
      </tbody>
    </table>
  );
}

function SecondarySection({ section, ccy }: { section: BreakdownSection; ccy: Ccy }) {
  const [open, setOpen] = useState(!section.collapsed);
  return (
    <div className="mt-3 rounded-md border border-slate-200">
      <button type="button" onClick={() => setOpen((o) => !o)} aria-expanded={open}
        className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left text-xs font-semibold text-slate-700 hover:bg-slate-50">
        <span className="inline-flex items-center gap-1">{open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}{section.title}</span>
        <Amount value={section.total} ccy={ccy} />
      </button>
      {open && (
        <div className="border-t border-slate-200 px-3 py-2">
          {section.note && <p className="mb-1 text-xs text-slate-500">{section.note}</p>}
          <SectionTable section={section} ccy={ccy} />
        </div>
      )}
    </div>
  );
}

interface Props {
  request: BreakdownRequest;
  position: PanelPosition;
  onMove: (p: PanelPosition) => void;
  onClose: () => void;
  /** The breakdown API (default: the cash projection's). */
  endpoint?: string;
}

type Load = { phase: 'loading' } | { phase: 'ready'; data: BreakdownReady } | { phase: 'computing' } | { phase: 'error'; error: string };

export default function BreakdownPanel({ request, position, onMove, onClose, endpoint = CASH_BREAKDOWN_ENDPOINT }: Props) {
  const { line, period, variant, ccy } = request;
  const reqKey = `${line}|${period}|${variant}|${ccy}`;
  // Each result remembers the request it answers; a newer request reads as loading until its own arrives.
  const [result, setResult] = useState<{ key: string; load: Load }>({ key: '', load: { phase: 'loading' } });
  const load: Load = result.key === reqKey ? result.load : { phase: 'loading' };
  const drag = useRef<{ dx: number; dy: number } | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    const done = (l: Load) => { if (!ctrl.signal.aborted) setResult({ key: reqKey, load: l }); };
    fetchBreakdown({ line, period, variant, ccy }, ctrl.signal, endpoint)
      .then((r) => {
        if (r.status === 'ready') done({ phase: 'ready', data: r });
        else if (r.status === 'computing') done({ phase: 'computing' });
        else done({ phase: 'error', error: r.error });
      })
      .catch((e: unknown) => done({ phase: 'error', error: e instanceof Error ? e.message : String(e) }));
    return () => ctrl.abort();
  }, [reqKey, line, period, variant, ccy, endpoint]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const startDrag = (e: ReactPointerEvent<HTMLDivElement>) => {
    if ((e.target as HTMLElement).closest('button')) return;
    drag.current = { dx: e.clientX - position.x, dy: e.clientY - position.y };
    e.currentTarget.setPointerCapture(e.pointerId);
  };
  const moveDrag = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (drag.current) onMove(clampPosition({ x: e.clientX - drag.current.dx, y: e.clientY - drag.current.dy }));
  };
  const endDrag = (e: ReactPointerEvent<HTMLDivElement>) => {
    drag.current = null;
    if (e.currentTarget.hasPointerCapture(e.pointerId)) e.currentTarget.releasePointerCapture(e.pointerId);
  };

  const data = load.phase === 'ready' ? load.data : null;
  const main = data ? data.sections[0] : null;
  const ties = data && main ? Math.abs(main.total - data.cell) < 1 : false;

  return (
    <div
      role="dialog"
      aria-modal="false"
      aria-labelledby="breakdown-title"
      className="fixed z-50 flex max-h-[75vh] flex-col rounded-lg border border-slate-300 bg-white shadow-2xl"
      style={{ left: position.x, top: position.y, width: `min(${PANEL_W}px, calc(100vw - 16px))` }}
    >
      <div
        className="flex cursor-move touch-none select-none items-start justify-between gap-3 rounded-t-lg border-b border-slate-200 bg-slate-50 px-4 py-2.5"
        onPointerDown={startDrag}
        onPointerMove={moveDrag}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
      >
        <div className="min-w-0">
          <div id="breakdown-title" className="flex items-center gap-2 text-sm font-semibold text-slate-900">
            <GripHorizontal size={14} className="shrink-0 text-slate-400" aria-hidden="true" />
            <span className="truncate">{data ? `${data.lineLabel} · ${data.periodLabel}` : 'Breakdown'}</span>
          </div>
          {data && (
            <div className="mt-0.5 text-xs text-slate-500">
              {STATUS_LABEL[data.periodStatus]} · {data.variant === 'plan' ? 'Plan' : 'Base'} · table cell <Amount value={data.cell} ccy={data.ccy} />
            </div>
          )}
        </div>
        <button type="button" onClick={onClose} aria-label="Close" className="rounded p-1 text-slate-500 hover:bg-slate-200 hover:text-slate-800">
          <X size={16} />
        </button>
      </div>

      <div className="overflow-y-auto px-4 py-3">
        {load.phase === 'loading' && (
          <div className="flex items-center gap-2 py-6 text-sm text-slate-500"><Loader2 size={16} className="animate-spin" />Loading the breakdown…</div>
        )}
        {load.phase === 'computing' && (
          <p className="py-4 text-sm text-slate-600">The projection is being rebuilt. Try again in a minute.</p>
        )}
        {load.phase === 'error' && <p className="py-4 text-sm text-rose-700">{load.error}</p>}
        {data && main && (
          <>
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{main.title}</div>
            {main.note && <p className="mt-0.5 text-xs text-slate-500">{main.note}</p>}
            <div className="mt-2"><SectionTable section={main} ccy={data.ccy} /></div>
            <div className="mt-2 flex items-center justify-between border-t-2 border-slate-300 pt-2 text-sm">
              <span className="inline-flex items-center gap-1.5 font-semibold text-slate-900">
                Total
                {ties && <span className="inline-flex items-center gap-1 text-xs font-normal text-emerald-700"><CheckCircle2 size={13} />equals the table cell</span>}
              </span>
              <Amount value={main.total} ccy={data.ccy} strong />
            </div>
            {data.sections.slice(1).map((s) => <SecondarySection key={s.id} section={s} ccy={data.ccy} />)}
            {data.notes.map((n) => <p key={n} className="mt-2 text-xs text-slate-500">{n}</p>)}
          </>
        )}
      </div>
    </div>
  );
}
