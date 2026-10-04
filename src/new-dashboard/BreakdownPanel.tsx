// Movable window with the breakdown of one table cell, or of one account row by department. Drag it by
// its title bar; it stays where it was put when another cell is opened. Esc or × closes it. Not modal:
// the table stays usable behind it. Account names open the department window (onDrill).
import { useEffect, useId, useRef, useState, type PointerEvent as ReactPointerEvent } from 'react';
import { CheckCircle2, ChevronDown, ChevronRight, ExternalLink, GripHorizontal, Info, Loader2, Users, X } from 'lucide-react';
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

type Drill = { onDrill?: (row: BreakdownRow) => void; activeRow?: string | null };

function RowLine({ row, ccy, indent, onDrill, activeRow }: { row: BreakdownRow; ccy: Ccy; indent: boolean } & Drill) {
  const adjust = row.kind === 'adjust';
  const canDrill = !!(onDrill && row.drillable);
  return (
    <tr className={`${adjust ? 'text-slate-600' : ''} ${activeRow === row.key ? 'bg-sky-50' : ''}`}>
      <td className={`py-1 pr-2 align-top ${indent ? 'pl-4' : ''}`}>
        <span className={`inline-flex items-center gap-1 ${adjust ? 'italic' : ''}`}>
          {canDrill ? (
            <button type="button" onClick={() => onDrill!(row)} title={`${row.label}: by department`}
              className="inline-flex items-center gap-1 text-left text-slate-900 underline decoration-slate-300 decoration-dotted underline-offset-2 hover:text-sky-800 hover:decoration-sky-500">
              {row.label}<Users size={11} className="shrink-0 text-slate-400" aria-hidden="true" />
            </button>
          ) : row.label}
          {row.hint && (
            <span title={row.hint} aria-label={row.hint} className="cursor-help not-italic text-slate-400"><Info size={12} /></span>
          )}
        </span>
        {row.ref && row.link && (
          <a href={row.link} target="_blank" rel="noopener noreferrer" title={`Open account ${row.ref} in NetSuite (register)`}
            className="ml-1.5 inline-flex items-center gap-0.5 font-mono text-[11px] text-sky-700 underline decoration-dotted underline-offset-2 hover:text-sky-900">
            {row.ref}<ExternalLink size={10} aria-hidden="true" />
          </a>
        )}
        {row.ref && !row.link && <span className="ml-1.5 font-mono text-[11px] text-slate-400">{row.ref}</span>}
      </td>
      <td className="whitespace-nowrap py-1 text-right align-top"><Amount value={row.amount} ccy={ccy} /></td>
    </tr>
  );
}

function SectionTable({ section, ccy, onDrill, activeRow }: { section: BreakdownSection; ccy: Ccy } & Drill) {
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const blocks = arrangeRows(section.rows);
  return (
    <table className="w-full text-[13px]">
      <tbody>
        {blocks.map((b, i) => {
          if (!b.group) return <RowLine key={b.rows[0].key} row={b.rows[0]} ccy={ccy} indent={false} onDrill={onDrill} activeRow={activeRow} />;
          const expanded = open[b.group] || b.rows.length <= GROUP_PREVIEW;
          const shown = expanded ? b.rows : b.rows.slice(0, GROUP_PREVIEW);
          return [
            <tr key={`g-${b.group}-${i}`} className="border-t border-slate-100">
              <td className="pt-2 pb-1 pr-2 font-medium text-slate-800">{b.group}</td>
              <td className="whitespace-nowrap pt-2 pb-1 text-right font-medium"><Amount value={b.total} ccy={ccy} /></td>
            </tr>,
            ...shown.map((r) => <RowLine key={r.key} row={r} ccy={ccy} indent onDrill={onDrill} activeRow={activeRow} />),
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

function SecondarySection({ section, ccy, onDrill, activeRow }: { section: BreakdownSection; ccy: Ccy } & Drill) {
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
          <SectionTable section={section} ccy={ccy} onDrill={onDrill} activeRow={activeRow} />
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
  /** Account names become buttons that call this (the department window). */
  onDrill?: (row: BreakdownRow) => void;
  /** Row key highlighted as the one open in the department window. */
  activeRow?: string | null;
  /** Esc closes this window (default). Off when the parent orders Esc across several windows. */
  escToClose?: boolean;
  /** Stacking order (default 50); onFocus fires on any pointer down, to bring it to the front. */
  zIndex?: number;
  onFocus?: () => void;
}

type Load = { phase: 'loading' } | { phase: 'ready'; data: BreakdownReady } | { phase: 'computing' } | { phase: 'error'; error: string };

export default function BreakdownPanel({
  request, position, onMove, onClose, endpoint = CASH_BREAKDOWN_ENDPOINT, onDrill, activeRow = null, escToClose = true, zIndex = 50, onFocus,
}: Props) {
  const { line, period, variant, ccy, row } = request;
  const reqKey = `${line}|${period}|${variant}|${ccy}|${row || ''}`;
  const titleId = useId();
  // Each result remembers the request it answers; a newer request reads as loading until its own arrives.
  const [result, setResult] = useState<{ key: string; load: Load }>({ key: '', load: { phase: 'loading' } });
  const load: Load = result.key === reqKey ? result.load : { phase: 'loading' };
  const drag = useRef<{ dx: number; dy: number } | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    const done = (l: Load) => { if (!ctrl.signal.aborted) setResult({ key: reqKey, load: l }); };
    fetchBreakdown({ line, period, variant, ccy, row }, ctrl.signal, endpoint)
      .then((r) => {
        if (r.status === 'ready') done({ phase: 'ready', data: r });
        else if (r.status === 'computing') done({ phase: 'computing' });
        else done({ phase: 'error', error: r.error });
      })
      .catch((e: unknown) => done({ phase: 'error', error: e instanceof Error ? e.message : String(e) }));
    return () => ctrl.abort();
  }, [reqKey, line, period, variant, ccy, row, endpoint]);

  useEffect(() => {
    if (!escToClose) return undefined;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose, escToClose]);

  const startDrag = (e: ReactPointerEvent<HTMLDivElement>) => {
    if ((e.target as HTMLElement).closest('button, a')) return;
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
  const account = data ? data.account : undefined;

  return (
    <div
      role="dialog"
      aria-modal="false"
      aria-labelledby={titleId}
      className="fixed flex max-h-[75vh] flex-col rounded-lg border border-slate-300 bg-white shadow-2xl"
      style={{ left: position.x, top: position.y, width: `min(${PANEL_W}px, calc(100vw - 16px))`, zIndex }}
      onPointerDownCapture={onFocus}
    >
      <div
        className="flex cursor-move touch-none select-none items-start justify-between gap-3 rounded-t-lg border-b border-slate-200 bg-slate-50 px-4 py-2.5"
        onPointerDown={startDrag}
        onPointerMove={moveDrag}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
      >
        <div className="min-w-0">
          <div id={titleId} className="flex items-center gap-2 text-sm font-semibold text-slate-900">
            <GripHorizontal size={14} className="shrink-0 text-slate-400" aria-hidden="true" />
            <span className="truncate">{data ? `${data.lineLabel} · ${data.periodLabel}` : row ? 'By department' : 'Breakdown'}</span>
            {account && account.link && (
              <a href={account.link} target="_blank" rel="noopener noreferrer" title={`Open account ${account.acct} in NetSuite (register)`}
                className="shrink-0 text-sky-700 hover:text-sky-900">
                <ExternalLink size={13} aria-hidden="true" />
              </a>
            )}
          </div>
          {data && !account && (
            <div className="mt-0.5 text-xs text-slate-500">
              {STATUS_LABEL[data.periodStatus]} · {data.variant === 'plan' ? 'Plan' : 'Base'} · table cell <Amount value={data.cell} ccy={data.ccy} />
            </div>
          )}
          {data && account && (
            <div className="mt-0.5 truncate text-xs text-slate-500">
              By department · {account.of} · account row <Amount value={data.cell} ccy={data.ccy} />
            </div>
          )}
        </div>
        <button type="button" onClick={onClose} aria-label="Close" className="rounded p-1 text-slate-500 hover:bg-slate-200 hover:text-slate-800">
          <X size={16} />
        </button>
      </div>

      <div className="overflow-y-auto px-4 py-3">
        {load.phase === 'loading' && (
          <div className="flex items-center gap-2 py-6 text-sm text-slate-500"><Loader2 size={16} className="animate-spin" />{row ? 'Loading the departments…' : 'Loading the breakdown…'}</div>
        )}
        {load.phase === 'computing' && (
          <p className="py-4 text-sm text-slate-600">The projection is being rebuilt. Try again in a minute.</p>
        )}
        {load.phase === 'error' && <p className="py-4 text-sm text-rose-700">{load.error}</p>}
        {data && main && (
          <>
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{main.title}</div>
            {main.note && <p className="mt-0.5 text-xs text-slate-500">{main.note}</p>}
            <div className="mt-2"><SectionTable section={main} ccy={data.ccy} onDrill={onDrill} activeRow={activeRow} /></div>
            <div className="mt-2 flex items-center justify-between border-t-2 border-slate-300 pt-2 text-sm">
              <span className="inline-flex items-center gap-1.5 font-semibold text-slate-900">
                Total
                {ties && (
                  <span className="inline-flex items-center gap-1 text-xs font-normal text-emerald-700">
                    <CheckCircle2 size={13} />{account ? 'equals the account row' : 'equals the table cell'}
                  </span>
                )}
              </span>
              <Amount value={main.total} ccy={data.ccy} strong />
            </div>
            {data.sections.slice(1).map((s) => <SecondarySection key={s.id} section={s} ccy={data.ccy} onDrill={onDrill} activeRow={activeRow} />)}
            {data.notes.map((n) => <p key={n} className="mt-2 text-xs text-slate-500">{n}</p>)}
          </>
        )}
      </div>
    </div>
  );
}
