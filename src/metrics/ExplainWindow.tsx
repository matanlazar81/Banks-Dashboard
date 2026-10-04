// Movable window with what one pack figure is made of (GET /api/metrics?detail=…): the steps from the
// source to the figure, how it is calculated, where the data comes from, and the items behind it. Drag it
// by its title bar; Esc or × closes it. Not modal: the pack stays usable behind it.
import { useEffect, useId, useState } from 'react';
import { ChevronDown, ChevronRight, GripHorizontal, Loader2, X } from 'lucide-react';
import { clampPosition, type PanelPosition } from '../new-dashboard/breakdown.ts';
import { useDrag } from '../new-dashboard/useDrag.ts';
import { fetchDetail } from './api.ts';
import { formatExplain } from './model.ts';
import type { Explain, ExplainTable } from './types.ts';

const W = 680;
// Where the window was last left, so the next figure opens there (until the page reloads).
let lastPosition: PanelPosition | null = null;

function Table({ table, open: initiallyOpen }: { table: ExplainTable; open: boolean }) {
  const [open, setOpen] = useState(initiallyOpen);
  const right = (unit: string) => (unit === 'text' ? 'text-left' : 'text-right tabular-nums');
  return (
    <div className="mt-3 rounded-md border border-slate-200">
      <button type="button" onClick={() => setOpen((o) => !o)} aria-expanded={open}
        className="flex w-full items-center gap-1 px-3 py-2 text-left text-xs font-semibold text-slate-700 hover:bg-slate-50">
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}{table.title}
      </button>
      {open && (
        <div className="overflow-x-auto border-t border-slate-200 px-3 py-2">
          {table.note && <p className="mb-1 text-[11px] text-slate-500">{table.note}</p>}
          {table.rows.length === 0 ? <p className="py-1 text-xs text-slate-500">None.</p> : (
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500">
                  {table.columns.map((c) => <th key={c.key} className={`py-1 pr-2 font-medium ${right(c.unit)}`}>{c.label}</th>)}
                </tr>
              </thead>
              <tbody>
                {table.rows.map((r, i) => (
                  <tr key={`r-${i}`} className="border-t border-slate-100">
                    {table.columns.map((c) => (
                      <td key={c.key} className={`py-1 pr-2 ${right(c.unit)} ${c.unit === 'text' ? 'text-slate-800' : ''}`}>{formatExplain(r[c.key], c.unit)}</td>
                    ))}
                  </tr>
                ))}
                {table.more > 0 && (
                  <tr className="border-t border-slate-100">
                    <td colSpan={table.columns.length} className="py-1 italic text-slate-500">…and {table.more} more (smaller; included in the total)</td>
                  </tr>
                )}
                {table.total && (
                  <tr className="border-t-2 border-slate-300 font-semibold">
                    {table.columns.map((c) => (
                      <td key={c.key} className={`py-1 pr-2 ${right(c.unit)}`}>{table.total && table.total[c.key] !== undefined ? formatExplain(table.total[c.key], c.unit) : ''}</td>
                    ))}
                  </tr>
                )}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  );
}

function Body({ d }: { d: Explain }) {
  return (
    <>
      <table className="w-full text-sm">
        <tbody>
          {d.summary.map((s, i) => (
            <tr key={`s-${i}`} className={s.strong ? 'border-t border-slate-300 font-semibold text-slate-900' : 'text-slate-700'}>
              <td className="py-1 pr-3">{s.label}</td>
              <td className="whitespace-nowrap py-1 text-right tabular-nums">{formatExplain(s.value, s.unit, s.sign)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {(d.notes || []).map((n) => <p key={n} className="mt-2 text-xs text-amber-800">{n}</p>)}
      <h3 className="mt-4 text-xs font-semibold uppercase tracking-wide text-slate-500">How it is calculated</h3>
      <ul className="mt-1 list-disc space-y-0.5 pl-4 text-xs text-slate-700">{d.formula.map((f) => <li key={f}>{f}</li>)}</ul>
      <h3 className="mt-3 text-xs font-semibold uppercase tracking-wide text-slate-500">Source</h3>
      <ul className="mt-1 list-disc space-y-0.5 pl-4 text-xs text-slate-700">{d.source.map((f) => <li key={f}>{f}</li>)}</ul>
      {d.tables.map((t, i) => <Table key={t.title} table={t} open={i === 0} />)}
    </>
  );
}

type Load = { phase: 'loading' } | { phase: 'ready'; data: Explain } | { phase: 'error'; error: string };

export default function ExplainWindow({ item, onClose }: { item: string; onClose: () => void }) {
  const titleId = useId();
  const [position, setPosition] = useState<PanelPosition>(
    () => clampPosition(lastPosition ?? { x: window.innerWidth - W - 24, y: 72 }, undefined, W),
  );
  const dragBar = useDrag(position, (p) => { lastPosition = p; setPosition(p); }, W);
  const [result, setResult] = useState<{ item: string; load: Load }>({ item: '', load: { phase: 'loading' } });
  const load: Load = result.item === item ? result.load : { phase: 'loading' };

  useEffect(() => {
    const ctrl = new AbortController();
    fetchDetail(item, ctrl.signal)
      .then((data) => { if (!ctrl.signal.aborted) setResult({ item, load: { phase: 'ready', data } }); })
      .catch((e: unknown) => { if (!ctrl.signal.aborted) setResult({ item, load: { phase: 'error', error: e instanceof Error ? e.message : String(e) } }); });
    return () => ctrl.abort();
  }, [item]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const d = load.phase === 'ready' ? load.data : null;
  return (
    <div
      role="dialog"
      aria-modal="false"
      aria-labelledby={titleId}
      className="fixed z-50 flex flex-col rounded-lg border border-slate-300 bg-white shadow-2xl"
      style={{ left: position.x, top: position.y, width: `min(${W}px, calc(100vw - 16px))`, maxHeight: `calc(100vh - ${position.y + 8}px)` }}
    >
      <div className="flex cursor-move touch-none select-none items-start justify-between gap-3 rounded-t-lg border-b border-slate-200 bg-slate-50 px-4 py-2.5" {...dragBar}>
        <div className="min-w-0">
          <div id={titleId} className="flex items-center gap-2 text-sm font-semibold text-slate-900">
            <GripHorizontal size={14} className="shrink-0 text-slate-400" aria-hidden="true" />
            <span className="truncate">{d ? d.title : 'How it is calculated'}</span>
            {d && <span className="shrink-0 tabular-nums">{formatExplain(d.value.value, d.value.unit)}</span>}
          </div>
          {d && <div className="mt-0.5 truncate text-xs text-slate-500">{d.subtitle}</div>}
        </div>
        <button type="button" onClick={onClose} aria-label="Close" className="rounded p-1 text-slate-500 hover:bg-slate-200 hover:text-slate-800"><X size={16} /></button>
      </div>
      <div className="overflow-y-auto px-4 py-3">
        {load.phase === 'loading' && <div className="flex items-center gap-2 py-6 text-sm text-slate-500"><Loader2 size={16} className="animate-spin" />Reading the data behind this figure…</div>}
        {load.phase === 'error' && <p className="py-4 text-sm text-rose-700">{load.error}</p>}
        {d && <Body d={d} />}
      </div>
    </div>
  );
}
