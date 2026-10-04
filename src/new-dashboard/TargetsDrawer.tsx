// Movable window with the projection-year targets (both projection pages). Drag it by its title bar,
// like the breakdown windows; it opens where it was last left. On the New Bank Dashboard every change
// shows at once in the Targets view behind it and Save shares them with everyone who opens the pages;
// the P&L Projection shows the saved targets read-only (editedOn) with their effect.
import { useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { ChevronDown, ChevronRight, GripHorizontal, Loader2, Plus, Trash2, X } from 'lucide-react';
import { ALL_DEPARTMENTS, describeTargets, type Targets, type TargetsBase } from '../forecast/targets.mjs';
import { clampPosition, type PanelPosition } from './breakdown.ts';
import { formatFull } from './model.ts';
import type { Ccy } from './types.ts';
import type { TargetsState } from './targets.ts';
import { useDrag } from './useDrag.ts';

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** Width of the targets window (narrower on small screens). */
const TARGETS_W = 448;
// Where the window was last left, so reopening it puts it back there (until the page reloads).
let lastPosition: PanelPosition | null = null;

export interface ImpactLine { label: string; plan: number; withTargets: number }

const num = (s: string) => {
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : 0;
};

function NumberBox({ value, onChange, step = 1, width = 'w-20', label }: { value: number; onChange: (n: number) => void; step?: number; width?: string; label: string }) {
  return (
    <input
      type="number"
      aria-label={label}
      step={step}
      value={Number.isFinite(value) ? value : 0}
      onChange={(e: { target: { value: string } }) => onChange(num(e.target.value))}
      className={`${width} rounded border border-slate-300 px-1.5 py-1 text-right text-xs tabular-nums focus:border-sky-500 focus:outline-none`}
    />
  );
}

function MonthSelect({ value, onChange, label }: { value: number; onChange: (m: number) => void; label: string }) {
  return (
    <select aria-label={label} value={value} onChange={(e: { target: { value: string } }) => onChange(parseInt(e.target.value, 10))}
      className="rounded border border-slate-300 px-1 py-1 text-xs focus:border-sky-500 focus:outline-none">
      {MONTHS.map((m, i) => <option key={m} value={i + 1}>{m}</option>)}
    </select>
  );
}

/** One value for every month, or 12 values ("By month"). */
function Monthly({ values, onChange, unit, step, label }: { values: number[]; onChange: (v: number[]) => void; unit: string; step: number; label: string }) {
  const same = values.every((v) => v === values[0]);
  const [open, setOpen] = useState(!same);
  return (
    <div>
      <div className="flex items-center gap-2">
        {same || !open
          ? <NumberBox label={`${label}, every month`} value={same ? values[0] : 0} step={step} onChange={(n) => onChange(Array(12).fill(n))} />
          : <span className="w-20 text-right text-xs italic text-slate-500">varies</span>}
        <span className="text-xs text-slate-500">{unit} {same || !open ? 'every month' : 'by month'}</span>
        <button type="button" onClick={() => setOpen((o) => !o)} className="ml-auto inline-flex items-center text-xs text-sky-700 hover:underline">
          {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}By month
        </button>
      </div>
      {open && (
        <div className="mt-1.5 grid grid-cols-6 gap-1">
          {values.map((v, i) => (
            <label key={MONTHS[i]} className="flex flex-col text-[10px] text-slate-500">
              {MONTHS[i]}
              <NumberBox label={`${label}, ${MONTHS[i]}`} value={v} step={step} width="w-full" onChange={(n) => onChange(values.map((x, k) => (k === i ? n : x)))} />
            </label>
          ))}
        </div>
      )}
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="border-t border-slate-200 px-4 py-3">
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">{title}</h3>
      {children}
    </section>
  );
}

interface Props {
  base: TargetsBase;
  state: TargetsState;
  impact: ImpactLine[];
  ccy: Ccy;
  /** What "revenue" means on this page (cash: expected revenue before the collection %). */
  revenueNote: string;
  onClose: () => void;
  /** Where the targets are set (e.g. 'the New Bank Dashboard'): the window then only shows the saved ones. */
  editedOn?: string;
}

const savedLine = (state: TargetsState) => (state.updatedAt
  ? `Saved ${new Date(state.updatedAt).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })}${state.updatedBy ? ` by ${state.updatedBy}` : ''}.`
  : 'No targets saved yet.');

export default function TargetsDrawer({ base, state, impact, ccy, revenueNote, onClose, editedOn }: Props) {
  const readOnly = !!editedOn;
  const t = readOnly ? state.saved : state.draft;
  const assumptions = readOnly ? describeTargets(t) : [];
  const set = (patch: (d: Targets) => Targets) => state.setDraft(patch(JSON.parse(JSON.stringify(t)) as Targets));
  const depts = base.departments;
  const firstDept = depts[0] || '';
  const fyServerBase = base.months.reduce((s, m) => s + (m.opexByCategory[t.server.category || base.serverCategory] || 0), 0);

  const titleId = useId();
  const [position, setPosition] = useState<PanelPosition>(
    () => clampPosition(lastPosition ?? { x: window.innerWidth - TARGETS_W - 16, y: 64 }, undefined, TARGETS_W),
  );
  const dragBar = useDrag(position, (p) => { lastPosition = p; setPosition(p); }, TARGETS_W);
  // In front of the breakdown windows while in use; behind them once anything else on the page is clicked.
  const [front, setFront] = useState(true);
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const onDown = (e: PointerEvent) => { if (!root.current?.contains(e.target as Node)) setFront(false); };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, []);

  return (
    <div
      ref={root}
      role="dialog"
      aria-modal="false"
      aria-labelledby={titleId}
      className="fixed flex flex-col rounded-lg border border-slate-300 bg-white shadow-2xl"
      style={{
        left: position.x, top: position.y, width: `min(${TARGETS_W}px, calc(100vw - 16px))`,
        maxHeight: `calc(100vh - ${position.y + 8}px)`, zIndex: front ? 52 : 45,
      }}
      onPointerDownCapture={() => setFront(true)}
    >
      <div
        className="flex cursor-move touch-none select-none items-start justify-between gap-3 rounded-t-lg border-b border-slate-200 bg-slate-50 px-4 py-3"
        {...dragBar}
      >
        <div>
          <h2 id={titleId} className="flex items-center gap-2 text-sm font-semibold text-slate-900">
            <GripHorizontal size={14} className="shrink-0 text-slate-400" aria-hidden="true" />{base.year} targets
          </h2>
          <p className="text-xs text-slate-500">
            {readOnly
              ? `On top of the Plan, ${base.year} only. Set on ${editedOn}; the Targets view applies the saved targets.`
              : `On top of the Plan, ${base.year} only. Changes show in the Targets view at once.`}
          </p>
        </div>
        <button type="button" onClick={onClose} aria-label="Close" className="rounded p-1 text-slate-500 hover:bg-slate-200 hover:text-slate-800"><X size={16} /></button>
      </div>

      <div className="overflow-y-auto">
        <div className="px-4 py-3">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-slate-500"><th className="text-left font-medium">FY {base.year}</th><th className="text-right font-medium">Plan</th><th className="text-right font-medium">Targets</th><th className="text-right font-medium">Change</th></tr>
            </thead>
            <tbody>
              {impact.map((l) => {
                const d = l.withTargets - l.plan;
                return (
                  <tr key={l.label} className="border-t border-slate-100">
                    <td className="py-1 font-medium text-slate-800">{l.label}</td>
                    <td className="py-1 text-right tabular-nums">{formatFull(l.plan, ccy)}</td>
                    <td className="py-1 text-right font-semibold tabular-nums">{formatFull(l.withTargets, ccy)}</td>
                    <td className={`py-1 text-right tabular-nums ${d < 0 ? 'text-rose-700' : d > 0 ? 'text-emerald-700' : 'text-slate-400'}`}>{d === 0 ? '–' : `${d > 0 ? '+' : ''}${formatFull(d, ccy)}`}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {readOnly ? (
          <Section title="Applied assumptions">
            {assumptions.length
              ? <ul className="list-disc space-y-1 pl-4 text-xs text-slate-700">{assumptions.map((l, i) => <li key={`a-${i}`}>{l}</li>)}</ul>
              : <p className="text-xs text-slate-500">No {base.year} targets saved yet. Set them on {editedOn}.</p>}
          </Section>
        ) : (<>
        <Section title="Revenue">
          <p className="mb-2 text-[11px] text-slate-500">{revenueNote}</p>
          <div role="radiogroup" aria-label="Revenue target" className="mb-2 flex gap-3 text-xs">
            {([['none', 'No change'], ['growth', 'Growth %'], ['newMrr', 'New MRR €']] as const).map(([v, l]) => (
              <label key={v} className="inline-flex items-center gap-1">
                <input type="radio" name="rev-mode" checked={t.revenue.mode === v} onChange={() => set((d) => { d.revenue.mode = v; return d; })} />{l}
              </label>
            ))}
          </div>
          {t.revenue.mode === 'growth' && (
            <Monthly label="Revenue growth" unit="% a month, compounding" step={0.1} values={t.revenue.growthPct} onChange={(v) => set((d) => { d.revenue.growthPct = v; return d; })} />
          )}
          {t.revenue.mode === 'newMrr' && (
            <Monthly label="New MRR" unit="€ new monthly revenue, cumulative" step={1000} values={t.revenue.newMrr} onChange={(v) => set((d) => { d.revenue.newMrr = v; return d; })} />
          )}
          <div className="mt-2">
            <div className="mb-1 text-xs font-medium text-slate-700">Churn</div>
            <Monthly label="Churn" unit="% of revenue lost a month" step={0.1} values={t.revenue.churnPct} onChange={(v) => set((d) => { d.revenue.churnPct = v; return d; })} />
          </div>
        </Section>

        <Section title="Payroll and hires">
          <div className="mb-1 text-xs font-medium text-slate-700">Salary change by department</div>
          {t.payroll.deptPct.map((p, i) => (
            <div key={`dp-${i}`} className="mb-1 flex items-center gap-1.5">
              <select aria-label="Department" value={p.dept} onChange={(e: { target: { value: string } }) => set((d) => { d.payroll.deptPct[i].dept = e.target.value; return d; })}
                className="min-w-0 flex-1 rounded border border-slate-300 px-1 py-1 text-xs">
                <option value={ALL_DEPARTMENTS}>All departments</option>
                {depts.map((dp) => <option key={dp} value={dp}>{dp}</option>)}
              </select>
              <NumberBox label="Salary change %" value={p.pct} step={0.5} width="w-16" onChange={(n) => set((d) => { d.payroll.deptPct[i].pct = n; return d; })} />
              <span className="text-xs text-slate-500">% from</span>
              <MonthSelect label="From month" value={p.from} onChange={(m) => set((d) => { d.payroll.deptPct[i].from = m; return d; })} />
              <button type="button" aria-label="Remove" onClick={() => set((d) => { d.payroll.deptPct.splice(i, 1); return d; })} className="p-1 text-slate-400 hover:text-rose-700"><Trash2 size={13} /></button>
            </div>
          ))}
          <button type="button" onClick={() => set((d) => { d.payroll.deptPct.push({ dept: ALL_DEPARTMENTS, pct: 0, from: 1 }); return d; })}
            className="mb-3 inline-flex items-center gap-1 text-xs text-sky-700 hover:underline"><Plus size={12} />Add a salary change</button>

          <div className="mb-1 text-xs font-medium text-slate-700">New hires</div>
          {t.payroll.hires.map((h, i) => (
            <div key={`h-${i}`} className="mb-1 flex flex-wrap items-center gap-1.5">
              <select aria-label="Department" value={h.dept} onChange={(e: { target: { value: string } }) => set((d) => { d.payroll.hires[i].dept = e.target.value; return d; })}
                className="min-w-0 flex-1 rounded border border-slate-300 px-1 py-1 text-xs">
                {(depts.includes(h.dept) ? depts : [h.dept, ...depts]).map((dp) => <option key={dp} value={dp}>{dp}</option>)}
              </select>
              <NumberBox label="People" value={h.count} step={1} width="w-12" onChange={(n) => set((d) => { d.payroll.hires[i].count = Math.max(1, Math.round(n)); return d; })} />
              <span className="text-xs text-slate-500">×</span>
              <NumberBox label="Monthly cost per person €" value={h.monthlyCost} step={500} width="w-20" onChange={(n) => set((d) => { d.payroll.hires[i].monthlyCost = n; return d; })} />
              <span className="text-xs text-slate-500">€/mo from</span>
              <MonthSelect label="Start month" value={h.start} onChange={(m) => set((d) => { d.payroll.hires[i].start = m; return d; })} />
              <button type="button" aria-label="Remove" onClick={() => set((d) => { d.payroll.hires.splice(i, 1); return d; })} className="p-1 text-slate-400 hover:text-rose-700"><Trash2 size={13} /></button>
            </div>
          ))}
          <button type="button" disabled={!firstDept} onClick={() => set((d) => { d.payroll.hires.push({ dept: firstDept, monthlyCost: 0, start: 1, count: 1 }); return d; })}
            className="inline-flex items-center gap-1 text-xs text-sky-700 hover:underline disabled:text-slate-400"><Plus size={12} />Add hires</button>
        </Section>

        <Section title="Operating expenses by category">
          <p className="mb-2 text-[11px] text-slate-500">% change on each month's operating expenses of the category (split as in the {base.year - 1} budget).</p>
          {base.categories.map((c) => {
            const isServer = t.server.enabled && c === t.server.category;
            return (
              <div key={c} className="mb-1 flex items-center gap-2 text-xs">
                <span className={`min-w-0 flex-1 truncate ${isServer ? 'text-slate-400' : 'text-slate-700'}`} title={c}>{c}</span>
                {isServer
                  ? <span className="text-[11px] italic text-slate-400">set by Server costs below</span>
                  : <><NumberBox label={`${c} change %`} value={t.opex.categoryPct[c] || 0} step={1} width="w-16"
                      onChange={(n) => set((d) => { if (n) d.opex.categoryPct[c] = n; else delete d.opex.categoryPct[c]; return d; })} /><span className="text-slate-500">%</span></>}
              </div>
            );
          })}
        </Section>

        <Section title="Server costs">
          <label className="mb-2 flex items-center gap-2 text-xs">
            <input type="checkbox" checked={t.server.enabled}
              onChange={(e: { target: { checked: boolean } }) => set((d) => { d.server.enabled = e.target.checked; if (!d.server.category) d.server.category = base.serverCategory; return d; })} />
            Set server costs as a % of revenue
          </label>
          {t.server.enabled && (
            <div className="space-y-2 text-xs">
              <div className="flex items-center gap-2">
                <NumberBox label="Server costs % of revenue" value={t.server.pctOfRevenue} step={0.5} width="w-16" onChange={(n) => set((d) => { d.server.pctOfRevenue = n; return d; })} />
                <span className="text-slate-500">% of revenue (after the revenue targets)</span>
              </div>
              <select aria-label="Server category" value={t.server.category || base.serverCategory}
                onChange={(e: { target: { value: string } }) => set((d) => { d.server.category = e.target.value; return d; })}
                className="w-full rounded border border-slate-300 px-1 py-1 text-xs">
                {base.categories.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
              <p className="text-[11px] text-slate-500">
                Replaces this category's {base.year} costs ({formatFull(fyServerBase, 'eur')} in the Plan).
                {state.serverRatioYtd != null && <> {base.year - 1} so far: {state.serverRatioYtd.toFixed(1)}% of customer revenue (NetSuite 640xxx).</>}
              </p>
            </div>
          )}
        </Section>
        </>)}
      </div>

      <div className="mt-auto rounded-b-lg border-t border-slate-200 bg-slate-50 px-4 py-3">
        {state.error && <p className="mb-2 text-xs text-rose-700">{state.error}</p>}
        {readOnly ? (
          <p className="text-[11px] text-slate-500">Set on {editedOn}. {savedLine(state)}</p>
        ) : (<>
        <div className="flex items-center gap-2">
          <button type="button" onClick={() => void state.save()} disabled={!state.dirty || state.saving}
            className="inline-flex items-center gap-1.5 rounded-md bg-slate-800 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-700 disabled:opacity-50">
            {state.saving && <Loader2 size={13} className="animate-spin" />}Save for everyone
          </button>
          <button type="button" onClick={state.reset} disabled={!state.dirty} className="rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs text-slate-700 hover:bg-slate-100 disabled:opacity-50">Undo changes</button>
          <button type="button" onClick={state.clear} className="rounded-md px-2 py-1.5 text-xs text-slate-500 hover:text-rose-700">Clear all</button>
        </div>
        <p className="mt-1.5 text-[11px] text-slate-500">
          {state.dirty ? 'Unsaved changes: only you see them until you save.' : savedLine(state)}
        </p>
        </>)}
      </div>
    </div>
  );
}
