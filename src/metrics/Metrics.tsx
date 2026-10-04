// Business Tools → Metrics: one short pack, the same shape every month, computed live from the dashboards'
// data (GET /api/metrics). Every figure says whether it is actual (A), forecast (F) or both (A+F).
import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import { Download, Loader2, RefreshCw } from 'lucide-react';
import { ComputingCard, ErrorCard, Skeleton, Warnings } from '../new-dashboard/PageParts.tsx';
import { fetchMetrics, saveSettings } from './api.ts';
import { formatCell, formatEur, formatEurFull, formatPct, monthName, STATUS_LONG, STATUS_SHORT } from './model.ts';
import ExplainWindow from './ExplainWindow.tsx';
import PeopleCharts from './PeopleCharts.tsx';
import type { Cell, Metric, MetricsPayload, MetricsSettings } from './types.ts';

// ── loading ─────────────────────────────────────────────────────────────────
type Phase = 'loading' | 'computing' | 'ready' | 'error';

function useMetrics() {
  const [phase, setPhase] = useState<Phase>('loading');
  const [data, setData] = useState<MetricsPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const timer = useRef<number | null>(null);

  const load = useCallback(async (refresh: boolean) => {
    if (timer.current) { window.clearTimeout(timer.current); timer.current = null; }
    setBusy(true);
    try {
      const body = await fetchMetrics(refresh);
      if (body.status === 'ready') {
        setData(body);
        setPhase('ready');
        setError(null);
        setElapsed(null);
      } else if (body.status === 'computing') {
        setPhase((p) => (p === 'ready' ? p : 'computing'));
        setElapsed(body.elapsedSec);
        timer.current = window.setTimeout(() => { void load(false); }, 5000);
      } else {
        setError(body.error);
        setPhase((p) => (p === 'ready' ? p : 'error'));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPhase((p) => (p === 'ready' ? p : 'error'));
    } finally {
      setBusy(false);
    }
  }, []);

  useEffect(() => {
    void load(false);
    return () => { if (timer.current) window.clearTimeout(timer.current); };
  }, [load]);

  return { phase, data, error, elapsed, busy, refresh: () => void load(true), reload: () => load(false) };
}

// ── small parts ─────────────────────────────────────────────────────────────
const CHIP: Record<string, string> = {
  actual: 'bg-slate-100 text-slate-700 border-slate-300',
  forecast: 'bg-white text-sky-700 border-sky-300',
  'actual+forecast': 'bg-sky-50 text-sky-800 border-sky-300',
};

function StatusChip({ status }: { status: Cell['status'] }) {
  return (
    <span title={STATUS_LONG[status]} className={`ml-1.5 inline-block rounded border px-1 text-[10px] font-semibold leading-4 ${CHIP[status]}`}>
      {STATUS_SHORT[status]}
    </span>
  );
}

function Card({ title, aside, children }: { title: string; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="rounded-lg border border-slate-200 bg-white">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-2.5">
        <h2 className="text-sm font-semibold text-slate-800">{title}</h2>
        {aside}
      </div>
      <div className="px-4 py-3">{children}</div>
    </section>
  );
}

type OnExplain = (item: string) => void;

function PackCell({ cell, unit, onExplain, active }: { cell: Cell | null; unit: Metric['unit']; onExplain: OnExplain; active: string | null }) {
  if (!cell || cell.value === null) return <td className="px-3 py-2 text-right text-slate-400">–</td>;
  const negative = unit === 'eur' && cell.value < 0;
  const value = `whitespace-nowrap font-semibold tabular-nums ${negative ? 'text-rose-700' : 'text-slate-900'}`;
  return (
    <td className={`px-3 py-2 text-right align-top ${cell.detail && active === cell.detail ? 'bg-sky-50' : ''}`}>
      {cell.detail ? (
        <button type="button" onClick={() => onExplain(cell.detail as string)} title="How it is calculated"
          className={`${value} underline decoration-slate-300 decoration-dotted underline-offset-4 hover:text-sky-800 hover:decoration-sky-500`}>
          {formatCell(cell, unit)}
        </button>
      ) : (
        <span className={value} title={unit === 'eur' ? formatEurFull(cell.value) : undefined}>{formatCell(cell, unit)}</span>
      )}
      <StatusChip status={cell.status} />
      <div className="whitespace-nowrap text-[11px] text-slate-500">{cell.label}</div>
    </td>
  );
}

function PackTable({ data, onExplain, active }: { data: MetricsPayload; onExplain: OnExplain; active: string | null }) {
  const [y0, y1] = data.years;
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[720px] text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-xs text-slate-500">
            <th className="px-3 py-2 text-left font-medium">Metric</th>
            <th className="px-3 py-2 text-right font-medium">Last month</th>
            <th className="px-3 py-2 text-right font-medium">Year to date</th>
            <th className="px-3 py-2 text-right font-medium">FY {y0}</th>
            <th className="px-3 py-2 text-right font-medium">FY {y1}</th>
          </tr>
        </thead>
        <tbody>
          {data.metrics.map((m) => (
            <tr key={m.key} className="border-b border-slate-100 last:border-0">
              <td className="px-3 py-2 align-top">
                <div className="font-semibold text-slate-900">{m.label}</div>
                <div className="text-[11px] text-slate-500">{m.note}</div>
              </td>
              <PackCell cell={m.lastMonth} unit={m.unit} onExplain={onExplain} active={active} />
              <PackCell cell={m.ytd} unit={m.unit} onExplain={onExplain} active={active} />
              <PackCell cell={m.fy[0]} unit={m.unit} onExplain={onExplain} active={active} />
              <PackCell cell={m.fy[1]} unit={m.unit} onExplain={onExplain} active={active} />
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Meter({ value, cap }: { value: number; cap: number }) {
  const pct = cap > 0 ? Math.min(100, (value / cap) * 100) : 100;
  const over = value > cap;
  return (
    <div className="h-2 w-full overflow-hidden rounded bg-slate-100" role="img" aria-label={`${Math.round((value / (cap || 1)) * 100)}% of the cap`}>
      <div className={`h-full ${over ? 'bg-rose-500' : pct > 85 ? 'bg-amber-500' : 'bg-emerald-500'}`} style={{ width: `${pct}%` }} />
    </div>
  );
}

// ── page ────────────────────────────────────────────────────────────────────
export default function Metrics() {
  const { phase, data, error, elapsed, busy, refresh, reload } = useMetrics();
  const [saving, setSaving] = useState<string | null>(null);
  const [explain, setExplain] = useState<string | null>(null);
  const closeExplain = useCallback(() => setExplain(null), []);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [exporting, setExporting] = useState(false);

  const save = useCallback(async (what: string, patch: Partial<MetricsSettings>) => {
    if (!data) return;
    setSaving(what);
    setSaveError(null);
    try {
      await saveSettings({ ...data.settings, ...patch });
      await reload();
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(null);
    }
  }, [data, reload]);

  async function onExport() {
    if (!data) return;
    setExporting(true);
    try {
      const { exportPackXlsx } = await import('./exportPack.ts');
      await exportPackXlsx(data);
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : 'The export failed.');
    } finally {
      setExporting(false);
    }
  }

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-6">
          <div>
            <h1 className="text-lg font-semibold text-slate-900">Metrics</h1>
            <p className="text-sm text-slate-500">
              LSports · monthly pack{data ? `, as of ${monthName(data.asOf.lastClosed)}` : ''} · A = actual, F = forecast
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button type="button" onClick={refresh} disabled={busy}
              className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100 disabled:opacity-60"
              title="Read ARR, NRR, churn, FX conversions and the ECB rate again">
              <RefreshCw size={14} className={busy ? 'animate-spin' : ''} />{busy ? 'Updating…' : 'Refresh'}
            </button>
            <button type="button" onClick={onExport} disabled={!data || exporting}
              className="inline-flex items-center gap-1.5 rounded-md bg-slate-800 px-2.5 py-1.5 text-xs font-medium text-white hover:bg-slate-700 disabled:opacity-60"
              title="Download the pack (Excel), same shape every month">
              {exporting ? <Loader2 size={14} className="animate-spin" /> : <Download size={14} />}Export
            </button>
          </div>
        </div>
      </header>

      <main className="space-y-4 px-4 py-4 sm:px-6">
        {phase === 'loading' && <Skeleton />}
        {phase === 'computing' && <ComputingCard elapsedSec={elapsed} />}
        {phase === 'error' && !data && <ErrorCard message={error} onRetry={refresh} />}

        {data && (
          <>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
              <span>Updated {new Date(data.generatedAt).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })}</span>
              <span aria-hidden="true">·</span>
              <span>Plan: {data.asOf.plan || '–'}</span>
              {data.targets && data.targets.active && (
                <>
                  <span aria-hidden="true">·</span>
                  <span className="cursor-help text-sky-800 underline decoration-dotted underline-offset-2" title={data.targets.assumptions.join('\n')}>
                    FY {data.targets.year} includes the {data.targets.year} targets set on the New Bank Dashboard
                    {data.targets.updatedAt ? ` (saved ${new Date(data.targets.updatedAt).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}${data.targets.updatedBy ? ` by ${data.targets.updatedBy}` : ''})` : ''}
                  </span>
                </>
              )}
              {data.refreshing && <span className="text-sky-700">The projections are being refreshed in the background.</span>}
              {error && <span className="text-amber-700">Last update failed: {error}</span>}
              {saveError && <span className="text-rose-700">{saveError}</span>}
            </div>
            <Warnings warnings={data.warnings} />

            <Card title="The pack" aside={<span className="text-[11px] text-slate-500">Click an underlined figure to see how it is calculated.</span>}>
              <PackTable data={data} onExplain={setExplain} active={explain} />
            </Card>
            {explain && <ExplainWindow item={explain} onClose={closeExplain} />}

            {data.people && (
              <Card title="Payroll / revenue and revenue per employee">
                <PeopleCharts people={data.people} />
              </Card>
            )}

            <CloudCard data={data} saving={saving} onSave={save} />

            <p className="max-w-5xl text-xs leading-relaxed text-slate-500">
              Revenue and EBITDA come from the P&amp;L Projection (NetSuite for closed months), net cash from the New Bank
              Dashboard (cash in the bank; there is no debt). ARR is MRR × 12 from Snowflake; its forecast is December
              customer revenue × 12. NRR compares what last year&apos;s customers pay now with what they paid 12 months
              earlier (GRR caps each at its old amount). Cloud is NetSuite 640xxx in closed months and the budget&apos;s
              cloud share of operating expenses after. All figures follow the Plan; the projection year adds the targets
              saved on the New Bank Dashboard (the same figures as both pages&apos; Targets view).
            </p>
          </>
        )}
      </main>
    </div>
  );
}

type SaveFn = (what: string, patch: Partial<MetricsSettings>) => Promise<void>;
const num = (s: string) => { const n = parseFloat(s); return Number.isFinite(n) ? n : 0; };
const INPUT = 'rounded border border-slate-300 px-1.5 py-1 text-xs tabular-nums focus:border-sky-500 focus:outline-none';
const BUTTON = 'inline-flex items-center gap-1 rounded-md border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 disabled:opacity-50';

function CloudCard({ data, saving, onSave }: { data: MetricsPayload; saving: string | null; onSave: SaveFn }) {
  const [cap, setCap] = useState(String(data.cloud.capPct));
  const [cat, setCat] = useState(data.settings.cloudCategory || data.cloud.category);
  const dirty = num(cap) !== data.cloud.capPct || cat !== (data.settings.cloudCategory || data.cloud.category);
  return (
    <Card title={`Cloud against the cap (${formatPct(data.cloud.capPct, 1)} of projected revenue)`}>
      <div className="space-y-3">
        {data.cloud.years.map((c) => (
          <div key={c.year}>
            <div className="mb-1 flex flex-wrap items-baseline justify-between gap-2 text-sm">
              <span className="font-semibold text-slate-900">FY {c.year}<StatusChip status={c.status} /></span>
              <span className={`text-xs font-semibold ${c.within ? 'text-emerald-700' : 'text-rose-700'}`}>
                {c.within ? `Within the cap · ${formatEur(c.headroom)} headroom` : `Over the cap by ${formatEur(-c.headroom)}`}
              </span>
            </div>
            <Meter value={c.total} cap={c.cap} />
            <div className="mt-1 flex flex-wrap justify-between gap-2 text-xs text-slate-500">
              <span>Cloud {formatEur(c.total)} = {formatPct(c.pctOfRevenue, 2)} of revenue {formatEur(c.revenue)}</span>
              <span>Cap {formatEur(c.cap)}</span>
            </div>
            {c.targets && (
              <p className="mt-0.5 text-[11px] text-sky-800">
                {c.targets.kind === 'server'
                  ? `Server costs set at ${formatPct(c.targets.pct, 1)} of customer revenue in the ${c.year} targets (the cap is on total revenue, so the share shown can be a little lower).`
                  : `The ${c.year} targets change this category by ${c.targets.pct > 0 ? '+' : ''}${formatPct(c.targets.pct, 1)}.`}
              </p>
            )}
          </div>
        ))}
        <div className="flex flex-wrap items-center gap-2 border-t border-slate-100 pt-2 text-xs text-slate-600">
          <label className="inline-flex items-center gap-1">Cap
            <input aria-label="Cap % of revenue" type="number" step={0.5} value={cap} onChange={(e: { target: { value: string } }) => setCap(e.target.value)} className={`${INPUT} w-16 text-right`} />%
          </label>
          <select aria-label="Cloud category" value={cat} onChange={(e: { target: { value: string } }) => setCat(e.target.value)} className={`${INPUT} min-w-0 flex-1`}>
            {(data.cloud.categories.length ? data.cloud.categories : [cat]).map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <button type="button" disabled={!dirty || saving !== null} className={BUTTON}
            onClick={() => void onSave('cloud', { cloudCapPct: num(cap), cloudCategory: cat })}>
            {saving === 'cloud' && <Loader2 size={12} className="animate-spin" />}Save
          </button>
        </div>
        <p className="text-[11px] text-slate-500">Closed months: NetSuite {data.cloud.accounts}. Forecast months: the budget&apos;s share of this category in operating expenses.</p>
      </div>
    </Card>
  );
}
