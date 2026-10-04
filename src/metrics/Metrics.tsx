// Business Tools → Metrics: one short pack, the same shape every month, computed live from the dashboards'
// data (GET /api/metrics). Every figure says whether it is actual (A), forecast (F) or both (A+F).
import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import { CheckCircle2, Download, Loader2, Plus, RefreshCw, Trash2 } from 'lucide-react';
import { ComputingCard, ErrorCard, Skeleton, Warnings } from '../new-dashboard/PageParts.tsx';
import { fetchDeposits, fetchMetrics, saveDeposits, saveSettings } from './api.ts';
import { formatCell, formatEur, formatEurFull, formatPct, monthName, STATUS_LONG, STATUS_SHORT } from './model.ts';
import PeopleCharts from './PeopleCharts.tsx';
import type { Cell, Deposit, Metric, MetricsPayload, MetricsSettings } from './types.ts';

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

function PackCell({ cell, unit }: { cell: Cell | null; unit: Metric['unit'] }) {
  if (!cell || cell.value === null) return <td className="px-3 py-2 text-right text-slate-400">–</td>;
  const negative = unit === 'eur' && cell.value < 0;
  return (
    <td className="px-3 py-2 text-right align-top">
      <span className={`whitespace-nowrap font-semibold tabular-nums ${negative ? 'text-rose-700' : 'text-slate-900'}`}
        title={unit === 'eur' ? formatEurFull(cell.value) : undefined}>
        {formatCell(cell, unit)}
      </span>
      <StatusChip status={cell.status} />
      <div className="whitespace-nowrap text-[11px] text-slate-500">{cell.label}</div>
    </td>
  );
}

function PackTable({ data }: { data: MetricsPayload }) {
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
              <PackCell cell={m.lastMonth} unit={m.unit} />
              <PackCell cell={m.ytd} unit={m.unit} />
              <PackCell cell={m.fy[0]} unit={m.unit} />
              <PackCell cell={m.fy[1]} unit={m.unit} />
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

            <Card title="The pack">
              <PackTable data={data} />
            </Card>

            {data.people && (
              <Card title="Payroll / revenue and revenue per employee">
                <PeopleCharts people={data.people} />
              </Card>
            )}

            <div className="grid gap-4 lg:grid-cols-2">
              <CloudCard data={data} saving={saving} onSave={save} />
              <RatesCard data={data} saving={saving} onSave={save} />
            </div>

            <FxCard data={data} />

            <DepositsCard data={data} onSaved={reload} />

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

function RatesCard({ data, saving, onSave }: { data: MetricsPayload; saving: string | null; onSave: SaveFn }) {
  const [rate, setRate] = useState(data.rates.usdEurPlanning == null ? '' : String(data.rates.usdEurPlanning));
  const live = data.rates.usdEurLive;
  const plan = data.rates.usdEurPlanning;
  const diff = plan && live ? ((live.rate - plan) / plan) * 100 : null;
  const dirty = (rate === '' ? null : num(rate)) !== plan;
  return (
    <Card title="USD/EUR rate">
      <div className="grid grid-cols-2 gap-3 text-sm">
        <div>
          <div className="text-xs uppercase tracking-wide text-slate-500">Planning rate</div>
          <div className="mt-0.5 text-xl font-semibold tabular-nums text-slate-900">{plan ? plan.toFixed(4) : '–'}</div>
          <div className="text-[11px] text-slate-500">USD per €</div>
        </div>
        <div>
          <div className="text-xs uppercase tracking-wide text-slate-500">ECB today</div>
          <div className="mt-0.5 text-xl font-semibold tabular-nums text-slate-900">{live ? live.rate.toFixed(4) : '–'}</div>
          <div className="text-[11px] text-slate-500">
            {live ? `${live.date} · ${live.source}` : 'unavailable'}
            {diff !== null && <span className={diff > 0 ? ' text-emerald-700' : ' text-rose-700'}> · {diff > 0 ? '+' : ''}{diff.toFixed(1)}% vs plan</span>}
          </div>
        </div>
      </div>
      <div className="mt-3 flex items-center gap-2 border-t border-slate-100 pt-2 text-xs text-slate-600">
        <label className="inline-flex items-center gap-1">Planning rate
          <input aria-label="USD/EUR planning rate" type="number" step={0.0001} value={rate} placeholder="e.g. 1.15"
            onChange={(e: { target: { value: string } }) => setRate(e.target.value)} className={`${INPUT} w-24 text-right`} />
        </label>
        <button type="button" disabled={!dirty || saving !== null} className={BUTTON}
          onClick={() => void onSave('rate', { usdEurPlanningRate: rate === '' ? null : num(rate) })}>
          {saving === 'rate' && <Loader2 size={12} className="animate-spin" />}Save
        </button>
      </div>
    </Card>
  );
}

function FxCard({ data }: { data: MetricsPayload }) {
  const { fx } = data;
  return (
    <Card title={`FX conversions, ${monthName(fx.month)}`}>
      {!fx.items.length ? <p className="text-sm text-slate-500">No currency conversions between bank accounts in {monthName(fx.month)}.</p> : (
        <>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-slate-500"><th className="text-left font-medium">Date</th><th className="text-left font-medium">From → to</th><th className="text-right font-medium">Amount</th><th className="text-right font-medium">€</th><th className="text-right font-medium">Rate</th></tr>
            </thead>
            <tbody>
              {fx.items.map((c) => (
                <tr key={`${c.tranid}-${c.date}`} className="border-t border-slate-100" title={c.from && c.to ? `${c.tranid}: ${c.from} → ${c.to}` : c.tranid}>
                  <td className="py-1 tabular-nums">{c.date}</td>
                  <td>{c.fromCurrency} → {c.toCurrency}</td>
                  <td className="text-right tabular-nums">{c.currency} {Math.round(c.amount).toLocaleString('en-US')}</td>
                  <td className="text-right tabular-nums">{formatEurFull(c.eur)}</td>
                  <td className="text-right tabular-nums">{c.rate ? `${c.rate.toFixed(4)} ${c.currency}/€` : '–'}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="mt-2 space-y-0.5 border-t border-slate-200 pt-1.5 text-xs font-medium text-slate-700">
            {fx.totals.map((t) => (
              <div key={`${t.pair}-${t.currency}`} className="flex justify-between gap-2">
                <span>{t.pair} ({t.count})</span>
                <span className="tabular-nums">{t.currency} {Math.round(t.amount).toLocaleString('en-US')} = {formatEurFull(t.eur)}{t.rate ? ` at ${t.rate.toFixed(4)}` : ''}</span>
              </div>
            ))}
          </div>
        </>
      )}
      <p className="mt-2 text-[11px] text-slate-500">NetSuite transfers between bank accounts in different currencies, by transaction date.</p>
    </Card>
  );
}

const today = () => new Date().toISOString().slice(0, 10);
const newId = () => `d${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

function DepositsCard({ data, onSaved }: { data: MetricsPayload; onSaved: () => Promise<void> }) {
  const [all, setAll] = useState<Deposit[] | null>(null);
  const [editing, setEditing] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [draft, setDraft] = useState<Deposit>({ id: '', bank: '', amount: 0, currency: 'EUR', placedOn: today(), maturity: null, confirmed: false, confirmedOn: null, note: '' });

  const open = async () => {
    setErr(null);
    try { setAll(await fetchDeposits()); setEditing(true); } catch (e) { setErr(e instanceof Error ? e.message : String(e)); }
  };
  const persist = async (next: Deposit[]) => {
    setBusy(true);
    setErr(null);
    try {
      const saved = await saveDeposits(next);
      setAll(saved.value);
      await onSaved();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  const confirm = async (id: string) => {
    const list = all || (await fetchDeposits().catch(() => null));
    if (!list) { setErr('The deposit tracker could not be loaded.'); return; }
    await persist(list.map((d) => (d.id === id ? { ...d, confirmed: true, confirmedOn: today() } : d)));
  };

  const shown = editing && all ? all : data.deposits.open;
  return (
    <Card
      title={`Deposits awaiting confirmation: ${data.deposits.openCount}`}
      aside={<button type="button" className={BUTTON} onClick={() => (editing ? setEditing(false) : void open())}>{editing ? 'Done' : `Manage (${data.deposits.total})`}</button>}
    >
      {err && <p className="mb-2 text-xs text-rose-700">{err}</p>}
      {!shown.length && !editing ? <p className="text-sm text-slate-500">Every deposit logged has its confirmation.</p> : (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-slate-500">
              <th className="text-left font-medium">Bank</th><th className="text-right font-medium">Amount</th><th className="text-left font-medium">Placed</th>
              <th className="text-left font-medium">Maturity</th><th className="text-left font-medium">Note</th><th className="text-right font-medium" />
            </tr>
          </thead>
          <tbody>
            {shown.map((d) => (
              <tr key={d.id} className="border-t border-slate-100">
                <td className="py-1 font-medium text-slate-800">{d.bank}</td>
                <td className="text-right tabular-nums">{d.currency} {Math.round(d.amount).toLocaleString('en-US')}</td>
                <td className="tabular-nums">{d.placedOn}</td>
                <td className="tabular-nums">{d.maturity || '–'}</td>
                <td className="text-slate-500">{d.note}</td>
                <td className="whitespace-nowrap py-1 text-right">
                  {d.confirmed
                    ? <span className="inline-flex items-center gap-1 text-emerald-700"><CheckCircle2 size={12} />{d.confirmedOn || 'confirmed'}</span>
                    : <button type="button" disabled={busy} className={BUTTON} onClick={() => void confirm(d.id)}>Confirmation received</button>}
                  {editing && all && (
                    <button type="button" aria-label="Remove" disabled={busy} className="ml-1 p-1 text-slate-400 hover:text-rose-700"
                      onClick={() => void persist(all.filter((x) => x.id !== d.id))}><Trash2 size={12} /></button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {editing && all && (
        <div className="mt-3 flex flex-wrap items-end gap-2 border-t border-slate-100 pt-2 text-xs text-slate-600">
          <input aria-label="Bank" placeholder="Bank" value={draft.bank} onChange={(e: { target: { value: string } }) => setDraft({ ...draft, bank: e.target.value })} className={`${INPUT} w-32`} />
          <input aria-label="Amount" type="number" step={1000} value={draft.amount || ''} placeholder="Amount" onChange={(e: { target: { value: string } }) => setDraft({ ...draft, amount: num(e.target.value) })} className={`${INPUT} w-28 text-right`} />
          <select aria-label="Currency" value={draft.currency} onChange={(e: { target: { value: string } }) => setDraft({ ...draft, currency: e.target.value })} className={INPUT}>
            {['EUR', 'USD', 'ILS', 'GBP', 'PLN', 'CHF'].map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <label className="inline-flex flex-col">Placed<input type="date" value={draft.placedOn} onChange={(e: { target: { value: string } }) => setDraft({ ...draft, placedOn: e.target.value })} className={INPUT} /></label>
          <label className="inline-flex flex-col">Maturity<input type="date" value={draft.maturity || ''} onChange={(e: { target: { value: string } }) => setDraft({ ...draft, maturity: e.target.value || null })} className={INPUT} /></label>
          <input aria-label="Note" placeholder="Note" value={draft.note} onChange={(e: { target: { value: string } }) => setDraft({ ...draft, note: e.target.value })} className={`${INPUT} w-40`} />
          <button type="button" disabled={busy || !draft.bank.trim() || !(draft.amount > 0)} className={BUTTON}
            onClick={() => { void persist([...all, { ...draft, id: newId(), bank: draft.bank.trim() }]); setDraft({ ...draft, id: '', bank: '', amount: 0, note: '' }); }}>
            {busy ? <Loader2 size={12} className="animate-spin" /> : <Plus size={12} />}Add deposit
          </button>
        </div>
      )}
    </Card>
  );
}
