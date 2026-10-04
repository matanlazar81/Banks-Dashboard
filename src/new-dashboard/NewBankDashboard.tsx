import { useCallback, useMemo, useState } from 'react';
import { Download, Loader2, RefreshCw } from 'lucide-react';
import { useProjection } from './useProjection.ts';
import { fetchProjection } from './api.ts';
import { buildTable, computeKpis, type Column } from './model.ts';
import KpiStrip from './KpiStrip.tsx';
import ProjectionTable from './ProjectionTable.tsx';
import BreakdownWindows from './BreakdownWindows.tsx';
import type { Ccy, VariantKey, YearView } from './types.ts';
import { ComputingCard, ErrorCard, Segmented, Skeleton, Warnings } from './PageParts.tsx';
import { readParam, writeParam } from './urlParams.ts';

function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-600">
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-slate-300 bg-slate-100" />Actual</span>
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-sky-300 bg-sky-50" />Current month</span>
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-slate-300 bg-white" />Forecast</span>
      <span><span className="text-sky-600">⚓</span> opens at the NetSuite bank balance</span>
      <span><span className="text-emerald-600">↩</span> rolled forward from December</span>
    </div>
  );
}

export default function NewBankDashboard() {
  const { phase, data, error, computingElapsedSec, refreshing, refresh } = useProjection(fetchProjection);
  const [variant, setVariant] = useState<VariantKey>(() => (readParam('plan') === 'base' ? 'base' : 'plan'));
  const [ccy, setCcy] = useState<Ccy>(() => (readParam('ccy') === 'ils' ? 'ils' : 'eur'));
  const [view, setView] = useState<YearView>(() => {
    const v = readParam('years');
    return v === 'current' || v === 'next' ? v : 'both';
  });
  const [exporting, setExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);
  // The breakdown windows: which cell is open (BreakdownWindows keeps where they were dragged to).
  const [openCell, setOpenCell] = useState<{ line: string; period: string; id: string } | null>(null);
  const onOpenCell = useCallback((line: string, col: Column) => {
    setOpenCell({ line, period: col.kind === 'fy' ? `FY-${col.year}` : (col.mKey as string), id: `${line}|${col.id}` });
  }, []);
  const closePanel = useCallback(() => setOpenCell(null), []);

  const table = useMemo(() => (data ? buildTable(data.variants[variant], ccy, view) : null), [data, variant, ccy, view]);
  const kpis = useMemo(() => (data ? computeKpis(data, variant, ccy) : null), [data, variant, ccy]);

  const chooseVariant = (v: VariantKey) => { setVariant(v); writeParam('plan', v === 'base' ? 'base' : null); };
  const chooseCcy = (c: Ccy) => { setCcy(c); writeParam('ccy', c === 'ils' ? 'ils' : null); };
  const chooseView = (v: YearView) => { setView(v); writeParam('years', v === 'both' ? null : v); };

  async function onExport() {
    if (!data || !table) return;
    setExporting(true);
    setExportError(null);
    try {
      const { exportProjectionXlsx } = await import('./exportXlsx.ts');
      await exportProjectionXlsx(data, table, variant, ccy);
    } catch (e) {
      setExportError(e instanceof Error ? e.message : 'The export failed.');
    } finally {
      setExporting(false);
    }
  }

  const [y0, y1] = data ? data.years : [null, null];
  const planTitle = data ? `Scenario "${data.plan.name}", the plan behind the official net-cash figure` : undefined;
  const updated = data ? new Date(data.generatedAt).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }) : null;
  const updatedDay = data ? new Date(data.generatedAt).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' }) : null;
  const warnings = data ? data.warnings : [];

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-6">
          <div>
            <h1 className="text-lg font-semibold text-slate-900">New Bank Dashboard</h1>
            <p className="text-sm text-slate-500">LSports · cash projection{y0 ? ` ${y0}–${y1}` : ''}</p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Segmented
              label="Forecast basis"
              value={variant}
              onChange={chooseVariant}
              options={[{ value: 'plan', label: 'Plan', title: planTitle }, { value: 'base', label: 'Base', title: 'Forecast without plan adjustments' }]}
            />
            <Segmented label="Currency" value={ccy} onChange={chooseCcy} options={[{ value: 'eur', label: 'EUR' }, { value: 'ils', label: 'ILS' }]} />
            {data && (
              <Segmented
                label="Years"
                value={view}
                onChange={chooseView}
                options={[{ value: 'current', label: String(y0) }, { value: 'next', label: String(y1) }, { value: 'both', label: 'Both' }]}
              />
            )}
            <button
              type="button"
              onClick={refresh}
              disabled={refreshing || phase === 'loading'}
              className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100 disabled:opacity-60"
              title="Recalculate from NetSuite and Snowflake"
            >
              <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
              {refreshing ? 'Updating…' : 'Refresh'}
            </button>
            <button
              type="button"
              onClick={onExport}
              disabled={!data || exporting}
              className="inline-flex items-center gap-1.5 rounded-md bg-slate-800 px-2.5 py-1.5 text-xs font-medium text-white hover:bg-slate-700 disabled:opacity-60"
              title="Download the table as shown (Excel)"
            >
              {exporting ? <Loader2 size={14} className="animate-spin" /> : <Download size={14} />}
              Export
            </button>
          </div>
        </div>
      </header>

      <main className="space-y-4 px-4 py-4 sm:px-6">
        {phase === 'loading' && <Skeleton />}
        {phase === 'computing' && <ComputingCard elapsedSec={computingElapsedSec} />}
        {phase === 'error' && !data && <ErrorCard message={error} onRetry={refresh} />}

        {data && table && kpis && (
          <>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
              <span>Updated {updatedDay} {updated}</span>
              <span aria-hidden="true">·</span>
              <span>{variant === 'plan' ? `Plan: ${data.plan.name}` : 'Base: no plan adjustments'}</span>
              {error && <span className="text-amber-700">Last update failed: {error} Showing the previous figures.</span>}
              {exportError && <span className="text-rose-700">Export failed: {exportError}</span>}
            </div>

            <Warnings warnings={warnings} />

            <KpiStrip kpis={kpis} ccy={ccy} />

            <section className="rounded-lg border border-slate-200 bg-white">
              <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-2.5">
                <div className="flex items-baseline gap-2">
                  <h2 className="text-sm font-semibold text-slate-800">Cash projection by month</h2>
                  <span className="text-xs text-slate-500">Click an underlined figure for its breakdown.</span>
                </div>
                <Legend />
              </div>
              <ProjectionTable
                table={table}
                ccy={ccy}
                anchorDate={data.bankToday ? data.bankToday.asOf : null}
                onOpenCell={onOpenCell}
                activeCell={openCell ? openCell.id : null}
              />
            </section>

            <BreakdownWindows cell={openCell} variant={variant} ccy={ccy} onClose={closePanel} />

            <p className="max-w-5xl text-xs leading-relaxed text-slate-500">
              Same engine and inputs as the Bank Dashboard and the nightly net-cash figure: revenue from the pipeline
              methodology, salary from the last closed payroll month. Actual months come from NetSuite bank activity, and
              the current month opens at the NetSuite bank balance. {y1} rolls forward from the December {y0} closing:
              salary at the Oct–Dec {y0} run-rate, vendors mirrored month by month and collections at the Oct–Dec average.
              No new pipeline, churn or FX revaluation is projected for {y1}. Dividends paid are shown on their own line,
              so every balance is the cash in the bank.
            </p>
          </>
        )}
      </main>
    </div>
  );
}
