import { useCallback, useMemo, useState } from 'react';
import { Download, Loader2, RefreshCw, SlidersHorizontal } from 'lucide-react';
import { applyCashTargets, variantWithTargets } from '../forecast/targets.mjs';
import { useProjection } from './useProjection.ts';
import { fetchProjection } from './api.ts';
import { buildTable, computeKpis, type Column } from './model.ts';
import KpiStrip from './KpiStrip.tsx';
import ProjectionTable from './ProjectionTable.tsx';
import BreakdownWindows from './BreakdownWindows.tsx';
import type { Ccy, ProjectionPayload, ViewKey, YearView } from './types.ts';
import { cellDelta, useTargets } from './targets.ts';
import TargetsDrawer, { type ImpactLine } from './TargetsDrawer.tsx';
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
  const [variant, setVariant] = useState<ViewKey>(() => {
    const p = readParam('plan');
    return p === 'base' || p === 'targets' ? p : 'plan';
  });
  const [targetsOpen, setTargetsOpen] = useState(false);
  const targets = useTargets(data ? data.targetsBase : null);
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

  // The Targets view is the Plan with the projection-year targets (saved or being edited) applied.
  const base = data && data.targetsBase ? data.targetsBase : null;
  const targetsVariant = useMemo(
    () => (data && base ? variantWithTargets(data.variants.plan, base, targets.draft, applyCashTargets) : null),
    [data, base, targets.draft],
  );
  const showTargets = variant === 'targets' && !!targetsVariant;
  const key = variant === 'base' ? 'base' : 'plan';
  const shown: ProjectionPayload | null = useMemo(
    () => (data && showTargets && targetsVariant ? { ...data, variants: { ...data.variants, plan: targetsVariant } } : data),
    [data, showTargets, targetsVariant],
  );
  const table = useMemo(() => (shown ? buildTable(shown.variants[key], ccy, view) : null), [shown, key, ccy, view]);
  const kpis = useMemo(() => (shown ? computeKpis(shown, key, ccy) : null), [shown, key, ccy]);
  const extraFor = useCallback((line: string, period: string) => {
    if (!data || !showTargets || !targetsVariant || !base) return null;
    return { label: `${base.year} targets`, hint: 'What the targets change in this cell, on top of the Plan.', amount: cellDelta(data.variants.plan, targetsVariant, line, period, ccy) };
  }, [data, showTargets, targetsVariant, base, ccy]);
  const impact: ImpactLine[] = useMemo(() => {
    if (!data || !base || !targetsVariant) return [];
    const plan = data.variants.plan.years.find((y) => y.year === base.year);
    const tgt = targetsVariant.years.find((y) => y.year === base.year);
    if (!plan || !tgt) return [];
    const sum = (rows: typeof plan.rows, k: 'collections' | 'salary' | 'vendors') => rows.reduce((s, r) => s + r[ccy][k], 0);
    return [
      { label: `Cash, Dec ${base.year}`, plan: plan.rows[11][ccy].closing, withTargets: tgt.rows[11][ccy].closing },
      { label: 'Collections', plan: sum(plan.rows, 'collections'), withTargets: sum(tgt.rows, 'collections') },
      { label: 'Salary', plan: sum(plan.rows, 'salary'), withTargets: sum(tgt.rows, 'salary') },
      { label: 'Vendors', plan: sum(plan.rows, 'vendors'), withTargets: sum(tgt.rows, 'vendors') },
    ];
  }, [data, base, targetsVariant, ccy]);

  const chooseVariant = (v: ViewKey) => { setVariant(v); writeParam('plan', v === 'plan' ? null : v); };
  const openTargets = () => { setTargetsOpen(true); chooseVariant('targets'); };
  const chooseCcy = (c: Ccy) => { setCcy(c); writeParam('ccy', c === 'ils' ? 'ils' : null); };
  const chooseView = (v: YearView) => { setView(v); writeParam('years', v === 'both' ? null : v); };

  async function onExport() {
    if (!data || !table) return;
    setExporting(true);
    setExportError(null);
    try {
      const { exportProjectionXlsx } = await import('./exportXlsx.ts');
      const named = showTargets && base ? { ...data, plan: { ...data.plan, name: `${data.plan.name} with the ${base.year} targets` } } : data;
      await exportProjectionXlsx(named, table, key, ccy);
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
              options={[
                { value: 'plan', label: 'Plan', title: planTitle },
                { value: 'base', label: 'Base', title: 'Forecast without plan adjustments' },
                ...(base ? [{ value: 'targets' as const, label: `Targets ${base.year}`, title: `The Plan with the ${base.year} targets` }] : []),
              ]}
            />
            {base && (
              <button
                type="button"
                onClick={openTargets}
                className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100"
                title={`Set the ${base.year} targets and see their effect`}
              >
                <SlidersHorizontal size={14} />
                {base.year} targets{targets.dirty ? ' •' : ''}
              </button>
            )}
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
              <span>
                {showTargets && base
                  ? `Targets ${base.year}: plan "${data.plan.name}" with the ${base.year} targets${targets.dirty ? ' (unsaved changes)' : ''}`
                  : variant === 'base' ? 'Base: no plan adjustments' : `Plan: ${data.plan.name}`}
              </span>
              {variant === 'targets' && !base && <span className="text-amber-700">Targets need the projection to be refreshed once.</span>}
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

            <BreakdownWindows cell={openCell} variant={key} ccy={ccy} onClose={closePanel} extraFor={extraFor} />

            {targetsOpen && base && (
              <TargetsDrawer
                base={base}
                state={targets}
                impact={impact}
                ccy={ccy}
                revenueNote={`Revenue here is the expected revenue before the plan's collection %; collections follow it at that %.`}
                onClose={() => setTargetsOpen(false)}
              />
            )}

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
