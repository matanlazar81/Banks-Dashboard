import { useMemo, useState } from 'react';
import { Download, Loader2, RefreshCw } from 'lucide-react';
import { useProjection } from '../new-dashboard/useProjection.ts';
import ProjectionTable, { type TableMarkers } from '../new-dashboard/ProjectionTable.tsx';
import { ComputingCard, ErrorCard, Segmented, Skeleton, Warnings } from '../new-dashboard/PageParts.tsx';
import { readParam, writeParam } from '../new-dashboard/urlParams.ts';
import { monthLongLabel } from '../new-dashboard/model.ts';
import type { YearView } from '../new-dashboard/types.ts';
import { fetchSalaryProjection } from './api.ts';
import { buildSalaryTable, computeSalaryKpis } from './model.ts';
import SalaryKpiStrip from './SalaryKpiStrip.tsx';
import type { Ccy } from './types.ts';

const MARKERS: TableMarkers = {
  anchorLine: null,
  rollForwardLine: '',
  closingLine: 'total',
  negativeLines: new Set(),
};

function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-600">
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-slate-300 bg-slate-100" />Actual (Snowflake)</span>
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-sky-300 bg-sky-50" />Current month (projected)</span>
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-slate-300 bg-white" />Forecast</span>
    </div>
  );
}

export default function SalaryProjection() {
  const { phase, data, error, computingElapsedSec, refreshing, refresh } = useProjection(fetchSalaryProjection);
  // Payroll is paid in shekels: ILS by default, ?ccy=eur for euros.
  const [ccy, setCcy] = useState<Ccy>(() => (readParam('ccy') === 'eur' ? 'eur' : 'ils'));
  const [view, setView] = useState<YearView>(() => {
    const v = readParam('years');
    return v === 'current' || v === 'next' ? v : 'both';
  });
  const [exporting, setExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);

  const table = useMemo(() => (data ? buildSalaryTable(data, ccy, view) : null), [data, ccy, view]);
  const kpis = useMemo(() => (data ? computeSalaryKpis(data, ccy) : null), [data, ccy]);

  const chooseCcy = (c: Ccy) => { setCcy(c); writeParam('ccy', c === 'eur' ? 'eur' : null); };
  const chooseView = (v: YearView) => { setView(v); writeParam('years', v === 'both' ? null : v); };

  const runRate = data ? monthLongLabel(data.basis.runRateMonth) : null;
  const method = runRate ? `Permanent: ${runRate} run-rate · Temporary: same month last year` : '';

  async function onExport() {
    if (!data || !table) return;
    setExporting(true);
    setExportError(null);
    try {
      const { exportTableXlsx } = await import('../new-dashboard/exportXlsx.ts');
      await exportTableXlsx({ what: 'salary projection', years: data.years, generatedAt: data.generatedAt, table, subtitle: method, ccy });
    } catch (e) {
      setExportError(e instanceof Error ? e.message : 'The export failed.');
    } finally {
      setExporting(false);
    }
  }

  const [y0, y1] = data ? data.years : [null, null];
  const updated = data ? new Date(data.generatedAt).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }) : null;
  const updatedDay = data ? new Date(data.generatedAt).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' }) : null;
  const through = data && data.actuals.through ? monthLongLabel(data.actuals.through) : null;

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-6">
          <div>
            <h1 className="text-lg font-semibold text-slate-900">Salary Projection</h1>
            <p className="text-sm text-slate-500">LSports · payroll by GL account{y0 ? ` ${y0}–${y1}` : ''}, permanent and temporary</p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Segmented label="Currency" value={ccy} onChange={chooseCcy} options={[{ value: 'ils', label: 'ILS' }, { value: 'eur', label: 'EUR' }]} />
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
              title="Recalculate from Snowflake"
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
              <span>{through ? `Actuals: Snowflake payroll through ${through}` : 'No closed month yet this year'}</span>
              <span aria-hidden="true">·</span>
              <span>{method}</span>
              <span aria-hidden="true">·</span>
              <span title="Forecast euros are the shekel forecast divided by this rate">
                Forecast €: ₪{data.basis.ilsPerEur.toFixed(2)} per € ({runRate} payroll)
              </span>
              {error && <span className="text-amber-700">Last update failed: {error} Showing the previous figures.</span>}
              {exportError && <span className="text-rose-700">Export failed: {exportError}</span>}
            </div>

            <Warnings warnings={data.warnings} />

            <SalaryKpiStrip kpis={kpis} ccy={ccy} />

            <section className="rounded-lg border border-slate-200 bg-white">
              <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-2.5">
                <div className="flex items-baseline gap-2">
                  <h2 className="text-sm font-semibold text-slate-800">Payroll by GL account and month</h2>
                  <span className="text-xs text-slate-500">Hover an account for how it is projected.</span>
                </div>
                <Legend />
              </div>
              <ProjectionTable table={table} ccy={ccy} anchorDate={null} markers={MARKERS} />
            </section>

            <p className="max-w-5xl text-xs leading-relaxed text-slate-500">
              Actual months are Snowflake's payroll accounts (76xxxx) for LSports, as booked in NetSuite. Permanent
              accounts (salaries and their burden: national insurance, pension, severance, education fund and the
              like) carry the last closed month, {runRate}, into every forecast month. Temporary accounts (bonuses,
              recuperation, military reserve, notice pay and other one-offs) repeat the same month of the previous
              year, so seasonal payments stay in their month: {y0} forecast months repeat {y0 ? y0 - 1 : ''}, and {y1} repeats {y0}.
              The current month is projected as a whole month. Forecasts are made in shekels; euros use the rate above.
            </p>
          </>
        )}
      </main>
    </div>
  );
}
