import { useCallback, useMemo, useState } from 'react';
import { Download, Loader2, RefreshCw, SlidersHorizontal } from 'lucide-react';
import { useProjection } from '../new-dashboard/useProjection.ts';
import BreakdownWindows from '../new-dashboard/BreakdownWindows.tsx';
import type { BreakdownFetcher, BreakdownResponse } from '../new-dashboard/breakdown.ts';
import ProjectionTable, { type TableMarkers } from '../new-dashboard/ProjectionTable.tsx';
import { ComputingCard, ErrorCard, Segmented, Skeleton, Warnings } from '../new-dashboard/PageParts.tsx';
import { readParam, writeParam } from '../new-dashboard/urlParams.ts';
import { monthLongLabel, type Column } from '../new-dashboard/model.ts';
import type { YearView } from '../new-dashboard/types.ts';
import { fetchSalaryProjection } from './api.ts';
import { isSalaryBreakdownLine, salaryBreakdown } from './breakdown.ts';
import { useClassification } from './classification.ts';
import ClassifyPanel from './ClassifyPanel.tsx';
import HeadcountSection from './HeadcountSection.tsx';
import { buildHeadcountTable, buildSalaryTable, computeSalaryKpis, withClassification } from './model.ts';
import OverridesSection from './OverridesSection.tsx';
import SalaryKpiStrip from './SalaryKpiStrip.tsx';
import type { Ccy } from './types.ts';

const MARKERS: TableMarkers = {
  anchorLine: null,
  rollForwardLine: '',
  closingLine: 'total',
  negativeLines: new Set(),
};

function Legend({ overrides }: { overrides: boolean }) {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-600">
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-slate-300 bg-slate-100" />Actual (NetSuite)</span>
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-sky-300 bg-sky-50" />Current month (projected)</span>
      <span className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded-sm border border-slate-300 bg-white" />Forecast</span>
      {overrides && <span className="italic text-slate-500">Italic: the override file's overrides, not in the totals</span>}
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
  const [classifyOpen, setClassifyOpen] = useState(false);
  const classification = useClassification();

  // The payload re-projected with the classification the page shows (saved choices plus unsaved edits).
  const shown = useMemo(() => (data ? withClassification(data, classification.current) : null), [data, classification.current]);
  const table = useMemo(() => (shown ? buildSalaryTable(shown, ccy, view) : null), [shown, ccy, view]);
  const headcountTable = useMemo(() => (shown ? buildHeadcountTable(shown, view) : null), [shown, view]);
  const kpis = useMemo(() => (shown ? computeSalaryKpis(shown, ccy) : null), [shown, ccy]);
  // Planned-change cells open their breakdown, built here from the payload (no server call).
  const [openCell, setOpenCell] = useState<{ line: string; period: string; id: string } | null>(null);
  const onOpenCell = useCallback((line: string, col: Column) => {
    setOpenCell({ line, period: col.kind === 'fy' ? `FY-${col.year}` : (col.mKey as string), id: `${line}|${col.id}` });
  }, []);
  const closeBreakdown = useCallback(() => setOpenCell(null), []);
  const breakdownLines = useMemo(() => new Set(table ? table.lines.map((l) => l.key).filter(isSalaryBreakdownLine) : []), [table]);
  const breakdownOf = useCallback<BreakdownFetcher>((req) => Promise.resolve<BreakdownResponse>(
    shown ? salaryBreakdown(shown, req) : { ok: false, status: 'error', error: 'The projection is not loaded yet.' },
  ), [shown]);

  const hasLine = (key: string) => !!table && table.lines.some((l) => l.key === key);
  const hasChanges = hasLine('total-changes');
  const hasIncrements = hasLine('total-increments');
  const hasOverrides = hasLine('total-overrides');
  const hasDeel = hasLine('total-deel');
  // What Total payroll adds from the current month on.
  const added = [hasChanges && 'hires and leavers', hasIncrements && 'override increments'].filter(Boolean).join(' and ');

  const chooseCcy = (c: Ccy) => { setCcy(c); writeParam('ccy', c === 'eur' ? 'eur' : null); };
  const chooseView = (v: YearView) => { setView(v); writeParam('years', v === 'both' ? null : v); };

  const runRate = data ? monthLongLabel(data.basis.runRateMonth) : null;
  const notProjected = shown ? shown.accounts.filter((a) => a.method === 'none') : [];
  const unclassified = shown ? shown.accounts.filter((a) => a.group === 'other') : [];
  const canClassify = !!(data && data.actual);
  const method = runRate
    ? `Permanent: ${runRate} run-rate · Temporary: same month last year for ${data ? data.years[0] : ''}, 0 in ${data ? data.years[1] : ''}${notProjected.length ? ` · Not projected: ${notProjected.map((a) => a.acct).join(', ')}` : ''}${added ? ` · ${added.charAt(0).toUpperCase()}${added.slice(1)} added from the current month` : ''}${hasDeel ? ' · Deel: average of its last months' : ''}`
    : '';

  async function onExport() {
    if (!data || !table) return;
    setExporting(true);
    setExportError(null);
    try {
      const { exportTableXlsx } = await import('../new-dashboard/exportXlsx.ts');
      // The headcount goes under the payroll (the same columns), as people.
      const sheet = headcountTable
        ? { ...table, lines: [...table.lines, { key: 'header-headcount', label: 'Headcount (HiBob), people', kind: 'header' as const, values: [] }, ...headcountTable.lines] }
        : table;
      await exportTableXlsx({ what: 'salary projection', years: data.years, generatedAt: data.generatedAt, table: sheet, subtitle: method, ccy });
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
            <p className="text-sm text-slate-500">LSports · payroll by GL account{y0 ? ` ${y0}–${y1}` : ''}, permanent and temporary, with Deel and the headcount</p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {canClassify && (
              <button
                type="button"
                onClick={() => setClassifyOpen(true)}
                className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100"
                title="Choose which accounts are permanent, temporary or actuals only"
              >
                <SlidersHorizontal size={14} />
                Permanent / temporary
                {classification.dirty && <span className="h-1.5 w-1.5 rounded-full bg-amber-500" aria-label="unsaved changes" />}
              </button>
            )}
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
              title="Recalculate from NetSuite"
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

        {data && shown && table && kpis && (
          <>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
              <span>Updated {updatedDay} {updated}</span>
              <span aria-hidden="true">·</span>
              <span>{through ? `Actuals: NetSuite payroll through ${through}, by posting period` : 'No closed month yet this year'}</span>
              <span aria-hidden="true">·</span>
              <span>{method}</span>
              <span aria-hidden="true">·</span>
              <span>
                {classification.updatedAt
                  ? `Classification saved ${new Date(classification.updatedAt).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}${classification.updatedBy ? ` by ${classification.updatedBy}` : ''}`
                  : "Finance's default classification"}
              </span>
              <span aria-hidden="true">·</span>
              <span title="Forecast euros are the shekel forecast divided by this rate">
                Forecast €: ₪{data.basis.ilsPerEur.toFixed(2)} per € ({runRate} payroll)
              </span>
              {error && <span className="text-amber-700">Last update failed: {error} Showing the previous figures.</span>}
              {exportError && <span className="text-rose-700">Export failed: {exportError}</span>}
            </div>

            <Warnings warnings={data.warnings} />

            {(classification.dirty || unclassified.length > 0) && (
              <div className="flex flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border border-amber-200 bg-amber-50 px-4 py-2 text-xs text-amber-900">
                <span>
                  {classification.dirty
                    ? 'Unsaved classification: the table shows your changes. Save them to apply them for everyone.'
                    : `Not classified yet, projected like a temporary account: ${unclassified.map((a) => `${a.acct} ${a.name}`).join(', ')}.`}
                </span>
                <button type="button" onClick={() => setClassifyOpen(true)} className="font-medium underline underline-offset-2 hover:text-amber-700">
                  Permanent / temporary
                </button>
              </div>
            )}

            <SalaryKpiStrip kpis={kpis} ccy={ccy} />

            <section className="rounded-lg border border-slate-200 bg-white">
              <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-2.5">
                <div className="flex items-baseline gap-2">
                  <h2 className="text-sm font-semibold text-slate-800">Payroll by GL account and month</h2>
                  <span className="text-xs text-slate-500">
                    Hover an account for how it is projected{breakdownLines.size ? `; click a planned change${hasDeel ? ' or Deel' : ''} for its breakdown` : ''}.
                  </span>
                </div>
                <Legend overrides={hasOverrides} />
              </div>
              <ProjectionTable
                table={table}
                ccy={ccy}
                anchorDate={null}
                markers={MARKERS}
                onOpenCell={onOpenCell}
                activeCell={openCell ? openCell.id : null}
                breakdownLines={breakdownLines}
              />
            </section>

            {headcountTable && data.headcount && (
              <HeadcountSection table={headcountTable} headcount={data.headcount} computedMonth={data.computedMonth} ccy={ccy} />
            )}

            {data.overrides && <OverridesSection payload={data} ccy={ccy} />}

            <p className="max-w-5xl text-xs leading-relaxed text-slate-500">
              Actual months are NetSuite's payroll accounts (76xxxx) for LSports by posting period, as in NetSuite's
              Profit and Loss. Permanent accounts (salaries and their burden: national insurance, pension, severance,
              education fund and the like) carry the last closed month, {runRate}, into every forecast month. Temporary
              accounts (bonuses, recuperation, maternity and other one-offs) repeat the same month of the previous
              year for the rest of {y0}, so seasonal payments stay in their month; in {y1} they are 0.
              {(hasChanges || hasIncrements || hasOverrides) && ` Between the two groups come the planned changes: ${[
                hasChanges && 'planned hires, leavers and returns from leave (HiBob\'s forecast, through Snowflake\'s payroll forecast)',
                hasIncrements && 'the override file\'s increments',
              ].filter(Boolean).join(' and ')}${hasChanges || hasIncrements ? ' are added to Total payroll from the current month on (closed months are NetSuite\'s actuals, which already include what happened)' : ''}${hasOverrides ? `${hasChanges || hasIncrements ? '; ' : ''}the override file's overrides, in italics, are for reference and not in Total payroll` : ''}.`}
              {notProjected.length > 0 && ` Actuals only, not projected: ${notProjected.map((a) => `${a.acct} ${a.name}`).join(', ')}.`}
              {hasDeel && ' Below Total payroll, Deel (people abroad) is its NetSuite costs on every account but payroll ones; forecast months carry the average of its last three months with postings. Payroll + Deel adds the two.'}
              {headcountTable && ' The headcount is HiBob\'s: employees at each month-end, people on leave included. From the current month on it moves with HiBob\'s planned hires and leavers; returns from leave and leave starts (maternity and others) are shown but do not change it.'}
              {' '}The current month is projected as a whole month. Forecasts are made in shekels; euros use the rate above.
              Use Permanent / temporary to change how an account is projected.
            </p>

            {classifyOpen && runRate && (
              <ClassifyPanel accounts={shown.accounts} state={classification} runRate={runRate} onClose={() => setClassifyOpen(false)} />
            )}

            {openCell && (
              <BreakdownWindows cell={openCell} variant="base" ccy={ccy} fetcher={breakdownOf} onClose={closeBreakdown} />
            )}
          </>
        )}
      </main>
    </div>
  );
}
