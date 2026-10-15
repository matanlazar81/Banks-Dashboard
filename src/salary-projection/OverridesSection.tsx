// The override file's payroll rows (OVERIDE_TEMP, loaded into Snowflake as future-cost overrides and
// increments), under the projection for reference: by month in the projection's columns, then the rows
// themselves with departments and comments. They are not part of the projection above.
import { useMemo } from 'react';
import ProjectionTable, { type TableMarkers } from '../new-dashboard/ProjectionTable.tsx';
import { formatFull, monthLongLabel } from '../new-dashboard/model.ts';
import type { YearView } from '../new-dashboard/types.ts';
import { buildOverrideTable } from './model.ts';
import type { Ccy, PayrollOverride, SalaryPayload } from './types.ts';

const MARKERS: TableMarkers = {
  anchorLine: null,
  rollForwardLine: '',
  closingLine: 'total',
  negativeLines: new Set(),
};

const MODE_TITLE: Record<PayrollOverride['mode'], string> = {
  Override: 'Replaces the budget of this account and month',
  Increment: 'Adds to the budget of this account and month',
};

interface Props {
  payload: SalaryPayload;
  ccy: Ccy;
  view: YearView;
}

function RowList({ rows, ccy }: { rows: PayrollOverride[]; ccy: Ccy }) {
  const amountOf = (r: PayrollOverride) => (ccy === 'eur' ? r.eur : r.ils);
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[720px] border-separate border-spacing-0 text-[13px]">
        <thead>
          <tr className="text-left text-xs text-slate-500">
            <th className="border-b border-slate-200 px-4 py-2 font-medium">Month</th>
            <th className="border-b border-slate-200 px-2 py-2 font-medium">Account</th>
            <th className="border-b border-slate-200 px-2 py-2 font-medium">Department</th>
            <th className="border-b border-slate-200 px-2 py-2 font-medium">Type</th>
            <th className="border-b border-slate-200 px-2 py-2 text-right font-medium">Amount</th>
            <th className="border-b border-slate-200 px-4 py-2 font-medium">Comment</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={`${r.month}|${r.acct}|${r.department}|${r.mode}|${i}`} className="align-top">
              <td className="whitespace-nowrap border-b border-slate-100 px-4 py-1.5 text-slate-700">{monthLongLabel(r.month)}</td>
              <td className="border-b border-slate-100 px-2 py-1.5 text-slate-700">{r.acct}{r.name ? ` - ${r.name}` : ''}</td>
              <td className="border-b border-slate-100 px-2 py-1.5 text-slate-600">{r.department || '–'}</td>
              <td className="border-b border-slate-100 px-2 py-1.5">
                <span
                  title={MODE_TITLE[r.mode]}
                  className={`rounded px-1.5 py-px text-[11px] font-medium ${r.mode === 'Override' ? 'bg-amber-100 text-amber-800' : 'bg-sky-100 text-sky-800'}`}
                >
                  {r.mode}
                </span>
              </td>
              <td
                className="whitespace-nowrap border-b border-slate-100 px-2 py-1.5 text-right tabular-nums text-slate-800"
                title={ccy === 'ils' && r.ilsEstimated ? `${formatFull(r.eur, 'eur')} in the file, at the forecast rate` : undefined}
              >
                {ccy === 'ils' && r.ilsEstimated ? '≈ ' : ''}{formatFull(amountOf(r), ccy)}
              </td>
              <td className="border-b border-slate-100 px-4 py-1.5 text-xs text-slate-600">{r.memo || '–'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function OverridesSection({ payload, ccy, view }: Props) {
  const { rows, error } = payload.overrides ?? { rows: [], error: null };
  const year = payload.years[0];
  const lastYear = payload.years[1];
  const monthly = useMemo(() => buildOverrideTable(payload, ccy, view), [payload, ccy, view]);
  const counts = { Override: rows.filter((r) => r.mode === 'Override').length, Increment: rows.filter((r) => r.mode === 'Increment').length };
  const estimated = ccy === 'ils' && rows.some((r) => r.ilsEstimated);

  return (
    <section className="rounded-lg border border-slate-200 bg-white">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1 border-b border-slate-200 px-4 py-2.5">
        <h2 className="text-sm font-semibold text-slate-800">Payroll in the override file, by month</h2>
        <span className="text-xs text-slate-500">
          Override sheet rows on payroll accounts from January {year} (Snowflake). For reference: not part of the projection above.
        </span>
      </div>

      {error && <p className="px-4 py-3 text-xs text-amber-700">{error}</p>}
      {!error && rows.length === 0 && <p className="px-4 py-3 text-xs text-slate-500">No payroll rows in the override file from January {year}.</p>}

      {monthly && monthly.table && <ProjectionTable table={monthly.table} ccy={ccy} anchorDate={null} markers={MARKERS} />}

      {rows.length > 0 && (
        <div className="space-y-1 border-t border-slate-200 px-4 py-2 text-[11px] text-slate-500">
          <p>
            {counts.Increment} increment{counts.Increment === 1 ? '' : 's'} (added to the budget) and {counts.Override} override
            {counts.Override === 1 ? '' : 's'} (replacing the budget). Each line sums an account's rows of one type across departments;
            hover an account for its departments and comments.
            {monthly && monthly.outside > 0 && ` ${monthly.outside} row${monthly.outside === 1 ? ' is' : 's are'} after December ${lastYear}: listed below only.`}
            {estimated && ' ≈ Where the file has euros only, shekels are at the forecast rate.'}
          </p>
        </div>
      )}

      {rows.length > 0 && (
        <details className="border-t border-slate-200">
          <summary className="cursor-pointer select-none px-4 py-2 text-xs font-medium text-slate-700 hover:bg-slate-50">
            The file's rows ({rows.length}), with departments and comments
          </summary>
          <RowList rows={rows} ccy={ccy} />
        </details>
      )}
    </section>
  );
}
