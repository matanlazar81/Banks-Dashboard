// The override file's payroll rows (OVERIDE_TEMP, loaded into Snowflake as future-cost overrides and
// increments), listed under the projection for reference. They are not part of the projection above.
import { formatFull, monthLongLabel } from '../new-dashboard/model.ts';
import type { Ccy, PayrollOverride } from './types.ts';

const MODE_TITLE: Record<PayrollOverride['mode'], string> = {
  Override: 'Replaces the budget of this account and month',
  Increment: 'Adds to the budget of this account and month',
};

interface Props {
  overrides: { rows: PayrollOverride[]; error: string | null };
  ccy: Ccy;
  /** The current year: rows are from its January. */
  year: number;
}

export default function OverridesSection({ overrides, ccy, year }: Props) {
  const { rows, error } = overrides;
  const amountOf = (r: PayrollOverride) => (ccy === 'eur' ? r.eur : r.ils);
  const total = (mode: PayrollOverride['mode']) => rows.filter((r) => r.mode === mode).reduce((s, r) => s + amountOf(r), 0);
  const counts = { Override: rows.filter((r) => r.mode === 'Override').length, Increment: rows.filter((r) => r.mode === 'Increment').length };
  const estimated = ccy === 'ils' && rows.some((r) => r.ilsEstimated);

  return (
    <section className="rounded-lg border border-slate-200 bg-white">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1 border-b border-slate-200 px-4 py-2.5">
        <h2 className="text-sm font-semibold text-slate-800">Payroll in the override file</h2>
        <span className="text-xs text-slate-500">
          Override sheet rows on payroll accounts from January {year} (Snowflake). For reference: not part of the projection above.
        </span>
      </div>

      {error && <p className="px-4 py-3 text-xs text-amber-700">{error}</p>}
      {!error && rows.length === 0 && <p className="px-4 py-3 text-xs text-slate-500">No payroll rows in the override file from January {year}.</p>}

      {rows.length > 0 && (
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
            <tfoot>
              <tr className="text-xs font-semibold text-slate-700">
                <td colSpan={4} className="px-4 py-2">
                  {counts.Increment} increment{counts.Increment === 1 ? '' : 's'}, {counts.Override} override{counts.Override === 1 ? '' : 's'}
                </td>
                <td className="whitespace-nowrap px-2 py-2 text-right tabular-nums">
                  {counts.Increment > 0 && <div>Increments {formatFull(total('Increment'), ccy)}</div>}
                  {counts.Override > 0 && <div>Overrides {formatFull(total('Override'), ccy)}</div>}
                </td>
                <td className="px-4 py-2 text-[11px] font-normal text-slate-500">
                  {estimated ? '≈ the file has euros only: shekels at the forecast rate.' : ''}
                </td>
              </tr>
            </tfoot>
          </table>
        </div>
      )}
    </section>
  );
}
