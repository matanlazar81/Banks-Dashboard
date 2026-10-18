import { KpiCard } from '../new-dashboard/KpiStrip.tsx';
import { formatFull, monthLongLabel } from '../new-dashboard/model.ts';
import type { SalaryKpis } from './model.ts';
import type { Ccy } from './types.ts';

export default function SalaryKpiStrip({ kpis, ccy }: { kpis: SalaryKpis; ccy: Ccy }) {
  const [current, next] = kpis.byYear;
  const through = kpis.ytd.through;
  const withChanges = (note: string, changes: number) => (Math.abs(changes) >= 0.5 ? `${note}, incl. ${formatFull(changes, ccy)} of planned changes` : note);
  const hc = kpis.headcount;
  const yearEnd = hc ? hc.yearEnd.find((y): y is { year: number; value: number } => y.value !== null) : undefined;
  return (
    <div className={`grid grid-cols-2 gap-3 ${hc ? 'lg:grid-cols-5' : 'lg:grid-cols-4'}`}>
      <KpiCard
        label="Payroll year to date"
        value={through ? kpis.ytd.value : null}
        ccy={ccy}
        note={through ? `NetSuite, January to ${monthLongLabel(through)}` : 'No closed month yet'}
      />
      {current && <KpiCard label={`Payroll FY ${current.year}`} value={current.value} ccy={ccy} note={withChanges('Actuals + forecast', current.changes)} />}
      {next && <KpiCard label={`Payroll FY ${next.year}`} value={next.value} ccy={ccy} note={withChanges('Projection', next.changes)} />}
      <KpiCard
        label="Permanent run-rate"
        value={kpis.runRate.value}
        ccy={ccy}
        note={`Per month, ${monthLongLabel(kpis.runRate.mKey)}`}
      />
      {hc && (
        <KpiCard
          label="Headcount today"
          value={hc.today}
          ccy={ccy}
          display={hc.today === null ? undefined : hc.today.toLocaleString('en-US')}
          note={`HiBob, ${new Date(`${hc.asOf}T12:00:00`).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}${yearEnd ? ` · Dec ${yearEnd.year}: ${yearEnd.value.toLocaleString('en-US')} forecast` : ''}`}
        />
      )}
    </div>
  );
}
