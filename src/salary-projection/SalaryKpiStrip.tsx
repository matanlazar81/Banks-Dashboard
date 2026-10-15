import { KpiCard } from '../new-dashboard/KpiStrip.tsx';
import { monthLongLabel } from '../new-dashboard/model.ts';
import type { SalaryKpis } from './model.ts';
import type { Ccy } from './types.ts';

export default function SalaryKpiStrip({ kpis, ccy }: { kpis: SalaryKpis; ccy: Ccy }) {
  const [current, next] = kpis.byYear;
  const through = kpis.ytd.through;
  return (
    <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <KpiCard
        label="Payroll year to date"
        value={through ? kpis.ytd.value : null}
        ccy={ccy}
        note={through ? `Snowflake, January to ${monthLongLabel(through)}` : 'No closed month yet'}
      />
      {current && <KpiCard label={`Payroll FY ${current.year}`} value={current.value} ccy={ccy} note="Actuals + forecast" />}
      {next && <KpiCard label={`Payroll FY ${next.year}`} value={next.value} ccy={ccy} note="Projection" />}
      <KpiCard
        label="Permanent run-rate"
        value={kpis.runRate.value}
        ccy={ccy}
        note={`Per month, ${monthLongLabel(kpis.runRate.mKey)}`}
      />
    </div>
  );
}
