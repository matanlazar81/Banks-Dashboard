import { KpiCard } from '../new-dashboard/KpiStrip.tsx';
import { monthLongLabel } from '../new-dashboard/model.ts';
import type { PnlKpis } from './model.ts';
import type { Ccy } from './types.ts';

export default function PnlKpiStrip({ kpis, ccy, firstYear }: { kpis: PnlKpis; ccy: Ccy; firstYear: number }) {
  const [current, next] = kpis.netByYear;
  const through = kpis.ebitdaYtd.through;
  return (
    <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <KpiCard
        label="EBITDA year to date"
        value={through ? kpis.ebitdaYtd.value : null}
        ccy={ccy}
        note={through ? `NetSuite, January to ${monthLongLabel(through)}` : 'No closed month yet'}
      />
      {current && <KpiCard label={`Net profit FY ${current.year}`} value={current.value} ccy={ccy} note="Actuals + forecast" />}
      {next && <KpiCard label={`Net profit FY ${next.year}`} value={next.value} ccy={ccy} note="Projection" />}
      <KpiCard
        label="Accumulated profit"
        value={kpis.accumulatedEnd.value}
        ccy={ccy}
        note={`${monthLongLabel(kpis.accumulatedEnd.mKey)}, since 1 January ${firstYear}`}
      />
    </div>
  );
}
