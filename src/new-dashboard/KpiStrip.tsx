import type { Ccy } from './types.ts';
import { formatFull, formatMillions, monthLongLabel, type Kpis } from './model.ts';

interface CardProps {
  label: string;
  value: number | null;
  ccy: Ccy;
  note: string;
  tone?: 'default' | 'warn';
}

function KpiCard({ label, value, ccy, note, tone = 'default' }: CardProps) {
  const negative = value != null && value < 0;
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
      <div className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</div>
      <div
        className={`mt-1 text-2xl font-semibold tabular-nums ${negative || tone === 'warn' ? 'text-rose-600' : 'text-slate-900'}`}
        title={value == null ? undefined : formatFull(value, ccy)}
      >
        {value == null ? '–' : formatMillions(value, ccy)}
      </div>
      <div className="mt-0.5 text-xs text-slate-500">{note}</div>
    </div>
  );
}

export default function KpiStrip({ kpis, ccy }: { kpis: Kpis; ccy: Ccy }) {
  const [current, next] = kpis.closings;
  const asOf = kpis.bankToday
    ? new Date(`${kpis.bankToday.asOf}T12:00:00`).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
    : '';
  return (
    <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <KpiCard
        label="Bank balance"
        value={kpis.bankToday ? kpis.bankToday.value : null}
        ccy={ccy}
        note={kpis.bankToday ? `NetSuite, ${asOf}` : 'Not available'}
      />
      {current && <KpiCard label={`Closing Dec ${current.year}`} value={current.value} ccy={ccy} note="Forecast" />}
      {next && <KpiCard label={`Closing Dec ${next.year}`} value={next.value} ccy={ccy} note="Projection" />}
      <KpiCard
        label="Lowest month-end"
        value={kpis.lowest ? kpis.lowest.value : null}
        ccy={ccy}
        note={kpis.lowest ? `${monthLongLabel(kpis.lowest.mKey)} (current and forecast months)` : 'Not available'}
        tone={kpis.lowest && kpis.lowest.value < 0 ? 'warn' : 'default'}
      />
    </div>
  );
}
