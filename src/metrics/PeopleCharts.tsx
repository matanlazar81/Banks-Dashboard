// Payroll / revenue and revenue per employee: one column per month through the last payroll JE, then the
// year to date. Plain HTML columns (the page stays a small bundle): one hue, the year-to-date column a
// darker step and set apart; each column's value on its cap, the details on hover or focus, and every
// figure in the table below.
import { useState } from 'react';
import { formatEur, formatEurFull, formatPct, monthName, MONTH_NAMES } from './model.ts';
import type { People } from './types.ts';

interface Column { key: string; label: string; value: number | null; tip: string[]; total?: boolean }

/** A round axis top a little above the largest value (1, 2, 2.5 or 5 × a power of ten). */
function niceTop(max: number): number {
  if (!(max > 0)) return 1;
  const p = 10 ** Math.floor(Math.log10(max));
  return ([1, 2, 2.5, 5, 10].find((s) => s * p >= max * 1.05) ?? 10) * p;
}

/** format: the axis; valueFormat: the value on each column and in its tooltip (default: format). */
function ColumnChart({ columns, format, valueFormat = format, label }: {
  columns: Column[]; format: (n: number) => string; valueFormat?: (n: number) => string; label: string;
}) {
  const [hover, setHover] = useState<string | null>(null);
  const top = niceTop(Math.max(0, ...columns.map((c) => c.value ?? 0)));
  const ticks = [top, top / 2, 0];
  return (
    <div className="flex gap-2" role="group" aria-label={label}>
      <div className="relative h-40 w-12 shrink-0 text-right text-[10px] tabular-nums text-slate-400">
        {ticks.map((t) => (
          <span key={t} className="absolute right-0 -translate-y-1/2" style={{ top: `${100 - (t / top) * 100}%` }}>{format(t)}</span>
        ))}
      </div>
      <div className="min-w-0 flex-1">
        <div className="relative h-40">
          {ticks.map((t) => (
            <div key={t} className="absolute inset-x-0 border-t border-slate-100" style={{ top: `${100 - (t / top) * 100}%` }} aria-hidden="true" />
          ))}
          <div className="absolute inset-0 flex items-end">
            {columns.map((c, i) => {
              const h = c.value !== null && c.value > 0 ? (c.value / top) * 100 : 0;
              const on = hover === c.key;
              const align = i < 2 ? 'left-0' : i >= columns.length - 2 ? 'right-0' : 'left-1/2 -translate-x-1/2';
              return (
                <div key={c.key} className={`relative flex h-full flex-1 items-end justify-center ${c.total ? 'ml-2 border-l border-slate-200 pl-2' : ''}`}>
                  {c.value !== null && !on && (
                    <span className={`absolute left-1/2 z-0 -translate-x-1/2 whitespace-nowrap text-[10px] ${c.total ? 'font-semibold text-slate-800' : 'font-medium text-slate-600'}`}
                      style={{ bottom: `calc(${h}% + 2px)` }}>{valueFormat(c.value)}</span>
                  )}
                  <div
                    tabIndex={0}
                    aria-label={`${c.label}: ${c.value === null ? 'no figure' : valueFormat(c.value)}`}
                    onPointerEnter={() => setHover(c.key)}
                    onPointerLeave={() => setHover((k) => (k === c.key ? null : k))}
                    onFocus={() => setHover(c.key)}
                    onBlur={() => setHover((k) => (k === c.key ? null : k))}
                    className="flex h-full w-full cursor-default items-end justify-center outline-none focus-visible:bg-slate-50"
                  >
                    <div
                      className={`w-full max-w-[24px] rounded-t ${c.total ? (on ? 'bg-sky-900' : 'bg-sky-800') : (on ? 'bg-sky-600' : 'bg-sky-500')}`}
                      style={{ height: `${h}%` }}
                    />
                  </div>
                  {on && (
                    <div className={`pointer-events-none absolute top-0 z-10 -translate-y-full whitespace-nowrap rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] shadow-lg ${align}`}>
                      <div className="font-semibold tabular-nums text-slate-900">{c.value === null ? '–' : valueFormat(c.value)}</div>
                      <div className="text-slate-500">{c.tip[0]}</div>
                      {c.tip.slice(1).map((t) => <div key={t} className="tabular-nums text-slate-600">{t}</div>)}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
        <div className="mt-1 flex border-t border-slate-300 pt-1 text-[10px] text-slate-500">
          {columns.map((c) => (
            <span key={c.key} className={`flex-1 truncate text-center ${c.total ? 'ml-2 pl-2 font-semibold text-slate-700' : ''}`}>{c.label}</span>
          ))}
        </div>
      </div>
    </div>
  );
}

const pct1 = (n: number) => formatPct(n, n >= 10 || n === 0 ? 0 : 1);
const people$ = (n: number) => (n === 0 ? '€0' : formatEur(n));

export default function PeopleCharts({ people }: { people: People }) {
  const { months, ytd } = people;
  if (!months.length || !ytd) {
    return <p className="text-sm text-slate-500">No closed month with its payroll JE posted yet this year.</p>;
  }
  const short = (mKey: string) => MONTH_NAMES[Number(mKey.slice(5)) - 1];
  const hc = (n: number | null) => (n === null ? 'employees –' : `${n} employees`);
  const payrollCols: Column[] = [
    ...months.map((m) => ({
      key: m.mKey, label: short(m.mKey), value: m.payrollPct,
      tip: [monthName(m.mKey), `Payroll ${formatEurFull(m.payroll)}`, `Revenue ${formatEurFull(m.revenue)}`],
    })),
    { key: 'ytd', label: ytd.label, value: ytd.payrollPct, total: true, tip: [`${ytd.label}, year to date`, `Payroll ${formatEurFull(ytd.payroll)}`, `Revenue ${formatEurFull(ytd.revenue)}`] },
  ];
  const perHead = months.some((m) => m.revenuePerEmployee !== null);
  const perHeadCols: Column[] = [
    ...months.map((m) => ({
      key: m.mKey, label: short(m.mKey), value: m.revenuePerEmployee,
      tip: [monthName(m.mKey), `Revenue ${formatEurFull(m.revenue)}`, hc(m.headcount),
        ...(m.byType && Object.keys(m.byType).length > 1 ? Object.entries(m.byType).sort((a, b) => b[1] - a[1]).map(([t, n]) => `  ${t}: ${n}`) : [])],
    })),
    {
      key: 'ytd', label: ytd.label, value: ytd.revenuePerEmployeeMonthly, total: true,
      tip: [`${ytd.label}: a month per employee, on average`, `Revenue ${formatEurFull(ytd.revenue)}`, `Average ${ytd.avgHeadcount ?? '–'} employees`],
    },
  ];

  return (
    <div className="space-y-4">
      <p className="text-xs text-slate-500">
        Through {monthName(people.through)}, the last month with its payroll JE posted in NetSuite
        {people.pending.length ? ` (${people.pending.map(monthName).join(', ')}: payroll JE not posted yet)` : ''}.
        Payroll is NetSuite 76xxxx before capitalised salaries; revenue is the P&amp;L&apos;s total revenue.
      </p>
      {/* A line between the two charts: across when they stack, down the middle side by side. */}
      <div className="grid divide-y divide-slate-200 lg:grid-cols-2 lg:divide-x lg:divide-y-0">
        <div className="pb-6 lg:pb-0 lg:pr-6">
          <div className="mb-2 flex flex-wrap items-baseline justify-between gap-2">
            <h3 className="text-sm font-semibold text-slate-800">Payroll / revenue</h3>
            <span className="text-xs text-slate-500">{ytd.label}: <span className="text-base font-semibold text-slate-900">{formatPct(ytd.payrollPct, 1)}</span></span>
          </div>
          <ColumnChart columns={payrollCols} format={pct1} valueFormat={(n) => formatPct(n, 1)} label="Payroll as a percentage of revenue, by month and year to date" />
        </div>
        <div className="pt-6 lg:pl-6 lg:pt-0">
          <div className="mb-2 flex flex-wrap items-baseline justify-between gap-2">
            <h3 className="text-sm font-semibold text-slate-800">Revenue per employee, a month</h3>
            <span className="text-xs text-slate-500">
              {ytd.label}: <span className="text-base font-semibold text-slate-900">{formatEur(ytd.revenuePerEmployee)}</span> per employee
              {ytd.revenuePerEmployeeAnnualised !== null && <> · {formatEur(ytd.revenuePerEmployeeAnnualised)} a year at this pace</>}
            </span>
          </div>
          {perHead
            ? <ColumnChart columns={perHeadCols} format={people$} label="Revenue per employee a month, by month and the year-to-date average" />
            : <p className="py-6 text-sm text-slate-500">Employees could not be read from HiBob right now.</p>}
          <p className="mt-1 text-[11px] text-slate-500">
            Employees: {people.company || 'the company'}&apos;s employees in HiBob active at each month-end (any employment type).
            The {ytd.label} column is the months&apos; revenue ÷ employee-months, so it compares with the months.
          </p>
        </div>
      </div>
      <details className="text-xs">
        <summary className="cursor-pointer text-sky-700 hover:underline">Show the figures</summary>
        <table className="mt-2 w-full">
          <thead>
            <tr className="text-slate-500">
              <th className="py-1 text-left font-medium">Month</th>
              <th className="text-right font-medium">Payroll</th>
              <th className="text-right font-medium">Revenue</th>
              <th className="text-right font-medium">Payroll / revenue</th>
              <th className="text-right font-medium">Employees</th>
              <th className="text-right font-medium">Revenue per employee</th>
            </tr>
          </thead>
          <tbody className="tabular-nums">
            {months.map((m) => (
              <tr key={m.mKey} className="border-t border-slate-100">
                <td className="py-1">{monthName(m.mKey)}</td>
                <td className="text-right">{formatEurFull(m.payroll)}</td>
                <td className="text-right">{formatEurFull(m.revenue)}</td>
                <td className="text-right">{formatPct(m.payrollPct, 1)}</td>
                <td className="text-right">{m.headcount ?? '–'}</td>
                <td className="text-right">{formatEurFull(m.revenuePerEmployee)}</td>
              </tr>
            ))}
            <tr className="border-t-2 border-slate-300 font-semibold">
              <td className="py-1">{ytd.label}</td>
              <td className="text-right">{formatEurFull(ytd.payroll)}</td>
              <td className="text-right">{formatEurFull(ytd.revenue)}</td>
              <td className="text-right">{formatPct(ytd.payrollPct, 1)}</td>
              <td className="text-right">{ytd.avgHeadcount ?? '–'} avg</td>
              <td className="text-right">{formatEurFull(ytd.revenuePerEmployee)}</td>
            </tr>
          </tbody>
        </table>
      </details>
    </div>
  );
}
