// The Charts view (the page's Table / Charts switch): the payroll by month stacked by the table's sections, Deel
// on top, and HiBob's headcount: employees at month-end, and the hires and leavers that move it. Read off the
// tables the Table view shows (charts.ts), with the same currency and year view; projected months (the current
// month and the forecast) are faded or dashed. The Table view is the charts' table: every figure is there.
// Loaded on demand (recharts), so the Table view does not wait for it.
import { Fragment, useMemo, type ReactNode } from 'react';
import { Bar, BarChart, CartesianGrid, Line, LineChart, ReferenceDot, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { formatCount, formatFull, monthLongLabel, type Accent, type ProjectionTable as Table } from '../new-dashboard/model.ts';
import { formatAxisMoney, headcountChart, payrollChart, type HeadcountPoint, type PayrollPoint, type PayrollSeries } from './charts.ts';
import type { Ccy, MonthStatus, SalaryPayload } from './types.ts';

/**
 * Each section's colour: the table's, at the step that keeps neighbours in the stack apart for colour-blind
 * readers (every stack the page can show was checked with a CVD palette validator). Not classified is a
 * deliberate grey.
 */
const SECTION_FILL: Record<Accent, string> = { sky: '#0ea5e9', amber: '#f59e0b', violet: '#8b5cf6', emerald: '#059669', slate: '#475569', pink: '#f472b6' };
const HIRES = '#2a78d6';
const LEAVERS = '#e34948';
const EMPLOYEES = '#475569';
const GRID = '#e2e8f0';
const AXIS = '#cbd5e1';
const TICK = { fontSize: 11, fill: '#64748b' };
const LABEL = { fontSize: 11, fill: '#334155' };
const CURSOR = { fill: '#f1f5f9' };
/** Projected months: fill opacity of their columns and of the dashed line. */
const FADED = 0.45;
/** Columns: at most this wide, a 2px surface gap between stacked segments, the end away from zero rounded. */
const BAR_MAX = 24;
const GAP = 2;
const RADIUS = 4;
const STATUS: Record<MonthStatus, string> = { actual: 'Actual', current: 'Current month, projected', forecast: 'Forecast' };

// ── the columns ──
const roundTop = (x: number, y: number, w: number, h: number, r: number) =>
  `M${x},${y + h}V${y + r}${r ? `A${r},${r} 0 0 1 ${x + r},${y}` : ''}H${x + w - r}${r ? `A${r},${r} 0 0 1 ${x + w},${y + r}` : ''}V${y + h}Z`;
const roundBottom = (x: number, y: number, w: number, h: number, r: number) =>
  `M${x},${y}H${x + w}V${y + h - r}${r ? `A${r},${r} 0 0 1 ${x + w - r},${y + h}` : ''}H${x + r}${r ? `A${r},${r} 0 0 1 ${x},${y + h - r}` : ''}Z`;

/** What recharts hands a column's shape: its box and the month it is for. */
interface SegmentBox { x?: number; y?: number; width?: number; height?: number; payload?: Record<string, unknown> }

/**
 * One segment of a stacked column (stackOffset "sign": amounts above zero stack up, below zero down). The
 * segment at the end away from zero is rounded; one that does not sit on zero leaves a 2px gap to the segment
 * between it and zero, unless the gap would hide it. keys: the stack's series, bottom first.
 */
function Segment({ box, seriesKey, keys, fill }: { box: SegmentBox; seriesKey: string; keys: string[]; fill: string }) {
  const { x = 0, y = 0, width = 0, height = 0, payload = {} } = box;
  const value = Number(payload[seriesKey]) || 0;
  if (!width || !height || !value) return <g />;
  const up = value > 0;
  const side = keys.filter((k) => (up ? Number(payload[k]) > 0 : Number(payload[k]) < 0));
  let top = Math.min(y, y + height);
  let bottom = Math.max(y, y + height);
  if (side[0] !== seriesKey && bottom - top > GAP + 1) {
    if (up) bottom -= GAP;
    else top += GAP;
  }
  const h = bottom - top;
  if (h <= 0) return <g />;
  const r = side[side.length - 1] === seriesKey ? Math.min(RADIUS, h, width / 2) : 0;
  return <path d={up ? roundTop(x, top, width, h, r) : roundBottom(x, top, width, h, r)} fill={fill} fillOpacity={payload.projected ? FADED : 1} />;
}

// ── the card, its key and the hover card ──
/**
 * A chart's card: title and note, the key (the series, then which months are actual and which projected), the
 * chart at the bottom, so charts side by side line up when their notes wrap differently.
 */
function ChartCard({ title, note, keyItems, statusItems, children }: { title: string; note: string; keyItems: ReactNode; statusItems?: ReactNode; children: ReactNode }) {
  return (
    <section className="flex flex-col rounded-lg border border-slate-200 bg-white">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5 border-b border-slate-200 px-4 py-2.5">
        <h2 className="text-sm font-semibold text-slate-800">{title}</h2>
        <span className="text-xs text-slate-500">{note}</span>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-1 px-4 pt-2.5 text-xs text-slate-600">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1">{keyItems}</div>
        {statusItems && <div className="flex flex-wrap items-center gap-x-4 gap-y-1">{statusItems}</div>}
      </div>
      <div className="mt-auto">{children}</div>
    </section>
  );
}

/** The key's actual and projected entries, for the kinds of month the chart shows. */
function StatusKey({ points }: { points: { projected: boolean }[] }) {
  return (
    <>
      {points.some((p) => !p.projected) && <KeyItem color="#64748b" label="Actual" />}
      {points.some((p) => p.projected) && <KeyItem color="#64748b" label="Projected" faded />}
    </>
  );
}

/** A key entry: a block for columns, a stroke for a line (solid or dashed); faded for the projected months. */
function KeyItem({ color, label, mark = 'bar', faded = false }: { color: string; label: string; mark?: 'bar' | 'line' | 'dash'; faded?: boolean }) {
  const opacity = faded ? FADED : 1;
  return (
    <span className="inline-flex items-center gap-1.5">
      {mark === 'bar'
        ? <span className="h-3 w-3 rounded-sm" style={{ backgroundColor: color, opacity }} />
        : (
          <svg width="16" height="4" aria-hidden="true">
            <line x1="0" y1="2" x2="16" y2="2" stroke={color} strokeWidth="2" strokeDasharray={mark === 'dash' ? '4 3' : undefined} strokeOpacity={opacity} />
          </svg>
        )}
      {label}
    </span>
  );
}

interface TipRow { key: string; color?: string; value: string; label: string; strong?: boolean }

/** The hover card: the month, then the values first, each series keyed by a short stroke of its colour. */
function Tip({ mKey, status, rows }: { mKey: string; status: MonthStatus; rows: (TipRow | 'rule')[] }) {
  return (
    <div className="rounded-md border border-slate-200 bg-white px-3 py-2 text-xs shadow-md">
      <div className="mb-1.5 flex items-baseline justify-between gap-4">
        <span className="font-medium text-slate-800">{monthLongLabel(mKey)}</span>
        <span className="text-slate-500">{STATUS[status]}</span>
      </div>
      <div className="grid grid-cols-[auto_auto_1fr] items-center gap-x-2 gap-y-0.5 tabular-nums">
        {rows.map((r, i) => (r === 'rule'
          ? <div key={`rule-${i}`} className="col-span-3 my-0.5 border-t border-slate-100" />
          : (
            <Fragment key={r.key}>
              <span className="h-0.5 w-2.5 rounded-full" style={r.color ? { backgroundColor: r.color } : undefined} />
              <span className={`text-right ${r.strong ? 'font-semibold text-slate-900' : 'font-medium text-slate-800'}`}>{r.value}</span>
              <span className="text-slate-500">{r.label}</span>
            </Fragment>
          )))}
      </div>
    </div>
  );
}

// ── payroll ──
function PayrollTip({ point, series, ccy }: { point: PayrollPoint; series: PayrollSeries[]; ccy: Ccy }) {
  const row = (s: PayrollSeries): TipRow => ({ key: s.key, color: SECTION_FILL[s.accent], value: formatFull(point[s.key], ccy), label: s.label });
  const deel = series.find((s) => s.key === 'deel');
  const rows: (TipRow | 'rule')[] = [
    ...series.filter((s) => s.key !== 'deel').map(row),
    'rule',
    { key: 'payroll', value: formatFull(point.payroll, ccy), label: 'Total payroll', strong: true },
  ];
  if (deel) rows.push(row(deel), 'rule', { key: 'total', value: formatFull(point.total, ccy), label: 'Payroll + Deel', strong: true });
  return <Tip mKey={point.mKey} status={point.status} rows={rows} />;
}

function PayrollChart({ table, ccy }: { table: Table<string>; ccy: Ccy }) {
  const { series, points, axis } = useMemo(() => payrollChart(table), [table]);
  const keys = series.map((s) => s.key);
  const deel = keys.includes('deel');
  return (
    <ChartCard
      title="Payroll by month"
      note={`${deel ? 'Total payroll by section, with Deel on top: a column is Payroll + Deel.' : 'Total payroll by section.'} Hover a month for its figures.`}
      keyItems={series.map((s) => <KeyItem key={s.key} color={SECTION_FILL[s.accent]} label={s.label} />)}
      statusItems={<StatusKey points={points} />}
    >
      <div className="h-80 px-2 pb-2 pt-1 tabular-nums">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={points} stackOffset="sign" margin={{ top: 12, right: 16, bottom: 0, left: 4 }}>
            <CartesianGrid vertical={false} stroke={GRID} />
            <XAxis dataKey="label" tickLine={false} axisLine={{ stroke: AXIS }} tick={TICK} minTickGap={8} />
            <YAxis tickLine={false} axisLine={false} tick={TICK} width={64} domain={axis.domain} ticks={axis.ticks} tickFormatter={(v: number) => formatAxisMoney(v, ccy)} />
            {axis.domain[0] < 0 && <ReferenceLine y={0} stroke={AXIS} />}
            <Tooltip
              cursor={CURSOR}
              isAnimationActive={false}
              content={({ active, payload }) => (active && payload && payload.length ? <PayrollTip point={payload[0].payload as PayrollPoint} series={series} ccy={ccy} /> : null)}
            />
            {series.map((s) => (
              <Bar
                key={s.key}
                dataKey={s.key}
                name={s.label}
                stackId="payroll"
                fill={SECTION_FILL[s.accent]}
                maxBarSize={BAR_MAX}
                isAnimationActive={false}
                shape={(box: unknown) => <Segment box={box as SegmentBox} seriesKey={s.key} keys={keys} fill={SECTION_FILL[s.accent]} />}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </ChartCard>
  );
}

// ── headcount ──
function HeadcountTip({ point }: { point: HeadcountPoint }) {
  const rows: TipRow[] = [
    { key: 'employees', color: EMPLOYEES, value: point.employees === null ? 'n/a' : point.employees.toLocaleString('en-US'), label: 'Employees at month-end', strong: true },
    { key: 'hires', color: HIRES, value: formatCount(point.hires, true), label: 'Hires' },
    { key: 'leavers', color: LEAVERS, value: formatCount(point.leavers, true), label: 'Leavers' },
  ];
  if (point.transfers) rows.push({ key: 'transfers', value: formatCount(point.transfers), label: 'Transfers between departments (not hires)' });
  if (point.openings) rows.push({ key: 'openings', value: formatCount(point.openings), label: 'Open positions (not counted)' });
  return <Tip mKey={point.mKey} status={point.status} rows={rows} />;
}

const headcountTip = ({ active, payload }: { active?: boolean; payload?: ReadonlyArray<{ payload?: unknown }> }) =>
  (active && payload && payload.length ? <HeadcountTip point={payload[0].payload as HeadcountPoint} /> : null);

/** A labelled dot on the line (the last actual month-end, the last month shown): an element, not a component. */
function endDot(point: HeadcountPoint) {
  if (point.employees === null) return null;
  return (
    <ReferenceDot
      key={point.mKey}
      x={point.label}
      y={point.employees}
      r={4}
      fill={EMPLOYEES}
      fillOpacity={point.projected ? FADED : 1}
      stroke="#ffffff"
      strokeWidth={2}
      label={{ ...LABEL, value: point.employees.toLocaleString('en-US'), position: 'top' }}
    />
  );
}

function HeadcountCharts({ table, headcount }: { table: Table<string>; headcount: NonNullable<SalaryPayload['headcount']> }) {
  const { points, employeesAxis, movesAxis } = useMemo(() => headcountChart(table, headcount), [table, headcount]);
  const known = points.filter((p) => p.employees !== null);
  if (!known.length || !employeesAxis) return null;
  const lastActual = [...known].reverse().find((p) => !p.projected);
  const last = known[known.length - 1];
  const keys = ['hires', 'leavers'];
  const box = (key: string, fill: string) => (b: unknown) => <Segment box={b as SegmentBox} seriesKey={key} keys={keys} fill={fill} />;
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <ChartCard
        title="Employees at month-end"
        note={`HiBob (${headcount.company}): employees on the last day of the month, people on leave included.`}
        keyItems={(
          <>
            {points.some((p) => !p.projected) && <KeyItem color={EMPLOYEES} label="Actual" mark="line" />}
            {points.some((p) => p.projected) && <KeyItem color={EMPLOYEES} label="Projected (by HiBob's dates)" mark="dash" faded />}
          </>
        )}
      >
        <div className="h-64 px-2 pb-2 pt-1 tabular-nums">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={points} margin={{ top: 20, right: 20, bottom: 0, left: 4 }}>
              <CartesianGrid vertical={false} stroke={GRID} />
              <XAxis dataKey="label" tickLine={false} axisLine={{ stroke: AXIS }} tick={TICK} minTickGap={8} />
              <YAxis tickLine={false} axisLine={false} tick={TICK} width={40} domain={employeesAxis.domain} ticks={employeesAxis.ticks} />
              <Tooltip cursor={{ stroke: AXIS, strokeWidth: 1 }} isAnimationActive={false} content={headcountTip} />
              <Line dataKey="employeesActual" stroke={EMPLOYEES} strokeWidth={2} dot={false} activeDot={{ r: 4, stroke: '#ffffff', strokeWidth: 2 }} isAnimationActive={false} />
              <Line
                dataKey="employeesProjected"
                stroke={EMPLOYEES}
                strokeOpacity={FADED}
                strokeWidth={2}
                strokeDasharray="4 3"
                dot={false}
                activeDot={{ r: 4, stroke: '#ffffff', strokeWidth: 2 }}
                isAnimationActive={false}
              />
              {lastActual && lastActual !== last && endDot(lastActual)}
              {endDot(last)}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </ChartCard>

      <ChartCard
        title="Hires and leavers"
        note="By HiBob's start and leave dates. Transfers between departments and open positions are not drawn: hover a month for them."
        keyItems={(
          <>
            <KeyItem color={HIRES} label="Hires" />
            <KeyItem color={LEAVERS} label="Leavers" />
          </>
        )}
        statusItems={<StatusKey points={points} />}
      >
        <div className="h-64 px-2 pb-2 pt-1 tabular-nums">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={points} stackOffset="sign" margin={{ top: 20, right: 20, bottom: 0, left: 4 }}>
              <CartesianGrid vertical={false} stroke={GRID} />
              <XAxis dataKey="label" tickLine={false} axisLine={false} tick={TICK} minTickGap={8} />
              <YAxis tickLine={false} axisLine={false} tick={TICK} width={40} domain={movesAxis.domain} ticks={movesAxis.ticks} tickFormatter={(v: number) => (v ? formatCount(v, true) : '0')} />
              <ReferenceLine y={0} stroke={AXIS} />
              <Tooltip cursor={CURSOR} isAnimationActive={false} content={headcountTip} />
              <Bar dataKey="hires" name="Hires" stackId="moves" fill={HIRES} maxBarSize={BAR_MAX} isAnimationActive={false} shape={box('hires', HIRES)} />
              <Bar dataKey="leavers" name="Leavers" stackId="moves" fill={LEAVERS} maxBarSize={BAR_MAX} isAnimationActive={false} shape={box('leavers', LEAVERS)} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </ChartCard>
    </div>
  );
}

interface Props {
  table: Table<string>;
  headcountTable: Table<string> | null;
  headcount: SalaryPayload['headcount'];
  ccy: Ccy;
}

export default function SalaryCharts({ table, headcountTable, headcount, ccy }: Props) {
  return (
    <div className="space-y-4">
      <PayrollChart table={table} ccy={ccy} />
      {headcountTable && headcount && <HeadcountCharts table={headcountTable} headcount={headcount} />}
      <p className="text-xs text-slate-500">
        The same figures as the Table view, in the currency and years chosen above; the Table view has every figure by account.
      </p>
    </div>
  );
}
