// Pure: what the FX Challenge page shows, from GET /api/fx-challenge and the rate being simulated. No React.
// The recommendation itself (planFromPayload, todayActions and their words) is shared with the daily mail.
import {
  bandIndex, bandLabels, conversionAt, describeAction, describeDue, describeKeep, keepCover, legsAt, nothingToday, pastConversions, planFromPayload, todayActions,
  type CashPlan, type Conversion, type ConversionLeg, type DealsView, type EurPosition, type EurVendorsAhead, type Fixing, type FxTable, type KeepCover, type PastMonth, type PastTotals, type Policy,
  type RateCheck, type ReservePlan,
  type ScheduleItem, type ShekelDue, type TodayAction,
} from '../forecast/fx-challenge.mjs';
import { formatEur } from '../metrics/model.ts';
import type { FxPayload } from './types.ts';

export interface ScheduleRow extends ScheduleItem {
  atSim: Conversion | null;
  /** With the bank's deals, the parts selling at the simulated rate (once fixed, at the fixed one). */
  simParts: ConversionLeg[] | null;
}
export interface YearTotal { year: string; eur: number; ils: number; vsSpotIls: number; expectedEur: number; count: number }
/** What to do today, as the daily mail says it: each action in words, the payments in shekels ahead, and what comes next
 * when there is nothing to do. */
export interface TodayView {
  actions: (TodayAction & { head: string; body: string })[];
  dues: (ShekelDue & { text: string })[];
  /** The euros to convert for the payments ahead, in all. */
  eur: number;
  nothing: string;
}

export interface FxModel {
  /** Today in Israel (YYYY-MM-DD). */
  today: string;
  /** The table the plan uses: the saved one, with the months the bank's deals cover worked out from them. */
  table: FxTable;
  labels: string[];
  /** Today's EUR/ILS (NetSuite), and the rate being simulated (today's unless one is typed). */
  spot: number | null;
  spotDate: string | null;
  simRate: number | null;
  spotBand: number | null;
  simBand: number | null;
  sigma: number;
  sigmaFromHistory: boolean;
  /** Today's rate against the last 90 days. */
  rateCheck: RateCheck | null;
  schedule: ScheduleRow[];
  totals: YearTotal[];
  position: EurPosition | null;
  inflow: ReturnType<typeof planFromPayload>['inflow'];
  /** Shekels for the payroll and the vendors; null without a payroll figure. */
  cash: CashPlan | null;
  plan: ReservePlan | null;
  /** The bank's deals behind the plan's days; null while none are saved. */
  deals: DealsView | null;
  /** The EUR vendor payments the plan expects on their days; null without NetSuite's payments by day. */
  eurVendors: EurVendorsAhead | null;
  /** Where the amount kept for the next conversion comes from: deposits released by then, and today's cash; text says
   * it in words (empty when no deposit is released by then). */
  keep: (KeepCover & { text: string }) | null;
  /** What to do today (at today's rate, whatever rate is simulated). */
  todo: TodayView;
  /** Every month since January 2026 whose conversion day has passed, and the totals. */
  past: PastMonth[];
  pastFrom: string;
  pastTotal: PastTotals;
}

export function buildFxModel(data: FxPayload, simRate: number | null, policy: Policy): FxModel {
  const base = planFromPayload(data, policy);
  const table = base.table;
  const bounds = table.bounds;
  const sim = simRate && simRate > 0 ? simRate : base.spot;
  // A conversion already fixed sells what it was fixed at, whatever rate is simulated.
  const legsOf = (month: string) => (base.dealLegs && base.dealLegs[month]) || null;
  const schedule: ScheduleRow[] = base.schedule.map((s) => {
    const legs = legsOf(s.fixing.month);
    return {
      ...s,
      atSim: s.fixedAt ? s.atSpot : sim ? conversionAt(s.fixing.row, bounds, sim) : null,
      simParts: s.fixedAt ? s.parts : legs && sim ? legsAt(legs, sim).filter((l) => l.eur > 0.5) : null,
    };
  });

  const byYear = new Map<string, YearTotal>();
  for (const s of schedule) {
    const year = s.fixing.month.slice(0, 4);
    const t = byYear.get(year) || { year, eur: 0, ils: 0, vsSpotIls: 0, expectedEur: 0, count: 0 };
    if (s.atSim) { t.eur += s.atSim.eur; t.ils += s.atSim.ils; t.vsSpotIls += s.atSim.vsSpotIls; }
    if (s.risk) t.expectedEur += s.risk.expectedEur;
    t.count += 1;
    byYear.set(year, t);
  }

  const past = pastConversions({ table, today: data.today, series: data.rates.series, conversions: data.conversions });
  const fmt = { eur: (n: number) => (Math.abs(n) < 0.5 ? '€0' : formatEur(n)), ils: formatIls, day: (iso: string) => formatDay(iso), rate: formatRate };
  const todo = todayActions(base, data.today);
  const cover = base.plan ? keepCover(base.plan) : null;

  return {
    today: data.today,
    table,
    labels: bandLabels(bounds),
    spot: base.spot,
    spotDate: base.spotDate,
    simRate: sim,
    spotBand: base.spotBand,
    simBand: sim ? bandIndex(bounds, sim) : null,
    sigma: base.sigma,
    sigmaFromHistory: base.sigmaFromHistory,
    rateCheck: base.rateCheck,
    schedule,
    totals: [...byYear.values()],
    position: base.position,
    inflow: base.inflow,
    cash: base.cash,
    plan: base.plan,
    deals: base.deals,
    eurVendors: base.eurVendors,
    keep: cover ? { ...cover, text: describeKeep(cover, fmt) } : null,
    todo: {
      actions: todo.actions.map((a) => ({ ...a, ...describeAction(a, base, data.today, fmt) })),
      dues: todo.ahead.dues.map((d) => ({ ...d, text: describeDue(d, data.today, fmt) })),
      eur: todo.ahead.eur,
      nothing: nothingToday(todo.ahead, data.today, fmt),
    },
    past: past.months,
    pastFrom: past.from,
    pastTotal: past.total,
  };
}

// ── formatting ──
const int = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });

/** ₪ in millions (2 decimals) from ₪1M, else in thousands. */
export function formatIls(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return '–';
  const sign = n < 0 ? '-' : '';
  const a = Math.abs(n);
  if (a >= 1_000_000) return `${sign}₪${(a / 1_000_000).toFixed(2)}M`;
  if (a >= 1_000) return `${sign}₪${int.format(Math.round(a / 1_000))}K`;
  return a < 0.5 ? '₪0' : `${sign}₪${int.format(Math.round(a))}`;
}

/** EUR/ILS with three decimals. */
export const formatRate = (r: number | null | undefined) => (r === null || r === undefined || !Number.isFinite(r) ? '–' : r.toFixed(3));

/** A conversion's rate as the table gives it (3.67), or to three decimals when the bank's deals give it (3.669). */
export const formatTableRate = (r: number | null | undefined) => (r === null || r === undefined || !Number.isFinite(r) ? '–' : String(+r.toFixed(3)));

/** "Sat 7 Nov" (or "Sat 7 Nov 2027" with the year). */
export function formatDay(iso: string | null | undefined, withYear = false): string {
  if (!iso) return '–';
  return new Date(`${iso.slice(0, 10)}T12:00:00Z`).toLocaleDateString('en-GB', {
    weekday: 'short', day: 'numeric', month: 'short', ...(withYear ? { year: 'numeric' } : {}), timeZone: 'UTC',
  });
}

/** "Mon 2 & Tue 3 Nov": days, the month once when they share it. */
export function formatDays(days: string[]): string {
  const parts = days.map((d) => formatDay(d));
  const month = (p: string) => p.slice(p.lastIndexOf(' ') + 1);
  const shown = parts.every((p) => month(p) === month(parts[0])) ? parts.map((p, i) => (i < parts.length - 1 ? p.slice(0, p.lastIndexOf(' ')) : p)) : parts;
  return shown.length > 1 ? `${shown.slice(0, -1).join(', ')} & ${shown[shown.length - 1]}` : shown[0] || '–';
}

/** A conversion's day, when it happens: with the bank's deals, the day(s) its euros leave (and the shekels come in) and
 * when EUR/ILS is fixed for them, "Wed 4 Nov (fixed Mon 2 Nov)": the parts selling at the rate shown, else all of its
 * parts, and once fixed the rate it was fixed at. Without, the table's day, "Sat 7 Nov (weekend)". */
export function conversionDays(f: Fixing, fixedAt: { rate: number } | null = null, parts: { expiry: string | null; settle: string }[] | null = null): string {
  if (!f.fixedOn || !f.paidOn) return `${formatDay(f.date)}${f.weekend ? ' (weekend)' : ''}`;
  const some = parts && parts.length ? parts : null;
  const on = some ? [...new Set(some.map((p) => p.settle))].sort() : f.paidOn;
  const fixed = some ? [...new Set(some.map((p) => p.expiry).filter((d): d is string => !!d))].sort() : f.fixedOn;
  return `${formatDays(on)} (fixed ${formatDays(fixed.length ? fixed : f.fixedOn)}${fixedAt ? ` at ${formatRate(fixedAt.rate)}` : ''})`;
}

export const formatPct0 = (p: number) => `${Math.round(p * 100)}%`;
