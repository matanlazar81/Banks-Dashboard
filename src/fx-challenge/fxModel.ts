// Pure: what the FX Challenge page shows, from GET /api/fx-challenge and the rate being simulated. No React.
// The recommendation itself (planFromPayload, todayActions and their words) is shared with the daily mail.
import {
  bandIndex, bandLabels, conversionAt, describeAction, describeDue, describeKeep, keepCover, nothingToday, pastConversions, planFromPayload, todayActions,
  type CashPlan, type Conversion, type EurPosition, type KeepCover, type PastMonth, type PastTotals, type Policy, type RateCheck, type ReservePlan,
  type ScheduleItem, type ShekelDue, type TodayAction,
} from '../forecast/fx-challenge.mjs';
import { formatEur } from '../metrics/model.ts';
import type { FxPayload } from './types.ts';

export interface ScheduleRow extends ScheduleItem {
  atSim: Conversion | null;
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
  const { table } = data;
  const bounds = table.bounds;
  const base = planFromPayload(data, policy);
  const sim = simRate && simRate > 0 ? simRate : base.spot;
  const schedule: ScheduleRow[] = base.schedule.map((s) => ({ ...s, atSim: sim ? conversionAt(s.fixing.row, bounds, sim) : null }));

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

/** "Sat 7 Nov" (or "Sat 7 Nov 2027" with the year). */
export function formatDay(iso: string | null | undefined, withYear = false): string {
  if (!iso) return '–';
  return new Date(`${iso.slice(0, 10)}T12:00:00Z`).toLocaleDateString('en-GB', {
    weekday: 'short', day: 'numeric', month: 'short', ...(withYear ? { year: 'numeric' } : {}), timeZone: 'UTC',
  });
}

export const formatPct0 = (p: number) => `${Math.round(p * 100)}%`;
