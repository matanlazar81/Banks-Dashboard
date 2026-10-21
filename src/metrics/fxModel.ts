// Pure: what the FX Challenge tab shows, from GET /api/metrics?view=fx and the rate being simulated. No React.
import {
  bandIndex, bandLabels, checkPast, conversionAt, eurPosition, fixingRisk, fixings, planReserve, typicalInflow,
  type Conversion, type EurPosition, type Fixing, type FixingRisk, type PastCheck, type Policy, type ReservePlan,
} from '../forecast/fx-challenge.mjs';
import type { FxPayload } from './types.ts';

/** The volatility used when NetSuite's history is too short: about EUR/ILS's over the last two years. */
export const FALLBACK_SIGMA = 0.075;

export interface ScheduleRow { fixing: Fixing; atSpot: Conversion | null; atSim: Conversion | null; risk: FixingRisk | null }
export interface YearTotal { year: string; eur: number; ils: number; vsSpotIls: number; expectedEur: number; count: number }
export interface PastRow extends PastCheck { others: { date: string; eur: number; ils: number }[] }

export interface FxModel {
  labels: string[];
  /** Today's EUR/ILS (NetSuite), and the rate being simulated (today's unless one is typed). */
  spot: number | null;
  spotDate: string | null;
  simRate: number | null;
  spotBand: number | null;
  simBand: number | null;
  sigma: number;
  sigmaFromHistory: boolean;
  schedule: ScheduleRow[];
  totals: YearTotal[];
  position: EurPosition | null;
  inflow: ReturnType<typeof typicalInflow>;
  plan: ReservePlan | null;
  past: PastRow[];
}

export function buildFxModel(data: FxPayload, simRate: number | null, policy: Policy): FxModel {
  const { table, today } = data;
  const bounds = table.bounds;
  const spot = data.rates.spot ? data.rates.spot.rate : null;
  const sim = simRate && simRate > 0 ? simRate : spot;
  const sigmaFromHistory = !!(data.rates.sigma && data.rates.sigma > 0);
  const sigma = sigmaFromHistory ? (data.rates.sigma as number) : FALLBACK_SIGMA;
  const all = fixings(table, today);

  const schedule: ScheduleRow[] = all.filter((f) => f.status !== 'past').map((f) => ({
    fixing: f,
    atSpot: spot ? conversionAt(f.row, bounds, spot) : null,
    atSim: sim ? conversionAt(f.row, bounds, sim) : null,
    risk: spot ? fixingRisk(f.row, bounds, spot, sigma, f.days / 365) : null,
  }));

  const byYear = new Map<string, YearTotal>();
  for (const s of schedule) {
    const year = s.fixing.month.slice(0, 4);
    const t = byYear.get(year) || { year, eur: 0, ils: 0, vsSpotIls: 0, expectedEur: 0, count: 0 };
    if (s.atSim) { t.eur += s.atSim.eur; t.ils += s.atSim.ils; t.vsSpotIls += s.atSim.vsSpotIls; }
    if (s.risk) t.expectedEur += s.risk.expectedEur;
    t.count += 1;
    byYear.set(year, t);
  }

  const position = data.balances ? eurPosition(data.balances.rows) : null;
  const inflow = typicalInflow(data.flows, data.inflowMonths);
  const upcoming = schedule.flatMap((s) => (s.atSpot && s.risk
    ? [{ month: s.fixing.month, date: s.fixing.date, days: s.fixing.days, conv: s.atSpot, risk: s.risk }] : []));
  const plan = position && upcoming.length
    ? planReserve({ upcoming, policy, eur: position, inflowPerDay: inflow ? inflow.perDay : null, today })
    : null;

  const past = all.filter((f) => f.status === 'past').map((f) => {
    const check = checkPast({ fixing: f, bounds, series: data.rates.series, conversions: data.conversions });
    const others = data.conversions
      .filter((c) => c.date.slice(0, 7) === f.month && (!check.booked || c.date !== check.booked.date))
      .map((c) => ({ date: c.date, eur: c.eur, ils: c.ils }));
    return { ...check, others };
  });

  return {
    labels: bandLabels(bounds),
    spot,
    spotDate: data.rates.spot ? data.rates.spot.date : null,
    simRate: sim,
    spotBand: spot ? bandIndex(bounds, spot) : null,
    simBand: sim ? bandIndex(bounds, sim) : null,
    sigma,
    sigmaFromHistory,
    schedule,
    totals: [...byYear.values()],
    position,
    inflow,
    plan,
    past,
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
