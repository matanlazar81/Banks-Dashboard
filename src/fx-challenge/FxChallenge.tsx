// Business Tools → FX Challenge: finance's monthly EUR→ILS conversions at Bank Hapoalim (fixed on the 7th by the
// conversion table), simulated at any rate, and how much EUR to keep for them (GET /api/fx-challenge; the logic is
// src/forecast/fx-challenge.mjs, the same as the server's tests).
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { Loader2, RefreshCw } from 'lucide-react';
import { ErrorCard, Segmented, Skeleton, Warnings } from '../new-dashboard/PageParts.tsx';
import { KpiCard } from '../new-dashboard/KpiStrip.tsx';
import { readParam, writeParam } from '../new-dashboard/urlParams.ts';
import { parseTable, POLICIES, POLICY_LABEL, tableToText, type DepositRelease, type FxTable, type OutcomeRange, type PathPoint, type Policy } from '../forecast/fx-challenge.mjs';
import { formatEur, monthName } from '../metrics/model.ts';
import { fetchFx, saveTable } from './api.ts';
import { buildFxModel, formatDay, formatIls, formatPct0, formatRate, type FxModel, type ScheduleRow } from './fxModel.ts';
import type { FxPayload } from './types.ts';

type Phase = 'loading' | 'ready' | 'error';

function useFx() {
  const [phase, setPhase] = useState<Phase>('loading');
  const [data, setData] = useState<FxPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const load = useCallback(async (refresh: boolean) => {
    setBusy(true);
    try {
      const body = await fetchFx(refresh);
      if (body.status === 'ready') { setData(body); setPhase('ready'); setError(null); }
      else { setError(body.error); setPhase((p) => (p === 'ready' ? p : 'error')); }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPhase((p) => (p === 'ready' ? p : 'error'));
    } finally {
      setBusy(false);
    }
  }, []);
  useEffect(() => { void load(false); }, [load]);
  return { phase, data, error, busy, refresh: () => void load(true), reload: () => load(false) };
}

function Card({ title, aside, children }: { title: string; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="rounded-lg border border-slate-200 bg-white">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-2.5">
        <h2 className="text-sm font-semibold text-slate-800">{title}</h2>
        {aside}
      </div>
      <div className="px-4 py-3">{children}</div>
    </section>
  );
}

const BUTTON = 'inline-flex items-center gap-1 rounded-md border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 disabled:opacity-50';
const INPUT = 'rounded border border-slate-300 px-1.5 py-1 text-xs tabular-nums focus:border-sky-500 focus:outline-none';
const TH = 'px-2 py-1.5 text-right font-medium text-slate-500';
const TD = 'px-2 py-1.5 text-right tabular-nums';
const SOURCE_LABEL = { mail: "the daily balances mail's sources", books: "NetSuite's books (EUR only)" } as const;
const THL = 'px-2 py-1.5 text-left font-medium text-slate-500';
/** Marks text that explains itself on hover. */
const HINT = 'cursor-help underline decoration-slate-300 decoration-dotted underline-offset-2';

/** A column header whose explanation shows on hover. */
function Th({ tip, left = false, children }: { tip: string; left?: boolean; children: ReactNode }) {
  return <th className={left ? THL : TH} title={tip}><span className={HINT}>{children}</span></th>;
}

/** One line per deposit: its date, its value then and its kind. */
const depositLines = (list: DepositRelease[]) => list.map((d) => `${formatDay(d.valueDate, true)} · ${formatEur(d.amount)} · ${d.label}`);

/** "below 3.3", "3.3 – 3.49", "4.295 or more": a range of EUR/ILS (a rate on an edge belongs to the range above). */
const rangeLabel = (o: OutcomeRange) => (o.lo === null ? `below ${o.hi}` : o.hi === null ? `${o.lo} or more` : `${o.lo} – ${o.hi}`);
const chance = (p: number) => (p < 0.005 ? '<1%' : `${Math.round(p * 100)}%`);
/** A conversion's outcomes, one line each, for a hover. */
const outcomeTip = (date: string, list: OutcomeRange[] | null | undefined) => (list ? [
  `If EUR/ILS on ${formatDay(date, true)} is:`,
  ...list.filter((o) => o.p >= 0.005).map((o) => `${rangeLabel(o)}: ${chance(o.p)} → ${formatEur(o.eur)} at ${o.rate}`),
].join('\n') : undefined);

export default function FxChallenge() {
  const { phase, data, error, busy, refresh, reload } = useFx();
  const [rateText, setRateText] = useState<string | null>(null);
  const [policy, setPolicyState] = useState<Policy>(() => {
    const p = readParam('reserve');
    return (POLICIES as string[]).includes(p || '') ? (p as Policy) : 'p95';
  });
  const setPolicy = (p: Policy) => { setPolicyState(p); writeParam('reserve', p === 'p95' ? null : p); };
  const typed = rateText !== null ? parseFloat(rateText) : NaN;
  const simRate = Number.isFinite(typed) && typed > 0 ? typed : null;
  const model = useMemo(() => (data ? buildFxModel(data, simRate, policy) : null), [data, simRate, policy]);

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-6">
          <div>
            <h1 className="text-lg font-semibold text-slate-900">FX Challenge</h1>
            <p className="text-sm text-slate-500">LSports · the EUR→ILS conversions on the 7th of each month, and what to keep for them</p>
          </div>
          <button type="button" onClick={refresh} disabled={busy} className={BUTTON} title="Read the rates, balances and conversions from NetSuite again">
            <RefreshCw size={14} className={busy ? 'animate-spin' : ''} />{busy ? 'Updating…' : 'Refresh'}
          </button>
        </div>
      </header>

      <main className="space-y-4 px-4 py-4 sm:px-6">
        {phase === 'loading' && <Skeleton />}
        {phase === 'error' && !data && <ErrorCard message={error} onRetry={refresh} />}
        {data && model && (
          <>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
              <span>Updated {new Date(data.computedAt).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })}</span>
              <span aria-hidden="true">·</span>
              <span>EUR/ILS {formatRate(model.spot)} on {formatDay(model.spotDate)} ({data.rates.provider} in NetSuite)</span>
              <span aria-hidden="true">·</span>
              <span title={model.sigmaFromHistory ? "From NetSuite's daily rates over the last 400 days" : 'Too little history in NetSuite: a typical EUR/ILS volatility is used'}>
                Volatility {formatPct0(model.sigma)} a year{model.sigmaFromHistory ? '' : ' (assumed)'}
              </span>
              {data.balances && (<><span aria-hidden="true">·</span><span>Balances: {SOURCE_LABEL[data.balances.source]}{data.balances.asOf ? `, ${new Date(data.balances.asOf).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })}` : ''}</span></>)}
              {error && <span className="text-amber-700">Last update failed: {error}</span>}
            </div>
            <Warnings warnings={data.warnings} />
            <Kpis model={model} />
            <Recommendation model={model} policy={policy} onPolicy={setPolicy} inflowMonths={data.inflowMonths} />
            <Simulation model={model} rateText={rateText} onRate={setRateText} />
            <TableCard data={data} model={model} onSaved={reload} />
            <Balances data={data} model={model} />
            <PastFixings model={model} />
            <p className="max-w-5xl text-xs leading-relaxed text-slate-500">
              On each conversion day (the 7th), that day&apos;s EUR/ILS picks a band of the table, and the cell is the euros sold
              (thousands) and the deals&apos; blended rate. A rate on a band&apos;s edge counts in the band above it; the deals&apos; documents
              decide the exact edge. The chance of each band comes from EUR/ILS moving like it has (volatility from NetSuite&apos;s
              daily rates) between today and the conversion day, around today&apos;s rate. The typical inflow is the EUR that came in
              less what was paid in EUR, before conversions, deposits and transfers, averaged over the last three closed months.
              Deposits that mature before a conversion day count for it; later ones are the backup.
            </p>
          </>
        )}
      </main>
    </div>
  );
}

// ── KPIs ──
function Kpis({ model }: { model: FxModel }) {
  const next = model.schedule[0];
  const plan = model.plan ? model.plan.next : null;
  return (
    <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <KpiCard label="EUR/ILS today" value={model.spot} ccy="eur" display={formatRate(model.spot)}
        note={model.spotBand !== null ? `Band ${model.labels[model.spotBand]} · ${formatDay(model.spotDate)}` : 'No rate from NetSuite'} />
      <KpiCard label="Next conversion" value={next && next.atSpot ? next.atSpot.eur : null} ccy="eur"
        display={next && next.atSpot ? `${formatEur(next.atSpot.eur)} → ${formatIls(next.atSpot.ils)}` : undefined}
        note={next ? `${formatDay(next.fixing.date)}${next.fixing.weekend ? ' (weekend)' : ''} at today's rate${next.atSpot ? `, ${next.atSpot.rate}` : ''}` : 'No conversion left in the table'} />
      <KpiCard label="Keep for it" value={plan ? plan.required : null} ccy="eur" display={plan ? formatEur(plan.required) : undefined}
        note={model.plan ? POLICY_LABEL[model.plan.policy] : 'Needs balances'} />
      <KpiCard label="Free to convert now" value={plan ? plan.freeNow : null} ccy="eur" tone={plan && plan.shortfall > 0 ? 'warn' : 'default'}
        display={plan ? formatEur(plan.freeNow) : undefined}
        note={plan ? (plan.shortfall > 0 ? `Short ${formatEur(plan.shortfall)} for the conversion` : plan.freeByFixing !== null ? `${formatEur(plan.freeByFixing)} before the conversion, with typical inflows` : 'Keeping the reserve liquid') : 'Needs balances'} />
    </div>
  );
}

// ── the recommendation ──
const KEEP_TIP: Record<Policy, string> = {
  today: "What the conversion sells if EUR/ILS is where it is today: that band's cell in the table. It is paid from Hapoalim EUR (583400).",
  p95: "Enough for 19 conversions in 20: how far EUR/ILS can move by that day from today, with the volatility of NetSuite's daily rates. It is paid from Hapoalim EUR (583400).",
  worst: "The largest amount in that month's row of the table, whatever the rate. It is paid from Hapoalim EUR (583400).",
};
const PATH_TIP = {
  fixing: "The conversion day, the 7th of each month: that day's EUR/ILS picks the band of the table. On a weekend the deal sets the day.",
  expected: "What the conversion sells on average: each band's euros, weighted by the chance that EUR/ILS lands in it that day, from today's rate. The bank sells one cell; this is for planning. Hover a figure for the chances.",
  released: 'EUR deposits that mature after the previous conversion (after today, for the first) and by this one, at the bank\'s value at maturity with interest. Hover a figure for each deposit and its date. They count only if the bank pays them out rather than rolling them over; a blocked deposit is left out.',
  available: "What you would hold on the day if nothing else is converted: today's liquid euros, plus the typical inflow and the deposits released by then, less what the earlier conversions take (at the amount kept).",
  convertible: 'How much you can convert yourself before this conversion, in total, as the euros come in, with this conversion and every later one still covered: the smallest spare from this row on. Euros converted now are gone at every later conversion too.',
};

const releaseTip = (p: PathPoint) => [
  `${p.releasedDeposits.length} deposit${p.releasedDeposits.length === 1 ? '' : 's'} maturing by ${formatDay(p.date, true)}:`,
  ...depositLines(p.releasedDeposits),
].join('\n');

function Recommendation({ model, policy, onPolicy, inflowMonths }: { model: FxModel; policy: Policy; onPolicy: (p: Policy) => void; inflowMonths: string[] }) {
  const plan = model.plan;
  const first = model.schedule[0];
  const aside = (
    <Segmented label="What to keep" value={policy} onChange={onPolicy}
      options={POLICIES.map((p) => ({ value: p, label: POLICY_LABEL[p], title: p === 'p95' ? 'Enough for 19 conversions in 20' : p === 'worst' ? "The largest amount in that month's row" : "What today's rate would sell" }))} />
  );
  if (!plan || !first || !first.risk || !first.atSpot) {
    return <Card title="What to keep, what is free" aside={aside}><p className="text-sm text-slate-500">The recommendation needs today&apos;s rate and the balances; see the notes above.</p></Card>;
  }
  const n = plan.next;
  // Deposits from NetSuite's books have no dates, so none of them shows as released on a day.
  const undated = !!model.position && model.position.deposits.length > 0 && model.position.deposits.every((d) => !d.valueDate);
  const later = plan.later.reduce((s, d) => s + d.amount, 0);
  return (
    <Card title="What to keep, what is free" aside={aside}>
      <div className="space-y-3 text-sm text-slate-700">
        <p className="text-base text-slate-900">
          Keep <strong>{formatEur(n.required)}</strong> in Hapoalim EUR (583400) for {formatDay(n.date)}
          {first.fixing.weekend ? ' (a weekend: the deal sets the day)' : ''}.
          {' '}Free to convert now: <strong>{formatEur(n.freeNow)}</strong>
          {n.freeNowWithBackup > n.freeNow ? <>, or <strong>{formatEur(n.freeNowWithBackup)}</strong> if the backup deposits cover a rate jump</> : null}.
        </p>
        <div className="space-y-1.5">
          <p className="text-[13px]">
            What the {formatDay(n.date)} conversion sells depends on EUR/ILS that day. At today&apos;s {formatRate(model.spot)}: {formatEur(n.todayAmount)} at
            {' '}{first.atSpot.rate} ({formatIls(first.atSpot.ils)}). On average (expected): <strong>{formatEur(first.risk.expectedEur)}</strong>.
          </p>
          <Outcomes s={first} spotBand={model.spotBand} />
        </div>
        <ul className="list-disc space-y-1 pl-5 text-[13px]">
          <li>
            Euros now: Hapoalim {formatEur(n.hapoalim)}, elsewhere {formatEur(n.other)}
            {n.moveToHapoalim > 0 ? <> (move <strong>{formatEur(n.moveToHapoalim)}</strong> to Hapoalim before the conversion)</> : null}
            {n.maturing > 0 ? `, deposits maturing by then ${formatEur(n.maturing)}` : ''}
            {n.backup > 0 ? `, backup deposits after it ${formatEur(n.backup)}` : ''}.
          </li>
          {n.shortfall > 0 && (
            <li className="text-rose-700">
              Today&apos;s euros are {formatEur(n.shortfall)} short of the reserve: it needs inflows before the conversion{n.backup > 0 ? ', or a backup deposit' : ''}.
            </li>
          )}
          {n.freeByFixing !== null && model.inflow && (
            <li>
              With the typical inflow ({formatEur(model.inflow.perMonth)} a month, {inflowMonths.map((m) => monthName(m).slice(0, 3)).join('–')}), about{' '}
              {formatEur(n.inflowUntil)} comes in before the conversion, so <strong>{formatEur(n.freeByFixing)}</strong> can be converted before it,
              in total, as the euros come in, with the next {plan.path.length} conversions still covered.
            </li>
          )}
        </ul>
        {plan.path.length > 1 && (
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b border-slate-200">
                  <Th left tip={PATH_TIP.fixing}>Conversion day</Th>
                  <Th tip={KEEP_TIP[plan.policy]}>Keep ({POLICY_LABEL[plan.policy].toLowerCase()})</Th>
                  <Th tip={PATH_TIP.expected}>Expected</Th>
                  <Th tip={undated ? `${PATH_TIP.released} NetSuite's books give no maturity dates, so this stays empty until the balances RESTlet is set up.` : PATH_TIP.released}>Deposits released</Th>
                  <Th tip={PATH_TIP.available}>Euros by then</Th>
                  <Th tip={PATH_TIP.convertible}>Can be converted before it</Th>
                </tr>
              </thead>
              <tbody>
                {plan.path.map((p) => (
                  <tr key={p.month} className="border-b border-slate-100">
                    <td className="px-2 py-1.5 text-left text-slate-700">{formatDay(p.date)}{p.month.slice(0, 4) !== model.schedule[0].fixing.month.slice(0, 4) ? ` ${p.month.slice(0, 4)}` : ''}</td>
                    <td className={TD}>{formatEur(p.required)}</td>
                    <td className={`${TD} text-slate-500`} title={outcomeTip(p.date, model.schedule.find((x) => x.fixing.month === p.month)?.outcomes)}>
                      <span className={HINT}>{formatEur(p.expected)}</span>
                    </td>
                    <td className={`${TD} text-slate-600`} title={p.releasedDeposits.length ? releaseTip(p) : undefined}>
                      {p.released > 0 ? <span className={HINT}>{formatEur(p.released)}</span> : '–'}
                    </td>
                    <td className={TD}>{formatEur(p.available)}</td>
                    <td className={`${TD} font-medium ${p.surplus < 0 ? 'text-rose-700' : 'text-emerald-700'}`}>{p.surplus < 0 ? `short ${formatEur(-p.surplus)}` : formatEur(p.convertibleBefore)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-1 text-[11px] text-slate-500">
              If nothing else is converted and inflows stay typical: the euros converted before a conversion day, in total, can reach that column and every conversion is still covered.
              {' '}Hover a header for what it means, an expected figure for its chances, and a deposits figure for each deposit and its date.
              {later > 0 && (
                <> After {formatDay(plan.path[plan.path.length - 1].date, true)}, a further <span className={HINT} title={depositLines(plan.later).join('\n')}>{formatEur(later)} in {plan.later.length} deposit{plan.later.length === 1 ? '' : 's'}</span> matures.</>
              )}
            </p>
          </div>
        )}
      </div>
    </Card>
  );
}

/** What the next conversion can sell, by the EUR/ILS range it lands in, with each range's chance. */
function Outcomes({ s, spotBand }: { s: ScheduleRow; spotBand: number | null }) {
  if (!s.outcomes) return null;
  return (
    <div className="overflow-x-auto">
      <table className="text-xs">
        <thead>
          <tr className="border-b border-slate-200">
            <Th left tip="EUR/ILS on the conversion day, by the table's ranges; neighbouring columns that sell the same amount are merged. A rate exactly on an edge counts in the range above.">If EUR/ILS on {formatDay(s.fixing.date)} is…</Th>
            <Th tip="How likely EUR/ILS is to be in that range on the day, from today's rate and how much EUR/ILS usually moves (the volatility above). An estimate, not a forecast.">Chance</Th>
            <Th tip="The euros the bank converts if the rate lands in that range.">It sells</Th>
            <Th tip="The deal's rate for that amount.">At</Th>
            <Th tip="The shekels received: the euros times the rate.">You receive</Th>
          </tr>
        </thead>
        <tbody>
          {s.outcomes.map((o) => {
            const today = spotBand !== null && o.bands.includes(spotBand);
            return (
              <tr key={o.bands[0]} className={`border-b border-slate-100 ${today ? 'bg-sky-50 font-medium text-slate-900' : o.p < 0.01 ? 'text-slate-400' : 'text-slate-700'}`}>
                <td className="px-2 py-1.5 text-left">{rangeLabel(o)}{today ? ' (today)' : ''}</td>
                <td className={TD}>{chance(o.p)}</td>
                <td className={TD}>{formatEur(o.eur)}</td>
                <td className={TD}>{o.rate}</td>
                <td className={TD}>{formatIls(o.ils)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── the simulation ──
const PRESETS = [-0.05, -0.02, 0, 0.02, 0.05];

function Simulation({ model, rateText, onRate }: { model: FxModel; rateText: string | null; onRate: (v: string | null) => void }) {
  const rate = model.simRate ?? 3.5;
  return (
    <Card title="Simulate the rate on the coming conversions" aside={<span className="text-[11px] text-slate-500">The recommendation stays on today&apos;s rate; this shows what each conversion sells at the rate you set.</span>}>
      <div className="mb-3 flex flex-wrap items-center gap-3 text-xs text-slate-600">
        <label className="inline-flex items-center gap-1.5">EUR/ILS
          <input aria-label="EUR/ILS to simulate" type="number" step={0.005} min={2} max={6} className={`${INPUT} w-20 text-right`}
            value={rateText ?? formatRate(model.spot)} onChange={(e: { target: { value: string } }) => onRate(e.target.value)} />
        </label>
        <input aria-label="EUR/ILS slider" type="range" min={3} max={4.5} step={0.005} value={rate} className="w-56 accent-sky-600"
          onChange={(e: { target: { value: string } }) => onRate(e.target.value)} />
        <div className="inline-flex gap-1">
          {PRESETS.map((p) => (
            <button key={p} type="button" className={BUTTON} disabled={!model.spot}
              onClick={() => onRate(p === 0 || !model.spot ? null : (model.spot * (1 + p)).toFixed(3))}>
              {p === 0 ? 'Today' : `${p > 0 ? '+' : ''}${Math.round(p * 100)}%`}
            </button>
          ))}
        </div>
        {model.simBand !== null && <span>Band <strong className="text-slate-900">{model.labels[model.simBand]}</strong></span>}
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b border-slate-200">
              <Th left tip="Each month left in the table, and its conversion day (the 7th; weekends flagged).">Conversion day</Th>
              <Th tip="The euros that conversion sells if EUR/ILS is at the simulated rate on the day: the table's cell for that band.">Sold at {formatRate(model.simRate)}</Th>
              <Th tip="The deals' blended rate in that cell.">Rate</Th>
              <Th tip="The euros sold times the cell's rate.">₪ received</Th>
              <Th tip="Against selling the same euros at the simulated rate itself: what the deal gains, or loses, over converting at that rate.">₪ vs that rate</Th>
              <Th tip="What the conversion sells on average: each band's euros weighted by its chance, from today's rate (not the simulated one). Hover a figure for the chances.">Expected €</Th>
              <Th tip="Enough for 19 conversions in 20, from today's rate.">95% €</Th>
              <Th tip="The largest amount in that month's row of the table.">Worst €</Th>
            </tr>
          </thead>
          <tbody>
            {model.schedule.map((s) => (
              <tr key={s.fixing.month} className="border-b border-slate-100">
                <td className="px-2 py-1.5 text-left text-slate-700">
                  {monthName(s.fixing.month)} <span className="text-slate-400">· {formatDay(s.fixing.date)}{s.fixing.weekend ? ' (weekend)' : ''}</span>
                </td>
                <td className={`${TD} font-medium text-slate-900`}>{s.atSim ? formatEur(s.atSim.eur) : '–'}</td>
                <td className={TD}>{s.atSim ? s.atSim.rate : '–'}</td>
                <td className={TD}>{s.atSim ? formatIls(s.atSim.ils) : '–'}</td>
                <td className={`${TD} ${s.atSim && s.atSim.vsSpotIls < 0 ? 'text-rose-700' : 'text-emerald-700'}`}>{s.atSim ? formatIls(s.atSim.vsSpotIls) : '–'}</td>
                <td className={`${TD} text-slate-500`} title={outcomeTip(s.fixing.date, s.outcomes)}>{s.risk ? <span className={HINT}>{formatEur(s.risk.expectedEur)}</span> : '–'}</td>
                <td className={`${TD} text-slate-500`}>{s.risk ? formatEur(s.risk.p95Eur) : '–'}</td>
                <td className={`${TD} text-slate-500`}>{s.risk ? formatEur(s.risk.worstEur) : '–'}</td>
              </tr>
            ))}
            {model.totals.map((t) => (
              <tr key={t.year} className="bg-slate-50 font-semibold text-slate-900">
                <td className="px-2 py-1.5 text-left">{t.year} ({t.count} conversion{t.count === 1 ? '' : 's'} left)</td>
                <td className={TD}>{formatEur(t.eur)}</td>
                <td className={TD}>{t.eur ? (t.ils / t.eur).toFixed(3) : '–'}</td>
                <td className={TD}>{formatIls(t.ils)}</td>
                <td className={`${TD} ${t.vsSpotIls < 0 ? 'text-rose-700' : 'text-emerald-700'}`}>{formatIls(t.vsSpotIls)}</td>
                <td className={`${TD} text-slate-600`}>{formatEur(t.expectedEur)}</td>
                <td className={TD} />
                <td className={TD} />
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

// ── the conversion table ──
function TableCard({ data, model, onSaved }: { data: FxPayload; model: FxModel; onSaved: () => Promise<void> }) {
  const [editing, setEditing] = useState(false);
  const { table } = data;
  const max = Math.max(1, ...table.rows.flatMap((r) => r.cells.map((c) => Math.abs(c.eurK))));
  const saved = data.tableSaved;
  return (
    <Card title={`The conversion table (${table.pair}, € thousands sold / rate, converted on the ${table.fixingDay}th)`}
      aside={(
        <div className="flex items-center gap-2 text-[11px] text-slate-500">
          <span>{saved ? `Saved${saved.at ? ` ${new Date(saved.at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}` : ''}${saved.by ? ` by ${saved.by}` : ''}` : 'The table received in October 2026'}</span>
          <button type="button" className={BUTTON} onClick={() => setEditing((v) => !v)}>{editing ? 'Close' : 'Edit table'}</button>
        </div>
      )}>
      {editing && <EditTable table={table} saved={!!saved} onSaved={onSaved} onClose={() => setEditing(false)} />}
      <div className="overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0.5 text-[11px]">
          <thead>
            <tr>
              <th className="px-1.5 py-1 text-left font-medium text-slate-500" title="The conversion month: its row says what each band sells">
                <span className={HINT}>Month</span>
              </th>
              {model.labels.map((l, i) => (
                <th key={l} className={`px-1.5 py-1 text-center font-medium ${i === model.simBand ? 'rounded bg-sky-700 text-white' : 'text-slate-500'}`}
                  title={`EUR/ILS ${l} on the conversion day${i === model.spotBand ? " (today's band)" : ''}. A rate on an edge counts in the band above.`}>
                  <span className={HINT}>{l}</span>{i === model.spotBand ? ' •' : ''}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[...table.rows].sort((a, b) => a.month.localeCompare(b.month)).map((r) => {
              const past = model.past.some((p) => p.month === r.month);
              return (
                <tr key={r.month} className={past ? 'opacity-45' : ''}>
                  <td className="whitespace-nowrap px-1.5 py-1 text-left text-slate-700">{monthName(r.month)}</td>
                  {r.cells.map((c, i) => (
                    <td key={i} className={`whitespace-nowrap rounded px-1.5 py-1 text-center tabular-nums text-slate-800 ${i === model.simBand ? 'ring-2 ring-sky-700' : ''}`}
                      style={{ backgroundColor: `rgba(14, 116, 144, ${(0.06 + 0.42 * Math.abs(c.eurK) / max).toFixed(3)})` }}
                      title={`${monthName(r.month)}, ${model.labels[i]}: sell ${formatEur(-c.eurK * 1000)} at ${c.rate} = ${formatIls(-c.eurK * 1000 * c.rate)}`}>
                      {-c.eurK} · {c.rate}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-1.5 text-[11px] text-slate-500">The highlighted column is the band at the simulated rate; • marks today&apos;s band. Past conversions are dimmed.</p>
    </Card>
  );
}

function EditTable({ table, saved, onSaved, onClose }: { table: FxTable; saved: boolean; onSaved: () => Promise<void>; onClose: () => void }) {
  const [text, setText] = useState('');
  const [day, setDay] = useState(String(table.fixingDay));
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const parsed = useMemo(() => (text.trim() ? parseTable(text, { fixingDay: parseInt(day, 10) || 0, pair: table.pair }) : null), [text, day, table.pair]);
  const same = !!(parsed && parsed.ok && JSON.stringify(parsed.table) === JSON.stringify({ ...table, rows: [...table.rows].sort((a, b) => a.month.localeCompare(b.month)) }));

  async function save(value: FxTable | null) {
    setSaving(true);
    setErr(null);
    try {
      await saveTable(value);
      await onSaved();
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="mb-3 space-y-2 rounded-md border border-sky-200 bg-sky-50/50 p-3 text-xs text-slate-700">
      <p>
        Paste the range from Excel: the header row of bands (&lt;3.3, 3.3-3.4, … &gt;4.295), then one row per month (10/2026, then
        &quot;-500 / 3.62&quot; per band). It is checked before it can be saved, and saved for everyone (each save is kept in the history).
      </p>
      <textarea aria-label="The conversion table, pasted from Excel" value={text} onChange={(e: { target: { value: string } }) => setText(e.target.value)}
        rows={6} spellCheck={false} className="w-full rounded border border-slate-300 bg-white p-2 font-mono text-[11px] focus:border-sky-500 focus:outline-none" />
      <div className="flex flex-wrap items-center gap-2">
        <button type="button" className={BUTTON} onClick={() => setText(tableToText(table))}>Start from the current table</button>
        <label className="inline-flex items-center gap-1">Conversion day
          <input aria-label="Conversion day" type="number" min={1} max={28} value={day} onChange={(e: { target: { value: string } }) => setDay(e.target.value)} className={`${INPUT} w-14 text-right`} />
        </label>
        <button type="button" className={BUTTON} disabled={saving || !parsed || !parsed.ok || same} onClick={() => parsed && parsed.table && void save(parsed.table)}>
          {saving && <Loader2 size={12} className="animate-spin" />}Save for everyone
        </button>
        {saved && <button type="button" className={BUTTON} disabled={saving} onClick={() => void save(null)}>Back to the table received in October 2026</button>}
        <button type="button" className={BUTTON} onClick={onClose}>Cancel</button>
      </div>
      {parsed && parsed.ok && parsed.table && (
        <p className="text-emerald-700">
          {parsed.table.rows.length} months ({monthName(parsed.table.rows[0].month)} – {monthName(parsed.table.rows[parsed.table.rows.length - 1].month)}),
          {' '}{parsed.table.bounds.length + 1} bands, converted on the {parsed.table.fixingDay}th{same ? ': the same as the current table.' : '.'}
        </p>
      )}
      {parsed && !parsed.ok && (
        <ul className="list-disc pl-5 text-rose-700">
          {parsed.errors.slice(0, 6).map((e) => <li key={e}>{e}</li>)}
          {parsed.errors.length > 6 && <li>…and {parsed.errors.length - 6} more.</li>}
        </ul>
      )}
      {err && <p className="text-rose-700">{err}</p>}
    </div>
  );
}

// ── balances ──
const KIND_LABEL: Record<string, string> = { current: 'Account', deposit: 'Deposit', card: 'Card', other: 'Other' };

function Balances({ data, model }: { data: FxPayload; model: FxModel }) {
  const b = data.balances;
  if (!b) return null;
  const lsports = b.rows.filter((r) => !r.company || /lsports/i.test(r.company));
  // Accounts first, largest first; then the deposits by the day they mature.
  const eur = lsports.filter((r) => r.currency === 'EUR' && (r.kind === 'current' || r.kind === 'deposit'))
    .sort((x, y) => {
      if (x.kind !== y.kind) return x.kind === 'current' ? -1 : 1;
      if (x.kind === 'deposit') return (x.valueDate || '9999').localeCompare(y.valueDate || '9999') || Math.abs(y.amount) - Math.abs(x.amount);
      return Math.abs(y.amount) - Math.abs(x.amount);
    });
  const otherCcy = lsports.filter((r) => r.currency !== 'EUR' && (r.kind === 'current' || r.kind === 'deposit'));
  const byCcy = new Map<string, number>();
  for (const r of otherCcy) byCcy.set(r.currency, (byCcy.get(r.currency) || 0) + r.amount);
  const elsewhere = b.rows.filter((r) => r.company && !/lsports/i.test(r.company));
  const p = model.position;
  return (
    <Card title={`LSports euros (${SOURCE_LABEL[b.source]})`}>
      <div className="grid gap-4 lg:grid-cols-[2fr_1fr]">
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="border-b border-slate-200">
                <Th left tip="The bank, and for a deposit its kind as the bank names it.">Where</Th>
                <Th left tip="Account: money held now. Deposit: paid out on the day it matures. Blocked: held by the bank, left out of the plan.">Kind</Th>
                <Th tip={b.source === 'mail' ? "The bank's balance; for a deposit, its value on the day it matures, with interest (the bank's API gives no value for today)." : "NetSuite's books, in euros."}>EUR</Th>
                <Th tip={b.source === 'mail' ? "For an account, the date of the bank's balance; for a deposit, the day it matures." : "NetSuite's books give no maturity dates."}>{b.source === 'mail' ? 'As of / matures' : 'Matures'}</Th>
              </tr>
            </thead>
            <tbody>
              {eur.map((r, i) => (
                <tr key={`${r.bank}-${r.kind}-${i}`} className="border-b border-slate-100">
                  <td className="px-2 py-1.5 text-left text-slate-700">{r.label && r.kind === 'deposit' ? `${r.bank} · ${r.label}` : r.bank}</td>
                  <td className="px-2 py-1.5 text-left text-slate-500">{KIND_LABEL[r.kind]}{r.blocked ? ' (blocked)' : ''}</td>
                  <td className={`${TD} text-slate-900`}>{formatEur(r.amount)}</td>
                  <td className={`${TD} text-slate-500`}>{r.kind === 'deposit' ? (r.valueDate ? formatDay(r.valueDate, true) : 'not in the books') : (r.asOf || '–')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="space-y-1.5 text-xs text-slate-600">
          {p && (
            <>
              <div className="flex justify-between">
                <span className={HINT} title="LSports' euro current account at Bank Hapoalim (583400, NetSuite 11001). The conversions are paid from it, so the amount to keep has to be here on the day.">Hapoalim EUR cash (583400)</span>
                <strong className="tabular-nums text-slate-900">{formatEur(p.hapoalim)}</strong>
              </div>
              <div className="flex justify-between">
                <span className={HINT} title="Euros in LSports' other accounts (PKO, PayPal, Payhound). They count for a conversion once moved to Hapoalim EUR.">Other EUR accounts (movable)</span>
                <strong className="tabular-nums text-slate-900">{formatEur(p.other)}</strong>
              </div>
              <div className="flex justify-between">
                <span className={HINT} title="LSports' EUR deposits, each at its value on the day it matures; it counts from that day. They are listed on the left by that day.">EUR deposits</span>
                <strong className="tabular-nums text-slate-900">{formatEur(p.deposits.reduce((s, d) => s + d.amount, 0))}</strong>
              </div>
            </>
          )}
          {byCcy.size > 0 && (
            <p className="border-t border-slate-100 pt-1.5 text-[11px] text-slate-500">
              Not used by the conversions: {[...byCcy.entries()].map(([c, v]) => `${c} ${Math.round(v).toLocaleString('en-US')}`).join(' · ')}.
            </p>
          )}
          {elsewhere.length > 0 && <p className="text-[11px] text-slate-500">Other companies&apos; balances (e.g. Bringits) are left out: the conversions are LSports&apos;.</p>}
          {b.source === 'books' && <p className="text-[11px] text-amber-700">Deposit maturities come with the daily mail&apos;s figures (the balances RESTlet). Until then every deposit is counted as backup.</p>}
          {b.notes.map((n) => <p key={n} className="text-[11px] text-slate-500">{n}</p>)}
        </div>
      </div>
    </Card>
  );
}

// ── past fixings ──
function PastFixings({ model }: { model: FxModel }) {
  if (!model.past.length) return null;
  return (
    <Card title="Past conversions: the table against what was booked">
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b border-slate-200">
              <Th left tip="A monthly conversion already past: its month and day.">Conversion day</Th>
              <Th tip="NetSuite's HSBC EUR/ILS on the conversion day.">EUR/ILS that day</Th>
              <Th tip="What the table says at that day's rate: the euros sold and the deals' blended rate.">The table</Th>
              <Th tip={'The "Conversion" / המרה journals from Hapoalim EUR (11001) to ILS (10002) within 3 days of the conversion day; ✓ when they match the table.'}>Booked (Hapoalim, &quot;Conversion&quot;)</Th>
              <Th tip="Shekels received against selling the same euros at that day's rate.">₪ vs that day&apos;s rate</Th>
              <Th left tip="Your other conversions booked the same month, apart from the monthly one.">Other conversions that month</Th>
            </tr>
          </thead>
          <tbody>
            {model.past.map((p) => (
              <tr key={p.month} className="border-b border-slate-100 align-top">
                <td className="px-2 py-1.5 text-left text-slate-700">{monthName(p.month)} · {formatDay(p.date)}</td>
                <td className={TD}>{p.spot ? formatRate(p.spot.rate) : '–'}</td>
                <td className={TD}>{p.conv ? `${formatEur(p.conv.eur)} @ ${p.conv.rate}` : '–'}</td>
                <td className={`${TD} ${p.booked ? (p.match ? 'text-emerald-700' : 'text-amber-700') : 'text-slate-400'}`}>
                  {p.booked ? `${formatDay(p.booked.date)}: ${formatEur(p.booked.eur)} → ${formatIls(p.booked.ils)} @ ${p.booked.rate ? p.booked.rate.toFixed(3) : '–'}${p.match ? ' ✓' : ' (differs)'}` : 'Nothing booked near it'}
                </td>
                <td className={`${TD} ${(p.gainVsSpotIls ?? 0) < 0 ? 'text-rose-700' : 'text-emerald-700'}`}>{p.gainVsSpotIls === null ? '–' : formatIls(p.gainVsSpotIls)}</td>
                <td className="px-2 py-1.5 text-left text-slate-500">
                  {p.others.length ? p.others.map((o) => `${formatDay(o.date)} ${formatEur(o.eur)} @ ${(o.ils / o.eur).toFixed(3)}`).join(' · ') : '–'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}
