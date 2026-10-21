// Business Tools → FX Challenge: finance's monthly EUR→ILS conversions at Bank Hapoalim (fixed on the 7th by the
// conversion table), simulated at any rate, and how much EUR to keep for them (GET /api/fx-challenge; the logic is
// src/forecast/fx-challenge.mjs, the same as the server's tests).
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { Loader2, RefreshCw } from 'lucide-react';
import { ErrorCard, Segmented, Skeleton, Warnings } from '../new-dashboard/PageParts.tsx';
import { KpiCard } from '../new-dashboard/KpiStrip.tsx';
import { readParam, writeParam } from '../new-dashboard/urlParams.ts';
import {
  parseTable, POLICIES, POLICY_LABEL, tableToText,
  type CashPlan, type DepositRelease, type FxTable, type OtherConversion, type OutcomeRange, type PathPoint, type Policy, type RateCheck, type ReservePlan,
} from '../forecast/fx-challenge.mjs';
import { formatEur, monthName } from '../metrics/model.ts';
import { fetchFx, fetchMail, saveMail, saveTable } from './api.ts';
import { buildFxModel, formatDay, formatIls, formatPct0, formatRate, type FxModel, type ScheduleRow } from './fxModel.ts';
import type { FxPayload, MailSettings } from './types.ts';

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

/** Euros in the workings: zero shows as €0, not –. */
const eur0 = (v: number) => (Math.abs(v) < 0.5 ? '€0' : formatEur(v));

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
            {model.cash && <ShekelsCard cash={model.cash} spot={model.spot} policy={policy} inflowMonths={data.inflowMonths} />}
            <Simulation model={model} rateText={rateText} onRate={setRateText} />
            <TableCard data={data} model={model} onSaved={reload} />
            <Balances data={data} model={model} />
            <PastFixings model={model} />
            <DailyMail />
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
  const cash = model.cash && plan ? model.cash : null;
  const needs = cash ? firstNeeds(cash) : [];
  const mustConvert = needs.reduce((s, x) => s + x.eur, 0);
  return (
    <div className={`grid grid-cols-2 gap-3 ${cash ? 'lg:grid-cols-5' : 'lg:grid-cols-4'}`}>
      <KpiCard label="EUR/ILS today" value={model.spot} ccy="eur" display={formatRate(model.spot)}
        note={model.spotBand !== null ? `Band ${model.labels[model.spotBand]} · ${formatDay(model.spotDate)}` : 'No rate from NetSuite'} />
      <KpiCard label="Next conversion" value={next && next.atSpot ? next.atSpot.eur : null} ccy="eur"
        display={next && next.atSpot ? `${formatEur(next.atSpot.eur)} → ${formatIls(next.atSpot.ils)}` : undefined}
        note={next ? `${formatDay(next.fixing.date)}${next.fixing.weekend ? ' (weekend)' : ''} at today's rate${next.atSpot ? `, ${next.atSpot.rate}` : ''}` : 'No conversion left in the table'} />
      <KpiCard label="Keep for it" value={plan ? plan.required : null} ccy="eur" display={plan ? formatEur(plan.required) : undefined}
        note={model.plan ? POLICY_LABEL[model.plan.policy] : 'Needs balances'} />
      {cash && plan ? (
        <>
          <KpiCard label="Convert for salaries" value={mustConvert} ccy="eur" display={eur0(mustConvert)}
            note={needs.length ? `By ${formatDay(needs[needs.length - 1].date)}: salaries, taxes and ILS vendors` : 'Covered by the shekels on hand and the conversion'} />
          <KpiCard label="Spare now" value={plan.spare} ccy="eur" tone={plan.short ? 'warn' : 'default'} display={eur0(plan.spare)}
            note={plan.short ? `Short ${formatEur(plan.short.first.eur)} by ${formatDay(plan.short.first.date)}` : 'For a new deposit, or to convert on a good day'} />
        </>
      ) : (
        <KpiCard label="Free to convert now" value={plan ? plan.freeNow : null} ccy="eur" tone={plan && plan.shortfall > 0 ? 'warn' : 'default'}
          display={plan ? formatEur(plan.freeNow) : undefined}
          note={plan ? (plan.shortfall > 0 ? `Short ${formatEur(plan.shortfall)} for the conversion` : plan.freeByFixing !== null ? `${formatEur(plan.freeByFixing)} before the conversion, with typical inflows` : 'Keeping the reserve liquid') : 'Needs balances'} />
      )}
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
  available: "What you would hold on the day if nothing else is converted: today's liquid euros, plus the typical inflow and the deposits released by then, less what the earlier conversions take (at the amount kept) and the conversions for salaries and vendors by then.",
  convertible: 'How much you can convert yourself before this conversion, in total, as the euros come in, with this conversion, every later one, the salaries and vendors still covered, and the euros for EUR vendors kept: the smallest spare from this row on. Euros converted now are gone at every later conversion too.',
  shekels: "Euros converted for salaries and ILS vendors after the previous conversion (after today, for the first) and by this one, at today's rate: what the shekels on hand and the table's conversions don't cover. Hover a figure for the days.",
};

/** The conversions for shekels up to the first month with salary payments (its 5th and 15th, and any before). */
function firstNeeds(cash: CashPlan) {
  const first = cash.months.find((m) => m.net > 0 || m.later > 0);
  return first ? cash.needs.filter((x) => x.month <= first.month) : [];
}
const WHY: Record<string, string> = { net: 'net salaries', later: 'pension, tax and social security', vendors: 'ILS vendors' };
const needLine = (x: CashPlan['needs'][number]) => `${formatDay(x.date, true)}: ${formatIls(x.ils)} → ${formatEur(x.eur)} (${x.why.map((w) => WHY[w] || w).join(', ')})`;

const releaseTip = (p: PathPoint) => [
  `${p.releasedDeposits.length} deposit${p.releasedDeposits.length === 1 ? '' : 's'} maturing by ${formatDay(p.date, true)}:`,
  ...depositLines(p.releasedDeposits),
].join('\n');

function Recommendation({ model, policy, onPolicy, inflowMonths }: { model: FxModel; policy: Policy; onPolicy: (p: Policy) => void; inflowMonths: string[] }) {
  const plan = model.plan;
  const first = model.schedule[0];
  // The conversion day whose workings are shown (clicking a row picks it), and whether they are open.
  const [picked, setPicked] = useState<string | null>(null);
  const [showWorkings, setShowWorkings] = useState(false);
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
  const chosen = picked && plan.path.some((p) => p.month === picked) ? picked : plan.path[0].month;
  const pick = (month: string) => { setPicked(month); setShowWorkings(true); };
  return (
    <Card title="What to keep, what is free" aside={aside}>
      <div className="space-y-3 text-sm text-slate-700">
        <p className="text-base text-slate-900">
          Keep <strong>{formatEur(n.required)}</strong> in Hapoalim EUR (583400) for {formatDay(n.date)}
          {first.fixing.weekend ? ' (a weekend: the deal sets the day)' : ''}.
          {model.cash ? null : (
            <>
              {' '}Free to convert now: <strong>{formatEur(n.freeNow)}</strong>
              {n.freeNowWithBackup > n.freeNow ? <>, or <strong>{formatEur(n.freeNowWithBackup)}</strong> if the backup deposits cover a rate jump</> : null}.
            </>
          )}
        </p>
        {model.cash && <CashLines cash={model.cash} plan={plan} today={model.today} />}
        {model.rateCheck && <RateLine rc={model.rateCheck} free={model.cash ? n.spare : n.freeNow} />}
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
              {formatEur(n.inflowUntil)} comes in before the conversion
              {n.freeByFixing > 0.5
                ? <>, so <strong>{formatEur(n.freeByFixing)}</strong> can be converted before it, in total, as the euros come in, with the next {plan.path.length} conversions{model.cash ? ', the salaries and vendors' : ''} still covered.</>
                : <>: {model.cash ? 'the salaries, vendors and this conversion need all of it' : 'this conversion needs all of it'}.</>}
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
                  {model.cash && <Th tip={PATH_TIP.shekels}>Salaries &amp; vendors</Th>}
                  <Th tip={PATH_TIP.available}>Euros by then</Th>
                  <Th tip={PATH_TIP.convertible}>Can be converted before it</Th>
                </tr>
              </thead>
              <tbody>
                {plan.path.map((p) => (
                  <tr key={p.month} onClick={() => pick(p.month)}
                    className={`cursor-pointer border-b border-slate-100 hover:bg-slate-50 ${showWorkings && p.month === chosen ? 'bg-sky-50' : ''}`}>
                    <td className="px-2 py-1.5 text-left text-slate-700">
                      <button type="button" className={HINT} title="Show how this row's figures are worked out">
                        {formatDay(p.date)}{p.month.slice(0, 4) !== model.schedule[0].fixing.month.slice(0, 4) ? ` ${p.month.slice(0, 4)}` : ''}
                      </button>
                    </td>
                    <td className={TD}>{formatEur(p.required)}</td>
                    <td className={`${TD} text-slate-500`} title={outcomeTip(p.date, model.schedule.find((x) => x.fixing.month === p.month)?.outcomes)}>
                      <span className={HINT}>{formatEur(p.expected)}</span>
                    </td>
                    <td className={`${TD} text-slate-600`} title={p.releasedDeposits.length ? releaseTip(p) : undefined}>
                      {p.released > 0 ? <span className={HINT}>{formatEur(p.released)}</span> : '–'}
                    </td>
                    {model.cash && (() => {
                      const prev = plan.path[plan.path.indexOf(p) - 1];
                      const here = model.cash.needs.filter((x) => x.date <= p.date && (prev ? x.date > prev.date : true));
                      return (
                        <td className={`${TD} text-slate-600`} title={here.length ? here.map(needLine).join('\n') : undefined}>
                          {p.shekels > 0.5 ? <span className={HINT}>{formatEur(p.shekels)}</span> : '–'}
                        </td>
                      );
                    })()}
                    <td className={TD} title={`${eur0(n.liquid)} cash today + ${eur0(p.inflow)} inflow + ${eur0(p.depositsBy)} deposits − ${eur0(p.earlier)} earlier conversions${model.cash ? ` − ${eur0(p.forShekels)} for salaries and vendors` : ''} = ${eur0(p.available)}. Click the row for the workings.`}>
                      <span className={HINT}>{formatEur(p.available)}</span>
                    </td>
                    <td className={`${TD} font-medium ${p.surplus < 0 ? 'text-rose-700' : 'text-emerald-700'}`}
                      title={`Spare on the day: ${eur0(p.available)} − ${eur0(p.required)}${n.eurFloor ? ` − ${eur0(n.eurFloor)} kept for EUR vendors` : ''} = ${eur0(p.surplus)}. ${p.limitedBy === p.month ? 'No later day is tighter' : `${formatDay(plan.path.find((x) => x.month === p.limitedBy)?.date)} is tighter`}, so ${eur0(p.convertibleBefore)}. Click the row for the workings.`}>
                      <span className={HINT}>{p.surplus < 0 ? `short ${formatEur(-p.surplus)}` : formatEur(p.convertibleBefore)}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-1 text-[11px] text-slate-500">
              If nothing else is converted and inflows stay typical: the euros converted before a conversion day, in total, can reach that column and every conversion is still covered.
              {' '}Hover a header for what it means and a figure for how it adds up; click a row for its full workings.
              {later > 0 && (
                <> After {formatDay(plan.path[plan.path.length - 1].date, true)}, a further <span className={HINT} title={depositLines(plan.later).join('\n')}>{formatEur(later)} in {plan.later.length} deposit{plan.later.length === 1 ? '' : 's'}</span> matures.</>
              )}
            </p>
          </div>
        )}
        <div>
          <button type="button" className={BUTTON} onClick={() => setShowWorkings((v) => !v)}>
            {showWorkings ? 'Hide the workings' : 'How are these figures worked out?'}
          </button>
        </div>
        {showWorkings && <Workings model={model} plan={plan} month={chosen} inflowMonths={inflowMonths} onClose={() => setShowWorkings(false)} />}
      </div>
    </Card>
  );
}

const RATE_WORD = { high: 'high', middle: 'mid-range', low: 'low' } as const;
const RATE_VERDICT = { high: 'a good day to convert', middle: 'an average day to convert', low: 'a weak day to convert' } as const;
const RATE_TONE = { high: 'text-emerald-700', middle: 'text-slate-900', low: 'text-amber-700' } as const;

/** Today's EUR/ILS against the last 90 days, and what the euros free now bring at it against the average. */
function RateLine({ rc, free }: { rc: RateCheck; free: number }) {
  const diff = free * (rc.rate - rc.average);
  return (
    <p className="text-[13px]" title={`Today's EUR/ILS against NetSuite's HSBC rates on the ${rc.n} business days of the last ${rc.days} days. High: above 3 days in 4 or more; low: above 1 in 4 or fewer. It only looks back: where EUR/ILS goes next is not known. Whatever you convert yourself, the conversion on the 7th still follows the table.`}>
      <span className={HINT}>EUR/ILS {formatRate(rc.rate)}</span> is {RATE_WORD[rc.level]} for the last {rc.days} days: above {formatPct0(rc.share)} of them
      {' '}(average {formatRate(rc.average)}, from {formatRate(rc.low)} to {formatRate(rc.high)}), so <strong className={RATE_TONE[rc.level]}>{RATE_VERDICT[rc.level]}</strong>
      {free > 0 ? <>: the {formatEur(free)} free now brings {formatIls(free * rc.rate)}, {formatIls(Math.abs(diff))} {diff >= 0 ? 'more' : 'less'} than at the average.</> : '. Nothing is spare to convert today.'}
    </p>
  );
}

/** When a conversion for shekels is due: "by Thu, 5 Nov", or now for the rest of this month's ILS vendors. */
const dueWhen = (x: CashPlan['needs'][number], today: string) => (x.date <= today ? "now, for the rest of this month's ILS vendors" : `by ${formatDay(x.date)}`);

/** The salaries and vendors to convert for, and what is spare: the recommendation's second line. */
function CashLines({ cash, plan, today }: { cash: CashPlan; plan: ReservePlan; today: string }) {
  const n = plan.next;
  const needs = firstNeeds(cash);
  const total = needs.reduce((s, x) => s + x.eur, 0);
  const due = cash.months.find((m) => m.net > 0 || m.later > 0);
  const by = due ? `${due.month}-15` : null;
  const ok = plan.deposits.filter((d) => d.eur > 0);
  return (
    <>
      <p className="text-[15px] text-slate-900">
        {needs.length ? (
          <>
            For the salaries and vendors due by {formatDay(by)}, convert at least <strong>{formatEur(total)}</strong>
            {needs.length > 1 ? <>: {needs.map((x, i) => <span key={x.date}>{i ? (i === needs.length - 1 ? ' and ' : ', ') : ''}{formatEur(x.eur)} {dueWhen(x, today)}</span>)}</> : <> {dueWhen(needs[0], today)}</>}.
          </>
        ) : (
          <>The shekels on hand and the {formatDay(n.date)} conversion cover the salaries and vendors due by {formatDay(by)}.</>
        )}
        {' '}{n.short ? (
          <span className="text-rose-700">
            Nothing is spare: on {formatDay(n.short.first.date)} the needs exceed the euros held and expected by <strong>{formatEur(n.short.first.eur)}</strong>
            {n.short.date !== n.short.first.date ? `, and by ${formatEur(n.short.eur)} on ${formatDay(n.short.date)}` : ''}
            {n.backup > 0 ? ', so a backup deposit may be needed' : ''}.
          </span>
        ) : (
          <>Spare now: <strong>{eur0(n.spare)}</strong>{n.spare > 0 ? ', for a new deposit or to convert on a good day' : ''}.</>
        )}
      </p>
      <p className="text-[13px]" title="A deposit's euros are away until it matures: how much can be locked until that day with every conversion, salary and vendor payment before it still met as the typical inflow comes in, and the euros for EUR vendors kept. Rounded down to €10K.">
        <span className={HINT}>New deposits</span>:{' '}
        {ok.length
          ? <>{ok.map((d) => `${formatEur(d.eur)} for ${d.months} month${d.months === 1 ? '' : 's'} (until ${formatDay(d.until)})`).join(', ')}, with everything before then still covered.</>
          : <>none now: the coming weeks need the euros held{n.short ? '' : ' and expected'}.</>}
      </p>
    </>
  );
}

/** Month by month: what the salaries and the ILS vendors need, what the table's conversion brings, what to convert. */
function ShekelsCard({ cash, spot, policy, inflowMonths }: { cash: CashPlan; spot: number | null; policy: Policy; inflowMonths: string[] }) {
  const months = inflowMonths.map((m) => monthName(m).slice(0, 3)).join('–');
  const total = cash.months.reduce((s, m) => ({ ils: s.ils + m.convertIls, eur: s.eur + m.convertEur }), { ils: 0, eur: 0 });
  const share = cash.share;
  return (
    <Card title="Salaries and vendors: the shekels each month needs">
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b border-slate-200">
              <Th left tip="The month the payments go out: the previous month's payroll, and the month's ILS vendor payments.">Paid in</Th>
              <Th tip="Net salaries, paid by the 5th: the previous month's Total payroll (the Salary Projection) times the net salaries' share of NetSuite's payroll journals.">Net salaries (by the 5th)</Th>
              <Th tip="Pension, withholding tax and social security, paid by the 15th: the rest of the previous month's payroll.">Pension, tax, social security (by the 15th)</Th>
              <Th tip={`ILS vendor payments: the average paid from the ILS accounts in ${months}, kept for vendors each month. This month: the rest of it.`}>ILS vendors</Th>
              <Th tip={`The shekels the conversion on the 7th brings when it sells the amount kept (${POLICY_LABEL[policy].toLowerCase()}), at that cell's rate.`}>The 7th&apos;s conversion</Th>
              <Th tip="What the shekels on hand and the 7th's conversion don't cover, converted by the day it is due, at today's rate. Hover a figure for the days.">Convert at least</Th>
            </tr>
          </thead>
          <tbody>
            {cash.months.map((m) => {
              const here = cash.needs.filter((x) => x.month === m.month);
              return (
                <tr key={m.month} className="border-b border-slate-100">
                  <td className="whitespace-nowrap px-2 py-1.5 text-left text-slate-700">
                    {monthName(m.month)} <span className="text-slate-400">· {m.payrollMonth && (m.net > 0.5 || m.later > 0.5) ? `${monthName(m.payrollMonth).slice(0, 3)} payroll` : 'the rest of the month'}</span>
                  </td>
                  <td className={TD}>{m.net > 0.5 ? formatIls(m.net) : '–'}</td>
                  <td className={TD}>{m.later > 0.5 ? formatIls(m.later) : '–'}</td>
                  <td className={TD}>{m.vendors > 0.5 ? formatIls(m.vendors) : '–'}</td>
                  <td className={`${TD} text-emerald-700`}>{m.tableIls > 0.5 ? `+${formatIls(m.tableIls)}` : '–'}</td>
                  <td className={`${TD} font-medium text-slate-900`} title={here.length ? here.map(needLine).join('\n') : undefined}>
                    {m.convertIls > 0.5 ? <span className={HINT}>{formatIls(m.convertIls)} · {formatEur(m.convertEur)}</span> : '–'}
                  </td>
                </tr>
              );
            })}
            <tr className="bg-slate-50 font-semibold text-slate-900">
              <td className="px-2 py-1.5 text-left">Until {formatDay(cash.until, true)}</td>
              <td className={TD} />
              <td className={TD} />
              <td className={TD} />
              <td className={TD} />
              <td className={TD}>{total.ils > 0.5 ? `${formatIls(total.ils)} · ${formatEur(total.eur)}` : '–'}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <ul className="mt-2 list-disc space-y-0.5 pl-5 text-[11px] text-slate-500">
        <li>Shekels on hand today: {cash.ilsNow === null ? 'not known' : formatIls(cash.ilsNow)} (LSports&apos; ILS accounts, the bank&apos;s balances). Converted at today&apos;s rate ({formatRate(spot)}).</li>
        <li>
          Payroll: the Salary Projection&apos;s Total payroll{cash.projectionAsOf ? ` (saved ${formatDay(cash.projectionAsOf.slice(0, 10))})` : ''}
          {share !== null ? `, split as NetSuite's payroll journals of ${months}: ${formatPct0(share)} net salaries (by the 5th of the next month), ${formatPct0(1 - share)} pension, withholding tax and social security (by the 15th).` : '.'}
        </li>
        <li>
          Vendors: {cash.vendors.ils === null ? 'ILS not known' : `${formatIls(cash.vendors.ils)} a month from the ILS accounts`}, and {formatEur(cash.vendors.eur)} a month in euros
          (kept in the euro accounts at all times), the averages of {months}.
        </li>
        {cash.notes.map((x) => <li key={x} className="text-amber-700">{x}</li>)}
      </ul>
    </Card>
  );
}

/** One worked figure: its steps, what each is based on, and the result on the last line. */
function Steps({ title, rows, note }: { title: string; rows: [string, string, string][]; note?: string }) {
  return (
    <div className="max-w-3xl">
      <p className="mb-1 font-semibold text-slate-800">{title}</p>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <tbody>
            {rows.map(([step, basis, amount], i) => (
              <tr key={step} className={i === rows.length - 1 ? 'border-t border-slate-300 font-semibold text-slate-900' : 'border-b border-slate-100'}>
                <td className="px-2 py-1 text-left align-top">
                  {step}
                  {basis && <span className="block text-[11px] font-normal text-slate-500">{basis}</span>}
                </td>
                <td className={`${TD} whitespace-nowrap align-top`}>{amount}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {note && <p className="mt-1 text-[11px] text-slate-500">{note}</p>}
    </div>
  );
}

/** How the plan's figures are worked out from today's numbers: what is free now (for the next conversion), and for
 * the chosen conversion day the euros held by then and what can be converted before it. */
function Workings({ model, plan, month, inflowMonths, onClose }: { model: FxModel; plan: ReservePlan; month: string; inflowMonths: string[]; onClose: () => void }) {
  const n = plan.next;
  const i = Math.max(0, plan.path.findIndex((x) => x.month === month));
  const p = plan.path[i];
  const limit = plan.path.find((x) => x.month === p.limitedBy) || p;
  const day = formatDay(p.date, true);
  const nextDay = formatDay(n.date, true);
  const setting = POLICY_LABEL[plan.policy].toLowerCase();
  const months = inflowMonths.map((m) => monthName(m).slice(0, 3)).join('–');
  const deposits = plan.path.slice(0, i + 1).reduce((s, x) => s + x.releasedDeposits.length, 0);
  const earlier = plan.path.slice(0, i);
  const freeNote = n.shortfall > 0
    ? `Today's cash is ${eur0(n.shortfall)} short of what is needed: money has to come in before the conversion${n.backup > 0 ? ', or a backup deposit be broken' : ''}.`
    : n.freeNowWithBackup > n.freeNow
      ? `If you would break a backup deposit (${eur0(n.backup)} maturing after it) should the rate jump, today's cash only has to cover today's band (${eur0(n.todayAmount)}), and ${eur0(n.freeNowWithBackup)} is free.`
      : undefined;
  const moveNote = n.moveToHapoalim > 0 ? ` The amount kept has to be in Hapoalim EUR cash on the day: move ${eur0(n.moveToHapoalim)} there from the other accounts.` : '';
  return (
    <div className="space-y-4 rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
      <div className="flex items-start justify-between gap-2">
        <p className="text-[13px] font-medium text-slate-900">
          How these figures are worked out, with today&apos;s numbers{plan.path.length > 1 ? ' (click a row above to switch the day)' : ''}
        </p>
        <button type="button" className={BUTTON} onClick={onClose}>Close</button>
      </div>
      {model.cash ? (
        <Steps title="Spare now (for a new deposit, or to convert on a good day)"
          note={`At every due from today to ${formatDay(model.cash.until, true)} (each conversion at the amount kept, each conversion for salaries and vendors), the spare is the euros held then (today's, the typical inflow and the deposits maturing by then, less what fell due before) less what is due and the euros kept for EUR vendors.${moveNote}`}
          rows={[
            ['Euro cash today', `The bank's balances: Hapoalim EUR cash ${eur0(n.hapoalim)} + other EUR accounts ${eur0(n.other)}`, eur0(n.liquid)],
            ['− Kept for EUR vendors', `A month of EUR vendor payments: the average of ${months}`, eur0(n.eurFloor)],
            ['= The most that can leave today', '', eur0(n.liquid - n.eurFloor)],
            ['The tightest due', n.tightest ? `${formatDay(n.tightest.date, true)} (${n.tightest.shekels ? 'salaries and vendors' : 'the conversion'}): its spare` : 'No due in the plan', n.tightest ? eur0(n.tightest.spare) : '–'],
            ['Spare now', 'The smaller of the two, never below zero', eur0(n.spare)],
          ]} />
      ) : (
        <Steps title={`Free to convert now (for the ${nextDay} conversion)`} note={freeNote || moveNote ? `${freeNote || ''}${moveNote}`.trim() : undefined} rows={[
          ['Euro cash today', `The bank's balances: Hapoalim EUR cash ${eur0(n.hapoalim)} + other EUR accounts ${eur0(n.other)}`, eur0(n.liquid)],
          [`Keep for ${nextDay}`, `Your setting: ${setting}`, eur0(n.required)],
          ['− Deposits maturing before it', "The bank's deposit dates: paid out by then", eur0(n.maturing)],
          ["= Needed from today's cash", 'What is kept, less those deposits (never below zero)', eur0(n.needLiquid)],
          ['Free to convert now', 'Cash today, less what is needed from it', eur0(n.freeNow)],
        ]} />
      )}
      <Steps title={`Euros by then (${day})`} rows={[
        ['Euro cash today', 'As above', eur0(n.liquid)],
        ['+ Typical inflow', model.inflow
          ? `NetSuite's books: ${formatEur(model.inflow.perMonth)} a month net (${months}), about ${formatEur(model.inflow.perDay)} a day × ${p.inflowDays} days, up to the day before`
          : 'Not known: the monthly EUR flows could not be read', eur0(p.inflow)],
        ['+ Deposits maturing by then', `The bank's deposit dates: ${deposits} deposit${deposits === 1 ? '' : 's'} from today to ${day}`, eur0(p.depositsBy)],
        ['− Earlier conversions', earlier.length ? `${earlier.map((x) => `${formatDay(x.date)} ${formatEur(x.required)}`).join(' + ')} (kept at your setting)` : 'None: this is the next conversion', earlier.length ? `−${eur0(p.earlier)}` : '€0'],
        ...(model.cash ? [['− For salaries and vendors', "Converted for net salaries, pension, tax, social security and ILS vendors by then, at today's rate (the table under this card)", p.forShekels > 0.5 ? `−${eur0(p.forShekels)}` : '€0'] as [string, string, string]] : []),
        ['= Euros by then', '', eur0(p.available)],
      ]} />
      <Steps title={`Can be converted before it (${day})`}
        note={`In total, counting from today, as the money comes in, including what you convert before the earlier days. Today itself, only the “${model.cash ? 'Spare now' : 'Free to convert now'}” amount is in hand.`}
        rows={[
          ['Euros by then', 'As above', eur0(p.available)],
          [`− Keep for ${day}`, `Your setting: ${setting}`, eur0(p.required)],
          ...(n.eurFloor ? [['− Kept for EUR vendors', `A month of EUR vendor payments: the average of ${months}`, eur0(n.eurFloor)] as [string, string, string]] : []),
          [`= Spare on ${day}`, p.surplus < 0 ? 'Short: this day needs more than will be held' : '', eur0(p.surplus)],
          ['Smallest spare from this day on', plan.path.slice(i).map((x) => `${formatDay(x.date)} ${eur0(x.surplus)}`).join(' · '), eur0(Math.max(0, limit.surplus))],
          ['Can be converted before it', limit.month === p.month ? "No later day is tighter, so it is this day's spare" : `${formatDay(limit.date, true)} is tighter: euros converted now are gone then too`, eur0(p.convertibleBefore)],
        ]} />
      <ul className="list-disc space-y-0.5 pl-5 text-[11px] text-slate-500">
        <li>You convert nothing else yourself and open no new deposits in the meantime.</li>
        <li>Money comes in at the average pace; real months are lumpier, so convert as it arrives.</li>
        <li>Each deposit is paid out on its date, not rolled over.</li>
        <li>{model.cash ? "The salaries follow the Salary Projection, and the vendors the last three months' average." : 'These figures say what you can convert, not whether you need the shekels.'}</li>
      </ul>
    </div>
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

// ── past conversions ──
/** A day's other conversions: "Mon 12 Oct €800K @ 3.435", or "bought €1.75M @ 3.768" for euros bought back. */
const otherLine = (o: OtherConversion) => `${formatDay(o.date)} ${o.eur < 0 ? 'bought ' : ''}${formatEur(Math.abs(o.eur))} @ ${o.rate.toFixed(3)}`;
const gainTone = (v: number | null) => ((v ?? 0) < 0 ? 'text-rose-700' : 'text-emerald-700');

function PastFixings({ model }: { model: FxModel }) {
  if (!model.past.length) return null;
  const t = model.pastTotal;
  const since = monthName(model.pastFrom);
  const all = { eur: t.booked.eur + t.others.eur, ils: t.booked.ils + t.others.ils, vs: t.booked.vsRateIls + t.others.vsRateIls };
  const firstInTable = model.past.find((p) => p.inTable);
  return (
    <Card title={`Past conversions since ${since}: the table against what was booked`}>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b border-slate-200">
              <Th left tip="Each month whose conversion day (the 7th) has passed.">Conversion day</Th>
              <Th tip="NetSuite's HSBC EUR/ILS on the conversion day.">EUR/ILS that day</Th>
              <Th tip={`What the table says at that day's rate: the euros sold and the deals' blended rate. The table starts with ${firstInTable ? monthName(firstInTable.month) : 'its first month'}; earlier months show what was booked only.`}>The table</Th>
              <Th tip={'The "Conversion" / המרה journals selling Hapoalim EUR (11001) for ILS (10002) on one day within 3 days of the conversion day: the day closest to the table (✓ when they match it), or before the table, the day with the best rate (the monthly deals beat the market\'s).'}>Booked (Hapoalim, &quot;Conversion&quot;)</Th>
              <Th tip="Shekels received against selling the same euros at the rate of the day they were booked (NetSuite's HSBC rate).">₪ vs that day&apos;s rate</Th>
              <Th left tip="The month's other Hapoalim conversions, by day: euros sold, or bought back with shekels (bought). Hover a figure for its gain against that day's rate.">Other conversions that month</Th>
            </tr>
          </thead>
          <tbody>
            {model.past.map((p) => (
              <tr key={p.month} className="border-b border-slate-100 align-top">
                <td className="whitespace-nowrap px-2 py-1.5 text-left text-slate-700">{monthName(p.month)} · {formatDay(p.date)}</td>
                <td className={TD}>{p.spot ? formatRate(p.spot.rate) : '–'}</td>
                <td className={`${TD} whitespace-nowrap ${p.inTable ? '' : 'text-slate-400'}`}>{p.conv ? `${formatEur(p.conv.eur)} @ ${p.conv.rate}` : p.inTable ? '–' : 'not in the table'}</td>
                <td className={`${TD} whitespace-nowrap ${!p.booked ? 'text-slate-400' : !p.inTable ? 'text-slate-700' : p.match ? 'text-emerald-700' : 'text-amber-700'}`}>
                  {p.booked ? `${formatDay(p.booked.date)}: ${formatEur(p.booked.eur)} → ${formatIls(p.booked.ils)} @ ${p.booked.rate ? p.booked.rate.toFixed(3) : '–'}${p.inTable ? (p.match ? ' ✓' : ' (differs)') : ''}` : 'Nothing booked near it'}
                </td>
                <td className={`${TD} ${gainTone(p.gainVsSpotIls)}`}>{p.gainVsSpotIls === null ? '–' : formatIls(p.gainVsSpotIls)}</td>
                <td className="px-2 py-1.5 text-left text-slate-500">
                  {p.others.length ? p.others.map((o, i) => (
                    <span key={`${o.date}${o.eur}`} title={o.vsRateIls === null ? undefined : `${formatIls(o.vsRateIls)} against that day's rate (${o.n} journal${o.n === 1 ? '' : 's'})`}>
                      {i ? ' · ' : ''}<span className={o.eur < 0 ? 'text-slate-700' : ''}>{otherLine(o)}</span>
                    </span>
                  )) : '–'}
                </td>
              </tr>
            ))}
            <tr className="bg-slate-50 align-top font-semibold text-slate-900">
              <td className="px-2 py-1.5 text-left">Since {since}</td>
              <td className={TD} />
              <td className={TD} />
              <td className={`${TD} whitespace-nowrap`}>{t.booked.eur ? `${formatEur(t.booked.eur)} → ${formatIls(t.booked.ils)} @ ${(t.booked.ils / t.booked.eur).toFixed(3)}` : '–'}</td>
              <td className={`${TD} ${gainTone(t.booked.vsRateIls)}`}>{formatIls(t.booked.vsRateIls)}</td>
              <td className="px-2 py-1.5 text-left font-normal text-slate-600">
                {t.others.sold || t.others.bought
                  ? <>{formatEur(t.others.sold)} sold{t.others.bought ? `, ${formatEur(t.others.bought)} bought back` : ''}: <span className={gainTone(t.others.vsRateIls)}>{formatIls(t.others.vsRateIls)}</span> against each day&apos;s rate</>
                  : '–'}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p className="mt-1.5 text-[11px] text-slate-500">
        All Hapoalim conversions since {since}: {formatEur(all.eur)} net → {formatIls(all.ils)}, {formatIls(all.vs)} against each day&apos;s rate.
        {model.past.some((p) => !p.inTable) && ' Before the table, the booked figure is the best-rate day near the 7th; the rest of the month is under other conversions.'}
      </p>
    </Card>
  );
}

// ── the daily mail ──
const LSPORTS_ADDRESS = /^[^\s@<>"',;]+@lsports\.eu$/i;
const atIsrael = (iso: string) => new Date(iso).toLocaleString('en-GB', { timeZone: 'Asia/Jerusalem', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });

/** Who gets the daily mail (scripts/fx-challenge-mail.cjs): kept here and saved for everyone, lsports.eu addresses only. */
function DailyMail() {
  // undefined while loading; null when this server has no such route yet (finance-it's route file is older).
  const [data, setData] = useState<MailSettings | null | undefined>(undefined);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [edited, setEdited] = useState<string[] | null>(null);
  const [draft, setDraft] = useState('');
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const load = useCallback(async () => {
    try {
      setData(await fetchMail());
      setLoadError(null);
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : String(e));
    }
  }, []);
  useEffect(() => { void load(); }, [load]);

  if (loadError) return <Card title="Daily mail"><p className="text-xs text-rose-700">{loadError}</p></Card>;
  if (data === undefined) return null;
  if (data === null) {
    return (
      <Card title="Daily mail">
        <p className="text-xs text-slate-600">
          Until finance-it&apos;s FX routes are updated (docs/fx-challenge.md, Deploying), who gets the daily mail is set on the server (FX_MAIL_TO in the
          dashboard&apos;s .env). After that it is kept here.
        </p>
      </Card>
    );
  }
  const saved = data.value ? data.value.to : null;
  const current = saved ?? data.envTo;
  const list = edited ?? current;
  const changed = edited !== null && edited.join(',') !== current.join(',');
  const add = () => {
    const a = draft.trim().toLowerCase();
    if (!LSPORTS_ADDRESS.test(a)) { setErr(`${draft.trim() || 'That'} is not an lsports.eu address.`); return; }
    if (list.includes(a)) { setErr(`${a} is on the list already.`); return; }
    setEdited([...list, a]);
    setDraft('');
    setErr(null);
  };
  async function save() {
    setSaving(true);
    setErr(null);
    try {
      await saveMail(list);
      setEdited(null);
      await load();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }
  const sent = data.lastSent;
  return (
    <Card title="Daily mail"
      aside={<span className="text-[11px] text-slate-500">{saved ? `Saved${data.updatedAt ? ` ${atIsrael(data.updatedAt)}` : ''}${data.updatedBy ? ` by ${data.updatedBy}` : ''}` : "From the server's settings (FX_MAIL_TO) until saved here"}</span>}>
      <div className="space-y-2.5 text-xs text-slate-700">
        <p>
          What to keep and what is free, every day at {String(data.hour).padStart(2, '0')}:00 Israel time{data.from ? `, from ${data.from}` : ''}, to the addresses below.
          {' '}{sent ? `Last sent ${atIsrael(sent.at)}${sent.recipients ? `, to ${sent.recipients} address${sent.recipients === 1 ? '' : 'es'}` : ''}.` : 'Not sent by the daily job yet.'}
        </p>
        <ul className="flex flex-wrap gap-1.5">
          {list.map((a) => (
            <li key={a} className="inline-flex items-center gap-1 rounded-full border border-slate-300 bg-slate-50 py-0.5 pl-2.5 pr-1 text-slate-800">
              {a}
              <button type="button" aria-label={`Remove ${a}`} title={`Remove ${a}`} onClick={() => { setEdited(list.filter((x) => x !== a)); setErr(null); }}
                className="rounded-full px-1.5 leading-4 text-slate-500 hover:bg-slate-200 hover:text-slate-900">×</button>
            </li>
          ))}
          {!list.length && <li className="text-amber-700">Nobody is on the list, so no mail is sent.</li>}
        </ul>
        <form noValidate className="flex flex-wrap items-center gap-2" onSubmit={(e: { preventDefault: () => void }) => { e.preventDefault(); add(); }}>
          <input aria-label="An lsports.eu address to add" type="email" placeholder="name@lsports.eu" value={draft}
            onChange={(e: { target: { value: string } }) => setDraft(e.target.value)} className={`${INPUT} w-56`} />
          <button type="submit" className={BUTTON} disabled={!draft.trim()}>Add</button>
          <button type="button" className={BUTTON} disabled={saving || (!changed && saved !== null)} onClick={() => void save()}>
            {saving && <Loader2 size={12} className="animate-spin" />}Save for everyone
          </button>
          {edited !== null && <button type="button" className={BUTTON} disabled={saving} onClick={() => { setEdited(null); setErr(null); }}>Undo changes</button>}
        </form>
        {err && <p className="text-rose-700">{err}</p>}
        {changed && !err && <p className="text-amber-700">Not saved yet: until you save, the mail goes to the list as it was.</p>}
        <p className="text-[11px] text-slate-500">lsports.eu addresses only, as the mail carries LSports&apos; balances. Each save is kept in the history with who made it.</p>
      </div>
    </Card>
  );
}
