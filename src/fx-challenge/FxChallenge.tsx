// Business Tools → FX Challenge: finance's monthly EUR→ILS conversions at Bank Hapoalim (fixed on the 7th by the
// conversion table), simulated at any rate, and how much EUR to keep for them (GET /api/fx-challenge; the logic is
// src/forecast/fx-challenge.mjs, the same as the server's tests).
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { Loader2, RefreshCw, Upload } from 'lucide-react';
import { ErrorCard, Segmented, Skeleton, Warnings } from '../new-dashboard/PageParts.tsx';
import { KpiCard } from '../new-dashboard/KpiStrip.tsx';
import { readParam, writeParam } from '../new-dashboard/urlParams.ts';
import {
  bandLabels, mergePositionReports, parsePositionReport, parseTable, POLICIES, POLICY_LABEL, tableToText,
  type CashPlan, type DepositRelease, type Fixing, type FxTable, type OtherConversion, type OutcomeRange, type PathPoint, type Policy, type PositionReport, type Positions,
  type RateCheck, type ReservePlan,
} from '../forecast/fx-challenge.mjs';
import { formatEur, monthName } from '../metrics/model.ts';
import { fetchFx, fetchMail, readSheetRows, saveMail, savePositions, saveTable, sendMailNow } from './api.ts';
import { buildFxModel, conversionDays, formatDay, formatDays, formatIls, formatPct0, formatRate, formatTableRate, type FxModel, type ScheduleRow } from './fxModel.ts';
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
/** When EUR/ILS decides a conversion: "on Mon 2 & Tue 3 Nov" (the bank's deals' fixing days), else "on Sat 7 Nov 2026". */
const fixedWhen = (f: Fixing) => `on ${f.fixedOn ? formatDays(f.fixedOn) : formatDay(f.date, true)}`;
/** A conversion's outcomes, one line each, for a hover. */
const outcomeTip = (f: Fixing, list: OutcomeRange[] | null | undefined) => (list ? [
  `If EUR/ILS ${fixedWhen(f)} is:`,
  ...list.filter((o) => o.p >= 0.005).map((o) => `${rangeLabel(o)}: ${chance(o.p)} → ${o.eur ? `${formatEur(o.eur)} at ${formatTableRate(o.rate)}` : 'nothing sold'}`),
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
            {model.plan && model.plan.coverage.length > 0 && <CoverageCard model={model} plan={model.plan} />}
            <DealsCard data={data} model={model} onSaved={reload} />
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
  const { todo } = model;
  const toConvert = todo.dues.filter((d) => d.eur > 0.5);
  const lastDue = todo.dues[todo.dues.length - 1];
  // What today's cash has to keep for the next conversion: what it takes, less the deposits released by then.
  const keep = model.keep;
  const one = keep && keep.deposits.length === 1;
  const keepEur = plan ? (keep ? keep.fromCash : plan.required) : null;
  const coverNote = !keep || !(keep.fromDeposits > 0.5) ? ''
    : keep.covered ? ` · ${formatEur(keep.required)}, covered by ${one ? 'a deposit' : `${keep.deposits.length} deposits`}`
      : ` · ${formatEur(keep.required)}, ${formatEur(keep.fromDeposits)} from ${one ? 'a deposit' : 'deposits'}`;
  return (
    <div className={`grid grid-cols-2 gap-3 ${cash ? 'lg:grid-cols-5' : 'lg:grid-cols-4'}`}>
      <KpiCard label="EUR/ILS today" value={model.spot} ccy="eur" display={formatRate(model.spot)}
        note={model.spotBand !== null ? `Band ${model.labels[model.spotBand]} · ${formatDay(model.spotDate)}` : 'No rate from NetSuite'} />
      <KpiCard label="Next conversion" value={next && next.atSpot ? next.atSpot.eur : null} ccy="eur"
        display={next && next.atSpot ? `${formatEur(next.atSpot.eur)} → ${formatIls(next.atSpot.ils)}` : undefined}
        note={next ? `${conversionDays(next.fixing, next.fixedAt, next.parts)}${next.fixedAt ? '' : " at today's rate"}${next.atSpot ? `, ${formatTableRate(next.atSpot.rate)}` : ''}` : 'No conversion left in the table'} />
      <KpiCard label="Keep for it" value={keepEur} ccy="eur" display={keepEur !== null ? eur0(keepEur) : undefined}
        note={model.plan ? `${POLICY_LABEL[model.plan.policy]}${coverNote}` : 'Needs balances'} />
      {cash && plan ? (
        <>
          <KpiCard label="Convert for salaries & vendors" value={todo.eur} ccy="eur" display={eur0(todo.eur)}
            note={toConvert.length
              ? toConvert.map((d) => `${eur0(d.eur)} ${d.by <= model.today ? 'now' : `by ${formatDay(d.by)}`}`).join(' · ')
              : lastDue ? `Covered to ${formatDay(lastDue.date)} by the shekels on hand` : 'Nothing due'} />
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
  fixing: "When the conversion happens: the euros leave Hapoalim EUR (583400) and the shekels come in. With the bank's deals, its parts' value days, two business days after EUR/ILS is fixed for them; without them, the table's day (the 7th), the euros taken the business day before.",
  expected: "What the conversion sells on average: each band's euros, weighted by the chance that EUR/ILS lands in it that day, from today's rate. The bank sells one cell; this is for planning. Hover a figure for the chances.",
  released: 'EUR deposits that mature after the previous conversion (after today, for the first) and by this one, at the bank\'s value at maturity with interest. Hover a figure for each deposit and its date. They count only if the bank pays them out rather than rolling them over; a blocked deposit is left out.',
  available: "What you would hold on the day if nothing else is converted: today's liquid euros, plus the typical inflow and the deposits released by then, less what the earlier conversions take (at the amount kept) and the conversions for salaries and vendors by then.",
  convertible: 'How much you can convert yourself before this conversion, in total, as the euros come in, with this conversion, every later one, the salaries and vendors still covered, and the euros for EUR vendors kept: the smallest spare from this row on. Euros converted now are gone at every later conversion too.',
  shekels: "Euros converted for salaries and ILS vendors after the previous conversion (after today, for the first) and by this one, at today's rate: what the shekels on hand and the table's conversions don't cover. Hover a figure for the days.",
};

const WHY: Record<string, string> = { net: 'net salaries', later: 'pension, tax and social security', cards: 'credit card bills', vendors: 'ILS vendors' };
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
  const keep = model.keep;
  // With the bank's deals, the conversion by the day(s) its euros leave (the plan's parts); without, by the table's day.
  const deals = !!first.fixing.fixedOn;
  const c0 = plan.coverage[0];
  const when = deals ? `the conversion of ${conversionDays(first.fixing, first.fixedAt, c0 && c0.month === first.fixing.month ? c0.legs : first.parts)}`
    : `${formatDay(n.date)}${first.fixing.weekend ? ' (a weekend: the deal sets the day)' : ''}`;
  // Deposits from NetSuite's books have no dates, so none of them shows as released on a day.
  const undated = !!model.position && model.position.deposits.length > 0 && model.position.deposits.every((d) => !d.valueDate);
  const later = plan.later.reduce((s, d) => s + d.amount, 0);
  const chosen = picked && plan.path.some((p) => p.month === picked) ? picked : plan.path[0].month;
  const pick = (month: string) => { setPicked(month); setShowWorkings(true); };
  return (
    <Card title="What to keep, what is free" aside={aside}>
      <div className="space-y-3 text-sm text-slate-700">
        <TodayLines model={model} />
        <p className="text-base text-slate-900">
          {keep && keep.text ? (
            <>
              {keep.covered ? <><strong>Nothing to keep</strong> for</> : <>Keep <strong>{eur0(keep.fromCash)}</strong> in Hapoalim EUR (583400) for</>}
              {' '}{when}:{' '}
              <span title="A deposit counts if the bank pays it into Hapoalim EUR (583400) by the conversion, rather than rolling it over.">{keep.text}</span>.
            </>
          ) : (
            <>Keep <strong>{formatEur(n.required)}</strong> in Hapoalim EUR (583400) for {when}.</>
          )}
          {model.cash ? null : (
            <>
              {' '}Free to convert now: <strong>{formatEur(n.freeNow)}</strong>
              {n.freeNowWithBackup > n.freeNow ? <>, or <strong>{formatEur(n.freeNowWithBackup)}</strong> if the backup deposits cover a rate jump</> : null}.
            </>
          )}
        </p>
        {model.cash && <CashLines model={model} plan={plan} />}
        {model.rateCheck && <RateLine rc={model.rateCheck} free={model.cash ? n.spare : n.freeNow} />}
        <div className="space-y-1.5">
          {first.fixedAt ? (
            <p className="text-[13px]">
              The {monthName(first.fixing.month)} conversion was fixed {fixedWhen(first.fixing)}: at NetSuite&apos;s EUR/ILS of {formatDay(first.fixedAt.date)},
              {' '}{formatRate(first.fixedAt.rate)}, it sells {formatEur(first.atSpot.eur)} at {formatTableRate(first.atSpot.rate)} ({formatIls(first.atSpot.ils)})
              {n.todayAmount < first.atSpot.eur - 0.5 ? <>, of which {formatEur(first.atSpot.eur - n.todayAmount)} is already paid</> : null}.
            </p>
          ) : (
            <p className="text-[13px]">
              What the {deals ? monthName(first.fixing.month) : formatDay(n.date)} conversion sells depends on EUR/ILS {deals ? fixedWhen(first.fixing) : 'that day'}. At
              {' '}today&apos;s {formatRate(model.spot)}: {formatEur(n.todayAmount)} at {formatTableRate(first.atSpot.rate)} ({formatIls(first.atSpot.ils)}). On average
              {' '}(expected): <strong>{formatEur(first.risk.expectedEur)}</strong>.
            </p>
          )}
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
                        {(() => { const c = plan.coverage.find((x) => x.month === p.month); return c && c.deals ? formatDays(c.on) : formatDay(p.date); })()}
                        {p.month.slice(0, 4) !== model.schedule[0].fixing.month.slice(0, 4) ? ` ${p.month.slice(0, 4)}` : ''}
                      </button>
                    </td>
                    <td className={TD}>{formatEur(p.required)}</td>
                    <td className={`${TD} text-slate-500`} title={(() => { const s = model.schedule.find((x) => x.fixing.month === p.month); return s ? outcomeTip(s.fixing, s.outcomes) : undefined; })()}>
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

const COVER_TIP = {
  day: "When the conversion happens: the euros leave Hapoalim EUR (583400) and the shekels come in. With the bank's deals, each part's value day, two business days after it is fixed (March 2027: Fri 5 and Mon 8 Mar); without them, the table's day (the 7th), the euros taken the business day before. Hover a day for its parts.",
  released: "EUR deposits paying out by the day the euros leave (each part's, with the bank's deals), and after the previous conversion's left (after today, for the first), at the bank's value then, interest included. Hover a figure for each deposit and its date. They count only if the bank pays them out rather than rolling them over.",
  carried: "What the deposits of the conversions before it left over: it rolls on to this one.",
  result: 'The deposits paid out before it, plus what is left from earlier, against what it takes, part by part. Short: the euros on hand or coming in have to fill the rest, or a new deposit timed for it.',
  ladder: "A new deposit that pays out the value day before the euros leave (the business day before, without the bank's deals), for the gap rounded up to €10K, as much as the payments before it leave room for; earliest first. A conversion less than two weeks away takes the rest from today's cash (the keep line above).",
};

/** The next conversions against the deposits, first in, first out, and the new deposits timed for their gaps. */
function CoverageCard({ model, plan }: { model: FxModel; plan: ReservePlan }) {
  const year0 = model.schedule[0] ? model.schedule[0].fixing.month.slice(0, 4) : '';
  return (
    <Card title="The conversions against your deposits (first in, first out)">
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b border-slate-200">
              <Th left tip={COVER_TIP.day}>Conversion day</Th>
              <Th tip={KEEP_TIP[plan.policy]}>It takes ({POLICY_LABEL[plan.policy].toLowerCase()})</Th>
              <Th tip={COVER_TIP.released}>Deposits paid out before it</Th>
              <Th tip={COVER_TIP.carried}>Left from earlier</Th>
              <Th tip={COVER_TIP.result}>Covered?</Th>
              <Th tip={COVER_TIP.ladder}>New deposit for it</Th>
            </tr>
          </thead>
          <tbody>
            {plan.coverage.map((c) => {
              const legs = plan.ladder.filter((l) => l.month === c.month);
              const over = c.have - c.required;
              const lastOut = c.legs[c.legs.length - 1].settle;
              // Deposits paying out after the last euros leave but by the 7th: too late here, they count for the next.
              const late = c.late && c.late.length
                ? [`Paid out after the euros leave on ${formatDay(lastOut, true)}, so counted for the next conversion:`, ...depositLines(c.late)]
                : [];
              const tip = [...depositLines(c.releasedDeposits), ...late].join('\n');
              const shortParts = c.legs.filter((l) => l.gap >= 0.5);
              return (
                <tr key={c.month} className="border-b border-slate-100">
                  <td className="whitespace-nowrap px-2 py-1.5 text-left text-slate-700"
                    title={c.deals
                      ? c.legs.map((l) => `${formatEur(l.required)} leaves on ${formatDay(l.settle, true)}${l.expiry ? `, fixed on ${formatDay(l.expiry, true)}` : ''}`).join('\n')
                      : `No deals from the bank for this month: the table's ${formatDay(c.date, true)}, its euros taken the business day before.`}>
                    <span className={HINT}>{formatDays(c.on)}</span>{c.month.slice(0, 4) !== year0 ? ` ${c.month.slice(0, 4)}` : ''}
                    {!c.deals && <span className="text-slate-400"> (euros leave {formatDay(c.takes)}, assumed)</span>}
                  </td>
                  <td className={`${TD} whitespace-nowrap`}>
                    {formatEur(c.required)}
                    {c.legs.length > 1 && <span className="text-slate-500"> ({c.legs.map((l) => formatEur(l.required)).join(' + ')})</span>}
                  </td>
                  <td className={`${TD} text-slate-600`} title={tip || undefined}>
                    {c.released > 0.5 ? <span className={HINT}>{formatEur(c.released)}</span> : late.length ? <span className={HINT}>– (one too late)</span> : '–'}
                  </td>
                  <td className={`${TD} text-slate-600`}>{c.carried > 0.5 ? formatEur(c.carried) : '–'}</td>
                  <td className={`${TD} whitespace-nowrap font-medium ${c.gap >= 0.5 ? 'text-rose-700' : 'text-emerald-700'}`}>
                    {c.gap >= 0.5
                      ? `${formatEur(c.gap)} short${c.legs.length > 1 ? ` (${shortParts.map((l) => formatDay(l.settle)).join(', ')})` : ''}`
                      : `covered${over >= 500 ? ` (+${formatEur(over)})` : ''}`}
                  </td>
                  <td className={`${TD} whitespace-nowrap text-slate-700`}>
                    {legs.length ? legs.map((l) => `${formatEur(l.eur)} until ${formatDay(l.until)}`).join(', ') : c.gap >= 0.5 && !c.timed ? "from today's cash" : '–'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-1 text-[11px] text-slate-500">
        A conversion happens when its euros leave (and the shekels come in): with the bank&apos;s deals (the card below), on each
        part&apos;s value day, two business days after it is fixed; without them, the euros are taken the business day before the
        table&apos;s 7th. Each deposit counts for the first part it pays out in time for, and what one doesn&apos;t take rolls on to the
        next. A conversion the deposits don&apos;t cover is short: the euros on hand or coming in fill the rest, or a new deposit that
        pays out the value day before (the last column, also under what to do today). Hover a header for what it means.
      </p>
    </Card>
  );
}

/** A button that picks Excel files (several at once). */
function PickFiles({ label, title, busy, onFiles }: { label: string; title?: string; busy: boolean; onFiles: (files: File[]) => void }) {
  return (
    <label className={`${BUTTON} cursor-pointer`} title={title}>
      {busy ? <Loader2 size={12} className="animate-spin" /> : <Upload size={12} />}{label}
      <input type="file" accept=".xlsx,.xls" multiple className="hidden" disabled={busy}
        onChange={(e: { currentTarget: HTMLInputElement }) => { const files = Array.from(e.currentTarget.files || []); e.currentTarget.value = ''; onFiles(files); }} />
    </label>
  );
}

const KIND_WORD = { options: 'Options', forwards: 'Forwards' } as const;
/** "Options of account 583400, Fri 30 Oct 2026": a report by its kind, account and day. */
const reportLabel = (r: { kind: 'options' | 'forwards'; account: string | null; asOf: string | null }) =>
  `${KIND_WORD[r.kind]}${r.account ? ` of account ${r.account}` : ''}, ${r.asOf ? formatDay(r.asOf, true) : 'day unknown'}`;

/** The bank's deals: Hapoalim's position reports (any number: the options and the forwards, of one account or more),
 * read here and saved for everyone. The months their options cover take their amounts from them (the conversion table
 * shows it), each conversion's euros leave on its parts' value days, and the forwards' shekels count when they come in;
 * the saved table fills the other months. */
function DealsCard({ data, model, onSaved }: { data: FxPayload; model: FxModel; onSaved: () => Promise<void> }) {
  const saved = data.positions || null;
  const d = model.deals;
  const [reads, setReads] = useState<{ name: string; report: PositionReport }[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  // What saving would keep: each file's deals in place of its account's saved report, the other saved reports as they are.
  const merged = useMemo(() => (reads ? mergePositionReports(saved, reads) : null), [reads, saved]);

  // Files picked are added to those already read (picked again, a file replaces its earlier read), so they can be
  // picked together or one by one before saving.
  async function read(files: File[]) {
    if (!files.length) return;
    setBusy(true);
    setErr(null);
    try {
      const got: { name: string; report: PositionReport }[] = [];
      for (const f of files) got.push({ name: f.name, report: parsePositionReport(await readSheetRows(f)) });
      setReads((prev) => [...(prev || []).filter((r) => !got.some((g) => g.name === r.name)), ...got]);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }
  async function save(value: Positions) {
    setBusy(true);
    setErr(null);
    try {
      await savePositions(value);
      await onSaved();
      setReads(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const usable = !!merged && merged.files.some((f) => f.used);
  const older = !!merged && merged.files.some((f) => f.used && f.older);
  const savedLabels = bandLabels(data.table.bounds);
  const pickTitle = "Hapoalim's position reports: OTCPositionDetails.xlsx (the options) and FXPositionDetails.xlsx (the forwards), of one account or more. Pick several at once, or add them one by one before saving.";
  return (
    <Card title="The bank's deals (how many euros, and when they leave)"
      aside={(
        <div className="flex items-center gap-2 text-[11px] text-slate-500">
          {saved && <span>Saved{saved.savedAt ? ` ${new Date(saved.savedAt).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}` : ''}{saved.savedBy ? ` by ${saved.savedBy}` : ''}</span>}
          <PickFiles label="Upload the bank's reports" title={pickTitle} busy={busy} onFiles={(files) => void read(files)} />
        </div>
      )}>
      {merged && (
        <div className="mb-3 space-y-2 rounded-md border border-sky-200 bg-sky-50/50 p-3 text-xs text-slate-700">
          <ul className="space-y-1">
            {merged.files.map((f) => (
              <li key={f.name}>
                <span className="font-medium">{f.name}</span>:{' '}
                {f.kind
                  ? `${reportLabel({ kind: f.kind, account: f.account, asOf: f.asOf })}: ${f.count} ${f.kind === 'options' ? 'EUR/ILS options' : 'forwards'}${f.skipped ? ` (${f.skipped} in other currencies left out)` : ''}`
                  : "not one of the bank's position reports"}
                {f.kind && !f.used && f.why && <span className="text-slate-500"> (left out: {f.why})</span>}
                {f.used && (
                  <span className="text-slate-500">
                    {f.replaces.length ? ` (replaces the one saved${f.replaces.some((r) => r.asOf) ? `, of ${f.replaces.map((r) => (r.asOf ? formatDay(r.asOf, true) : 'an unknown day')).join(' and ')}` : ''})` : saved ? ' (added to the ones saved)' : ''}
                  </span>
                )}
                {f.older && (
                  <p className="font-medium text-amber-700">
                    Older than the one saved, of {formatDay(f.older, true)}: saving it puts back the deals as they were on {formatDay(f.asOf, true)}.
                  </p>
                )}
                {f.used && !f.asOf && (
                  <p className="text-amber-700">The report&apos;s day (תאריך הפקה) was not found, so it can&apos;t be checked against the one saved: make sure it is the latest.</p>
                )}
                {f.kind && f.problems.length > 0 && (
                  <ul className="list-disc pl-5 text-rose-700">
                    {f.problems.slice(0, 4).map((p) => <li key={p}>{p}</li>)}
                    {f.problems.length > 4 && <li>…and {f.problems.length - 4} more.</li>}
                  </ul>
                )}
              </li>
            ))}
          </ul>
          {usable && merged.kept.length > 0 && (
            <p className="text-slate-600">Kept as saved: {merged.kept.map((k) => `${reportLabel(k)} (${k.deals.length} deals)`).join('; ')}.</p>
          )}
          <div className="flex flex-wrap items-center gap-2">
            <button type="button" className={BUTTON} disabled={busy || !usable || !merged.value} onClick={() => { if (merged.value) void save(merged.value); }}>
              {busy && <Loader2 size={12} className="animate-spin" />}{older ? 'Save anyway' : 'Save for everyone'}
            </button>
            <PickFiles label="Add more files" title={pickTitle} busy={busy} onFiles={(files) => void read(files)} />
            <button type="button" className={BUTTON} disabled={busy} onClick={() => setReads(null)}>Cancel</button>
            <span className="text-slate-500">
              Each report replaces its account&apos;s saved one of its kind; other accounts&apos; stay. Saved for everyone, each save kept in the
              history. Only the deals and the account&apos;s own number are kept: no branch, no valuation.
            </span>
          </div>
        </div>
      )}
      {err && <p className="mb-2 text-xs text-rose-700">{err}</p>}
      {d ? (
        <div className="space-y-1.5 text-xs text-slate-700">
          <p>
            {d.options > 0 && <>{d.options} options{d.from && d.to ? `, fixed ${monthName(d.from)} – ${monthName(d.to)}` : ''}</>}
            {d.options > 0 && d.forwards > 0 && '; '}
            {d.forwards > 0 && <>{d.forwards} forwards</>}.
            {d.options > 0 && <>{' '}They give those months&apos; amounts (the conversion table below) and the days each conversion&apos;s euros leave (the conversions above); its shekels come in then.</>}
          </p>
          {d.reports.length > 0 && (
            <p className="text-slate-600">
              From {d.reports.length === 1 ? 'one report' : `${d.reports.length} reports`}:{' '}
              {d.reports.map((r) => `${reportLabel(r)} (${r.count} deal${r.count === 1 ? '' : 's'}${r.name ? `, ${r.name}` : ''})`).join('; ')}.
            </p>
          )}
          {d.flows.length > 0 && (
            <p title="The forwards netted by value day. A sale bought back the same day moves only shekels.">
              <span className={HINT}>Forwards ahead</span>:{' '}
              {d.flows.map((f) => `${formatIls(Math.abs(f.ils))} ${f.ils >= 0 ? 'in' : 'out'}${Math.abs(f.eur) >= 0.5 ? `, ${formatEur(Math.abs(f.eur))} ${f.eur > 0 ? 'in' : 'out'}` : ''} on ${formatDay(f.date)}`).join(' · ')}
              {d.flows.every((f) => Math.abs(f.eur) < 0.5) ? ' (no euros move)' : ''}
            </p>
          )}
          {d.missing.length > 0 && (
            <p className="text-amber-700">
              No deals for {d.missing.map((m) => monthName(m)).join(', ')}: the saved table gives {d.missing.length === 1 ? 'its' : 'their'} amounts,
              {' '}and {d.missing.length === 1 ? 'its' : 'their'} euros are taken the business day before the 7th.
            </p>
          )}
          {d.mismatches.length > 0 && (
            <p className="text-slate-600" title={d.mismatches.map((m) => `${monthName(m.month)}: ${m.bands.map((b) => `${savedLabels[b.band]} saved table ${formatEur(b.table)}, deals ${formatEur(b.deals)}`).join('; ')}`).join('\n')}>
              <span className={HINT}>The saved table differs from the deals for {d.mismatches.map((m) => monthName(m.month)).join(', ')}</span>: the deals&apos; amounts are used, so the saved table needs no update.
            </p>
          )}
          {d.mismatches.length === 0 && d.options > 0 && <p className="text-slate-500">The saved table agrees with the deals in every band.</p>}
        </div>
      ) : (
        <p className="text-xs text-slate-600">
          Upload Hapoalim&apos;s position reports (OTCPositionDetails.xlsx and FXPositionDetails.xlsx, &quot;פירוט עסקאות&quot;, of one account
          or more): the months their options cover then take their amounts from them, each conversion&apos;s euros leave on its deals&apos;
          own value days, and the forwards&apos; shekels count when they come in. Until then, the table gives the amounts and the plan
          takes the euros the business day before the 7th.
        </p>
      )}
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

/** What to do today, from today's balances and rate, as the daily mail says it (urgent ones in red); or what comes
 * next. */
function TodayLines({ model }: { model: FxModel }) {
  const { todo } = model;
  if (!todo.actions.length) return model.cash ? <p className="text-[15px] text-slate-900">{todo.nothing}</p> : null;
  return (
    <ul className="space-y-1 text-[15px] text-slate-900" title="What to do today, from today's balances and EUR/ILS: the same lines as the daily mail, which goes out every day but Saturday when there is a recommendation.">
      {todo.actions.map((a) => (
        <li key={a.key}><strong className={a.urgent ? 'text-rose-700' : 'text-slate-900'}>{a.head}</strong>. {a.body}</li>
      ))}
    </ul>
  );
}

/** The payments in shekels ahead (what to convert for each, or what covers it), and what is spare. */
function CashLines({ model, plan }: { model: FxModel; plan: ReservePlan }) {
  const n = plan.next;
  const { todo } = model;
  const ok = plan.deposits.filter((d) => d.eur > 0);
  return (
    <>
      {todo.dues.length > 0 && (
        <div>
          <p className="text-[13px] font-medium text-slate-800">Payments in shekels ahead</p>
          <ul className="list-disc space-y-0.5 pl-5 text-[13px]">
            {todo.dues.map((d) => <li key={d.date} className={d.eur > 0.5 ? 'text-slate-900' : 'text-slate-500'}>{d.text}.</li>)}
          </ul>
        </div>
      )}
      <p className="text-[15px] text-slate-900">
        {n.short ? (
          <span className="text-rose-700">
            Nothing is spare: on {formatDay(n.short.first.date)} the needs exceed the euros held and expected by <strong>{formatEur(n.short.first.eur)}</strong>
            {n.short.date !== n.short.first.date ? `, and by ${formatEur(n.short.eur)} on ${formatDay(n.short.date)}` : ''}
            {n.backup > 0 ? ', so a backup deposit may be needed' : ''}.
          </span>
        ) : (
          <>Spare now: <strong>{eur0(n.spare)}</strong>{n.spare > 0 ? ', for a new deposit or to convert on a good day' : ''}.</>
        )}
      </p>
      {!todo.actions.some((a) => a.kind === 'deposit') && (
        <p className="text-[13px]" title="A deposit's euros are away until it matures: how much can be locked until that day with every conversion, salary and vendor payment before it still met as the typical inflow comes in, and the euros for EUR vendors kept. Rounded down to €10K.">
          <span className={HINT}>New deposits</span>:{' '}
          {ok.length
            ? <>{ok.map((d) => `${formatEur(d.eur)} for ${d.months} month${d.months === 1 ? '' : 's'} (until ${formatDay(d.until)})`).join(', ')}, with everything before then still covered.</>
            : <>none now: the coming weeks need the euros held{n.short ? '' : ' and expected'}.</>}
        </p>
      )}
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
              <Th tip={`The credit card bills, paid on the 10th: the month before's card spending (the average of ${months}; for this month's spending, at least what was spent so far; for a month that is over, what was spent).`}>Credit cards (the 10th)</Th>
              <Th tip="Pension, withholding tax and social security, paid by the 14th: the rest of the previous month's payroll.">Pension, tax, social security (by the 14th)</Th>
              <Th tip={`ILS vendor payments by bank transfer and Masav batch: the average of ${months}, kept from the 5th of each month. This month: the rest of it.`}>ILS vendors</Th>
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
                  <td className={TD}>{m.cards > 0.5 ? formatIls(m.cards) : '–'}</td>
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
              <td className={TD} />
              <td className={TD}>{total.ils > 0.5 ? `${formatIls(total.ils)} · ${formatEur(total.eur)}` : '–'}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <ul className="mt-2 list-disc space-y-0.5 pl-5 text-[11px] text-slate-500">
        <li>
          Shekels on hand today: {cash.onHand === null ? 'not known' : `${formatIls(cash.onHand.total)}: ${formatIls(cash.onHand.current)} in LSports' ILS current accounts and ${formatIls(cash.onHand.deposits)} in ILS deposits, which are released the day a payment goes out`}
          {' '}(the bank&apos;s balances). Converted at today&apos;s rate ({formatRate(spot)}).
        </li>
        <li>
          Payroll: the Salary Projection&apos;s Total payroll{cash.projectionAsOf ? ` (saved ${formatDay(cash.projectionAsOf.slice(0, 10))})` : ''}
          {share !== null ? `, split as NetSuite's payroll journals of ${months}: ${formatPct0(share)} net salaries (by the 5th of the next month), ${formatPct0(1 - share)} pension, withholding tax and social security (by the 14th).` : '.'}
        </li>
        <li>
          Vendors: {cash.vendors.ils === null ? 'ILS not known' : `${formatIls(cash.vendors.cards)} a month on the credit cards and ${formatIls(cash.vendors.other)} a month by bank transfer and Masav`}, and {formatEur(cash.vendors.eur)} a month in euros
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
  const c0 = plan.coverage[0];
  const nextDay = c0 && c0.deals ? formatDays(c0.on) : formatDay(n.date, true);
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
            <Th left tip="EUR/ILS on the day it is fixed, by the table's ranges; neighbouring columns that sell the same amount are merged. A rate exactly on an edge counts in the range above.">If EUR/ILS {fixedWhen(s.fixing)} is…</Th>
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
                <td className={TD}>{o.eur ? formatEur(o.eur) : 'nothing'}</td>
                <td className={TD}>{o.eur ? formatTableRate(o.rate) : '–'}</td>
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
              <Th left tip="Each month left, and when: with the bank's deals, the days its parts are fixed (EUR/ILS then picks what they sell) and paid (the euros leave, the shekels come in); without, the table's day (the 7th; weekends flagged). A month stays until its last part is paid.">Conversion day</Th>
              <Th tip="The euros that conversion sells if EUR/ILS is at the simulated rate on the day: the table's cell for that band. Once fixed, what it was fixed at, whatever the rate.">Sold at {formatRate(model.simRate)}</Th>
              <Th tip="The deals' blended rate in that cell.">Rate</Th>
              <Th tip="The euros sold times the cell's rate.">₪ received</Th>
              <Th tip="Against selling the same euros at the simulated rate itself: what the deal gains, or loses, over converting at that rate.">₪ vs that rate</Th>
              <Th tip="What the conversion sells on average: each band's euros weighted by its chance, from today's rate (not the simulated one). Hover a figure for the chances.">Expected €</Th>
              <Th tip="Enough for 19 conversions in 20, from today's rate to the day it is fixed.">95% €</Th>
              <Th tip="The largest amount in that month's row of the table; once fixed, what it was fixed at.">Worst €</Th>
            </tr>
          </thead>
          <tbody>
            {model.schedule.map((s) => (
              <tr key={s.fixing.month} className="border-b border-slate-100">
                <td className="px-2 py-1.5 text-left text-slate-700">
                  {monthName(s.fixing.month)} <span className="text-slate-400">· {conversionDays(s.fixing, s.fixedAt, s.simParts)}</span>
                </td>
                <td className={`${TD} font-medium text-slate-900`}>{s.atSim ? formatEur(s.atSim.eur) : '–'}</td>
                <td className={TD}>{s.atSim ? formatTableRate(s.atSim.rate) : '–'}</td>
                <td className={TD}>{s.atSim ? formatIls(s.atSim.ils) : '–'}</td>
                <td className={`${TD} ${s.atSim && s.atSim.vsSpotIls < 0 ? 'text-rose-700' : 'text-emerald-700'}`}>{s.atSim ? formatIls(s.atSim.vsSpotIls) : '–'}</td>
                <td className={`${TD} text-slate-500`} title={outcomeTip(s.fixing, s.outcomes)}>{s.risk ? <span className={HINT}>{formatEur(s.risk.expectedEur)}</span> : '–'}</td>
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
/** The table the plan uses: the months the bank's deals cover worked out from them, the others as saved. Editing changes
 * the saved table. */
function TableCard({ data, model, onSaved }: { data: FxPayload; model: FxModel; onSaved: () => Promise<void> }) {
  const [editing, setEditing] = useState(false);
  const { table } = model;
  const max = Math.max(1, ...table.rows.flatMap((r) => r.cells.map((c) => Math.abs(c.eurK))));
  const saved = data.tableSaved;
  const d = model.deals;
  const fromDeals = new Set(d ? d.months : []);
  const newEdges = table.bounds.filter((b) => !data.table.bounds.includes(b));
  const dealsTip = d ? `From the bank's deals${d.asOf.options ? ` of ${formatDay(d.asOf.options, true)}` : ''}: in each band, the euros their options sell with EUR/ILS in its middle, at their blended rate.` : '';
  return (
    <Card title={`The conversion table (${table.pair}, € thousands sold / rate, converted on the ${table.fixingDay}th)`}
      aside={(
        <div className="flex items-center gap-2 text-[11px] text-slate-500">
          <span>{saved ? `Saved${saved.at ? ` ${new Date(saved.at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}` : ''}${saved.by ? ` by ${saved.by}` : ''}` : 'The table received in October 2026'}</span>
          <button type="button" className={BUTTON} onClick={() => setEditing((v) => !v)}>{editing ? 'Close' : 'Edit table'}</button>
        </div>
      )}>
      {editing && <EditTable table={data.table} saved={!!saved} dealMonths={d ? d.months : []} onSaved={onSaved} onClose={() => setEditing(false)} />}
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
              const deal = fromDeals.has(r.month);
              return (
                <tr key={r.month} className={past ? 'opacity-45' : ''}>
                  <td className="whitespace-nowrap px-1.5 py-1 text-left text-slate-700" title={deal ? dealsTip : undefined}>
                    {deal ? <span className={HINT}>{monthName(r.month)}</span> : monthName(r.month)}
                  </td>
                  {r.cells.map((c, i) => (
                    <td key={i} className={`whitespace-nowrap rounded px-1.5 py-1 text-center tabular-nums text-slate-800 ${i === model.simBand ? 'ring-2 ring-sky-700' : ''}`}
                      style={{ backgroundColor: `rgba(14, 116, 144, ${(0.06 + 0.42 * Math.abs(c.eurK) / max).toFixed(3)})` }}
                      title={`${monthName(r.month)}, ${model.labels[i]}: ${c.eurK ? `sell ${formatEur(-c.eurK * 1000)} at ${c.rate} = ${formatIls(-c.eurK * 1000 * c.rate)}` : 'nothing sold'}${deal ? " (the bank's deals)" : ''}`}>
                      {c.eurK ? `${-c.eurK} · ${formatTableRate(c.rate)}` : '0'}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-1.5 text-[11px] text-slate-500">
        The highlighted column is the band at the simulated rate; • marks today&apos;s band. Past conversions are dimmed.
        {d && d.from && d.to && (
          <>
            {' '}{d.from === d.to ? monthName(d.from) : `${monthName(d.from)} – ${monthName(d.to)}`} come{d.from === d.to ? 's' : ''} from the bank&apos;s deals
            (underlined; hover a month for how), the other months from the saved table
            {newEdges.length ? `, and the deals add ${newEdges.join(', ')} to the bands` : ''}.
          </>
        )}
      </p>
    </Card>
  );
}

function EditTable({ table, saved, dealMonths, onSaved, onClose }: { table: FxTable; saved: boolean; dealMonths: string[]; onSaved: () => Promise<void>; onClose: () => void }) {
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
      {dealMonths.length > 0 && (
        <p className="text-amber-700">
          The bank&apos;s deals give the amounts for {dealMonths.length === 1 ? monthName(dealMonths[0]) : `${monthName(dealMonths[0])} – ${monthName(dealMonths[dealMonths.length - 1])}`}:
          {' '}what you save here counts for the other months only. To change those, upload the bank&apos;s new reports.
        </p>
      )}
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
  // A total after each month's deposits, by the month they mature.
  const monthOf = (r: (typeof eur)[number] | undefined) => (r && r.kind === 'deposit' && r.valueDate ? r.valueDate.slice(0, 7) : null);
  const rowsAndTotals: ({ row: (typeof eur)[number] } | { month: string; total: number; count: number; blocked: number })[] = [];
  eur.forEach((r, i) => {
    rowsAndTotals.push({ row: r });
    const m = monthOf(r);
    if (m && m !== monthOf(eur[i + 1])) {
      const same = eur.filter((x) => monthOf(x) === m);
      rowsAndTotals.push({ month: m, total: same.reduce((s, x) => s + x.amount, 0), count: same.length, blocked: same.filter((x) => x.blocked).length });
    }
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
              {rowsAndTotals.map((it, i) => ('row' in it ? (
                <tr key={`${it.row.bank}-${it.row.kind}-${i}`} className="border-b border-slate-100">
                  <td className="px-2 py-1.5 text-left text-slate-700">{it.row.label && it.row.kind === 'deposit' ? `${it.row.bank} · ${it.row.label}` : it.row.bank}</td>
                  <td className="px-2 py-1.5 text-left text-slate-500">{KIND_LABEL[it.row.kind]}{it.row.blocked ? ' (blocked)' : ''}</td>
                  <td className={`${TD} text-slate-900`}>{formatEur(it.row.amount)}</td>
                  <td className={`${TD} text-slate-500`}>{it.row.kind === 'deposit' ? (it.row.valueDate ? formatDay(it.row.valueDate, true) : 'not in the books') : (it.row.asOf || '–')}</td>
                </tr>
              ) : (
                <tr key={`total-${it.month}`} className="border-b border-slate-200 bg-slate-50">
                  <td colSpan={2} className="px-2 py-1.5 text-left font-medium text-slate-700">
                    Total for {monthName(it.month)}: {it.count} deposit{it.count === 1 ? '' : 's'}{it.blocked ? ` (${it.blocked} blocked)` : ''}
                  </td>
                  <td className={`${TD} font-semibold text-slate-900`}>{formatEur(it.total)}</td>
                  <td className={TD} />
                </tr>
              )))}
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
                <td className={`${TD} whitespace-nowrap ${p.inTable ? '' : 'text-slate-400'}`}>{p.conv ? `${formatEur(p.conv.eur)} @ ${formatTableRate(p.conv.rate)}` : p.inTable ? '–' : 'not in the table'}</td>
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
  const [sending, setSending] = useState(false);
  const [sentNote, setSentNote] = useState<string | null>(null);
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
  async function sendNow() {
    if (!window.confirm(`Send today's FX Challenge mail now to ${current.length} address${current.length === 1 ? '' : 'es'}?`)) return;
    setSending(true);
    setErr(null);
    setSentNote(null);
    try {
      const r = await sendMailNow();
      setSentNote(`Sent to ${r.recipients} address${r.recipients === 1 ? '' : 'es'}: ${r.subject}`);
      await load();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setSending(false);
    }
  }
  const sent = data.lastSent;
  const check = data.lastCheck;
  return (
    <Card title="Daily mail"
      aside={<span className="text-[11px] text-slate-500">{saved ? `Saved${data.updatedAt ? ` ${atIsrael(data.updatedAt)}` : ''}${data.updatedBy ? ` by ${data.updatedBy}` : ''}` : "From the server's settings (FX_MAIL_TO) until saved here"}</span>}>
      <div className="space-y-2.5 text-xs text-slate-700">
        <p>
          Checked every day but Saturday at {String(data.hour).padStart(2, '0')}:00 Israel time, and sent{data.from ? ` from ${data.from}` : ''} to the addresses below
          {' '}when there is a recommendation: a conversion for a payment the balances don&apos;t cover, a shortfall, euros to move to Hapoalim, a high rate to convert on, or spare euros for a deposit.
          {' '}{sent ? `Last sent ${atIsrael(sent.at)}${sent.manual ? ' (Send now)' : ''}${sent.recipients ? `, to ${sent.recipients} address${sent.recipients === 1 ? '' : 'es'}` : ''}.` : 'Not sent by the daily job yet.'}
          {check ? ` Last check ${atIsrael(check.at)}: no mail (${check.why}).` : ''}
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
          <button type="button" className={BUTTON} disabled={sending || saving || changed || !current.length} onClick={() => void sendNow()}
            title={changed ? 'Save the list first: Send now goes to the saved list.' : !current.length ? 'Nobody is on the list.' : "Read today's figures and send the mail now, with or without a recommendation. It counts as today's mail."}>
            {sending && <Loader2 size={12} className="animate-spin" />}Send now
          </button>
        </form>
        {err && <p className="text-rose-700">{err}</p>}
        {sentNote && !err && <p className="text-emerald-700">{sentNote}</p>}
        {changed && !err && <p className="text-amber-700">Not saved yet: until you save, the mail goes to the list as it was.</p>}
        <p className="text-[11px] text-slate-500">lsports.eu addresses only, as the mail carries LSports&apos; balances. Each save is kept in the history with who made it.</p>
      </div>
    </Card>
  );
}
