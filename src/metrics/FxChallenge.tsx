// Business Tools → Metrics → FX Challenge: finance's monthly EUR→ILS conversions at Bank Hapoalim (fixed on the
// 7th by the conversion table), simulated at any rate, and how much EUR to keep for them (GET /api/metrics?view=fx;
// the logic is src/forecast/fx-challenge.mjs, the same as the server's tests).
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { Loader2, RefreshCw } from 'lucide-react';
import { ErrorCard, Segmented, Skeleton, Warnings } from '../new-dashboard/PageParts.tsx';
import { KpiCard } from '../new-dashboard/KpiStrip.tsx';
import { readParam, writeParam } from '../new-dashboard/urlParams.ts';
import { parseTable, POLICIES, POLICY_LABEL, tableToText, type FxTable, type Policy } from '../forecast/fx-challenge.mjs';
import { fetchFx, patchSettings } from './api.ts';
import { formatEur, monthName } from './model.ts';
import { buildFxModel, formatDay, formatIls, formatPct0, formatRate, type FxModel } from './fxModel.ts';
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

export default function FxChallenge({ tabs }: { tabs: ReactNode }) {
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
          <div className="flex flex-wrap items-center gap-3">
            <div>
              <h1 className="text-lg font-semibold text-slate-900">Metrics</h1>
              <p className="text-sm text-slate-500">LSports · FX Challenge: the EUR→ILS conversions fixed on the 7th, and what to keep for them</p>
            </div>
            {tabs}
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
              On each fixing, that day&apos;s EUR/ILS picks a band of the table, and the cell is the euros sold (thousands) and the
              deals&apos; blended rate. A rate on a band&apos;s edge counts in the band above it; the deals&apos; documents decide the exact
              edge. The chance of each band comes from EUR/ILS moving like it has (volatility from NetSuite&apos;s daily rates)
              between today and the fixing, around today&apos;s rate. The typical inflow is the EUR that came in less what was paid in
              EUR, before conversions, deposits and transfers, averaged over the last three closed months. Deposits that mature
              before a fixing count for it; later ones are the backup.
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
      <KpiCard label="Next fixing" value={next && next.atSpot ? next.atSpot.eur : null} ccy="eur"
        display={next && next.atSpot ? `${formatEur(next.atSpot.eur)} → ${formatIls(next.atSpot.ils)}` : undefined}
        note={next ? `${formatDay(next.fixing.date)}${next.fixing.weekend ? ' (weekend)' : ''} at today's rate${next.atSpot ? `, ${next.atSpot.rate}` : ''}` : 'No fixing left in the table'} />
      <KpiCard label="Keep for it" value={plan ? plan.required : null} ccy="eur" display={plan ? formatEur(plan.required) : undefined}
        note={model.plan ? POLICY_LABEL[model.plan.policy] : 'Needs balances'} />
      <KpiCard label="Free to convert now" value={plan ? plan.freeNow : null} ccy="eur" tone={plan && plan.shortfall > 0 ? 'warn' : 'default'}
        display={plan ? formatEur(plan.freeNow) : undefined}
        note={plan ? (plan.shortfall > 0 ? `Short ${formatEur(plan.shortfall)} for the fixing` : plan.freeByFixing !== null ? `${formatEur(plan.freeByFixing)} by the fixing, with typical inflows` : 'Keeping the reserve liquid') : 'Needs balances'} />
    </div>
  );
}

// ── the recommendation ──
function Recommendation({ model, policy, onPolicy, inflowMonths }: { model: FxModel; policy: Policy; onPolicy: (p: Policy) => void; inflowMonths: string[] }) {
  const plan = model.plan;
  const first = model.schedule[0];
  const aside = (
    <Segmented label="What to keep" value={policy} onChange={onPolicy}
      options={POLICIES.map((p) => ({ value: p, label: POLICY_LABEL[p], title: p === 'p95' ? 'Enough for 19 fixings in 20' : p === 'worst' ? "The largest amount in the fixing's row" : "What today's rate would sell" }))} />
  );
  if (!plan || !first || !first.risk || !first.atSpot) {
    return <Card title="What to keep, what is free" aside={aside}><p className="text-sm text-slate-500">The recommendation needs today&apos;s rate and the balances; see the notes above.</p></Card>;
  }
  const n = plan.next;
  // The bands with a real chance on the next fixing, for the sentence under the headline.
  const likely = first.risk.probs.map((p, i) => ({ p, i, eur: -first.fixing.row.cells[i].eurK * 1000, rate: first.fixing.row.cells[i].rate }))
    .filter((b) => b.p >= 0.02).sort((a, b) => a.i - b.i);
  return (
    <Card title="What to keep, what is free" aside={aside}>
      <div className="space-y-3 text-sm text-slate-700">
        <p className="text-base text-slate-900">
          Keep <strong>{formatEur(n.required)}</strong> in Hapoalim EUR (583400) for {formatDay(n.date)}
          {first.fixing.weekend ? ' (a weekend: the deal sets the day)' : ''}.
          {' '}Free to convert now: <strong>{formatEur(n.freeNow)}</strong>
          {n.freeNowWithBackup > n.freeNow ? <>, or <strong>{formatEur(n.freeNowWithBackup)}</strong> if the backup deposits cover a rate jump</> : null}.
        </p>
        <ul className="list-disc space-y-1 pl-5 text-[13px]">
          <li>
            At today&apos;s {formatRate(model.spot)} the fixing sells {formatEur(n.todayAmount)} at {first.atSpot.rate} ({formatIls(first.atSpot.ils)}).
            {' '}Expected {formatEur(first.risk.expectedEur)}; {likely.map((b) => `${model.labels[b.i]}: ${formatEur(b.eur)} (${formatPct0(b.p)})`).join(' · ')}.
            {' '}The table&apos;s worst case is {formatEur(first.risk.worstEur)}.
          </li>
          <li>
            Euros now: Hapoalim {formatEur(n.hapoalim)}, elsewhere {formatEur(n.other)}
            {n.moveToHapoalim > 0 ? <> (move <strong>{formatEur(n.moveToHapoalim)}</strong> to Hapoalim before the fixing)</> : null}
            {n.maturing > 0 ? `, deposits maturing by then ${formatEur(n.maturing)}` : ''}
            {n.backup > 0 ? `, backup deposits after it ${formatEur(n.backup)}` : ''}.
          </li>
          {n.shortfall > 0 && (
            <li className="text-rose-700">
              Today&apos;s euros are {formatEur(n.shortfall)} short of the reserve: it needs inflows before the fixing{n.backup > 0 ? ', or a backup deposit' : ''}.
            </li>
          )}
          {n.freeByFixing !== null && model.inflow && (
            <li>
              With the typical inflow ({formatEur(model.inflow.perMonth)} a month, {inflowMonths.map((m) => monthName(m).slice(0, 3)).join('–')}), about{' '}
              {formatEur(n.inflowUntil)} comes in before the fixing, so <strong>{formatEur(n.freeByFixing)}</strong> can be converted before it,
              in total, as the euros come in, with the next {plan.path.length} fixings still covered.
            </li>
          )}
        </ul>
        {plan.path.length > 1 && (
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="px-2 py-1.5 text-left font-medium text-slate-500">Fixing</th>
                  <th className={TH}>Keep ({POLICY_LABEL[plan.policy].toLowerCase()})</th>
                  <th className={TH}>Expected</th>
                  <th className={TH} title="Today's liquid euros, plus typical inflows and deposits maturing by then, less the earlier fixings at the amount kept">Euros by then</th>
                  <th className={TH} title="Euros converted before a fixing are gone at it and every later one">Can be converted before it</th>
                </tr>
              </thead>
              <tbody>
                {plan.path.map((p) => (
                  <tr key={p.month} className="border-b border-slate-100">
                    <td className="px-2 py-1.5 text-left text-slate-700">{formatDay(p.date)}{p.month.slice(0, 4) !== model.schedule[0].fixing.month.slice(0, 4) ? ` ${p.month.slice(0, 4)}` : ''}</td>
                    <td className={TD}>{formatEur(p.required)}</td>
                    <td className={`${TD} text-slate-500`}>{formatEur(p.expected)}</td>
                    <td className={TD}>{formatEur(p.available)}</td>
                    <td className={`${TD} font-medium ${p.surplus < 0 ? 'text-rose-700' : 'text-emerald-700'}`}>{p.surplus < 0 ? `short ${formatEur(-p.surplus)}` : formatEur(p.convertibleBefore)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-1 text-[11px] text-slate-500">If nothing else is converted and inflows stay typical: the euros converted before a fixing, in total, can reach that column and every fixing is still covered.</p>
          </div>
        )}
      </div>
    </Card>
  );
}

// ── the simulation ──
const PRESETS = [-0.05, -0.02, 0, 0.02, 0.05];

function Simulation({ model, rateText, onRate }: { model: FxModel; rateText: string | null; onRate: (v: string | null) => void }) {
  const rate = model.simRate ?? 3.5;
  return (
    <Card title="Simulate the rate on the fixings" aside={<span className="text-[11px] text-slate-500">The recommendation stays on today&apos;s rate; this shows what each fixing converts at the rate you set.</span>}>
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
              <th className="px-2 py-1.5 text-left font-medium text-slate-500">Fixing</th>
              <th className={TH}>Sold at {formatRate(model.simRate)}</th>
              <th className={TH}>Rate</th>
              <th className={TH}>₪ received</th>
              <th className={TH} title="Against selling the same euros at the simulated rate">₪ vs that rate</th>
              <th className={TH} title="Weighted by the chance of each band, from today's rate">Expected €</th>
              <th className={TH} title="Enough for 19 fixings in 20, from today's rate">95% €</th>
              <th className={TH}>Worst €</th>
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
                <td className={`${TD} text-slate-500`}>{s.risk ? formatEur(s.risk.expectedEur) : '–'}</td>
                <td className={`${TD} text-slate-500`}>{s.risk ? formatEur(s.risk.p95Eur) : '–'}</td>
                <td className={`${TD} text-slate-500`}>{s.risk ? formatEur(s.risk.worstEur) : '–'}</td>
              </tr>
            ))}
            {model.totals.map((t) => (
              <tr key={t.year} className="bg-slate-50 font-semibold text-slate-900">
                <td className="px-2 py-1.5 text-left">{t.year} ({t.count} fixing{t.count === 1 ? '' : 's'} left)</td>
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
    <Card title={`The conversion table (${table.pair}, € thousands sold / rate, fixing on the ${table.fixingDay}th)`}
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
              <th className="px-1.5 py-1 text-left font-medium text-slate-500">Month</th>
              {model.labels.map((l, i) => (
                <th key={l} className={`px-1.5 py-1 text-center font-medium ${i === model.simBand ? 'rounded bg-sky-700 text-white' : 'text-slate-500'}`}
                  title={i === model.spotBand ? "Today's band" : undefined}>
                  {l}{i === model.spotBand ? ' •' : ''}
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
      <p className="mt-1.5 text-[11px] text-slate-500">The highlighted column is the band at the simulated rate; • marks today&apos;s band. Past fixings are dimmed.</p>
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
      await patchSettings({ fxChallenge: value });
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
        <label className="inline-flex items-center gap-1">Fixing day
          <input aria-label="Fixing day" type="number" min={1} max={28} value={day} onChange={(e: { target: { value: string } }) => setDay(e.target.value)} className={`${INPUT} w-14 text-right`} />
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
          {' '}{parsed.table.bounds.length + 1} bands, fixing on the {parsed.table.fixingDay}th{same ? ': the same as the current table.' : '.'}
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
  const eur = lsports.filter((r) => r.currency === 'EUR' && (r.kind === 'current' || r.kind === 'deposit'))
    .sort((x, y) => (x.kind === y.kind ? Math.abs(y.amount) - Math.abs(x.amount) : x.kind === 'current' ? -1 : 1));
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
                <th className="px-2 py-1.5 text-left font-medium text-slate-500">Where</th>
                <th className="px-2 py-1.5 text-left font-medium text-slate-500">Kind</th>
                <th className={TH}>EUR</th>
                <th className={TH}>{b.source === 'mail' ? 'As of / matures' : 'Matures'}</th>
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
              <div className="flex justify-between"><span>Hapoalim EUR (the fixings' account)</span><strong className="tabular-nums text-slate-900">{formatEur(p.hapoalim)}</strong></div>
              <div className="flex justify-between"><span>Other EUR accounts (movable)</span><strong className="tabular-nums text-slate-900">{formatEur(p.other)}</strong></div>
              <div className="flex justify-between"><span>EUR deposits</span><strong className="tabular-nums text-slate-900">{formatEur(p.deposits.reduce((s, d) => s + d.amount, 0))}</strong></div>
            </>
          )}
          {byCcy.size > 0 && (
            <p className="border-t border-slate-100 pt-1.5 text-[11px] text-slate-500">
              Not used by the fixings: {[...byCcy.entries()].map(([c, v]) => `${c} ${Math.round(v).toLocaleString('en-US')}`).join(' · ')}.
            </p>
          )}
          {elsewhere.length > 0 && <p className="text-[11px] text-slate-500">Other companies&apos; balances (e.g. Bringits) are left out: the fixings are LSports&apos;.</p>}
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
    <Card title="Past fixings: the table against what was booked">
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b border-slate-200">
              <th className="px-2 py-1.5 text-left font-medium text-slate-500">Fixing</th>
              <th className={TH}>EUR/ILS that day</th>
              <th className={TH}>The table</th>
              <th className={TH}>Booked (Hapoalim, &quot;Conversion&quot;)</th>
              <th className={TH} title="Shekels received against selling the same euros at that day's rate">₪ vs that day&apos;s rate</th>
              <th className="px-2 py-1.5 text-left font-medium text-slate-500">Other conversions that month</th>
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
