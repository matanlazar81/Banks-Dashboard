#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// The FX Challenge's daily mail: what to keep for the next conversion, what is free to convert, whether today's
// EUR/ILS makes it a good day to convert, what the next conversion can sell, the euros and the next six conversions,
// as on the page (Business Tools → FX Challenge), read fresh from NetSuite. HTML, with a plain-text part.
//
//   node scripts/fx-challenge-mail.cjs              read and send now (a test; the daily mail still goes)
//   node scripts/fx-challenge-mail.cjs --dry-run    read and print it, send nothing (--html=<file> also writes the HTML)
//   node scripts/fx-challenge-mail.cjs --scheduled  the cron, every hour: sends once a day from FX_MAIL_HOUR, Israel
//                                                   time, so daylight saving needs no change. An incomplete read
//                                                   (no balances, no rate) is tried again the next two hours.
//
// Who gets it is kept on the page ("Daily mail", saved for everyone in data/fx-challenge-mail.json); until a list is
// saved there, FX_MAIL_TO. lsports.eu addresses only, unless FX_MAIL_ALLOW_EXTERNAL=1 (the mail carries LSports'
// balances). An empty list on the page sends to nobody.
//
// The checkout's .env:
//   FX_MAIL_TO      recipients until the page has a list, comma-separated
//   FX_MAIL_FROM    the sender (default SMTP_USER); the mail server has to allow sending as it
//   FX_MAIL_HOUR    the hour of the daily mail, Israel time (default 10)
//   FX_MAIL_POLICY  what to keep: today | p95 (default) | worst, as the page's "What to keep"
//   FX_MAIL_LINK    the page's address, for a link at the bottom (optional)
//   The mail server: FX_MAIL_SMTP_URL, else NET_CASH_SMTP_URL, else SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS
//   (the backend's mailer); FX_MAIL_SMTP_HELO, else NET_CASH_SMTP_HELO, else the sender's domain, for Google's relay.
//
// Exit codes: 0 sent, skipped (not the hour, sent already) or printed; 1 not sent (the log says why).
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
try { require('dotenv').config({ path: path.join(ROOT, '.env'), quiet: true }); } catch { /* the environment may be set already */ }

const fxs = require(path.join(ROOT, 'server', 'fx-challenge.cjs'));
const { readDoc } = require(path.join(ROOT, 'server', 'json-store.cjs'));

const MARKER_FILE = fxs.MAIL_SENT_FILE;
const TAG = '[fx-mail]';

// ── formatting (as the page, with fixed day names so every server writes the same) ──
const int = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function money(symbol, n) {
  if (n === null || n === undefined || !Number.isFinite(n)) return '–';
  const sign = n < 0 ? '-' : '';
  const a = Math.abs(n);
  if (a >= 1e6) return `${sign}${symbol}${(a / 1e6).toFixed(2)}M`;
  if (a >= 1e3) return `${sign}${symbol}${int.format(Math.round(a / 1e3))}K`;
  return a < 0.5 ? `${symbol}0` : `${sign}${symbol}${int.format(Math.round(a))}`;
}
const eur = (n) => money('€', n);
const ils = (n) => money('₪', n);
const rate = (r) => (r === null || r === undefined || !Number.isFinite(r) ? '–' : r.toFixed(3));
const pct = (p) => `${Math.round(p * 100)}%`;
/** "Sat 7 Nov", or "Sat 7 Nov 2026" with the year. */
function day(iso, withYear = false) {
  if (!iso) return '–';
  const d = new Date(`${String(iso).slice(0, 10)}T12:00:00Z`);
  return `${WEEKDAYS[d.getUTCDay()]} ${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]}${withYear ? ` ${d.getUTCFullYear()}` : ''}`;
}
const monthShort = (mKey) => MONTHS[Number(mKey.slice(5)) - 1];
const rangeLabel = (o) => (o.lo === null ? `below ${o.hi}` : o.hi === null ? `${o.lo} or more` : `${o.lo} – ${o.hi}`);
const chance = (p) => (p < 0.005 ? '<1%' : `${Math.round(p * 100)}%`);
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

const RATE_WORD = { high: 'high', middle: 'mid-range', low: 'low' };
const RATE_VERDICT = { high: 'a good day to convert', middle: 'an average day to convert', low: 'a weak day to convert' };
const RATE_COLOR = { high: '#047857', middle: '#0f172a', low: '#b45309' };
const RATE_SUBJECT = { high: 'a good day', middle: 'an average day', low: 'a weak day' };
const SOURCE = { mail: "the daily balances mail's sources", books: "NetSuite's books (EUR only, no deposit dates)" };

// ── the mail ──
// Each part is written twice, as HTML and as text: a part is { html, text }.
const para = (html, text, style = '') => ({ html: `<p style="margin:0 0 10px;font-size:14px;line-height:1.5;${style}">${html}</p>`, text });
const strong = (s, color = '#0f172a') => `<strong style="color:${color};">${esc(s)}</strong>`;
const heading = (s) => ({ html: `<p style="margin:18px 0 6px;font-size:13px;font-weight:bold;color:#0f172a;">${esc(s)}</p>`, text: `\n${s.toUpperCase()}` });

/** A table: head and rows of plain strings (left-aligned first column); `mark(i)` highlights a row, `dim(i)` greys it;
 * `compact` for six columns on a phone. */
function table(head, rows, { mark = () => false, dim = () => false, compact = false } = {}) {
  const pad = compact ? '4px 4px' : '5px 6px';
  const size = compact ? 12 : 13;
  const cell = (c, i, style) => `<td style="padding:${pad};text-align:${i ? 'right' : 'left'};border-bottom:1px solid #f1f5f9;font-size:${size}px;white-space:nowrap;${style}">${esc(c)}</td>`;
  const html = `<table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin:2px 0 12px;">`
    + `<tr>${head.map((h, i) => `<th style="padding:${pad};text-align:${i ? 'right' : 'left'};font-weight:normal;font-size:${size - 1}px;color:#64748b;border-bottom:1px solid #e2e8f0;">${esc(h)}</th>`).join('')}</tr>`
    + rows.map((r, n) => {
      const style = mark(n) ? 'background:#e0f2fe;font-weight:bold;color:#0f172a;' : dim(n) ? 'color:#94a3b8;' : 'color:#334155;';
      return `<tr>${r.map((c, i) => cell(c, i, style)).join('')}</tr>`;
    }).join('')
    + '</table>';
  const all = [head, ...rows];
  const width = head.map((_, i) => Math.max(...all.map((r) => String(r[i]).length)));
  const text = all.map((r, n) => r.map((c, i) => (i ? String(c).padStart(width[i]) : String(c).padEnd(width[i]))).join('  ') + (n && mark(n - 1) ? '  <' : '')).join('\n');
  return { html, text };
}

const list = (items) => ({
  html: `<ul style="margin:0 0 10px;padding-left:20px;font-size:13px;line-height:1.5;">${items.map((x) => `<li style="margin-bottom:4px;${x.style || ''}">${x.html}</li>`).join('')}</ul>`,
  text: items.map((x) => `- ${x.text}`).join('\n'),
});

function page(title, subtitle, parts, footer) {
  const html = '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>'
    + '<body style="margin:0;padding:0;background:#f1f5f9;">'
    + '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;"><tr><td align="center" style="padding:12px 2px;">'
    + '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:720px;background:#ffffff;border:1px solid #e2e8f0;border-radius:8px;font-family:Arial,Helvetica,sans-serif;color:#334155;">'
    + `<tr><td style="padding:14px 20px;border-bottom:1px solid #e2e8f0;"><div style="font-size:12px;color:#64748b;">${esc(subtitle)}</div>`
    + `<div style="font-size:18px;font-weight:bold;color:#0f172a;margin-top:2px;">${esc(title)}</div></td></tr>`
    + `<tr><td style="padding:16px 10px;">${parts.map((p) => p.html).join('')}</td></tr>`
    + `<tr><td style="padding:12px 20px;border-top:1px solid #e2e8f0;font-size:11px;line-height:1.5;color:#64748b;">${footer.map((f) => f.html).join('<br>')}</td></tr>`
    + '</table></td></tr></table></body></html>';
  const text = [title, subtitle, '', ...parts.map((p) => p.text), '', '--', ...footer.map((f) => f.text)].join('\n');
  return { html, text };
}

/**
 * The mail from GET /api/fx-challenge's answer (readFxChallenge): { subject, html, text }. fx is
 * src/forecast/fx-challenge.mjs; the recommendation is its planFromPayload, the same as the page's.
 */
function renderMail(fx, data, { policy = 'p95', link = null } = {}) {
  const v = fx.planFromPayload(data, policy);
  const plan = v.plan;
  const first = v.schedule[0];
  const rc = v.rateCheck;
  const today = data.today;
  const parts = [];
  let title;
  let subject;

  if (plan && first && first.atSpot && first.risk) {
    const n = plan.next;
    const cash = v.cash;
    const months = (data.inflowMonths || []).map(monthShort).join('–');
    const weekend = first.fixing.weekend ? ' (a weekend: the deal sets the day)' : '';
    // What can go: with the salaries known, the spare (every due met); without them, the reserve's free euros.
    const spare = cash ? n.spare : n.freeNow;
    if (cash) {
      const firstMonth = cash.months.find((m) => m.net > 0 || m.later > 0);
      const needs = firstMonth ? cash.needs.filter((x) => x.month <= firstMonth.month) : [];
      const must = needs.reduce((s2, x) => s2 + x.eur, 0);
      const dueBy = firstMonth ? `${firstMonth.month}-15` : null;
      const by = needs.length ? needs[needs.length - 1].date : null;
      title = must > 0.5 ? `Convert ${eur(must)} for salaries by ${day(by)} · keep ${eur(n.required)} for ${day(n.date)}` : `Keep ${eur(n.required)} for ${day(n.date)} · salaries covered`;
      subject = `FX Challenge ${day(today)}: ${must > 0.5 ? `convert ${eur(must)} for salaries by ${day(by)}` : 'salaries covered'}`
        + ` · ${n.short ? `short ${eur(n.short.first.eur)} by ${day(n.short.first.date)}` : `spare ${eur(spare)}`}`
        + ` · keep ${eur(n.required)} for ${day(n.date)}${rc ? ` · ${RATE_SUBJECT[rc.level]} (${rate(rc.rate)})` : ''}`;
      parts.push(para(`Keep ${strong(eur(n.required))} in Hapoalim EUR (583400) for ${esc(day(n.date))}${weekend}.`,
        `Keep ${eur(n.required)} in Hapoalim EUR (583400) for ${day(n.date)}${weekend}.`, 'font-size:15px;'));
      const convertText = needs.length
        ? `For the salaries and vendors due by ${day(dueBy)}, convert at least ${eur(must)}${needs.length > 1 ? `: ${needs.map((x) => `${eur(x.eur)} by ${day(x.date)}`).join(' and ')}` : ` by ${day(needs[0].date)}`}.`
        : `The shekels on hand and the ${day(n.date)} conversion cover the salaries and vendors due by ${day(dueBy)}.`;
      const spareText = n.short
        ? `Nothing is spare: on ${day(n.short.first.date)} the needs exceed the euros held and expected by ${eur(n.short.first.eur)}`
          + `${n.short.date !== n.short.first.date ? `, and by ${eur(n.short.eur)} on ${day(n.short.date)}` : ''}${n.backup > 0 ? ', so a backup deposit may be needed' : ''}.`
        : `Spare now: ${eur(spare)}${spare > 0 ? ', for a new deposit or to convert on a good day' : ''}.`;
      parts.push(para(`${esc(convertText).replace(esc(eur(must)), strong(eur(must)))} <span style="color:${n.short ? '#be123c' : '#0f172a'};">${esc(spareText)}</span>`,
        `${convertText} ${spareText}`, 'font-size:15px;'));
      const ok = plan.deposits.filter((d) => d.eur > 0);
      const dep = ok.length
        ? `New deposits: ${ok.map((d) => `${eur(d.eur)} for ${d.months} month${d.months === 1 ? '' : 's'} (until ${day(d.until)})`).join(', ')}, with everything before then still covered as the typical inflow comes in.`
        : `New deposits: none now. The coming weeks need the euros held${n.short ? '' : ' and expected'}.`;
      parts.push(para(esc(dep), dep, 'font-size:13px;'));
    } else {
      title = n.shortfall > 0
        ? `Keep ${eur(n.required)} for ${day(n.date)}: today's euros are ${eur(n.shortfall)} short`
        : `Keep ${eur(n.required)} for ${day(n.date)} · free to convert now ${eur(n.freeNow)}`;
      subject = `FX Challenge ${day(today)}: ${n.shortfall > 0 ? `${eur(n.shortfall)} short for ${day(n.date)}` : `${eur(n.freeNow)} free to convert`}`
        + `${rc ? `, ${RATE_SUBJECT[rc.level]} (EUR/ILS ${rate(rc.rate)})` : ''} · keep ${eur(n.required)} for ${day(n.date)}`;
      const backup = n.freeNowWithBackup > n.freeNow;
      parts.push(para(
        `Keep ${strong(eur(n.required))} in Hapoalim EUR (583400) for ${esc(day(n.date))}${weekend}. `
        + `Free to convert now: ${strong(eur(n.freeNow))}${backup ? `, or ${strong(eur(n.freeNowWithBackup))} if the backup deposits cover a rate jump` : ''}.`,
        `Keep ${eur(n.required)} in Hapoalim EUR (583400) for ${day(n.date)}${weekend}. `
        + `Free to convert now: ${eur(n.freeNow)}${backup ? `, or ${eur(n.freeNowWithBackup)} if the backup deposits cover a rate jump` : ''}.`,
        'font-size:15px;',
      ));
    }
    if (rc) {
      const diff = spare * (rc.rate - rc.average);
      const lead = `EUR/ILS ${rate(rc.rate)} is ${RATE_WORD[rc.level]} for the last ${rc.days} days: above ${pct(rc.share)} of them (average ${rate(rc.average)}, from ${rate(rc.low)} to ${rate(rc.high)}), so `;
      const tail = spare > 0
        ? `: the ${eur(spare)} ${cash ? 'spare' : 'free'} now brings ${ils(spare * rc.rate)}, ${ils(Math.abs(diff))} ${diff >= 0 ? 'more' : 'less'} than at the average.`
        : '. Nothing is spare to convert today.';
      parts.push(para(`${esc(lead)}${strong(RATE_VERDICT[rc.level], RATE_COLOR[rc.level])}${esc(tail)}`, `${lead}${RATE_VERDICT[rc.level].toUpperCase()}${tail}`));
    }

    if (cash) {
      parts.push(heading('Salaries and vendors: the shekels each month needs'));
      parts.push(table(['Paid in', 'Net (5th)', 'Tax, pension, SS (15th)', 'ILS vendors', 'The 7th brings', 'Convert'],
        cash.months.map((m) => [
          `${monthShort(m.month)} ${m.month.slice(2, 4)}`, m.net > 0.5 ? ils(m.net) : '–', m.later > 0.5 ? ils(m.later) : '–', m.vendors > 0.5 ? ils(m.vendors) : '–',
          m.tableIls > 0.5 ? `+${ils(m.tableIls)}` : '–', m.convertIls > 0.5 ? `${eur(m.convertEur)}` : '–',
        ]), { compact: true }));
      const notes = [
        `Shekels on hand today: ${cash.ilsNow === null ? 'not known' : ils(cash.ilsNow)}. Converted at today's rate (${rate(v.spot)}); each month's euros by the day they are due: ${cash.needs.slice(0, 4).map((x) => `${day(x.date)} ${eur(x.eur)}`).join(', ')}${cash.needs.length > 4 ? ', …' : ''}.`,
        `Payroll: the Salary Projection's Total payroll${cash.projectionAsOf ? ` (saved ${day(cash.projectionAsOf.slice(0, 10))})` : ''}${cash.share !== null ? `, ${pct(cash.share)} net salaries (by the 5th of the next month) and ${pct(1 - cash.share)} pension, withholding tax and social security (by the 15th), as NetSuite's payroll journals of ${months}` : ''}.`,
        `Vendors: ${cash.vendors.ils === null ? 'ILS not known' : `${ils(cash.vendors.ils)} a month from the ILS accounts`}, and ${eur(cash.vendors.eur)} a month kept in euros at all times (the averages of ${months}). The 7th brings the shekels of the amount kept.`,
        ...cash.notes,
      ];
      parts.push(list(notes.map((x) => ({ html: esc(x), text: x, style: 'font-size:12px;color:#64748b;' }))));
    }

    parts.push(heading(`What the ${day(n.date)} conversion sells`));
    parts.push(para(esc(`It depends on EUR/ILS that day. At today's ${rate(v.spot)}: ${eur(n.todayAmount)} at ${first.atSpot.rate} (${ils(first.atSpot.ils)}). On average (expected): ${eur(first.risk.expectedEur)}.`),
      `It depends on EUR/ILS that day. At today's ${rate(v.spot)}: ${eur(n.todayAmount)} at ${first.atSpot.rate} (${ils(first.atSpot.ils)}). On average (expected): ${eur(first.risk.expectedEur)}.`));
    const shown = (first.outcomes || []).filter((o) => o.p >= 0.005 || (v.spotBand !== null && o.bands.includes(v.spotBand)));
    parts.push(table(['If EUR/ILS that day is', 'Chance', 'It sells', 'At', 'You receive'],
      shown.map((o) => [`${rangeLabel(o)}${v.spotBand !== null && o.bands.includes(v.spotBand) ? ' (today)' : ''}`, chance(o.p), eur(o.eur), String(o.rate), ils(o.ils)]),
      { mark: (i) => v.spotBand !== null && shown[i].bands.includes(v.spotBand), dim: (i) => shown[i].p < 0.01 }));

    parts.push(heading('Your euros'));
    const items = [];
    const euros = `Euros now: Hapoalim ${eur(n.hapoalim)}, elsewhere ${eur(n.other)}`
      + `${n.moveToHapoalim > 0 ? ` (move ${eur(n.moveToHapoalim)} to Hapoalim before the conversion)` : ''}`
      + `${n.maturing > 0 ? `, deposits maturing by then ${eur(n.maturing)}` : ''}${n.backup > 0 ? `, backup deposits after it ${eur(n.backup)}` : ''}.`;
    items.push({ html: esc(euros), text: euros });
    let adds;
    if (cash) {
      const t = n.tightest;
      adds = `How the spare adds up: euro cash ${eur(n.liquid)} − ${eur(n.eurFloor)} kept for EUR vendors = ${eur(n.liquid - n.eurFloor)} at most today`
        + `${t ? `; the tightest due, ${day(t.date)} (${t.shekels ? 'salaries and vendors' : 'the conversion'}), ${t.spare < 0 ? `is short ${eur(-t.spare)}` : `leaves ${eur(t.spare)} after it`}` : ''}; so ${eur(n.spare)}.`;
    } else {
      adds = n.shortfall > 0
        ? `How it adds up: keep ${eur(n.required)} − deposits maturing before it ${eur(n.maturing)} = ${eur(n.needLiquid)} needed from today's cash of ${eur(n.liquid)}: short ${eur(n.shortfall)}. It needs inflows before the conversion${n.backup > 0 ? ', or a backup deposit' : ''}.`
        : `How it adds up: keep ${eur(n.required)} − deposits maturing before it ${eur(n.maturing)} = ${eur(n.needLiquid)} needed from today's cash; ${eur(n.liquid)} − ${eur(n.needLiquid)} = ${eur(n.freeNow)} free.`;
    }
    items.push({ html: esc(adds), text: adds, style: (cash ? n.short : n.shortfall > 0) ? 'color:#be123c;' : '' });
    if (n.freeByFixing !== null && v.inflow) {
      const s2 = `With the typical inflow (${eur(v.inflow.perMonth)} a month, ${months}), about ${eur(n.inflowUntil)} comes in before the conversion`
        + (n.freeByFixing > 0.5
          ? `, so ${eur(n.freeByFixing)} can be converted before it, in total, as the euros come in, with the next ${plan.path.length} conversions${cash ? ', the salaries and vendors' : ''} still covered.`
          : `: ${cash ? 'the salaries, vendors and this conversion need all of it' : 'this conversion needs all of it'}.`);
      items.push({ html: esc(s2), text: s2 });
    }
    parts.push(list(items));

    if (plan.path.length > 1) {
      const year0 = plan.path[0].month.slice(0, 4);
      parts.push(heading(`The next ${plan.path.length} conversions, if nothing else is converted`));
      parts.push(table(
        cash
          ? ['Conversion day', 'Keep', 'Deposits', 'Salaries', 'Euros by then', 'Spare before']
          : ['Conversion day', `Keep (${fx.POLICY_LABEL[plan.policy].toLowerCase()})`, 'Deposits released', 'Euros by then', 'Can be converted before it'],
        plan.path.map((p) => [
          `${day(p.date)}${p.month.slice(0, 4) !== year0 ? ` ${p.month.slice(0, 4)}` : ''}`, eur(p.required),
          p.released > 0 ? eur(p.released) : '–', ...(cash ? [p.shekels > 0.5 ? eur(p.shekels) : '–'] : []),
          eur(p.available), p.surplus < 0 ? `short ${eur(-p.surplus)}` : eur(p.convertibleBefore),
        ]),
        { compact: !!cash },
      ));
      const dated = plan.path.filter((p) => p.releasedDeposits.length);
      const lines = dated.map((p) => `By ${day(p.date)}: ${p.releasedDeposits.map((d) => `${day(d.valueDate)} ${eur(d.amount)}`).join(' · ')}`);
      const later = plan.later.reduce((s2, d) => s2 + d.amount, 0);
      if (later > 0) lines.push(`After ${day(plan.path[plan.path.length - 1].date, true)}: a further ${eur(later)} in ${plan.later.length} deposit${plan.later.length === 1 ? '' : 's'}.`);
      if (lines.length) {
        parts.push(para(esc('Deposits released, each on the day it matures (if the bank pays it out rather than rolling it over):'), 'Deposits released, each on the day it matures (if the bank pays it out rather than rolling it over):', 'font-size:13px;margin-bottom:4px;'));
        parts.push(list(lines.map((l) => ({ html: esc(l), text: l }))));
      }
      const explain = cash
        ? `Keep: ${fx.POLICY_LABEL[plan.policy].toLowerCase()}. Salaries: the euros converted for salaries and ILS vendors since the conversion before. Euros by then: cash today, plus the typical inflow and the deposits released by then, less the earlier conversions at the amount kept and the salaries. Spare before it: what can be converted before it, in total, with every later conversion, the salaries and vendors still covered and ${eur(n.eurFloor)} kept for EUR vendors.`
        : 'Euros by then: cash today, plus the typical inflow and the deposits released by then, less the earlier conversions at the amount kept. Can be converted before it: the smallest spare from that day on, so every later conversion is still covered.';
      parts.push(para(esc(explain), explain, 'font-size:12px;color:#64748b;'));
    }
  } else {
    title = 'No recommendation today';
    subject = `FX Challenge ${day(today)}: no recommendation today`;
    const why = !v.schedule.length ? 'No conversion is left in the table.' : 'It needs today\'s rate and the balances; the notes below say what could not be read.';
    parts.push(para(esc(why), why));
    if (rc) {
      const s = `EUR/ILS ${rate(rc.rate)} is ${RATE_WORD[rc.level]} for the last ${rc.days} days: above ${pct(rc.share)} of them (average ${rate(rc.average)}, from ${rate(rc.low)} to ${rate(rc.high)}).`;
      parts.push(para(esc(s), s));
    }
  }

  if (data.warnings && data.warnings.length) {
    parts.push({
      html: `<div style="margin:12px 0 0;padding:8px 12px;background:#fffbeb;border:1px solid #fde68a;border-radius:6px;font-size:12px;line-height:1.5;color:#92400e;">${data.warnings.map(esc).join('<br>')}</div>`,
      text: `\nNOTES\n${data.warnings.map((w) => `- ${w}`).join('\n')}`,
    });
  }

  const b = data.balances;
  const asOf = b && b.asOf ? `, ${new Date(b.asOf).toLocaleString('en-GB', { timeZone: 'Asia/Jerusalem', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })}` : '';
  const footer = [
    `What to keep: ${fx.POLICY_LABEL[policy]} (FX_MAIL_POLICY). EUR/ILS: NetSuite's HSBC rate; volatility ${pct(v.sigma)} a year${v.sigmaFromHistory ? '' : ' (assumed)'}.`,
    `Balances: ${b ? `${SOURCE[b.source]}${asOf}` : 'not read'}. Typical inflow: NetSuite's books (${(data.inflowMonths || []).map(monthShort).join('–')}).`,
    'Estimates from today\'s numbers: the chances come from how EUR/ILS has moved, and the rate check looks back 90 days. Where EUR/ILS goes next is not known.',
  ].map((s) => ({ html: esc(s), text: s }));
  if (link) footer.push({ html: `The page: <a href="${esc(link)}" style="color:#0369a1;">FX Challenge</a>`, text: `The page: ${link}` });
  footer.push({ html: 'Sent every day by the bank dashboard (scripts/fx-challenge-mail.cjs).', text: 'Sent every day by the bank dashboard (scripts/fx-challenge-mail.cjs).' });

  const { html, text } = page(title, `FX Challenge · ${day(today, true)}`, parts, footer);
  return { subject, html, text };
}

/** When nothing could be read: a short mail saying so. */
function failureMail(today, error) {
  const why = String((error && error.message) || error || 'unknown error').slice(0, 300);
  const s = `The FX Challenge could not be read from NetSuite today (${why}). The page shows the figures again once NetSuite answers.`;
  const { html, text } = page('Not read today', `FX Challenge · ${day(today, true)}`, [para(esc(s), s)], [{ html: 'Sent by the bank dashboard (scripts/fx-challenge-mail.cjs).', text: 'Sent by the bank dashboard (scripts/fx-challenge-mail.cjs).' }]);
  return { subject: `FX Challenge ${day(today)}: not read today`, html, text };
}

// ── settings ──
function parseArgs(argv) {
  const a = { dryRun: false, scheduled: false, html: null };
  for (const x of argv) {
    if (x === '--dry-run') a.dryRun = true;
    else if (x === '--scheduled') a.scheduled = true;
    else if (x.startsWith('--html=')) a.html = x.slice('--html='.length);
    else throw new Error(`unknown option ${x} (--dry-run, --scheduled, --html=<file>)`);
  }
  return a;
}

/** The recipients (FX_MAIL_TO as written, or the page's list) as addresses: lsports.eu ones only, unless allowExternal
 * (the mail carries LSports' balances). `where` names the list in the errors. */
function parseRecipients(value, { allowExternal = false, where = 'FX_MAIL_TO' } = {}) {
  const to = Array.isArray(value) ? [...new Set(value.map((s) => String(s).trim()).filter(Boolean))] : fxs.splitAddresses(value);
  const errors = [];
  if (!to.length) errors.push(where === 'FX_MAIL_TO' ? 'FX_MAIL_TO is not set (and no list is saved on the page)' : `nobody is on ${where}`);
  for (const a of to) {
    if (!/^[^\s@<>"',;]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$/.test(a)) errors.push(`"${a}" in ${where} is not an address`);
    else if (!allowExternal && !/@lsports\.eu$/i.test(a)) errors.push(`${a} is outside lsports.eu (FX_MAIL_ALLOW_EXTERNAL=1 allows it)`);
  }
  return { to, errors };
}

/** The mail server from the environment: { transport, host, user } or null. The password stays in the transport. */
function smtpFrom(env) {
  const url = env.FX_MAIL_SMTP_URL || env.NET_CASH_SMTP_URL;
  if (url) {
    let u;
    try { u = new URL(url); } catch { throw new Error(`${env.FX_MAIL_SMTP_URL ? 'FX_MAIL_SMTP_URL' : 'NET_CASH_SMTP_URL'} is not a URL`); }
    const secure = u.protocol === 'smtps:';
    const user = u.username ? decodeURIComponent(u.username) : null;
    return {
      host: u.hostname,
      user,
      transport: { host: u.hostname, port: u.port ? parseInt(u.port, 10) : (secure ? 465 : 587), secure, auth: user ? { user, pass: decodeURIComponent(u.password || '') } : undefined },
    };
  }
  if (env.SMTP_HOST) {
    const port = parseInt(env.SMTP_PORT || '587', 10);
    return {
      host: env.SMTP_HOST,
      user: env.SMTP_USER || null,
      transport: { host: env.SMTP_HOST, port, secure: port === 465, auth: env.SMTP_USER && env.SMTP_PASS ? { user: env.SMTP_USER, pass: env.SMTP_PASS } : undefined },
    };
  }
  return null;
}

/**
 * Everything the mail needs, with what is wrong (a send stops on any of it). saved: the page's list (an array, maybe
 * empty) or null when none is saved, and FX_MAIL_TO is used. nobody: the page's list is empty.
 */
function mailConfig(env, saved = null) {
  const errors = [];
  const source = Array.isArray(saved) ? 'page' : 'env';
  const { to, errors: toErrors } = parseRecipients(source === 'page' ? saved : env.FX_MAIL_TO,
    { allowExternal: env.FX_MAIL_ALLOW_EXTERNAL === '1', where: source === 'page' ? "the page's list" : 'FX_MAIL_TO' });
  errors.push(...toErrors);
  let smtp = null;
  try { smtp = smtpFrom(env); } catch (e) { errors.push(e.message); }
  if (!smtp && !errors.some((e) => /URL/.test(e))) errors.push('no mail server: set FX_MAIL_SMTP_URL (or NET_CASH_SMTP_URL, or SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS)');
  const from = String(env.FX_MAIL_FROM || (smtp && smtp.user) || '').trim();
  if (!from) errors.push('FX_MAIL_FROM is not set (nor SMTP_USER)');
  else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(from)) errors.push(`FX_MAIL_FROM "${from}" is not an address`);
  if (smtp) {
    const helo = env.FX_MAIL_SMTP_HELO || env.NET_CASH_SMTP_HELO || (from.includes('@') ? from.split('@').pop() : null);
    if (helo) smtp.transport.name = helo;
  }
  const hour = fxs.mailHourOf(env.FX_MAIL_HOUR);
  const policy = ['today', 'p95', 'worst'].includes(env.FX_MAIL_POLICY) ? env.FX_MAIL_POLICY : 'p95';
  const link = /^https?:\/\/\S+$/.test(String(env.FX_MAIL_LINK || '').trim()) ? env.FX_MAIL_LINK.trim() : null;
  return { to, source, nobody: source === 'page' && !to.length, from, smtp, hour, policy, link, errors };
}

/** The hour in Israel (0–23). */
const israelHour = (ms) => Number(new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Jerusalem', hour: '2-digit', hourCycle: 'h23' }).format(new Date(ms))) % 24;

/** --scheduled: 'wait' before the hour, 'done' once today's mail went, 'send' at the hour and the next one, then 'last'
 * (send even an incomplete mail rather than none). */
function scheduleState({ hour, sendHour, sentDay, today }) {
  if (sentDay === today) return 'done';
  if (hour < sendHour) return 'wait';
  return hour >= sendHour + 2 ? 'last' : 'send';
}

function readMarker(file) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return null; }
}

/**
 * Reads, writes and sends (or prints) the mail; returns the exit code. deps (tests): env, clock(), reads (the
 * NetSuite reads, as readFxChallenge takes them), tableFile, mailFile (the page's list), markerFile, nodemailer.
 */
async function main(argv = process.argv.slice(2), deps = {}) {
  const env = deps.env || process.env;
  const args = parseArgs(argv);
  const now = (deps.clock || Date.now)();
  const today = fxs.israelDay(now);
  const cfg = mailConfig(env, fxs.savedRecipients(deps.mailFile || fxs.MAIL_FILE));
  const markerFile = deps.markerFile || MARKER_FILE;
  let state = 'send';
  if (args.scheduled) {
    const last = readMarker(markerFile);
    const hour = israelHour(now);
    state = scheduleState({ hour, sendHour: cfg.hour, sentDay: last && last.day, today });
    if (state === 'wait' || state === 'done') return 0;
    if (cfg.nobody) {
      if (hour === cfg.hour) console.log(`${TAG} ${today}: nobody is on the page's list, so no mail today`);
      return 0;
    }
  }
  if (!args.dryRun && cfg.errors.length) {
    console.error(`${TAG} ${today}: not sent: ${cfg.errors.join('; ')}`);
    return 1;
  }

  const fx = await fxs.loadFxModule();
  let data = null;
  let failure = null;
  try {
    data = await fxs.readFxChallenge({ reads: deps.reads || fxs.defaultNsReads(), tableDoc: readDoc(deps.tableFile || fxs.TABLE_FILE), clock: () => now });
  } catch (e) {
    failure = e;
  }
  const complete = !!data && !!fx.planFromPayload(data, cfg.policy).plan;
  if (args.scheduled && !complete && state !== 'last') {
    console.log(`${TAG} ${today}: ${failure ? `not read (${failure.message})` : 'the recommendation is incomplete'}; trying again next hour`);
    return 0;
  }
  const mail = data ? renderMail(fx, data, { policy: cfg.policy, link: cfg.link }) : failureMail(today, failure);

  if (args.dryRun) {
    console.log(`Subject: ${mail.subject}\n\n${mail.text}`);
    if (args.html) { fs.writeFileSync(args.html, mail.html); console.log(`\n${TAG} HTML written to ${args.html}`); }
    const list = cfg.source === 'page' ? "the page's list" : 'FX_MAIL_TO';
    console.log(cfg.errors.length ? `\n${TAG} a real send would stop: ${cfg.errors.join('; ')}` : `\n${TAG} a real send goes to ${cfg.to.length} recipient(s) (${list}) from ${cfg.from} via ${cfg.smtp.host}`);
    return 0;
  }

  const nodemailer = deps.nodemailer || require('nodemailer');
  await nodemailer.createTransport(cfg.smtp.transport).sendMail({ from: cfg.from, to: cfg.to.join(', '), subject: mail.subject, text: mail.text, html: mail.html });
  console.log(`${TAG} ${today}: sent to ${cfg.to.length} recipient(s) from ${cfg.from} via ${cfg.smtp.host}: ${mail.subject}`);
  if (args.scheduled) {
    fs.mkdirSync(path.dirname(markerFile), { recursive: true });
    fs.writeFileSync(markerFile, JSON.stringify({ day: today, at: new Date(now).toISOString(), recipients: cfg.to.length, subject: mail.subject }));
  }
  return 0;
}

if (require.main === module) {
  main().then((code) => process.exit(code), (e) => {
    console.error(`${TAG} failed: ${e && e.message ? e.message : e}`);
    process.exit(1);
  });
}

module.exports = { main, renderMail, failureMail, parseArgs, parseRecipients, smtpFrom, mailConfig, scheduleState, israelHour, formatDay: day, MARKER_FILE };
