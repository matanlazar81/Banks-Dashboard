#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────────────────────
// The FX Challenge's daily mail, in Hebrew and to the point: what to do today from today's balances and rate
// (todayActions: shekels to convert for a payment the balances don't cover, a shortfall, euros to move, spare euros),
// the payments in shekels ahead and today's position, as on the page (Business Tools → FX Challenge), read fresh from
// NetSuite. The tables and the workings stay on the page. HTML, with a plain-text part.
//
//   node scripts/fx-challenge-mail.cjs              read and send now (a test; the daily check still runs)
//   node scripts/fx-challenge-mail.cjs --dry-run    read and print it, and whether today's check would send it; sends
//                                                   nothing (--html=<file> also writes the HTML)
//   node scripts/fx-challenge-mail.cjs --scheduled  the cron, every hour: checks once a day from FX_MAIL_HOUR, Israel
//                                                   time, so daylight saving needs no change, and sends every day but
//                                                   Saturday when there is a recommendation (mailDecision). An
//                                                   incomplete read (no balances, no rate) is tried again the next two
//                                                   hours.
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
//   FX_MAIL_ALWAYS  1: send every day, Saturday included, with or without a recommendation
//   The mail server: FX_MAIL_SMTP_URL, else NET_CASH_SMTP_URL, else SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS
//   (the backend's mailer); FX_MAIL_SMTP_HELO, else NET_CASH_SMTP_HELO, else the sender's domain, for Google's relay.
//
// Exit codes: 0 sent, skipped (not the hour, checked already, no recommendations) or printed; 1 not sent (the log says why).
// ─────────────────────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
if (require.main === module) {
  // Run from the cron or by hand: the checkout's .env. (The page's Send now, in the server, passes its env instead.)
  // dotenv 17 prints a line each time a .env is loaded; the NetSuite reads load it again, so quiet for the whole run.
  process.env.DOTENV_CONFIG_QUIET = process.env.DOTENV_CONFIG_QUIET || 'true';
  try { require('dotenv').config({ path: path.join(ROOT, '.env'), quiet: true }); } catch { /* the environment may be set already */ }
}

const fxs = require(path.join(ROOT, 'server', 'fx-challenge.cjs'));
const { readDoc } = require(path.join(ROOT, 'server', 'json-store.cjs'));

const MARKER_FILE = fxs.MAIL_SENT_FILE;
const TAG = '[fx-mail]';

// ── formatting: the mail is in Hebrew, right to left; amounts stay €1.21M and ₪4.92M, and the day names are fixed, so
// every server writes the same ──
const int = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const WEEKDAYS = ["יום א'", "יום ב'", "יום ג'", "יום ד'", "יום ה'", "יום ו'", 'שבת'];
const MONTHS = ['ינואר', 'פברואר', 'מרץ', 'אפריל', 'מאי', 'יוני', 'יולי', 'אוגוסט', 'ספטמבר', 'אוקטובר', 'נובמבר', 'דצמבר'];

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
/** "יום ה' 5.11", or "יום ה' 5.11.2026" with the year. */
function day(iso, withYear = false) {
  if (!iso) return '–';
  const d = new Date(`${String(iso).slice(0, 10)}T12:00:00Z`);
  return `${WEEKDAYS[d.getUTCDay()]} ${d.getUTCDate()}.${d.getUTCMonth() + 1}${withYear ? `.${d.getUTCFullYear()}` : ''}`;
}
const monthName = (mKey) => MONTHS[Number(String(mKey).slice(5, 7)) - 1] || mKey;
/** "היום", "מחר", "בעוד 15 ימים". */
function inDays(from, to) {
  const n = Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000);
  return n <= 0 ? 'היום' : n === 1 ? 'מחר' : `בעוד ${n} ימים`;
}
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

const LEVEL = { high: 'גבוה', middle: 'בינוני', low: 'נמוך' };
const SOURCE = { mail: 'מקורות מייל היתרות היומי', books: 'ספרי NetSuite (אירו בלבד)' };
const POLICY = { today: 'לפי השער היום', p95: '95% מתנודות השער', worst: 'המקרה הגרוע בטבלה' };

// ── the recommendations in Hebrew: the same actions and payment days as the page (todayActions, duesAhead) ──
const ITEM = {
  net: (i) => `שכר נטו של ${monthName(i.payrollMonth)}`,
  later: (i) => `פנסיה, מס וביטוח לאומי של ${monthName(i.payrollMonth)}`,
  cards: (i) => `חיובי כרטיסי האשראי של ${monthName(i.spendMonth)}`,
  vendors: (i) => `${i.rest ? 'יתרת ' : ''}תשלומי הספקים בשקלים של ${monthName(i.month)}`,
};

/** What a payment day pays: "שכר נטו של אוקטובר ותשלומי הספקים בשקלים של נובמבר". */
function whatOf(due) {
  const parts = (due.items || []).map((i) => (ITEM[i.why] ? ITEM[i.why](i) : i.why));
  return parts.length > 1 ? `${parts.slice(0, -1).join(', ')} ו${parts[parts.length - 1]}` : parts[0] || '';
}
const rateIs = (rc) => `שער EUR/ILS ${rate(rc.rate)} ${LEVEL[rc.level]} ביחס ל-${rc.days} הימים האחרונים`;

/** A recommendation in words: { head: what to do, body: why, from the day's balances }. */
function actionText(a, v, today) {
  const n = v.plan.next;
  const rc = v.rateCheck;
  if (a.kind === 'convert') {
    const d = a.due;
    const now = a.by <= today;
    const when = d.date <= today ? 'עד סוף החודש' : `ב${day(d.date)}${d.by !== d.date ? ` (צריך להיות מוכן עד ${day(d.by)})` : ''}`;
    const held = d.before < -0.5 ? `ועד אז החשבון במינוס ${ils(-d.before)}` : `ועד אז יהיו ביתרה ${ils(Math.max(0, d.before))}`;
    let body = `${whatOf(d)}: ${ils(d.ils)} יוצאים ${when}, ${held}.`;
    if (rc && !now && rc.level === 'high') body += ` ${rateIs(rc)}: יום טוב להמיר.`;
    if (rc && !now && rc.level === 'low') body += ` ${rateIs(rc)}: אם אפשר, כדאי לחכות, אבל לא יאוחר מ${day(a.by)}.`;
    return { head: `להמיר ${eur(a.eur)} ${now ? 'היום' : `עד ${day(a.by)}`}`, body };
  }
  if (a.kind === 'convert-early') {
    const part = a.eur < a.due.eur - 0.5 ? ` (${eur(a.due.eur)} בסך הכול; מעבר ל-${eur(n.eurFloor)} השמורים לספקים באירו יש היום ${eur(a.eur)})` : '';
    return { head: `להמיר ${eur(a.eur)} היום`, body: `${rateIs(rc)}: יום טוב להקדים את ההמרה עבור ${whatOf(a.due)}, הנדרשת עד ${day(a.by)}${part}.` };
  }
  if (a.kind === 'short') {
    const what = a.shekels ? 'ההמרה לשקלים הנדרשת אז' : `ההמרה של ${day(a.date)}`;
    const fix = n.backup > 0 ? ` כדאי להיערך לשבירה של עד ${eur(Math.min(a.eur, n.backup))} מהפיקדונות שנפתחים מאוחר יותר.` : ' כדאי לשמור כל אירו פנוי עד אז.';
    return { head: `חוסר של ${eur(a.eur)} עד ${day(a.date)}`, body: `האירו שיש ושצפוי להיכנס עד אז לא מכסים את ${what} ואת ${eur(n.eurFloor)} השמורים לספקים באירו.${fix}` };
  }
  if (a.kind === 'move') {
    const fromDeposits = Math.min(n.required, n.maturing || 0);
    const dep = fromDeposits > 0.5 ? `, מזה ${eur(fromDeposits)} מפיקדונות שמשתחררים עד אז` : '';
    return { head: `להעביר ${eur(a.eur)} לחשבון האירו בפועלים עד ${day(a.by)}`, body: `ההמרה של ${day(a.date)} יורדת מחשבון האירו בפועלים (583400), שבו ${eur(n.hapoalim)}; צריך שיהיו בו ${eur(n.required)}${dep}.` };
  }
  if (a.kind === 'convert-spare') {
    return { head: `להמיר היום את ${eur(a.eur)} הפנויים`, body: `${rateIs(rc)}. התמורה: ${ils(a.eur * rc.rate)}, וכל ההמרות, המשכורות והספקים עדיין מכוסים.` };
  }
  // A new deposit: one line when every term holds the same.
  const last = a.options[a.options.length - 1];
  const options = a.options.length > 1 && a.options.every((o) => Math.abs(o.eur - a.eur) < 0.5)
    ? `${eur(a.eur)} ל-${a.options.map((o) => o.months).join(', ').replace(/, (\d+)$/, ' או $1')} חודשים (הארוך עד ${day(last.until)})`
    : a.options.map((o) => `${eur(o.eur)} ${o.months === 1 ? 'לחודש' : `ל-${o.months} חודשים`} (עד ${day(o.until)})`).join(', או ');
  return { head: `להפקיד ${eur(a.eur)} בפיקדון חדש`, body: `${options}, וכל התשלומים עד אז עדיין מכוסים לפי התקבולים הרגילים.` };
}

/** What to keep in Hapoalim EUR for the next conversion (fx.keepCover): nothing when the deposits released by then
 * cover what it takes ("אין צורך לשמור כסף להמרה של שבת 7.11: …"), else the rest, with what the deposits bring. */
function keepLine(cover, date, policy) {
  const ds = cover.deposits;
  const conversion = `להמרה של ${day(date)}`;
  if (!(cover.fromDeposits > 0.5) || !ds.length) return `להשאיר ${eur(cover.required)} בפועלים ${conversion} (${policy}).`;
  const last = ds[ds.length - 1].valueDate;
  if (cover.covered) {
    const total = ds.reduce((s, d) => s + d.amount, 0);
    const which = ds.length === 1 ? `הפיקדון שמשתחרר ב${day(last)} (${eur(total)}) מכסה` : `${ds.length} פיקדונות שמשתחררים עד ${day(last)} (${eur(total)}) מכסים`;
    return `אין צורך לשמור כסף ${conversion}: ${which} את ה-${eur(cover.required)} שהיא מוכרת מחשבון האירו בפועלים (${policy}).`;
  }
  const which = ds.length === 1 ? `מהפיקדון שמשתחרר ב${day(last)}` : `מ-${ds.length} פיקדונות שמשתחררים עד ${day(last)}`;
  return `להשאיר ${eur(cover.fromCash)} בפועלים ${conversion} (${policy}): היא מוכרת ${eur(cover.required)}, ו-${eur(cover.fromDeposits)} מזה מגיעים ${which}.`;
}

/** A payment day in the list: "יום ה' 5.11, בעוד 15 ימים: שכר נטו …, ₪4.92M: להמיר €1.21M". */
function dueText(d, today) {
  const when = d.date <= today ? 'עכשיו' : `${day(d.date)}${d.by !== d.date ? ` (מוכן עד ${day(d.by)})` : ''}, ${inDays(today, d.by)}`;
  const action = d.eur > 0.5 ? `להמיר ${eur(d.eur)}` : d.after ? `מכוסה מההמרה של ${day(d.after)}` : 'מכוסה מהיתרה בשקלים';
  return `${when}: ${whatOf(d)}, ${ils(d.ils)}: ${action}`;
}

// ── the mail ──
// Each part is written twice, as HTML and as text: a part is { html, text }.
const para = (html, text, style = '') => ({ html: `<p style="margin:0 0 10px;font-size:14px;line-height:1.6;${style}">${html}</p>`, text });
const strong = (s, color = '#0f172a') => `<strong style="color:${color};">${esc(s)}</strong>`;
const heading = (s) => ({ html: `<p style="margin:16px 0 6px;font-size:13px;font-weight:bold;color:#0f172a;">${esc(s)}</p>`, text: `\n${s}` });
const list = (items) => ({
  html: `<ul style="margin:0 0 10px;padding:0 20px 0 0;font-size:14px;line-height:1.6;">${items.map((x) => `<li style="margin-bottom:6px;${x.style || ''}">${x.html}</li>`).join('')}</ul>`,
  text: items.map((x) => `- ${x.text}`).join('\n'),
});

function page(title, subtitle, parts, footer) {
  const html = '<!doctype html><html dir="rtl" lang="he"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>'
    + '<body dir="rtl" style="margin:0;padding:0;background:#f1f5f9;">'
    + '<table role="presentation" dir="rtl" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;"><tr><td align="center" style="padding:12px 2px;">'
    + '<table role="presentation" dir="rtl" width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;background:#ffffff;border:1px solid #e2e8f0;border-radius:8px;font-family:Arial,Helvetica,sans-serif;color:#334155;direction:rtl;text-align:right;">'
    + `<tr><td style="padding:14px 20px;border-bottom:1px solid #e2e8f0;"><div style="font-size:12px;color:#64748b;">${esc(subtitle)}</div>`
    + `<div style="font-size:18px;font-weight:bold;color:#0f172a;margin-top:2px;">${esc(title)}</div></td></tr>`
    + `<tr><td style="padding:16px 14px;">${parts.map((p) => p.html).join('')}</td></tr>`
    + `<tr><td style="padding:12px 20px;border-top:1px solid #e2e8f0;font-size:11px;line-height:1.6;color:#64748b;">${footer.map((f) => f.html).join('<br>')}</td></tr>`
    + '</table></td></tr></table></body></html>';
  const text = [title, subtitle, '', ...parts.map((p) => p.text), '', '--', ...footer.map((f) => f.text)].join('\n');
  return { html, text };
}

/**
 * The mail from GET /api/fx-challenge's answer (readFxChallenge), in Hebrew and to the point: today's recommendations
 * (todayActions, the same as the page's) with the balances behind each, the payments in shekels ahead, today's
 * balances in one paragraph, and a link to the page for the rest. fx is src/forecast/fx-challenge.mjs.
 * Returns { subject, html, text, actions }.
 */
function renderMail(fx, data, { policy = 'p95', link = null } = {}) {
  const v = fx.planFromPayload(data, policy);
  const plan = v.plan;
  const first = v.schedule[0];
  const rc = v.rateCheck;
  const today = data.today;
  const parts = [];
  let title;
  let heads = [];
  let actions = [];

  if (plan && first && first.atSpot && first.risk) {
    const n = plan.next;
    const cash = v.cash;
    const todo = fx.todayActions(v, today);
    actions = todo.actions;
    const said = actions.map((a) => actionText(a, v, today));
    heads = said.map((s) => s.head);
    if (said.length) {
      parts.push(list(said.map((s, i) => ({ html: `${strong(s.head, actions[i].urgent ? '#be123c' : '#0f172a')}. ${esc(s.body)}`, text: `${s.head}. ${s.body}` }))));
    } else {
      const last = todo.ahead.dues[todo.ahead.dues.length - 1];
      const s = last ? `אין היום המלצות: היתרה בשקלים וההמרות מכסות את התשלומים עד ${day(last.date)}.` : 'אין היום המלצות.';
      parts.push(para(esc(s), s, 'font-size:15px;'));
    }
    title = said.length ? said[0].head : 'אין היום המלצות';
    if (todo.ahead.dues.length) {
      parts.push(heading('תשלומים בשקלים בתקופה הקרובה'));
      parts.push(list(todo.ahead.dues.map((d) => { const s = `${dueText(d, today)}.`; return { html: esc(s), text: s, style: d.eur > 0.5 ? 'color:#0f172a;' : 'color:#64748b;' }; })));
    }
    // Today's balances, in one paragraph.
    const shekels = cash && cash.onHand ? `${ils(cash.onHand.total)} בשקלים (${ils(cash.onHand.current)} בעו"ש ו-${ils(cash.onHand.deposits)} בפיקדונות), ` : '';
    const status = `${shekels}${eur(n.hapoalim)} בחשבון האירו בפועלים ו-${eur(n.other)} בחשבונות אחרים. `
      + `${keepLine(fx.keepCover(plan), n.date, POLICY[plan.policy])}`
      + `${cash ? '' : ` פנוי להמרה עכשיו: ${eur(n.freeNow)}.`}`
      + `${rc ? ` ${rateIs(rc)}.` : ''}`;
    parts.push(heading('המצב היום'));
    parts.push(para(esc(status), status, 'font-size:13px;'));
  } else {
    title = 'אין היום המלצה';
    const why = !v.schedule.length ? 'לא נשארו המרות בטבלה.' : 'ההמלצה צריכה את שער היום ואת היתרות, ואחד מהם לא נקרא היום.';
    parts.push(para(esc(why), why));
  }

  if (data.warnings && data.warnings.length) {
    const s = 'שימו לב: חלק מהנתונים לא נקראו היום, ולכן ההמלצה עלולה להיות חלקית. הפרטים בדף.';
    parts.push({ html: `<div style="margin:12px 0 0;padding:8px 12px;background:#fffbeb;border:1px solid #fde68a;border-radius:6px;font-size:12px;line-height:1.5;color:#92400e;">${esc(s)}</div>`, text: `\n${s}` });
  }

  const b = data.balances;
  const asOf = b && b.asOf ? `, ${new Date(b.asOf).toLocaleString('en-GB', { timeZone: 'Asia/Jerusalem', day: 'numeric', month: 'numeric', hour: '2-digit', minute: '2-digit' })}` : '';
  const footer = [];
  if (link) footer.push({ html: `הפירוט המלא בדף: <a href="${esc(link)}" style="color:#0369a1;">FX Challenge</a>`, text: `הפירוט המלא בדף: ${link}` });
  const about = `יתרות: ${b ? `${SOURCE[b.source]}${asOf}` : 'לא נקראו'}. שער: HSBC ב-NetSuite. נשלח בימים א'–ו' כשיש המלצה (scripts/fx-challenge-mail.cjs).`;
  footer.push({ html: esc(about), text: about });

  const subject = `FX ${day(today)} · ${heads.length ? heads.slice(0, 2).join(' · ') : title}`;
  const { html, text } = page(title, `FX Challenge · ${day(today, true)}`, parts, footer);
  return { subject, html, text, actions };
}

/**
 * Whether the daily mail goes today: on any day but Saturday (the bank works Sunday to Friday), when there is at least
 * one recommendation (todayActions); never without one. always (FX_MAIL_ALWAYS=1): every day, Saturday included, with
 * or without. Returns { send, why } (why: when it doesn't).
 */
function mailDecision(actions, { today, always = false }) {
  if (always) return { send: true, why: null };
  if (offDay(today)) return { send: false, why: 'Saturday' };
  return actions.length ? { send: true, why: null } : { send: false, why: 'no recommendations' };
}

/** When nothing could be read: a short mail saying so. */
function failureMail(today, error) {
  const why = String((error && error.message) || error || 'unknown error').slice(0, 300);
  const s = `לא ניתן היה לקרוא היום את הנתונים מ-NetSuite (${why}). הדף יציג אותם שוב כש-NetSuite יענה.`;
  const sent = 'נשלח מלוח הבנקים (scripts/fx-challenge-mail.cjs).';
  const { html, text } = page('הנתונים לא נקראו היום', `FX Challenge · ${day(today, true)}`, [para(esc(s), s)], [{ html: esc(sent), text: sent }]);
  return { subject: `FX ${day(today)} · הנתונים לא נקראו היום`, html, text, actions: [] };
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

function writeMarker(file, marker) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(marker));
}

/** Saturday in Israel: no mail (the bank works Sunday to Friday). */
const offDay = (today) => new Date(`${today}T12:00:00Z`).getUTCDay() === 6;

const actionsOf = (mail) => mail.actions.map((a) => ({ key: a.key, eur: Math.round(a.eur) }));

/**
 * The page's Send now: today's mail, read fresh, sent at once with or without a recommendation, to the page's list (or
 * FX_MAIL_TO). It counts as today's mail, so the 10:00 check doesn't send it again today.
 * deps: env, reads, tableFile, mailFile, markerFile, clock(), nodemailer, by (who asked).
 * Returns { ok: true, recipients, subject, at } or { ok: false, status, error }.
 */
async function sendNow(deps = {}) {
  const env = deps.env || process.env;
  const now = (deps.clock || Date.now)();
  const today = fxs.israelDay(now);
  const at = new Date(now).toISOString();
  const cfg = mailConfig(env, fxs.savedRecipients(deps.mailFile || fxs.MAIL_FILE));
  if (cfg.nobody) return { ok: false, status: 400, error: 'Nobody is on the list, so there is no one to send it to.' };
  if (cfg.errors.length) return { ok: false, status: 400, error: `Not sent: ${cfg.errors.join('; ')}.` };
  const fx = await fxs.loadFxModule();
  let data;
  try {
    data = await fxs.readFxChallenge({ reads: deps.reads || fxs.defaultNsReads(), tableDoc: readDoc(deps.tableFile || fxs.TABLE_FILE), clock: () => now });
  } catch (e) {
    return { ok: false, status: 502, error: `NetSuite could not be read (${String((e && e.message) || e).slice(0, 200)}).` };
  }
  const mail = renderMail(fx, data, { policy: cfg.policy, link: cfg.link });
  const nodemailer = deps.nodemailer || require('nodemailer');
  await nodemailer.createTransport(cfg.smtp.transport).sendMail({ from: cfg.from, to: cfg.to.join(', '), subject: mail.subject, text: mail.text, html: mail.html });
  writeMarker(deps.markerFile || MARKER_FILE, {
    day: today, at, quiet: false, why: null,
    lastSent: { day: today, at, recipients: cfg.to.length, subject: mail.subject, actions: actionsOf(mail), manual: true, by: deps.by || null },
  });
  console.log(`${TAG} ${today}: sent by hand${deps.by ? ` by ${deps.by}` : ''} to ${cfg.to.length} recipient(s) from ${cfg.from} via ${cfg.smtp.host}: ${mail.subject}`);
  return { ok: true, recipients: cfg.to.length, subject: mail.subject, at };
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
  const always = env.FX_MAIL_ALWAYS === '1';
  const marker = fxs.readMailMarker(markerFile);
  const at = new Date(now).toISOString();
  let state = 'send';
  if (args.scheduled) {
    const hour = israelHour(now);
    state = scheduleState({ hour, sendHour: cfg.hour, sentDay: marker && marker.day, today });
    if (state === 'wait' || state === 'done') return 0;
    if (cfg.nobody) {
      if (hour === cfg.hour) console.log(`${TAG} ${today}: nobody is on the page's list, so no mail today`);
      return 0;
    }
    if (!always && offDay(today)) {
      writeMarker(markerFile, { day: today, at, quiet: true, why: 'Saturday', lastSent: marker ? marker.lastSent : null });
      console.log(`${TAG} ${today}: no mail on Saturdays`);
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
  // A mail only with a recommendation; a read that failed is always said.
  const decision = complete ? mailDecision(mail.actions, { today, always }) : { send: true, why: null };

  if (args.dryRun) {
    console.log(`Subject: ${mail.subject}\n\n${mail.text}`);
    if (args.html) { fs.writeFileSync(args.html, mail.html); console.log(`\n${TAG} HTML written to ${args.html}`); }
    const list = cfg.source === 'page' ? "the page's list" : 'FX_MAIL_TO';
    console.log(cfg.errors.length ? `\n${TAG} a real send would stop: ${cfg.errors.join('; ')}` : `\n${TAG} a real send goes to ${cfg.to.length} recipient(s) (${list}) from ${cfg.from} via ${cfg.smtp.host}`);
    console.log(`${TAG} today's scheduled run ${decision.send ? 'sends it' : `sends nothing (${decision.why})`}`);
    return 0;
  }
  if (args.scheduled && !decision.send) {
    writeMarker(markerFile, { day: today, at, quiet: true, why: decision.why, lastSent: marker ? marker.lastSent : null });
    console.log(`${TAG} ${today}: no mail (${decision.why})`);
    return 0;
  }

  const nodemailer = deps.nodemailer || require('nodemailer');
  await nodemailer.createTransport(cfg.smtp.transport).sendMail({ from: cfg.from, to: cfg.to.join(', '), subject: mail.subject, text: mail.text, html: mail.html });
  console.log(`${TAG} ${today}: sent to ${cfg.to.length} recipient(s) from ${cfg.from} via ${cfg.smtp.host}: ${mail.subject}`);
  if (args.scheduled) {
    writeMarker(markerFile, { day: today, at, quiet: false, why: null, lastSent: { day: today, at, recipients: cfg.to.length, subject: mail.subject, actions: actionsOf(mail) } });
  }
  return 0;
}

if (require.main === module) {
  main().then((code) => process.exit(code), (e) => {
    console.error(`${TAG} failed: ${e && e.message ? e.message : e}`);
    process.exit(1);
  });
}

module.exports = { main, sendNow, renderMail, failureMail, mailDecision, parseArgs, parseRecipients, smtpFrom, mailConfig, scheduleState, israelHour, formatDay: day, MARKER_FILE };
