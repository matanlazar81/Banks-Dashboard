# FX Challenge

Business Tools → **FX Challenge**: finance's monthly EUR→ILS conversion program at Bank Hapoalim (583400),
simulated at any rate, and how much EUR to keep for it.

On each fixing (the 7th), that day's EUR/ILS picks a band of the conversion table. The cell is the euros sold
(€ thousands) and the deals' blended rate. October 2026 checks out:
- EUR/ILS was 3.425, so the band was 3.4–3.49 and the cell €800K at 3.67.
- It was booked on 6 October as three "Conversion" journals: €200K @4.10, €300K @3.56 and €300K @3.49, which
  is ₪2,935,000.

The page calls each fixing a **conversion**, and its day (the 7th) the **conversion day**. The logic is
`src/forecast/fx-challenge.mjs`, shared by the server, the page and the tests. The page was
briefly a tab of the Metrics page before it got its own sidebar item.

## URLs

| Where | What |
|---|---|
| `fx-challenge.html` | the page (a small bundle, never loads the old dashboard) |
| `GET /api/fx-challenge` | what the page shows (`server/fx-challenge.cjs`); `?refresh=true` reads NetSuite again |
| `GET/PUT /api/fx-challenge/table` | the conversion table, saved for everyone (`null` = the default) |
| `GET/PUT /api/fx-challenge/mail` | who gets the daily mail, saved for everyone (`null` = the server's `FX_MAIL_TO`) |
| `POST /api/fx-challenge/mail/send` | the page's **Send now**: the daily mail at once, to the saved list |
| finance-it | sidebar item → iframe of `<bank-dashboard static base>/fx-challenge.html`; every `/api/fx-challenge` route is mounted once, by prefix (`createFxChallengeRouter`), so new ones need no finance-it change (see Deploying) |

## The page

- **The table** is the one received in October 2026 (fixings October 2026 to December 2027) until someone
  saves another.
  - To change it: Edit table, then paste the range from Excel (the header row of bands, then `10/2026` and
    `-500 / 3.62` per band). The page checks it, then Save for everyone.
  - "Back to the table received in October 2026" removes the saved one.
  - Bands are lower-inclusive (a rate on an edge counts in the band above); the deals decide the edges.
- **Simulate**: type a rate or use the slider or presets. The schedule shows each remaining fixing at that
  rate (euros, rate, shekels, shekels against that rate) beside its risk from today's rate.
- **Risk of a fixing**: EUR/ILS on the day is lognormal around today's rate (no drift), with the annual
  volatility of NetSuite's daily HSBC rate over 400 days (7.5% assumed when there is too little history).
  That gives each band's chance, the expected euros, the 95% amount (enough for 19 fixings in 20) and the
  table's worst case.
- **What the next conversion can sell** (`outcomeRanges`): a table of the EUR/ILS ranges on its day (neighbouring
  bands that sell the same amount merged), each with its chance, the euros sold, the rate and the shekels; today's
  range is highlighted. Hovering an expected figure anywhere on the page shows the same chances for that month.
- **What to keep, what is free** (`planReserve`), for the next fixing:
  - Keep: today's band, the 95% amount (the default) or the worst case (`?reserve=today|worst`).
  - It is paid from Hapoalim EUR (583400); the other EUR accounts can be moved there before the day.
  - EUR deposits that mature before the fixing count for it; later ones are the backup (broken early only
    if needed). Without the mail's balances there are no dates, so every deposit is backup.
  - The keep line (and the mail's) is what today's cash has to keep: what the conversion takes, less the deposits
    released by then (`keepCover`, `describeKeep`). "Nothing to keep for Sat 7 Nov: the deposit released on Thu 5 Nov
    (€1.00M) covers the €800K it takes from Hapoalim EUR (583400)", or "Keep €300K in Hapoalim EUR (583400) for Sat 7
    Nov: it takes €800K, and the deposit released on Thu 5 Nov brings €500K of it". The **Keep for it** card shows
    the same amount (€0 when covered), with what the conversion takes and what covers it under it.
  - Free to convert now = liquid euros less what the reserve still needs. It is also shown with the backup
    covering a jump above today's band, and by the fixing with the typical inflow (customers less EUR
    payments, before conversions, deposits and transfers, last 3 closed months).
  - The path shows the next 6 fixings if nothing else is converted. Its **Deposits released** column is the EUR
    deposits maturing after the previous fixing (after today, for the first) and by that one; hover a figure for
    each deposit's date and value. Deposits maturing after the last fixing shown are listed under the table.
    They count only if the bank pays them out rather than rolling them over.
- **The conversions against your deposits (first in, first out)** (`planReserve`'s `coverage` and `ladder`): the next
  6 conversions at the setting, each against the EUR deposits paying out before the bank takes its euros. The bank
  takes them the business day before the 7th (`conversionTakes`): NetSuite's "Conversion" journals show October
  2026's, for Wed 7 Oct, on Tue 6 Oct, and in 2026 a 7th on a weekend went through on the Wednesday or Thursday
  before. Each deposit counts for the first conversion whose euros the bank takes after it pays out, and what one
  conversion doesn't take rolls on to the next ("Left from earlier"). So a deposit paying out on the 7th itself, or
  on Fri 5 Mar for Sun 7 Mar (taken Thu 4 Mar), counts for the conversion after. What the deposits don't cover is the
  conversion's gap ("€128K short"): the euros on hand or coming in fill it, or a new deposit timed for it. **New
  deposit for it** is that deposit: it pays out the business day before the bank takes the euros (Thu 7 Jan: taken
  Wed 6 Jan, so Tue 5 Jan; Sun 7 Feb: taken Thu 4 Feb, so Wed 3 Feb), holds the gap rounded up to €10K, and is never
  more than the dues before it pays out leave room for (each new deposit locks its euros away from them), earliest
  gap first, from today's euros above the floor. The next conversion's gap is the cash to keep (the keep line, which
  counts the same deposits), and a deposit is only timed when it pays out 14 days or more from today.
- **What to do today** (`todayActions`, the first lines of the recommendation and of the daily mail): from today's
  balances and rate, urgent ones first (in red):
  - **Convert** for a payment in shekels that the shekels on hand (and the conversions before it) don't cover, by the
    business day it has to be ready: said from 2 business days (Sunday to Thursday) before that day, in red, and not
    earlier (`notice`), whatever the rate. Finance converts at once, and the figure moves until then; before that,
    the payment is only in the payments ahead. (An early conversion on a high EUR/ILS was dropped on 27 Oct 2026.)
  - **Short**: the euros held and expected fall short of a due (urgent in its last 5 business days).
  - **Move** euros to Hapoalim EUR in the last 3 business days before the bank takes the euros (the business day
    before the 7th), when it holds less than to keep there.
  - **Deposits for the conversions the deposits leave short** (`ladder`): "Put €440K in deposits for the January to
    March conversions: €130K until Wed 6 Jan for Thu 7 Jan (€128K short), …". It comes before any other use of spare
    euros.
  - **Spare** euros: **put them in a new deposit** for 1, 3 or 6 months (the terms that hold at least €50K, as one
    line when every term holds the same), but only when no conversion ahead is left short by €50K or more. Nothing is
    converted for the rate alone, however high EUR/ILS is (converting the spare on a high rate was dropped on 27 Oct
    2026); the rate check stays on the page as information.
  - Amounts under €50K are left out. With no recommendation, the page says so and no mail goes.
- **Payments in shekels ahead** (`duesAhead`, `describeDue`): each payment day up to the first month with salaries
  (its 5th, 10th and 14th, and any before): when (a Friday or Saturday one is ready by the Thursday) and in how many
  days, what it pays, how much, and what to convert for it, or what covers it (the shekels on hand, or a conversion).
- **Salaries, vendors and new deposits**: once the payroll is known (it nearly always is), the plan also covers the
  shekels each month needs. The KPI **Convert for salaries & vendors** is the euros to convert for the payments
  ahead, in all, with each amount by its day; **New deposits** says how much can go into a new deposit for 1, 3 or 6
  months.
  - **Payroll**: the Salary Projection's Total payroll by month (`totalPayrollByMonth`, the same figure as its
    page), from the figures that page saved on the server (`data/salary-projection-cache.json`; open the Salary
    Projection once if there are none). It is split as NetSuite's payroll journals of the last 3 closed months
    (`fetchPayrollLiabilities`, `payrollSplit`). Credits to the employees (200000/200001) are the **net salaries,
    paid by the 5th** of the next month. The rest (withholding tax 180013, pension 180069, social security 180012)
    is **paid by the 14th** (`PAYROLL_DUE`). In 2026 the net salaries were about 48–51%. A month the projection lacks
    carries the last journal's total.
  - **Vendors**: the average of the last 3 closed months (`fetchIlsVendorPayments`: vendor payments and checks out of
    the ILS accounts; the payroll is paid by journal, so it is not in them; nor are dividends, paid as vendor payments
    against an equity journal, as on 3 September 2026 and in June), in two parts:
    - **the credit cards** (AMEX, Isracard, MasterCard): the bank pays their bill **on the 10th** (`CARD_BILL_DAY`)
      for the month before's spending (`cardBills`). A month that is over is billed what was spent; this month's
      spending at least the average (it is still going); later months the average.
    - **bank transfers and Masav batches** (the batches clear through the credit card type account 17036 MASAV IL): a
      monthly need in shekels from the 5th.

    EUR vendor payments (the flows' vendor kind) are **kept in the euro accounts at all times** (the EUR floor); the
    typical inflow already nets them.
  - **The shekels on hand** (`ilsOnHand`): LSports' ILS current accounts (an overdraft counts against them) and its
    **ILS deposits**, which finance releases the day a payment goes out (a blocked deposit is left out); 0 when not
    known (NetSuite's books).
  - **The shekels** (`shekelNeeds`):
    - today, the rest of this month's transfers and Masav;
    - on the 5th, the previous month's net salaries and the month's transfers and Masav;
    - on the 7th, the table's shekels, for the amount kept at its cell's rate (consistent with what the plan
      keeps);
    - on the 10th, the credit card bills;
    - on the 14th, pension, tax and social security.

    Each gap is converted by its day at today's rate. The card **Salaries and vendors: the shekels each month
    needs** shows it month by month.
  - **The euros** (`planReserve`): the conversions for shekels come out of the euros by each conversion (the
    plan's **Salaries & vendors** column), and the EUR floor is kept. At every due (each conversion at the amount
    kept, each conversion for shekels) the spare is the euros held then (today's, the typical inflow and the
    deposits maturing by then, less what fell due before), less what is due and the floor.
    - **Spare now** (converted for good) is the smallest spare of all dues to the end of the 6th conversion's
      month, never more than today's euros above the floor.
    - **A new deposit maturing on a day** can hold the smallest spare of the dues before that day (euros in a
      deposit come back), never more than today's euros above the floor. It is rounded down to €10K.
    - **Short**: the first day a gap opens, and the largest gap.
  - The rate check then speaks of the spare instead of the free euros.
- **Rate check** (under "Keep … Free to convert now"): today's EUR/ILS against the 90 days before it (`rateCheck`,
  NetSuite's HSBC rate, business days): the share of those days it beats, their average and range, and a verdict:
  **a good day to convert** when it beats 3 days in 4 or more, **a weak day** when it beats 1 in 4 or fewer, else an
  average day. It also says what the euros free now bring at today's rate against the average. It only looks back;
  the conversion on the 7th follows the table whatever is converted on top.
- **Past conversions since January 2026** (`pastConversions`, `HISTORY_FROM`): every month whose conversion day has
  passed, with the "Conversion" / המרה journals (Hapoalim EUR 11001 ↔ ILS 10002):
  - booked: the day within 3 days of the 7th closest to the table (✓ when it matches it); for a month before the
    table (January to September 2026), the day with the best rate, since the monthly deals beat the market's;
  - the shekels gained against that day's rate;
  - the month's other conversions by day, with euros bought back with shekels listed as "bought" (as on 2 January,
    2 February and 2 March 2026), each against its day's rate (hover);
  - a total row, and under the table everything since January: euros sold net, shekels, and the gain against each
    day's rate.
- **LSports euros**: the accounts, then each EUR deposit by the day it matures, with a total after each month's
  deposits ("Total for Jan 2027: 2 deposits €908K").
- Every column header explains itself on hover (the dotted underline marks it), and so does every figure in the plan
  (its sum, or its chances).
- **How are these figures worked out?** (under the plan, or click a conversion day): the workings with today's
  numbers, step by step with where each comes from: free to convert now (for the next conversion), and for the day
  clicked the euros by then (cash today + typical inflow + deposits matured − earlier conversions) and what can be
  converted before it (the spare, and the tightest later day). `planReserve` returns each row's parts for it
  (`inflow`, `inflowDays`, `depositsBy`, `earlier`, `limitedBy`).

## Sources

Each read is cached for `FX_CHALLENGE_TTL_MIN` (default 30 minutes); Refresh reads again. A failed read is
a warning, and the rest still shows.

| What | From |
|---|---|
| EUR/ILS | NetSuite `currencyrate` (HSBC), back to the first past conversion and at least 400 days (the volatility uses the last 400) |
| Balances | the daily balances mail's RESTlet (`payhound_api` `ph_rl_balances.js`) when `NS_BALANCES_RESTLET=<script>:<deploy>` is set in the checkout's `.env`; else NetSuite's books (EUR bank accounts in the primary book, exact in euros, no deposit dates) |
| Monthly EUR flows | NetSuite's books, four months back |
| Conversions | the Hapoalim "Conversion" / המרה journals (EUR 11001 ↔ ILS 10002, euros bought back included), from 20 December 2025, or before the table's first month if it is earlier |
| Payroll | NetSuite's payroll journals (what each owes: net salaries, tax, pension, social security), four months back; the Salary Projection's Total payroll by month, from the figures its page saved (`data/salary-projection-cache.json`) |
| ILS vendor payments | NetSuite's books: vendor payments and checks out of the ILS accounts (book 2; bank and credit card type, so the Masav batches count), four months back, the credit cards' apart |

The RESTlet gives what the bank imports last fetched: Bank Hapoalim and its deposits (several times a day),
PKO (each morning's statement), PayPal (01:00 and 13:00) and Payhound (about every 20 minutes). Leumi and
MoneyNet carry no balance in their files, so they are not in it. Only LSports' euros count.

## The daily mail

`scripts/fx-challenge-mail.cjs` checks every day at 10:00 Israel time and mails **only when there is a
recommendation**. It reads NetSuite and the bank balances fresh (no cache) and computes the same figures as the page
(`planFromPayload`, `todayActions`).

**When it goes** (`mailDecision`): every day but Saturday (the bank works Sunday to Friday; finance writes to it on
Fridays too), when there is at least one recommendation (a conversion for a payment the balances don't cover, a
shortfall, euros to move to Hapoalim before a conversion, deposits for the conversions the deposits leave short, or
spare euros for a new deposit), and never without one. So a conversion for the salaries is in the mail, in red, from
2 business days before it has to be ready until the balances show it converted. A Saturday, or a day with no recommendation, sends nothing. `FX_MAIL_ALWAYS=1` sends
every day, Saturday included.

**Who gets it** is kept on the page, in the **Daily mail** card at the bottom:
- Add or remove lsports.eu addresses, then **Save for everyone**.
- The list is saved in `data/fx-challenge-mail.json`, with a history line per save (who and when), as the table is.
- Until a list is saved there, the mail goes to `FX_MAIL_TO` in the checkout's `.env`.
- An empty list sends to nobody.
- The card also shows the sender, the hour, when the mail last went, and the last check that sent nothing (and why).
- **Send now** (`POST /api/fx-challenge/mail/send`, `sendNow`) reads today's figures fresh and sends the mail at once to
  the saved list, with or without a recommendation (save a changed list first). It counts as today's mail, so the
  10:00 check doesn't send it again that day. JSON and same-origin as a save, one at a time and once a minute at most;
  the log names who sent it.

The mail is in Hebrew, right to left, and short: what to act on, and why. Amounts stay as on the page (€1.16M,
₪4.91M), days read "יום ה' 5.11". The tables and the workings (the shekels by month, what the conversion sells range
by range, how "free to convert" adds up, the next six conversions) stay on the page. HTML with a plain-text part:
- the subject: the day and the first two recommendations (`FX יום א' 25.10 · להמיר €1.16M עד יום ה' 5.11 · …`);
- the recommendations, each with the balances behind it ("להמיר €1.16M עד יום ה' 5.11. שכר נטו של אוקטובר ותשלומי
  הספקים בשקלים של נובמבר: ₪4.91M יוצאים ביום ה' 5.11, ועד אז יהיו ביתרה ₪873K."); urgent ones in red;
- the next 6 conversions against the deposits, first in, first out, at the mail's setting: covered ("שבת 7.11:
  €800K, מכוסה מהפיקדונות (€806K)") or short, in red ("יום ה' 7.1: €1.05M, הפיקדונות מכסים €922K: חסרים €128K");
- the payments in shekels ahead: the day, how many days to it, what it pays, and what covers it (a conversion, or
  the shekels on hand);
- today's position in one paragraph: the shekels on hand (current and deposits), the euros at Hapoalim and elsewhere,
  what to keep at Hapoalim for the next conversion (with the setting), and the rate check;
- a note when part of the data could not be read, then a link to the page and the sources.

When NetSuite can't be read at all, a short mail says so (`failureMail`).

Set it up in the checkout's `.env` (immutable on the server: `sudo chattr -i .env`, edit, `sudo chattr +i .env`):

```bash
FX_MAIL_TO=matan.l@lsports.eu,dotan@lsports.eu   # until a list is saved on the page; lsports.eu only, unless FX_MAIL_ALLOW_EXTERNAL=1
FX_MAIL_FROM=matan.l@lsports.eu
# FX_MAIL_HOUR=10                 # Israel time (default 10)
# FX_MAIL_POLICY=today            # today (default: today's scenario) | p95 | worst, as the page's "What to keep"
# FX_MAIL_LINK=https://<finance-it>/business-tools/fx-challenge
# FX_MAIL_ALWAYS=1                # send every day, Saturday included, with or without a recommendation
# The mail server: FX_MAIL_SMTP_URL, else NET_CASH_SMTP_URL, else SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS (the
# backend's mailer, as the net-cash mail). FX_MAIL_SMTP_HELO (else NET_CASH_SMTP_HELO, else the sender's domain).
```

The mail server decides whether the mail can go out as `FX_MAIL_FROM`:
- Google's SMTP relay (`smtp-relay.gmail.com`, allowlisted IP) sends as any lsports.eu address.
- An account's own login (an app password) sends as that account. Gmail replaces another `From` with the account
  unless that address is one of its "Send mail as" addresses.

Then:

```bash
node scripts/fx-challenge-mail.cjs --dry-run --html=/tmp/fx-mail.html   # print it (and the HTML), and whether today's check sends it; sends nothing
node scripts/fx-challenge-mail.cjs                                     # send one now (a test)
# crontab -e: every hour; it checks once a day from 10:00 Israel time, so summer time needs no change
0 * * * * cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && /usr/bin/node scripts/fx-challenge-mail.cjs --scheduled >> /var/log/fx-challenge-mail.log 2>&1
```

The scheduled run behaves like this:
- It checks once a day and records it in `data/fx-challenge-mail-last.json`: the day, and either the mail that went
  (to how many, its subject and its recommendations) or why none went. A run by hand from the command line doesn't
  record anything, so the daily check still runs; the page's Send now counts as the day's mail.
- With an empty list on the page it sends nothing, and says so once, at the hour.
- If the balances or the rate could not be read, it tries again the next two hours, then sends what it has, with
  the reason.
- If the mail server refuses, it tries again every hour that day.
- The log has one line per check: `[fx-mail] 2026-10-22: sent to 2 recipient(s) from … via …: <subject>`,
  `[fx-mail] 2026-10-22: no mail (no recommendations)`, or `[fx-mail] 2026-10-24: no mail on Saturdays`.

## Saving

- The table is shared by everyone who can open the page. It is stored in `data/fx-challenge-table.json`
  (git-ignored).
- Every save is appended to `data/fx-challenge-table-history.jsonl`, with who and when.
- Saves are validated (the same rules as the page), JSON only, same-origin, and carry finance-it's CSRF
  token (`server/json-store.cjs`).
- While the page was a Metrics tab, the table was saved in the Metrics settings. That copy is not read: paste
  it again if one was saved there.

## Deploying

This version takes the tab off the Metrics page, so do steps 1 and 2 together: until the sidebar item
exists, the page opens only at its URL.

1. **Bank dashboard**: Pull & Build (the build now has six pages: `fx-challenge.html` is new), then
   `pm2 restart finance-it-backend` (server code changed).
2. **finance-it** (manual, not in this repository), the same way Metrics and the Salary Projection were added:
   1. **Role and sidebar item**, in `shared/src/types`:
      - Add `FX_CHALLENGE = 'fx_challenge'` to `UserRole`.
      - Add a `BUSINESS_TOOLS` entry **FX Challenge** → `/business-tools/fx-challenge` with that role, right
        after **Metrics**. This one list drives the sidebar item, the Users Management checkbox and the hub
        entry.
      - If user roles are stored as a Postgres enum or checked by a constraint, add a migration for the new
        value, like the earlier Business Tools roles.
   2. **Frontend route** `/business-tools/fx-challenge`: the same iframe component as the other pages, with
      `page="fx-challenge.html"`.
   3. **Backend**: update `src/routes/cash-projection.ts` from `docs/backend-cash-projection-route.ts`. It
      mounts every `/api/fx-challenge` route behind the `fx_challenge` role, by prefix. If finance-it-backend
      mounts the shared API (`docs/backend-bank-dashboard-api.ts`), the routes are already included.
   4. Build finance-it and `pm2 restart finance-it-backend`.
3. **Access**: tick **FX Challenge** for the users who need it in Users Management. Admins always see it.

**Since the daily mail's recipients moved to the page** (`GET/PUT /api/fx-challenge/mail`), finance-it's route file
mounts the whole `/api/fx-challenge` prefix once (`createFxChallengeRouter`). Copy the file once more (step 2.3,
then 2.4); after that, a new FX route needs no finance-it change. Until then, the page's Daily mail card says the
list is still `FX_MAIL_TO`, and everything else works as before.

```bash
cd /home/ubuntu/finance-it
cp extra-apps/bank-dashboard/docs/backend-cash-projection-route.ts backend/src/routes/cash-projection.ts
git diff --stat
cd backend && npm run build && pm2 restart finance-it-backend
pm2 logs finance-it-backend --lines 200 --nostream | grep fx-challenge   # [fx-challenge] mounted from …
```

## Checking it

```bash
node scripts/test-fx-challenge.cjs   # bands, risk, the reserve plan, salaries, cards, vendors and new deposits, what to do today and when the mail goes, past conversions, the rate check, the pasted table, its validation, the routes, the daily mail, the NetSuite reads
```

On the server, `pm2 logs finance-it-backend --lines 300 --nostream | grep fx-challenge` shows
`[fx-challenge] mounted` (finance-it's route file), and why a read failed (`[fx-challenge] mail balances
unavailable: …`).
