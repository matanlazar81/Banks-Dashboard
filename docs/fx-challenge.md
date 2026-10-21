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
  - Free to convert now = liquid euros less what the reserve still needs. It is also shown with the backup
    covering a jump above today's band, and by the fixing with the typical inflow (customers less EUR
    payments, before conversions, deposits and transfers, last 3 closed months).
  - The path shows the next 6 fixings if nothing else is converted. Its **Deposits released** column is the EUR
    deposits maturing after the previous fixing (after today, for the first) and by that one; hover a figure for
    each deposit's date and value. Deposits maturing after the last fixing shown are listed under the table.
    They count only if the bank pays them out rather than rolling them over.
- **Salaries, vendors and new deposits**: once the payroll is known (it nearly always is), the plan also covers the
  shekels each month needs. The headline then reads "Keep … for 7 Nov. For the salaries and vendors due by 15 Nov,
  convert at least … by 5 Nov. Spare now: …", and **New deposits** says how much can go into a new deposit for 1, 3
  or 6 months.
  - **Payroll**: the Salary Projection's Total payroll by month (`totalPayrollByMonth`, the same figure as its
    page), from the figures that page saved on the server (`data/salary-projection-cache.json`; open the Salary
    Projection once if there are none). It is split as NetSuite's payroll journals of the last 3 closed months
    (`fetchPayrollLiabilities`, `payrollSplit`). Credits to the employees (200000/200001) are the **net salaries,
    paid by the 5th** of the next month. The rest (withholding tax 180013, pension 180069, social security 180012)
    is **paid by the 15th**. In 2026 the net salaries were about 48–51%. A month the projection lacks carries the
    last journal's total.
  - **Vendors**: the average of the last 3 closed months. ILS vendor payments (vendor payments and checks out of
    the ILS accounts, `fetchIlsVendorPayments`; the payroll is paid by journal, so it is not in them) are a monthly
    need in shekels. EUR vendor payments (the flows' vendor kind) are **kept in the euro accounts at all times**
    (the EUR floor); the typical inflow already nets them.
  - **The shekels** (`shekelNeeds`), from the ILS on hand (the mail's balances; an overdraft counts against them; 0 when
    not known):
    - today, the rest of this month's vendor payments;
    - on the 5th, the previous month's net salaries and the month's vendor payments;
    - on the 7th, the table's shekels, for the amount kept at its cell's rate (consistent with what the plan
      keeps);
    - on the 15th, pension, tax and social security.

    Each gap is converted by its day at today's rate ("now" for the rest of this month's vendors). The card **Salaries and vendors: the shekels each month
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
- **LSports euros**: the accounts, then each EUR deposit by the day it matures.
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
| ILS vendor payments | NetSuite's books: vendor payments and checks out of the ILS accounts (book 2), four months back |

The RESTlet gives what the bank imports last fetched: Bank Hapoalim and its deposits (several times a day),
PKO (each morning's statement), PayPal (01:00 and 13:00) and Payhound (about every 20 minutes). Leumi and
MoneyNet carry no balance in their files, so they are not in it. Only LSports' euros count.

## The daily mail

`scripts/fx-challenge-mail.cjs` sends the recommendation every day. It reads NetSuite fresh (no cache) and computes
the same figures as the page (`planFromPayload`).

**Who gets it** is kept on the page, in the **Daily mail** card at the bottom:
- Add or remove lsports.eu addresses, then **Save for everyone**.
- The list is saved in `data/fx-challenge-mail.json`, with a history line per save (who and when), as the table is.
- Until a list is saved there, the mail goes to `FX_MAIL_TO` in the checkout's `.env`.
- An empty list sends to nobody.
- The card also shows the sender, the hour, and when the mail last went.

The mail is HTML with a plain-text part:
- the subject: what is free to convert (or short), the rate verdict, and what to keep for the next conversion;
- what to keep, what to convert for the salaries and vendors and by when, what is spare (or short), new deposits for
  1, 3 and 6 months, and the rate check;
- the shekels each month needs (net salaries by the 5th; tax, pension and social security by the 15th; ILS vendors;
  the 7th's conversion; what to convert);
- what the next conversion can sell, range by range (today's range highlighted);
- your euros, with how "free to convert now" adds up and the typical inflow;
- the next six conversions, and each deposit on the conversion it counts for;
- the page's notes (what could not be read), and the setting and sources at the bottom.

Set it up in the checkout's `.env` (immutable on the server: `sudo chattr -i .env`, edit, `sudo chattr +i .env`):

```bash
FX_MAIL_TO=matan.l@lsports.eu,dotan@lsports.eu   # until a list is saved on the page; lsports.eu only, unless FX_MAIL_ALLOW_EXTERNAL=1
FX_MAIL_FROM=matan.l@lsports.eu
# FX_MAIL_HOUR=10                 # Israel time (default 10)
# FX_MAIL_POLICY=p95              # today | p95 | worst, as the page's "What to keep"
# FX_MAIL_LINK=https://<finance-it>/business-tools/fx-challenge
# The mail server: FX_MAIL_SMTP_URL, else NET_CASH_SMTP_URL, else SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS (the
# backend's mailer, as the net-cash mail). FX_MAIL_SMTP_HELO (else NET_CASH_SMTP_HELO, else the sender's domain).
```

The mail server decides whether the mail can go out as `FX_MAIL_FROM`:
- Google's SMTP relay (`smtp-relay.gmail.com`, allowlisted IP) sends as any lsports.eu address.
- An account's own login (an app password) sends as that account. Gmail replaces another `From` with the account
  unless that address is one of its "Send mail as" addresses.

Then:

```bash
node scripts/fx-challenge-mail.cjs --dry-run --html=/tmp/fx-mail.html   # print it (and the HTML); sends nothing
node scripts/fx-challenge-mail.cjs                                     # send one now (a test)
# crontab -e: every hour; it sends once a day from 10:00 Israel time, so summer time needs no change
0 * * * * cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && /usr/bin/node scripts/fx-challenge-mail.cjs --scheduled >> /var/log/fx-challenge-mail.log 2>&1
```

The scheduled run behaves like this:
- It sends once a day and records the day (and to how many) in `data/fx-challenge-mail-last.json`. A run by hand
  doesn't record it, so the daily mail still goes.
- With an empty list on the page it sends nothing, and says so once, at the hour.
- If the balances or the rate could not be read, it tries again the next two hours, then sends what it has, with
  the reason.
- If the mail server refuses, it tries again every hour that day.
- The log has one line per send or retry: `[fx-mail] 2026-10-22: sent to 2 recipient(s) from … via …`.

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
node scripts/test-fx-challenge.cjs   # bands, risk, the reserve plan, salaries, vendors and new deposits, past conversions, the rate check, the pasted table, its validation, the routes, the daily mail, the NetSuite reads
```

On the server, `pm2 logs finance-it-backend --lines 300 --nostream | grep fx-challenge` shows
`[fx-challenge] mounted` (finance-it's route file), and why a read failed (`[fx-challenge] mail balances
unavailable: …`).
