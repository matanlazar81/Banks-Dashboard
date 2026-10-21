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
| finance-it | sidebar item → iframe of `<bank-dashboard static base>/fx-challenge.html` (see Deploying) |

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
- **Past conversions**: the table at that day's rate against the "Conversion" / המרה journals booked within
  3 days (Hapoalim EUR 11001 → ILS 10002), the shekels gained against that day's rate, and the month's other
  conversions.
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
| EUR/ILS | NetSuite `currencyrate` (HSBC), the last 400 days |
| Balances | the daily balances mail's RESTlet (`payhound_api` `ph_rl_balances.js`) when `NS_BALANCES_RESTLET=<script>:<deploy>` is set in the checkout's `.env`; else NetSuite's books (EUR bank accounts in the primary book, exact in euros, no deposit dates) |
| Monthly EUR flows | NetSuite's books, four months back |
| Conversions | the Hapoalim "Conversion" / המרה journals, from just before the table's first fixing |

The RESTlet gives what the bank imports last fetched: Bank Hapoalim and its deposits (several times a day),
PKO (each morning's statement), PayPal (01:00 and 13:00) and Payhound (about every 20 minutes). Leumi and
MoneyNet carry no balance in their files, so they are not in it. Only LSports' euros count.

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
   3. **Backend**: update `src/routes/cash-projection.ts` from `docs/backend-cash-projection-route.ts`. It now
      also mounts `GET /api/fx-challenge` and `GET/PUT /api/fx-challenge/table` behind the `fx_challenge`
      role. If finance-it-backend mounts the shared API (`docs/backend-bank-dashboard-api.ts`), the routes are
      already included.
   4. Build finance-it and `pm2 restart finance-it-backend`.
3. **Access**: tick **FX Challenge** for the users who need it in Users Management. Admins always see it.

## Checking it

```bash
node scripts/test-fx-challenge.cjs   # bands, risk, the reserve plan, past fixings, the pasted table, its validation, both routes, the NetSuite reads
```

On the server, `pm2 logs finance-it-backend --lines 300 --nostream | grep fx-challenge` shows
`[fx-challenge] mounted` (finance-it's route file), and why a read failed (`[fx-challenge] mail balances
unavailable: …`).
