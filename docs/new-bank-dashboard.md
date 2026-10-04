# New Bank Dashboard

A light, separate page for the monthly cash projection: **actuals + forecast for the current year,
rolled forward into the next year**, for LSports. It sits next to the existing Bank Dashboard
(nothing in `src/App.tsx` changed) and is meant to appear under **Business Tools → New Bank Dashboard**
in finance-it.

Why it is fast: the browser makes **one request** (`GET /api/cash-projection`) and only renders the
answer. The server computes the projection, caches it on disk, and refreshes it in the background.
The page bundle is React plus a few small files; it never downloads the 12k-line `App.tsx`, recharts
or html2canvas, and the Excel library loads only when someone clicks Export.

Its accrual twin, the **P&L Projection** (same engine and inputs, NetSuite P&L actuals, accumulated
profit), is described in [pnl-projection.md](pnl-projection.md).

## URLs

| Where | URL |
|---|---|
| Dev (`npm run dev`) | `http://localhost:5176/new-bank-dashboard.html` |
| Standalone server (`node server.cjs`) | `http://<server>:8790/new-bank-dashboard.html` |
| finance-it | sidebar item → iframe of `<bank-dashboard static base>/new-bank-dashboard.html` (see below) |

View options are kept in the URL: `?plan=base`, `?ccy=ils`, `?years=current|next`.

## How it works

```
browser ──GET /api/cash-projection──► server/cash-projection.cjs
                                        ├─ gatherInputs()            scripts/net-cash-forecast-compute.cjs (the nightly job's own input assembly)
                                        ├─ loadScenarioDataAsync()   official plan (NET_CASH_SCENARIO_NAME, default "Exit plan June26")
                                        ├─ computeCashflowForecast() src/forecast/forecast-core.mjs  → current year
                                        ├─ buildNextYearInputs()     src/forecast/roll-forward.mjs   → next year
                                        └─ cache: data/cash-projection-cache.json
```

- **Variants:** *Plan* (the official plan's adjustments) and *Base* (no adjustments) are computed
  together; the toggle is instant.
- **Caching:** fresh for `CASH_PROJECTION_TTL_MIN` (30) minutes and within the same calendar month.
  After that, the cached figures are served immediately while one background recompute runs; the
  page polls and swaps in the new figures. Refresh asks for a recompute (at most once a minute).
- **First load:** with an empty cache the API answers `202 computing` and the page shows a progress
  card. NetSuite calls go through the shared NetSuite queue, so a cold computation takes a few
  minutes. Prime the cache after a deploy (`--write-cache`, below) to skip that wait.
- **Rolling window:** current year + next year. Today that is 2026 + 2027; from 1 January it becomes
  2027 + 2028 (the NetSuite feeds only cover the current year).

## What the table shows

Rows are line items, columns are months: `Jan–Dec <current> │ FY ║ Jan–Dec <next> │ FY`, in € or ₪
thousands (hover any cell for the exact amount).

| Line | Meaning |
|---|---|
| Opening balance | Previous month's closing. The current month (⚓) opens at the NetSuite bank balance of the previous month-end; the first projection month (↩) opens at December's closing. |
| Collections (AR), Pipeline, Churn | Inflows. Churn is shown as a deduction. |
| Salary, Vendors, Other | Outflows. Other = taxes, intercompany, fees, transfers; in parentheses when it is a net inflow. |
| Reval (FX) | Booked revaluation (actual months) or currency-defense budget (forecast months). |
| Net change | Total inflows − total outflows + reval. Dividends are not included (next line). |
| Dividend paid | Dividend distributions and their withholding tax paid from the bank (NetSuite), shown as a deduction. Future dividends are not forecast. |
| Closing balance | Opening + net change − dividend paid: the cash in the bank at month-end. |
| incl. bank re-anchor | Only when material: the difference between the model's previous closing and the bank balance the current month opens from. Already inside the opening balance; in the FY column it explains why Jan opening + Σ net change − Σ dividend paid ≠ Dec closing. |
| Monthly gap | Closing − opening: how much the bank balance rose (+, green) or fell (−, red) in the month, after dividends (= net change − dividend paid). FY: the sum of the months. |

Column status: **Actual** (closed months, from NetSuite bank activity), **Current** (actual so far +
remaining forecast), **Forecast**. Dividends are kept out of Vendors/Other, as in the Bank Dashboard.
Unlike the Bank Dashboard's operating view, they are not added back to the balances: they appear as
**Dividend paid**, so every balance is the cash in the bank and the current month opens exactly at the
NetSuite bank balance.

## Breakdown of a cell

Click an underlined figure (Collections, Pipeline, Churn, Salary, Vendors, Other, Reval, Dividend paid;
any month, or the FY column for the full year). A window opens that can be dragged by its title bar and
stays where it was put when another cell is opened; Esc or × closes it. It follows the Plan/Base and
€/₪ toggles.

Every breakdown adds up to the table cell: the listed rows, then labelled adjustment rows for what they
do not explain. In ₪, an "FX conversion difference" row appears when a forecast adjustment had to be
converted at the month's rate.

| Line | Actual months | Forecast months | Next year |
|---|---|---|---|
| Salary | Booked payroll by NetSuite account (Snowflake FCT_EXPENSE) + cash timing to the bank figure | The last closed payroll month's accounts + difference to the by-department basis + hires, leavers and overrides + plan changes | The Oct–Dec run-rate by department + changes |
| Vendors | Booked costs by NetSuite account, by category (FCT_EXPENSE) + cash timing | Vendor budget by NetSuite account (FCT_BUDGET) + budget overrides + plan adjustments | The same month of the current year, with its breakdown |
| Collections | Bank lines classified as collections (NetSuite groups them by type, not customer) | Expected revenue by customer (Snowflake) × collection rate + open pipeline deals | Oct–Dec average × collection rate + deals |
| Pipeline | – | Each forecast month's new MRR (projected MRR × calibration factor), cumulative + pipeline % | – |
| Churn | – | Run-rate × forecast months elapsed, with the customers lost in the run-rate's quarter as context | – |
| Other | Bank lines; the dividend withholding tax is moved to Dividend paid | – | – |
| Reval | FX bank lines, or the booked P&L revaluation | Currency-defense budget × defense % | – |
| Dividend paid | Distribution + withholding tax | – | – |

Actual months also have a collapsed "Bank lines" section for Salary and Vendors: the NetSuite bank
movements the cell is built from. Data comes from `GET /api/cash-projection/breakdown`
(`server/cash-projection-breakdown.cjs`): engine details saved with the cached projection (never sent with
the projection itself), plus Snowflake reads cached for `CASH_PROJECTION_TTL_MIN`, each using the same
table and filters as the feed of its line.

**Accounts by department.** Click an account's name in the breakdown and a second movable window opens
with that account by department. Its total equals the account row: the departments come from the
row's own table and months (booked costs from FCT_EXPENSE, budget rows from FCT_BUDGET, a mirrored
month from the month it mirrors; a full year sums its months). Amounts without a department show as
"Difference to the account row". Esc closes the department window first; the window clicked last is in
front. Same request as the cell, plus `&row=<row key>` (for example `row=acct:640001`).

**Account numbers link to NetSuite.** An account number opens the account's register in NetSuite
(subsidiary LSports Data): the months its amount was booked in, or the last 3 closed months for a budget
row. The NetSuite account ids are read with the projection (one light query); without them, or without
`NETSUITE_ACCOUNT_ID`, the numbers show without links.

## Same logic as the Bank Dashboard

- **Current year:** identical engine inputs to the nightly net-cash job (`net-cash-forecast-compute.cjs`
  main()): same feeds, same budget-override merges, same scenario knobs, basis forced to *Pipeline*
  revenue + *Last-Actual* salary. The Plan's December closing plus its FY Dividend paid therefore equals
  the official net-cash forecast (an operating-view figure) for the same data.
- **Next year** (port of the Bank Dashboard's projection-year loader, `App.tsx:2633-2856`):
  - opening = current-year December closing in bank cash (the engine runs on the operating-view
    closing; the current year's dividends are taken out of every next-year balance for display);
  - salary = Oct–Dec payroll budget by department, scaled to the Oct–Dec salary the current year
    shows, flat across the year (falls back to the flat Oct–Dec budget average without the breakdown);
  - vendors = current year mirrored month by month; collections = Oct–Dec average;
  - no new pipeline and no churn; the open pipeline still feeds Collections through the
    probability filter, as before;
  - scenario month-% maps: the next year's own maps when set, otherwise salary %, collection % and
    currency-defense % inherit the current year's (pipeline % does not).
- The roll-forward is rebuilt from live data on every computation, which equals the old dashboard
  right after "↻ from <year>". The old dashboard instead reads a stored snapshot file that can be
  stale; `--snapshot-file` reproduces that for comparisons.

Two behaviours are kept on purpose for parity and are worth a methodology decision later:

1. **Next-year FX reval is 0.** The old projection year ends up with an empty finance budget, so no
   currency-defense reval is projected.
2. **Month-% knobs apply twice in the next year.** Its salary and collections baselines are Oct–Dec
   averages that already include the current year's month-% adjustments, and those maps are then
   inherited and applied again (e.g. a −4% salary knob for Nov–Dec also lowers next Nov–Dec by 4%).

## Deploying

On the server, in the bank-dashboard checkout that finance-it uses
(`/home/ubuntu/finance-it/extra-apps/bank-dashboard`):

```bash
cd /home/ubuntu/finance-it/extra-apps/bank-dashboard
git fetch origin && git checkout master && git pull --ff-only origin master
npm ci
node scripts/test-cash-projection.cjs          # synthetic checks, no external calls
node scripts/test-forecast-core.cjs
npm run build                                  # or your usual finance-it deploy, which builds this repo
node scripts/cash-projection.cjs --write-cache # optional: prime the cache (real NetSuite/Snowflake pull)
pm2 restart finance-it-backend                 # whatever serves /api/* (banks-dashboard for server.cjs)
```

Then check that the existing Bank Dashboard still opens: the build now has two pages that share the
React chunk.

### finance-it backend: serve `/api/cash-projection` and its breakdown

- If finance-it-backend mounts the shared API (`docs/backend-bank-dashboard-api.ts`; pm2 logs show
  `[bank-dashboard] shared API mounted`), both routes are already included.
- Otherwise add `docs/backend-cash-projection-route.ts` (one file + one call). It mounts
  `GET /api/cash-projection` and `GET /api/cash-projection/breakdown`. finance-it-backend has no global
  `/api` login gate, so the file guards both with the Bank Dashboard role.
- Server-side changes in this repo (`server/`, `src/forecast/`) take effect after a restart of whatever
  serves `/api/*`; a page-only change needs only the build.

### finance-it frontend: the sidebar entry

finance-it is not in this repository, so this part is a short manual change there:

1. Under **Business Tools**, add **New Bank Dashboard** after **Bank Dashboard**, linking to
   `/business-tools/new-bank-dashboard`.
2. That route renders the same iframe component the Bank Dashboard page uses, with the iframe `src`
   pointing at `new-bank-dashboard.html` in the same static location as the dashboard's `index.html`.
3. Keep the same sandbox attributes and add `allow-downloads` if it is missing (Export).
4. Make sure the deploy copies `dist/new-bank-dashboard.html` along with `dist/index.html` and `dist/assets/`.

### Keeping it warm

A morning cron right after the nightly job means nobody waits:

```
10 6 * * *  cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && node scripts/cash-projection.cjs --write-cache >> data/cash-projection-cron.log 2>&1
```

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `NET_CASH_SCENARIO_NAME` | `Exit plan June26` | The official plan shown as *Plan* (shared with the nightly job) |
| `CASH_PROJECTION_TTL_MIN` | `30` | Minutes before cached figures are refreshed in the background |
| `CASH_PROJECTION_TIMEOUT_MIN` | `10` | A computation running longer is abandoned |
| `CASH_PROJECTION_PREWARM` | off | `1` = compute 30 s after the server starts if the cache is empty |

## Checking it

```bash
node scripts/test-cash-projection.cjs                 # synthetic: roll-forward rules, table arithmetic, cache behaviour, UI model
node scripts/cash-projection.cjs --dry-run            # real data: both years, Plan and Base
node scripts/net-cash-forecast-compute.cjs --dry-run  # its December closing must equal the Plan's + FY Dividend paid
node scripts/cash-projection.cjs --compare=data/parity/rows-2026.json            # vs old dashboard (?fccapture=1)
node scripts/cash-projection.cjs --compare=data/parity/rows-2027.json --snapshot-file=data/budgets/2027-lsports.json
```

For `--compare`, open the old dashboard with `?fccapture=1` on the same plan (Revenue: Pipeline,
Salary: Last Actual), run `copy(JSON.stringify(window.__fcRows))` in the browser console for each year,
and save the result under `data/parity/` (git-ignored: these are internal financial figures).
`scripts/fixtures/cash-projection-sample.json` is a synthetic payload for UI checks.

## Limitations

- LSports only (the shared engine and the nightly job's inputs are LSports-specific).
- After 1 January the window moves to the new year + the next one; keeping a closed year visible
  needs year-aware NetSuite queries.
- Plan is the official plan only; personal scenarios stay in the Bank Dashboard.
