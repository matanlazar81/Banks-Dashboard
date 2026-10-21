# Metrics

Business Tools → **Metrics**: one short pack in the same shape every month, computed live each time
the page opens, from the data the New Bank Dashboard and the P&L Projection already use. Every figure
is tagged **A** (actual), **F** (forecast) or **A+F** (actual months plus forecast months). All
figures follow the Plan.

## URLs

| Where | What |
|---|---|
| `metrics.html` | the page (a small bundle, never loads the old dashboard) |
| `GET /api/metrics` | the pack (`server/metrics.cjs`); `?refresh=true` reads ARR, NRR, churn and employees again |
| `GET /api/metrics?detail=<item>` | what one figure is made of: `nrr`, `churn-quarter`, `churn-ytd`, `churn-forecast` |
| `GET /api/metrics?view=fx` | the FX Challenge tab (`server/fx-challenge.cjs`); `&refresh=true` reads NetSuite again |
| `GET/PUT /api/metrics/settings` | the cloud cap and the FX Challenge's conversion table (`server/metrics-settings.cjs`) |
| `GET/PUT /api/metrics/deposits` | the old deposit tracker: no longer on the page, kept mounted for finance-it's route file |
| finance-it | sidebar item → iframe of `<bank-dashboard static base>/metrics.html` (see Deploying) |

## The pack

| Row | Last month | Year to date | FY (this year) | FY (next year) |
|---|---|---|---|---|
| Revenue | P&L total revenue of the last closed month (A) | closed months (A) | closed + forecast months (A+F) | forecast (F) |
| EBITDA | same, P&L operating profit | (A) | (A+F) | (F) |
| Net cash | the NetSuite bank balance at the last month-end (A) | change since 1 January (A) | December closing, New Bank Dashboard (F) | December closing (F) |
| ARR | MRR × 12 now (Snowflake, A) | – | December customer revenue (after pipeline and churn) × 12 (F) | same (F) |
| NRR | trailing 12 months to the last closed month (A), with GRR | – | – | – |
| Churn | churned MRR of the last full quarter (A) | churned MRR this year so far (A) | revenue the forecast loses to churn (F) | – (not projected) |

There is no debt data, so **net cash is the cash in the bank**.

**FY (next year) follows the 2027 targets** saved on the New Bank Dashboard. These are the same figures as the
Targets view on both projection pages (`src/forecast/targets.mjs`):
- revenue, EBITDA and ARR from the P&L with the targets;
- December cash from the cash projection with the targets;
- cloud as described below.

The page says so next to the Plan name (hover for the assumptions), and so does the export. A save on the New
Bank Dashboard shows at the next load. With no targets saved, the column is the Plan.

**NRR**: for the customers who had revenue 12 months earlier, their revenue this month ÷ their revenue
then. Customers new since then are not counted. **GRR** caps each customer at their old amount, so it
measures only what was kept. Source: Snowflake revenue by customer and month (actual months, test
customers excluded). The page shows the last closed month; the API also returns the last 6 months.

## Payroll / revenue and revenue per employee

Two column charts. Each has one column per month from January to the **last month whose payroll JE is
posted** in NetSuite, then a year-to-date column (darker, set apart). Each column shows its value on top
(payroll / revenue to one decimal); hover a column for its figures;
**Show the figures** lists them all, and the export carries them too.

**Which months are shown.** A closed month's payroll counts as posted when it is at least half the
year's largest month. If September is closed but its payroll JE isn't posted yet, the charts end in
August, and the page says September is pending.

**Payroll / revenue**
- **Payroll:** NetSuite 76xxxx by posting period, gross: before the capitalised salaries (950000).
- **Revenue:** the P&L's total revenue, the same as the pack.
- **Year to date:** Σ payroll ÷ Σ revenue.

**Revenue per employee**
- **Monthly column:** the month's revenue ÷ employees at month-end.
- **Employees:** HiBob (Snowflake `DIM_EMPLOYEE__FINANCE`) employees of the P&L's company (LSports,
  `METRICS_HEADCOUNT_COMPANY`), any employment type. An employee counts when they started on or before
  the month's last day and had not left before it. Hover a month for the split by employment type.
  HiBob's Bringits department counts too: its people's pay is in the same payroll (as on the Salary Projection).
  HiBob's records are counted as people, as on the Salary Projection (`peopleOf` in `server/metrics.cjs`): an
  employee with more than one record (a move to another department) counts once, and the October 2026 move to
  Bringits (HiBob's Technology group) counts from before it.
- **Year-to-date column:** the months' revenue ÷ employee-months, i.e. revenue a month per employee, so it
  compares with the monthly columns.
- **The figure above the chart:** the months' revenue per average employee, and that pace over a full
  year.
- **Data read:** start dates, leave dates, the employment type, the department and the employee id, with no names,
  cached like the other reads. The id only tells one person's records apart; it never leaves the server.
- **If HiBob can't be read:** payroll / revenue still shows, and revenue per employee is empty with a
  warning.

## Cloud against the cap

- **Cloud** = NetSuite accounts 640xxx (Cloud Infrastructure & DevOps) in closed months. In forecast
  months it is the budget's share of that category in the month's operating expenses. Next year uses
  the same split as the 2027 targets.
- **Next year, with the targets:**
  - When the targets set **server costs as a % of revenue** for this category, cloud = that % × customer
    revenue after the revenue targets, month by month.
  - Otherwise, a **% change** the targets give the category scales it.
- **Cap** = cap % (default 8%) × projected FY total revenue (actual + forecast months, after the targets).
  Server costs are a % of *customer* revenue, while the cap is on *total* revenue, which includes other
  revenue. So cloud at 8% in the targets can show a little under 8% here.
- Shown for both years: cloud, cap, headroom (negative when over) and cloud as a % of revenue.
- The cap % and the category are settings on the page.

## Click a figure: how it is calculated

The underlined figures open a movable window with what they are made of: the steps from the source to
the figure, how it is calculated, where the data comes from, and the items behind it. Each one adds up
to the figure in the cell.

- **NRR**:
  - the bridge: last year's customers' revenue then, + expansion, − contraction, − churned, = their
    revenue now, then NRR and GRR;
  - the customers in each group, by name, with the new customers listed but not counted;
  - the last 6 months of NRR and GRR.
  - Source: Snowflake revenue by customer and month (the same rows as the figure).
- **Churn, last full quarter** and **year so far**: every opportunity marked churned in Snowflake
  (`DIM_OPPORTUNITY__FINANCE`) with its customer, churn month, currency and MRR, by quarter. Amounts in
  another currency are added as they are, as the figure does, and the window says so.
- **Churn, FY forecast months**:
  - the forecast's churn is a run-rate, not a list of customers: last full quarter's churned MRR ÷ 3,
    times each month's place in the forecast (it piles up);
  - month by month, with the Plan's hand-set months marked;
  - the quarter's churned customers shown as context.

The breakdown reads only when clicked, and is cached like the pack's reads.

## FX Challenge (second tab)

**Metrics | FX Challenge** in the header (`?tab=fx`). Finance's monthly EUR→ILS conversion program at
Bank Hapoalim (583400): on each fixing (the 7th) that day's EUR/ILS picks a band of the conversion
table, and the cell is the euros sold (€ thousands) and the deals' blended rate. October 2026 checks
out: EUR/ILS 3.425 → band 3.4–3.49 → €800K at 3.67, booked on 6 October as three "Conversion" journals
(€200K @4.10, €300K @3.56, €300K @3.49 = ₪2,935,000). The logic is `src/forecast/fx-challenge.mjs`,
shared by the server, the page and the tests.

- **The table** is the one received in October 2026 (fixings October 2026 to December 2027) until
  someone saves another: Edit table → paste the range from Excel (the header row of bands, then
  `10/2026` and `-500 / 3.62` per band) → it is checked → Save for everyone (Metrics settings,
  `fxChallenge`, with history). "Back to the table received in October 2026" removes the saved one.
  Bands are lower-inclusive (a rate on an edge counts in the band above); the deals decide the edges.
- **Simulate**: type a rate or use the slider or presets. The schedule shows each remaining fixing at
  that rate (euros, rate, shekels, shekels against that rate) beside its risk from today's rate.
- **Risk of a fixing**: EUR/ILS on the day is lognormal around today's rate (no drift), with the
  annual volatility of NetSuite's daily HSBC rate over 400 days (7.5% assumed when there is too little
  history). That gives each band's chance, the expected euros, the 95% amount (enough for 19 fixings
  in 20) and the table's worst case.
- **What to keep, what is free** (`planReserve`), for the next fixing:
  - Keep: today's band, the 95% amount (default) or the worst case (`?reserve=today|worst`).
  - It is paid from Hapoalim EUR (583400); the other EUR accounts can be moved there before the day.
  - EUR deposits that mature before the fixing count for it; later ones are the backup (broken early
    only if needed). Without the mail's balances there are no dates, so every deposit is backup.
  - Free to convert now = liquid euros less what the reserve still needs; also shown if the backup
    covers a jump above today's band, and by the fixing with the typical inflow (customers less EUR
    payments, before conversions, deposits and transfers, last 3 closed months). The path shows the
    next 6 fixings if nothing else is converted.
- **Past fixings**: the table at that day's rate against the "Conversion" / המרה journals booked within
  3 days (Hapoalim EUR 11001 → ILS 10002), the shekels gained against that day's rate, and the month's
  other conversions.
- **Sources** (cached `METRICS_TTL_MIN`, a failed read is a warning): EUR/ILS from `currencyrate`
  (HSBC); balances from the daily balances mail's RESTlet (`payhound_api` `ph_rl_balances.js`) when
  `NS_BALANCES_RESTLET=<script>:<deploy>` is set, else NetSuite's books (EUR bank accounts in the
  primary book, exact in euros, no deposit dates); monthly EUR flows and the conversions from the
  books. It answers without waiting for the projections.

## Saving

- Settings (the cloud cap and the FX Challenge's table) are shared by everyone who can open the page.
  Both tabs save by reading the saved settings, merging their change and writing it back, so neither
  overwrites the other's latest save. They are stored in
  `data/metrics-settings.json` (git-ignored).
- Every save is appended to a `-history.jsonl` file next to them, with who and when.
- Saves are validated (ranges, known fields only), JSON only, same-origin, and carry finance-it's CSRF
  token (`server/json-store.cjs`, shared with the 2027 targets).
- The innovation envelope, the USD/EUR planning rate, FX conversions and the deposit tracker were
  removed from the page. Settings saved with the envelope or the rate still load and save; those fields
  are ignored.

## How it is computed

- The two projections come from their cached handlers (`handler.current()`), so the page adds no
  NetSuite pull.
- The saved targets (`data/projection-targets.json`) are read on every request.
- ARR, churn, NRR (revenue by customer) and employees are read once and cached for `METRICS_TTL_MIN`
  (default 30 minutes).
- A source that fails shows as an empty cell and a warning. The rest of the pack still shows.
- While a projection is computing for the first time, the page shows "computing" and polls.

## Deploying

1. **Pull & Build** (the build now has four pages: `metrics.html` is new), then
   `pm2 restart finance-it-backend`.
2. finance-it backend: mount the Metrics routes in `backend/src/routes/cash-projection.ts` (the block is
   in `docs/backend-cash-projection-route.ts`).
3. finance-it frontend: a Business Tools sidebar item **Metrics** → `/business-tools/metrics`, the same
   iframe component as the other pages with `page="metrics.html"`, `allow-downloads` kept (Export).

Access: the **Metrics** checkbox in finance-it's Users Management (role `metrics`). Every Business Tools
tab has its own checkbox, from one list in finance-it (`shared/src/types` `BUSINESS_TOOLS`): adding a tab
there adds its sidebar item, checkbox and hub entry.

## Checking it

```bash
node scripts/test-metrics.cjs   # synthetic: every pack item, NRR, cloud cap, targets, payroll ratios, breakdowns, handlers, export rows
node scripts/test-fx-challenge.cjs   # the FX Challenge: bands, risk, the reserve plan, past fixings, the pasted table, its validation, ?view=fx, the NetSuite reads
```
