# Metrics

Business Tools → **Metrics**: one short pack in the same shape every month, computed live each time
the page opens, from the data the New Bank Dashboard and the P&L Projection already use. Every figure
is tagged **A** (actual), **F** (forecast) or **A+F** (actual months plus forecast months). All
figures follow the Plan.

## URLs

| Where | What |
|---|---|
| `metrics.html` | the page (a small bundle, never loads the old dashboard) |
| `GET /api/metrics` | the pack (`server/metrics.cjs`); `?refresh=true` reads ARR, NRR, churn, FX and the ECB rate again |
| `GET/PUT /api/metrics/settings` | planning rate, cloud cap, innovation envelope (`server/metrics-settings.cjs`) |
| `GET/PUT /api/metrics/deposits` | the deposit tracker |
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
- **Year-to-date column:** the months' revenue ÷ employee-months, i.e. revenue a month per employee, so it
  compares with the monthly columns.
- **The figure above the chart:** the months' revenue per average employee, and that pace over a full
  year.
- **Data read:** only start dates, leave dates and the employment type, with no names or ids, cached like
  the other reads.
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

## Innovation envelope

Extra innovation spending on top of the forecast (from Dotan's pack: "say if the innovation envelope is
in or out"). It is an amount for a year, spread evenly over the months from a start month to December.
It only applies to forecast months: a closed month keeps what was booked. The card explains this in one
line.

- **In**: the pack's EBITDA and December cash include it.
- **Out**: they don't.

Either way, the envelope card shows EBITDA and December cash both with and without it. The envelope is
treated as **on top of** the forecast. If it is already inside the budget, set it to Out.

## Rates and FX conversions

- **USD/EUR planning rate**: a setting (USD per €), shown next to today's ECB rate (Frankfurter, with
  open.er-api.com as backup) and the difference in %.
- **FX conversions**: last month's NetSuite transfers between bank accounts in different currencies,
  by transaction date. Each one shows the amount in the transfer's currency, its € value (primary book)
  and the rate (units per €), with totals per currency pair. Transfers within one currency are moves,
  not conversions, and are left out.

## Deposits awaiting confirmation

NetSuite and Snowflake hold no confirmation status, so the page keeps a small tracker:

- **Manage** lists every deposit and adds one (bank, amount, currency, placed, maturity, note).
- **Confirmation received** closes it.
- The pack lists the open ones, oldest first.

## Saving

- Settings and deposits are shared by everyone who can open the page. They are stored in
  `data/metrics-settings.json` and `data/metrics-deposits.json` (git-ignored).
- Every save is appended to a `-history.jsonl` file next to them, with who and when.
- Saves are validated (ranges, known fields only), JSON only, same-origin, and carry finance-it's CSRF
  token (`server/json-store.cjs`, shared with the 2027 targets).

## How it is computed

- The two projections come from their cached handlers (`handler.current()`), so the page adds no
  NetSuite pull.
- The saved targets (`data/projection-targets.json`) are read on every request.
- ARR, churn, NRR, FX conversions and the ECB rate are read once and cached for `METRICS_TTL_MIN`
  (default 30 minutes; the ECB rate for at most an hour).
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
node scripts/test-metrics.cjs   # synthetic: every pack item, NRR, cloud cap, envelope, targets, payroll ratios, FX, deposits, handlers, export rows
```
