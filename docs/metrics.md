# Metrics

Business Tools → **Metrics**: one short pack in the same shape every month, computed live each time
the page opens, from the data the New Bank Dashboard and the P&L Projection already use. Every figure
is tagged **A** (actual), **F** (forecast) or **A+F** (actual months plus forecast months). All
figures follow the Plan.

The FX Challenge was briefly a second tab here; it is its own page now (`docs/fx-challenge.md`).

## URLs

| Where | What |
|---|---|
| `metrics.html` | the page (a small bundle, never loads the old dashboard) |
| `GET /api/metrics` | the pack (`server/metrics.cjs`); `?refresh=true` reads ARR, NRR, churn and employees again |
| `GET /api/metrics?detail=<item>` | what one figure is made of: `nrr`, `churn-quarter`, `churn-ytd`, `churn-forecast` |
| `GET /api/metrics?detail=nrr&all=true` | the same, plus every customer (id, group, December and now, currency, CRM churn date) and each month: what NRR's Excel is built from |
| `GET/PUT /api/metrics/settings` | the cloud cap (`server/metrics-settings.cjs`) |
| `GET/PUT /api/metrics/deposits` | the old deposit tracker: no longer on the page, kept mounted for finance-it's route file |
| finance-it | sidebar item → iframe of `<bank-dashboard static base>/metrics.html` (see Deploying) |

## The pack

| Row | Last month | Year to date | FY (this year) | FY (next year) |
|---|---|---|---|---|
| Revenue | P&L total revenue of the last closed month (A) | closed months (A) | closed + forecast months (A+F) | forecast (F) |
| EBITDA | same, P&L operating profit | (A) | (A+F) | (F) |
| Net cash | the NetSuite bank balance at the last month-end (A) | change since 1 January (A) | December closing, New Bank Dashboard (F) | December closing (F) |
| ARR | MRR × 12 now (Snowflake, A) | – | December customer revenue (after pipeline and churn) × 12 (F) | same (F) |
| NRR | – | contracted MRR from 1 January to the last closed month (A), with GRR | – | – |
| Churn | churned MRR of the last full quarter (A) | churned MRR this year so far (A) | revenue the forecast loses to churn (F) | – (not projected) |

There is no debt data, so **net cash is the cash in the bank**.

**FY (next year) follows the 2027 targets** saved on the New Bank Dashboard. These are the same figures as the
Targets view on both projection pages (`src/forecast/targets.mjs`):
- revenue, EBITDA and ARR from the P&L with the targets;
- December cash from the cash projection with the targets;
- cloud as described below.

The page says so next to the Plan name (hover for the assumptions), and so does the export. A save on the New
Bank Dashboard shows at the next load. With no targets saved, the column is the Plan.

**NRR** is counted from 1 January on **contracted MRR**, the opportunities' MRR that ARR is made of, not on
the revenue booked in a month. A customer whose contract runs counts in full, whatever was billed or paid that
month, and nothing is forecast. The base is the customers who had MRR in December of the year before, i.e.
going into 1 January:
- **NRR** = their MRR in the last closed month ÷ their December MRR;
- customers with no December MRR (new this year) are not counted;
- **GRR** caps each customer at their December MRR, so it measures only what was kept.

In January the base is still the December before, so it measures one month. At the year's end (December
against December) it is the full year. On 1 January itself it would be 100%: nothing has changed yet.

**Source**: Snowflake `FCT_OPPORTUNITY__MONTHLY__FINANCE` (`OPPORTUNITY_MRR` by opportunity and month),
summed by customer through `DIM_OPPORTUNITY__FINANCE`. Test customers are excluded, and names and CRM churn
dates come from `DIM_CUSTOMER__FINANCE`. Both dimensions are reduced to one row per id first, so a duplicate
can't multiply the MRR. MRR is in each contract's currency and added as it is (as the churn figures do): a
customer's own change is right, and only its weight in the total moves. The breakdown says how many customers
have contracts in another currency.

Until October 2026, NRR was counted on the monthly revenue of `FCT_CUSTOMER__MONTHLY__FINANCE`. That counted a
customer with a running contract but nothing booked that month as churned, and one-off revenue swelled the
December base.

The page shows NRR in the Year to date column; the API also returns each month of the year so far
(`nrrTrend`).

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
  - the bridge: the December customers' MRR then, + expansion, − contraction, − churned, = their MRR now,
    then NRR and GRR;
  - the customers in each group, by name, with this year's new customers listed but not counted;
  - how many churned customers have a churn date in the CRM, and how many customers have contracts in another
    currency;
  - NRR and GRR month by month this year, each against the same December.
  - Source: Snowflake contracted MRR by customer and month (the same rows as the figure).
  - **Excel** (top of the window) downloads `lsports-nrr-<month>.xlsx` for review, e.g. by the CRO. The
    calculation is live formulas over the customer rows (`src/metrics/nrrWorkbook.ts`), so every step can be
    checked or changed:
    - **Summary**:
      - how it is calculated and the source;
      - the bridge (December MRR, expansion, contraction, churned, unchanged, MRR now), each step with its
        number of customers;
      - NRR, GRR and the figure on the page;
      - the new customers not counted;
      - the churned customers with a churn date in the CRM;
      - what is worth checking: an account that moved to another customer ID shows as churned and again as
        new, and a churned customer with no CRM churn date ended its contracts without being marked churned.
    - **Customers**: every customer, unchanged and new ones included, with:
      - its group, December and current MRR, the change, whether it counts and the amount kept for GRR;
      - its contracts' currency, its CRM churn date and its Snowflake customer ID.

      The sheet has a filter and totals.
    - **Month by month**: NRR and GRR for each month of the year so far.
- **Churn, last full quarter** and **year so far**: every opportunity marked churned in Snowflake
  (`DIM_OPPORTUNITY__FINANCE`) with its customer, churn month, currency and MRR, by quarter. Amounts in
  another currency are added as they are, as the figure does, and the window says so.
- **Churn, FY forecast months**:
  - the forecast's churn is a run-rate, not a list of customers: last full quarter's churned MRR ÷ 3,
    times each month's place in the forecast (it piles up);
  - month by month, with the Plan's hand-set months marked;
  - the quarter's churned customers shown as context.

The breakdown reads only when clicked, and is cached like the pack's reads.

## Saving

- Settings (the cloud cap) are shared by everyone who can open the page. A save reads the saved settings,
  merges its change and writes it back, so it never undoes another save made since the page loaded. They
  are stored in `data/metrics-settings.json` (git-ignored).
- Every save is appended to a `-history.jsonl` file next to them, with who and when.
- Saves are validated (ranges, known fields only), JSON only, same-origin, and carry finance-it's CSRF
  token (`server/json-store.cjs`, shared with the 2027 targets).
- The innovation envelope, the USD/EUR planning rate, FX conversions and the deposit tracker were
  removed from the page. Settings saved with the envelope or the rate, or with the FX Challenge's table
  (kept here while it was a tab), still load and save; those fields are ignored.

## How it is computed

- The two projections come from their cached handlers (`handler.current()`), so the page adds no
  NetSuite pull.
- The saved targets (`data/projection-targets.json`) are read on every request.
- ARR, churn, NRR (contracted MRR by customer) and employees are read once and cached for `METRICS_TTL_MIN`
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
```
