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

**NRR**: for the customers who had revenue 12 months earlier, their revenue this month ÷ their revenue
then. Customers new since then are not counted. **GRR** caps each customer at their old amount, so it
measures only what was kept. Source: Snowflake revenue by customer and month (actual months, test
customers excluded). The page shows the last closed month; the API also returns the last 6 months.

## Cloud against the cap

- **Cloud** = NetSuite accounts 640xxx (Cloud Infrastructure & DevOps) in closed months. In forecast
  months it is the budget's share of that category in the month's operating expenses. Next year uses
  the same split as the 2027 targets.
- **Cap** = cap % (default 8%) × projected FY total revenue (actual + forecast months).
- Shown for both years: cloud, cap, headroom (negative when over) and cloud as a % of revenue.
- The cap % and the category are settings on the page.

## Innovation envelope

An amount for a year, spread evenly over the months from a start month to December. It only applies
to forecast months: a closed month keeps what was booked.

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

Until finance-it has a separate Metrics permission, the page uses the Bank Dashboard permission.

## Checking it

```bash
node scripts/test-metrics.cjs   # synthetic: every pack item, NRR, cloud cap, envelope, FX, deposits, handlers, export rows
```
