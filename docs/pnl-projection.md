# P&L Projection

The New Bank Dashboard's projection on an **accrual (P&L) basis**, with the **accumulated net
profit**: actuals + forecast for the current year, rolled forward into the next year, for LSports.
It appears under **Business Tools → P&L Projection**, right below New Bank Dashboard in finance-it.

Same design as the New Bank Dashboard ([new-bank-dashboard.md](new-bank-dashboard.md)): one request
(`GET /api/pnl-projection`), a server-side computation cached on disk and refreshed in the background,
a small page bundle that never loads `App.tsx`, and a movable breakdown window for every figure.

## URLs

| Where | URL |
|---|---|
| Dev (`npm run dev`) | `http://localhost:5176/pnl-projection.html` |
| Standalone server (`node server.cjs`) | `http://<server>:8790/pnl-projection.html` |
| finance-it | sidebar item → iframe of `<bank-dashboard static base>/pnl-projection.html` (see below) |

View options are kept in the URL: `?plan=base`, `?ccy=ils`, `?years=current|next`.

## How it works

```
browser ──GET /api/pnl-projection──► server/pnl-projection.cjs
                                       ├─ loadProjectionInputs()    server/projection-inputs.cjs: the cash projection's inputs
                                       │                            (one NetSuite/Snowflake pull serves both pages)
                                       ├─ ns.fetchPnlActuals()      NetSuite GL by account and month, both books
                                       ├─ sf.fetchPnlBudgetExtras() Snowflake FCT_BUDGET: depreciation, tax
                                       ├─ computeCashflowForecast() src/forecast/forecast-core.mjs → current year
                                       ├─ buildNextYearInputs()     src/forecast/roll-forward.mjs   → next year
                                       └─ cache: data/pnl-projection-cache.json
```

- **Actual months** (every month before the current one) are NetSuite's P&L, account by account,
  mapped to lines by `server/pnl-lines.cjs`.
- **The current month** is projected as a whole month: its partial postings are not a month's P&L.
  The 1st-of-month reversal of the FX revaluation, for example, would otherwise show as a loss.
- **Forecast months** run the cash projection's engine on the same inputs and plan. The current month
  is treated as a forecast month, and there is no collection rate, because revenue is recognised, not
  collected.
- **Variants, caching, refresh, polling** work exactly as on the New Bank Dashboard
  (`createProjectionHandler` in `server/cash-projection.cjs`).

## The lines

Display signs follow the cash page:
- revenue lines are positive;
- costs are shown positive and subtracted, so the Salaries CAPEX credit appears in parentheses;
- lines below EBITDA are shown as their effect on profit, so a cost is in parentheses.

| Line | NetSuite accounts (actual months) | Forecast, current year | Next year |
|---|---|---|---|
| Accumulated profit, opening | – | Previous month's closing; 0 on 1 January | December's closing (↩), no reset |
| Customer revenue | 400001/2, 400008–11, 400018, 400026, 410xxx, 4000, 4050 | Snowflake monthly revenue + open deals at the plan's minimum probability (the cash Collections formula at 100%) | Oct–Dec run-rate (revenue + pipeline − churn), flat, + open deals |
| Pipeline | – | Cumulative new MRR × calibration factor, from the current month, × pipeline % | – |
| Churn | – | Run-rate × forecast months | – |
| Other & intercompany revenue | Every other Income account (Statscore/Bringits I/C, sub-lease, asset sales, 400019, 660010) | Average of the last 3 closed months | Same |
| **Total revenue** | = "Sales" of the EBITDA report | | |
| Payroll | 76xxxx | Cash engine salary: last closed payroll month by department + hires/leavers/overrides + plan | Oct–Dec run-rate by department + plan |
| Salaries CAPEX | 950000 (a credit) | The last closed month with an amount, flat | Same |
| Operating expenses | 6xxxxx–7xxxxx except payroll, depreciation and 780030; 800011; COGS | Cash engine vendors: FCT_BUDGET + overrides + plan vendor % | This year mirrored month by month |
| **Total operating costs** | = "Overheads" of the EBITDA report | | |
| **Operating profit (EBITDA)** | = "Operating Profit" of the EBITDA report | | |
| FX revaluation | 800028–800031 | Currency-defense budget × defense % | – (as on the cash page) |
| Finance, net | Other 800xxx, 190002, 900002/3 | Average of the last 3 closed months | Same |
| Depreciation | 7805xx | FCT_BUDGET when the year has a depreciation budget, else the average of the last 3 closed months (it is posted monthly) | Same rule |
| Tax & other | 9xxxxx, 780030, 180018, anything else | FCT_BUDGET when the year has one, else none | Same rule |
| **Net profit** | Sum of every P&L account | | |
| Add back: depreciation / finance, net / FX revaluation / tax & other | Each line below EBITDA, reversed | | |
| **EBITDA (from net profit)** | Net profit + the four add-backs; equals Operating profit (EBITDA) | | |
| Accumulated profit, closing | Opening + net profit | | |

The bridge after Net profit shows how net profit comes back to EBITDA, one line per item outside
EBITDA. A cost is added back (positive), an income is taken out (in parentheses). Its last line always
equals Operating profit (EBITDA) above; the tests check this for every month and full year.

`scripts/test-pnl-projection.cjs` checks that every account lands on exactly one line, so net profit
can never miss an account.

## Equal to NetSuite

- Actual months are not modelled: each line is the sum of its NetSuite accounts, so lines, EBITDA and
  net profit equal NetSuite by construction (€: primary book, ₪: ILS book).
- Months are **accounting (posting) periods**, as in NetSuite's Profit and Loss report (subsidiary
  LSports Data). Checked against that report for Jan–Sep 2026: Sales, Payroll, Depreciation, CAPEX and
  Other Expenses match it to the cent. `PNL_NS_DATE_BASIS=trandate` switches to transaction dates.
- The page's **Operating profit (EBITDA)** excludes depreciation and finance. The Profit and Loss
  report's "Operating Profit" includes them (it equals the page's EBITDA + Finance, net + Depreciation);
  **Net profit** is the same figure on both.
- Expense reports must be visible to the NetSuite login the server uses. A role that cannot see them
  leaves out employee expenses (per diem, taxi, hotels, …), a few thousand euros a month.
- To check a month against the report, run `node scripts/pnl-projection.cjs --reconcile` on the
  server. It prints Sales, Overheads, the lines below them and Net profit for every closed month, by
  posting period and by transaction date.
- The cell breakdown of an actual cost line also shows Snowflake's FCT_EXPENSE for the same accounts,
  with the difference to NetSuite, as a check of the Snowflake data.

## Breakdown of a cell

Click an underlined figure: any line except the subtotals, any month, or the FY column. The window is
the New Bank Dashboard's (`GET /api/pnl-projection/breakdown`, `server/pnl-projection-breakdown.cjs`),
and every breakdown adds up to the cell.

| Line | Actual months | Forecast months |
|---|---|---|
| Revenue lines | NetSuite accounts | Expected revenue + open deals; the next year's run-rate; the 3 months of the average |
| Pipeline / Churn | – | Cohorts by month + pipeline %; run-rate × months |
| Payroll | NetSuite accounts + Snowflake check | Basis month by department + hires/leavers/overrides + plan changes |
| Salaries CAPEX | NetSuite 950000 + Snowflake check | The month carried flat |
| Operating expenses | NetSuite accounts + Snowflake check | Vendor budget by account, grouped by category + overrides + plan changes; the next year: the same month's NetSuite accounts (its budget by account while that month is not closed) |
| FX, Finance, Depreciation, Tax | NetSuite accounts + Snowflake check | Defense budget × %; the months of the average; budget by account |

**Account numbers link to NetSuite.** On a row that is a NetSuite account, the account number opens
the account's register in NetSuite (subsidiary LSports Data) in a new tab. The register covers:
- an actual month: that month;
- the FY column: the year's closed months;
- the next year's operating expenses: the month they mirror;
- Salaries CAPEX: the month carried flat;
- any other forecast month: the last 3 closed months.

The links need `NETSUITE_ACCOUNT_ID` in the server's `.env`, which is already there for the NetSuite
API. The account's NetSuite internal id is read with the actuals. A projection cached before this
change shows account numbers without links until the next refresh.

## Deploying

Together with the New Bank Dashboard, on the server in the bank-dashboard checkout that finance-it
uses (`/home/ubuntu/finance-it/extra-apps/bank-dashboard`):

```bash
git pull --ff-only origin master && npm ci
node scripts/test-pnl-projection.cjs            # synthetic checks, no external calls
node scripts/test-cash-projection.cjs
npm run build                                   # builds dist/pnl-projection.html as a third page
node scripts/pnl-projection.cjs --reconcile     # optional: compare with NetSuite's EBITDA P&L
node scripts/pnl-projection.cjs --write-cache   # optional: prime the cache
pm2 restart finance-it-backend                  # server code changed: Pull & Build alone is not enough
```

### finance-it backend

- If finance-it-backend mounts the shared API (`docs/backend-bank-dashboard-api.ts`), both routes are
  already included.
- Otherwise update `src/routes/cash-projection.ts` from `docs/backend-cash-projection-route.ts`. It now
  also mounts `/api/pnl-projection` and `/api/pnl-projection/breakdown`, behind the Bank Dashboard
  role.

### finance-it frontend: the sidebar entry

A short manual change in finance-it (not in this repository):

1. Under **Business Tools**, add **P&L Projection** right after **New Bank Dashboard**, linking to
   `/business-tools/pnl-projection`.
2. That route renders the same iframe component, with `src` pointing at `pnl-projection.html` in the
   same static location as `new-bank-dashboard.html`. Keep `allow-downloads` for Export.
3. Make sure the deploy copies `dist/pnl-projection.html` with the other pages and `dist/assets/`.

### Keeping it warm

```
12 6 * * *  cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && node scripts/pnl-projection.cjs --write-cache >> data/pnl-projection-cron.log 2>&1
```

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `PNL_NS_DATE_BASIS` | `period` | `trandate`: NetSuite actuals by transaction date instead of posting period |
| `PNL_PROJECTION_TTL_MIN` | `CASH_PROJECTION_TTL_MIN` (30) | Minutes before cached figures are refreshed in the background |
| `PNL_PROJECTION_TIMEOUT_MIN` | `CASH_PROJECTION_TIMEOUT_MIN` (10) | A computation running longer is abandoned |
| `PNL_PROJECTION_PREWARM` | `CASH_PROJECTION_PREWARM` | `1` = compute 30 s after the server starts if the cache is empty |
| `PROJECTION_INPUTS_REUSE_MIN` | `5` | Minutes one input pull is shared by the two pages (`0` = never) |
| `NET_CASH_SCENARIO_NAME` | `Exit plan June26` | The official plan shown as *Plan* (shared) |

## Checking it

```bash
node scripts/test-pnl-projection.cjs          # synthetic: mapping, NetSuite actuals, engine parity, roll-forward, cache, breakdowns, UI model
node scripts/pnl-projection.cjs --dry-run     # real data: both years, Plan and Base
node scripts/pnl-projection.cjs --reconcile   # real data: NetSuite P&L by month, both date bases
```

`scripts/fixtures/pnl-projection-sample.json` is a synthetic payload for UI checks.

## Limitations

- LSports only, like the New Bank Dashboard.
- After 1 January the window moves to the new year + the next one, and the accumulated profit starts
  again from that January.
- The month just closed counts as actual from the 1st of the next month. Late NetSuite postings
  (accruals, depreciation, revaluation) appear on the next refresh.
- Next-year revenue keeps the cash page's open-deal rule: deals at or above the minimum probability are
  added on top of the Oct–Dec run-rate, which can already contain the ones closing in the current year.
