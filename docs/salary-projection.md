# Salary Projection

Payroll by GL account, month by month: NetSuite actuals for the current year, and a projection for the
rest of the year and the next one. Each payroll account is **permanent** (recurring pay and its burden) or
**temporary** (one-offs and seasonal payments), and the two are projected differently.

- Dev: `http://localhost:5176/salary-projection.html` (`npm run dev`)
- Standalone server: `http://<server>:8790/salary-projection.html`
- finance-it: **Business Tools → Salary Projection** (`/business-tools/salary-projection`), an iframe of
  `salary-projection.html`

Page code: `src/salary-projection/`. Data call: `GET /api/salary-projection` (`server/salary-projection.cjs`).

## How it works

**Source.** NetSuite GL by account and **posting period** (`fetchPnlActuals` in `netsuite-api.cjs`). It
is the same read as the P&L Projection, and it equals NetSuite's Profit and Loss report. Filters: LSports
(subsidiary 3), every `76xxxx` account (the P&L Projection's Payroll line). ₪ comes from the ILS book and
€ from the primary book. It reads January of the previous year through December of the current year, in
one NetSuite call queued with the other dashboard calls.

Why not Snowflake: `FCT_EXPENSE` has no postings for 760017 (bonus) and 760019 (maternity) in 2026 (checked
October 2026). Every other account matched NetSuite exactly. `node scripts/salary-projection.cjs --reconcile`
compares the two by account and month, which shows when Snowflake is fixed.

**Actual months.** They run through the **last closed month**: the latest month before the current one
whose permanent payroll is at least half the largest of the last 12 months. A month not fully posted yet is
projected instead, and a warning says so. The current month is always projected as a whole month, because
its postings are partial.

**Forecast months**
| Accounts | Rule |
|---|---|
| Permanent | The last closed month, carried flat into every forecast month of both years. |
| Temporary | The current year's forecast months repeat the same month of the previous year (keeps the seasonality). The next year is 0: temporary payments are not projected beyond December. |
| 760023 Military reserve refund, 760029 Lieu of prior notice, 760008 Option employees expenses, 760014 Vacation expenses | Actuals only: forecast months are 0 (`ACTUALS_ONLY`). 2026's refunds and notice pay are not expected to recur; options and vacation are year-end accruals that change every year. |
| Not classified | Any other `76xxxx` account with postings and no choice yet. It is projected like a temporary account, and the page asks someone to pick. |

The projection is made in **₪**, the currency payroll is paid in. Forecast **€** = ₪ ÷ the ILS/EUR ratio
of the last closed month's permanent payroll; the page shows the rate. Actual months show NetSuite's own €.

**Classification.** The default is finance's file `ProfitandLoss988.xlsx` (yellow = permanent, green =
temporary), held in `SALARY_GL_GROUPS` and `ACTUALS_ONLY` in `server/salary-projection.cjs`:

| Permanent | Temporary |
|---|---|
| 760001 Gross Salaries | 760015 Recuperation expenses |
| 760002 National Insurance expenses | 760016 Employer' tax |
| 760003 Gemel expenses | 760017 One time payments (Bonus, grants) |
| 760004 Severance expenses | 760019 One time payments (Maternity leave benefit) |
| 760005 Zkufut Debit | 760023 Military reserve refund |
| 760006 Zkufut Credit | 760024 National insurance Damage fees |
| 760007 Education fund | 760029 Lieu of prior notice |
| 760009 Disability Insurance | 760030 Gemel Pension expenses |
| 760018 Gross-up expenses | 760020 Reducing salary expenses, option exercise |
| 760025 Salaries payments during military reserve | 76003 Social security, injury payment |
| | 760008 Option employees expenses |
| | 760014 Vacation expenses |

760020, 76003, 760008 and 760014 are not in the file; finance classified them as temporary (760008 and
760014 actuals only). Every listed account shows, even when it is zero.

**Changing it on the page.** **Permanent / temporary** in the page header opens a panel with every account
and three choices: Permanent, Temporary, Actuals only. The table and KPIs change at once (the payload carries
the actual months, and the page re-projects with `src/forecast/salary-projection.mjs`, the same code as the
server). **Save for everyone** stores the accounts that differ from the default in
`data/salary-classification.json` (a history line per save in `data/salary-classification-history.jsonl`,
with who saved it). **Defaults** goes back to finance's classification. Saving needs the page's role
(`salary_projection`) and goes through `GET/PUT /api/salary-projection/classification`. Changing the default
itself still means editing `SALARY_GL_GROUPS` / `ACTUALS_ONLY` and deploying.

**Planned changes.** Between permanent and temporary, the table shows the planned changes on **every payroll
account** (Snowflake's `IS_PAYROLL`, i.e. the 76xxxx accounts, as the old Bank Dashboard's salary overrides),
from January of the current year, from two Snowflake sources (`plannedLines` in `src/salary-projection/model.ts`):

1. **Hires, leavers and returns from leave**: Snowflake's HR-driven payroll forecast, `FCT_EXPENSE` sources
   `Open position` (planned hires), `Terminated` (leavers, negative) and `Leave end` (returns from leave), summed
   by month and account (`fetchPayrollChanges` in `snowflake-api.cjs`; no employee detail is read). One line per
   source summed across the accounts (**Planned hires**, **Leavers**, **Returns from leave**; hovering one names
   its accounts), then **Total headcount changes**. Each month's amount is what that month adds to the forecast.
   The forecast's own baseline (`payroll future baseline`) is not used: the page's baseline is NetSuite's last
   closed month.
   **Only changes marked TRUE count** (`IS_SOURCE_ACTUAL`, read with the rows). A change marked FALSE is not
   applied, in any month. A TRUE change in a closed month is history, so NetSuite's actuals already hold it. In
   October 2026 the data team said every row was FALSE, so no planned change counted; the page's note and
   `--dry-run` say how many rows are FALSE.
2. **The override file**: the override sheet's rows (`OVERIDE_TEMP`, loaded into `FCT_EXPENSE` as
   `future_cost_override` / `future_cost_increment`) on the same accounts (the sheet's column A)
   (`fetchPayrollOverrides`). One line per account and type, summing the account's rows of a month across
   departments; hovering a line shows its departments and comments. The load into `FCT_EXPENSE` currently drops
   the payroll accounts (checked October 2026: none on 760001 or 760017, while the sheet has rows on both), so
   these lines appear once the load includes them.
   - **Increments**, then **Total increments**.
   - **Overrides**, in italics, then **Total overrides (not in total payroll)**: for reference. An override
     replaces one department's budget for the account and month, and this page projects by account, so adding
     it would count that department twice.

How they count:
- **Total payroll adds the hires, leavers, returns from leave (marked TRUE) and increments** in the current and
  forecast months (`countedChangeRows` / `countedIncrementRows` in `src/forecast/salary-projection.mjs`). Closed
  months are NetSuite's actuals, which already include what happened, so a planned change there is neither shown
  nor added.
- The accounts' own lines and the Total permanent / Total temporary subtotals leave them out. With no permanent
  account, the planned changes come first.
- **KPIs**: full-year payroll adds them (it equals the table's FY Total payroll) and says how much; year to date
  and the run-rate are NetSuite's.
- Amounts are in the page's currency. When Snowflake has one currency only, the other is at the forecast rate
  (override rows with euros only are marked ≈).
- Both are read with the projection. If Snowflake fails, the page still loads: a warning says Total payroll
  leaves the hires and leavers out, and the override section says the file could not be read.
- `--dry-run` adds them to Total payroll the same way, and marks the override rows that count with `+`.
- **Clicking a figure** opens its breakdown, in the same movable window as the cash and P&L projections
  (`src/salary-projection/breakdown.ts`, built on the page from the payload, no server call):
  - **Planned hires / Leavers / Returns from leave:** by account (click one for its departments), and by
    department across the accounts. **Total headcount changes:** the same, by type and account.
  - **The override file's lines:** by department, hovering one shows the file's comments; a full year also by
    month. **Total increments / Total overrides:** by account and department.
  - Each window's first list adds up to the clicked figure. The hires and leavers are read by department for
    this (monthly totals, no employee detail).

**Payroll in the override file**, under the table: the counts, how many increments fall in closed months, and
the file's rows (collapsed): month, account, department, type (Override replaces the budget of that account and
month; Increment adds to it), amount and comment. Rows after December of the next year are only there, and a
note counts them.

**Deel** (people abroad), below Total payroll. Deel is booked in NetSuite as a vendor, not on the payroll
accounts, so it is read by vendor (`fetchVendorSpend` in `netsuite-api.cjs`):
- **The read.** Vendors whose name or ID contains "deel" (`DEEL_VENDOR`), LSports, by posting period, both books.
  It takes their GL lines on expense accounts: the vendor on the transaction (bills) or on the line (journals).
  It reads January of the previous year to December of the current one, in one call through the NetSuite queue.
- **Payroll accounts are left out.** Deel posted on a payroll account (76xxxx) is already in Total payroll; the
  Deel header names such accounts.
- **Lines.** One line per account (e.g. 660004 Contractors), then **Total Deel**, then **Payroll + Deel**
  (Total payroll + Total Deel). Total payroll itself is unchanged.
- **Actual months** are NetSuite's as booked.
- **Forecast months** use the run-rate (`deelRunRate` / `deelByMonth` in `src/forecast/salary-projection.mjs`,
  finance's choice in October 2026): the average of the last six months up to the last closed month
  (`DEEL_RUN_RATE_MONTHS`), or of the months since Deel's first bill when that is later.
  - A month without a bill counts as 0, so a bill booked a month late and the month that then holds two average
    out.
  - Each account gets its sum over the six months ÷ 6, so Deel moving to another account leaves the total right.
- **Clicking a Deel figure:**
  - An actual month shows it by vendor; Total Deel also shows it by account.
  - A forecast month shows the months it averages, each ÷ their number.
  - A year shows it by month; Total Deel also shows it by account.

**Headcount (HiBob)**, a table of its own under the payroll table (people, not money; the same columns and year
view). It holds counts only: no names or IDs leave the server.
- **The count is HiBob's employees only**, aligned with HiBob (finance's choice in October 2026). Every month comes
  from HiBob's employees in Snowflake and their start and leave dates (`DIM_EMPLOYEE`, read as the Metrics page
  reads them: `sfEmployeeSpans` and `employedOn` in `server/metrics.cjs`, company `METRICS_HEADCOUNT_COMPANY`,
  default LSports). Future dates count too, so forecast months already hold who is signed to start and who is
  leaving. Each month shows:
  - **Employees at month-end**: people employed on the last day, people on leave included.
  - **Hires**: people counted at this month-end but not the one before.
  - **Leavers**: the reverse.
- **Beside the count, not in it**, HiBob's planned events (`FCT_HEADCOUNT_EVENT`, counted by month, type,
  sub-type, employee and opening status in `fetchHeadcountPlan`; `headcountSeries` in
  `src/forecast/salary-projection.mjs`):
  - **Open positions (not counted)**: an increase with no employee, in the month it is planned for, from the
    current month on. Openings planned for earlier months whose status starts with "Open" show in the current
    month, as the old dashboard carries them.
  - **Planned leavers, no employee (not counted)**: a decrease with no employee.
  - **Returns from leave and leave starts**: a sub-type about leave (e.g. `Leave end`, maternity). People on leave
    are employees, so the count does not move.
  - **Events of a known employee** (e.g. `Hired`, `Terminated`) are left out: their dates are already in HiBob's
    employees.
- What the data held in October 2026: `FCT_HEADCOUNT_EVENT` had only future hires (8 open positions, October 2026
  to January 2027, and 1 signed hire), with no leavers and no leave events.
- **The FY column** shows employees in December, and the other lines summed.
- **Clicking a figure** opens its breakdown in people, in the same movable window as the payroll figures
  (`headcountBreakdown` in `src/salary-projection/breakdown.ts`):
  - **Employees, hires and leavers:** by HiBob group (department) and by employment type. A full year shows hires
    and leavers by month first, and employees as at December.
  - **Open positions:** by department (hovering one lists its roles) and by role. The other HiBob events are shown
    by department.
  - The server sends counts by group and employment type only (`DIM_EMPLOYEE.EMPLOYEE_GROUP`), plus each open
    position's department and role (`FCT_HEADCOUNT_EVENT`). It sends no names or IDs.
- **The KPI** "Headcount today" shows HiBob's employees today and in December by their dates.
- **Money:** the planned hires, leavers and returns from leave in Total payroll come from Snowflake's payroll
  forecast (above), and only those marked TRUE count. Leave starts have no money in that forecast.

**The page**
- KPIs: payroll year to date, full-year payroll for each year (with the planned changes), the permanent
  monthly run-rate, and the headcount today (HiBob).
- The table: sections, each with a header row and a subtotal: **Permanent**, **Headcount changes** (hires,
  leavers, returns from leave; counted), **Override file** (increments counted, overrides for reference),
  **Temporary** and, when there are any, **Not classified**; then the total payroll; then **Deel** and
  **Payroll + Deel**. Hovering a header says how its section is projected. The Excel export keeps the headers as
  label-only rows, and adds the headcount under the table.
- **Section colours.** Each section has its own colour: Permanent blue, Headcount changes amber, Override file
  violet, Temporary green, Not classified grey, Deel pink. The header row is a band of that colour, and a bar of it
  runs beside each line's label (stronger on the subtotal). Total payroll and Payroll + Deel stay neutral. A legend
  above the table names the sections shown. The Excel export colours the header rows and the line labels the same
  way. The colours are an optional `accent` on the shared table's lines, so the other pages are unchanged.
- Under the table: the headcount by month (above), then the override file's rows (above).
- **Charts** (the Table / Charts switch, left of Permanent / temporary; `?tab=charts` opens it) show the same
  figures in place of the tables:
  - **Payroll by month:** stacked columns by section in the sections' colours, Deel on top, so a column is
    Payroll + Deel. Projected months (the current one and the forecast) are faded. Only Total payroll's parts
    are drawn: the override file's overrides are not.
  - **Employees at month-end:** HiBob's count as a line, dashed in projected months.
  - **Hires and leavers:** hires above zero, leavers below.
  - Hovering a month lists its figures; open positions show there, not counted.
  - The charts are read off the tables (`src/salary-projection/charts.ts`), so they follow the currency and
    years chosen. `SalaryCharts.tsx` draws them with recharts, loaded only when the Charts view opens.
  - The chart colours are the sections' at the step that keeps neighbouring sections apart for colour-blind
    readers. Not classified is a deliberate grey.
- Controls: Table / Charts, Permanent / temporary (the classification panel), ILS (default) or EUR, one year or
  both, Refresh, and Export (Excel, the table as shown).
- Hovering an account shows how it is projected; clicking a planned-change or Deel figure opens its breakdown
  (above).

Responses, caching and refresh work exactly like the P&L Projection (`createProjectionHandler` in
`server/cash-projection.cjs`). The cache file is `data/salary-projection-cache.json`. The page has no plan,
so a change of `NET_CASH_SCENARIO_NAME` does not invalidate it.

## Deploying

On the server, in the bank-dashboard checkout that finance-it uses
(`/home/ubuntu/finance-it/extra-apps/bank-dashboard`):

```bash
git pull --ff-only origin master && npm ci
node scripts/test-salary-projection.cjs           # synthetic checks, no external calls
npm run build                                     # builds dist/salary-projection.html as a fifth page
node scripts/salary-projection.cjs --dry-run      # real data: both years by account, ₪ thousands
node scripts/salary-projection.cjs --reconcile    # real data: Snowflake vs NetSuite by account and month
node scripts/salary-projection.cjs --write-cache  # optional: prime the cache
pm2 restart finance-it-backend                    # server code changed: Pull & Build alone is not enough
```

### finance-it backend

- If finance-it-backend mounts the shared API (`docs/backend-bank-dashboard-api.ts`), the route is already
  included.
- Otherwise update `src/routes/cash-projection.ts` from `docs/backend-cash-projection-route.ts`. It now
  also mounts `/api/salary-projection` and `GET/PUT /api/salary-projection/classification` behind the new
  `salary_projection` role (step 1 below).

### finance-it: sidebar, role and route (manual, not in this repository)

finance-it is not in this repository, so this part is a short manual change there, the same way Metrics
and P&L Projection were added (`docs/metrics.md`, `docs/pnl-projection.md`).

1. **Role and sidebar item**, in `shared/src/types`:
   - Add `SALARY_PROJECTION = 'salary_projection'` to `UserRole`.
   - Add a `BUSINESS_TOOLS` entry **Salary Projection** → `/business-tools/salary-projection` with that
     role, right after **P&L Projection**. This one list drives the sidebar item, the Users Management
     checkbox and the hub entry.
   - If user roles are stored as a Postgres enum or checked by a constraint, add a migration for the new
     value, like the earlier Business Tools roles.
2. **Hide Bank Commission JE**: remove (or comment out) its entry in the same `BUSINESS_TOOLS` list. The
   sidebar item, checkbox and hub tile disappear. Its route, role and backend can stay, so nothing else
   breaks and it can come back by restoring the entry.
3. **Frontend route** `/business-tools/salary-projection`: the same iframe component as the other pages,
   with `page="salary-projection.html"`. Keep the sandbox attributes and `allow-downloads` (Export).
4. **Deploy**: Pull & Build (it copies `dist/salary-projection.html` with the other pages), then
   `pm2 restart finance-it-backend`.
5. **Access**: tick **Salary Projection** for the users who need it in Users Management. Admins always see it.

### Keeping it warm

```
14 6 * * *  cd /home/ubuntu/finance-it/extra-apps/bank-dashboard && node scripts/salary-projection.cjs --write-cache >> data/salary-projection-cron.log 2>&1
```

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `SALARY_PROJECTION_TTL_MIN` | `CASH_PROJECTION_TTL_MIN` (30) | Minutes before cached figures are refreshed in the background |
| `SALARY_PROJECTION_TIMEOUT_MIN` | `CASH_PROJECTION_TIMEOUT_MIN` (10) | A computation running longer is abandoned |
| `SALARY_PROJECTION_PREWARM` | `CASH_PROJECTION_PREWARM` | `1` = compute 30 s after the server starts if the cache is empty |

NetSuite settings are the server's usual `NETSUITE_*` variables (`NET_CASH_SUBSIDIARY`, default 3); NetSuite
also gives Deel. Snowflake (`SNOWFLAKE_*`) is used for the planned changes (hires, leavers, returns from leave,
the override file), the headcount (HiBob; `METRICS_HEADCOUNT_COMPANY`, default LSports, shared with the Metrics
page) and by `--reconcile`.

## Checking it

```bash
node scripts/test-salary-projection.cjs          # synthetic: classification, last closed month, run-rate, same month last year, actuals only, €, cache, UI model, planned changes, Deel, headcount
node scripts/salary-projection.cjs --dry-run     # real data: also Deel's vendors, accounts and run-rate months, the headcount by month, and HiBob's event types with the line each counts in
node scripts/salary-projection.cjs --reconcile   # real data: Snowflake vs NetSuite P&L, payroll accounts, closed months
```

`scripts/fixtures/salary-projection-sample.json` is a synthetic payload for UI checks
(`node scripts/test-salary-projection.cjs --write-fixture`).

## Limitations

- LSports only, like the other projection pages.
- Permanent accounts stay flat at the last closed month. Planned hires, leavers and returns from leave (and the
  override file's increments) on every payroll account count in Total payroll, not in the account lines. The
  override file's overrides are not applied. Some override-file rows describe the same events as the hires and
  leavers (e.g. a termination already in HiBob), and both count. The P&L Projection's payroll forecast works
  differently, so the two pages can differ in forecast months.
- Temporary accounts repeat last year as it was for the rest of the current year (a bonus month repeats in the
  same month) and are 0 in the next year. Accounts that should not repeat at all are "Actuals only" (by default
  760023, 760029, 760008 and 760014).
- Salaries CAPEX (950000) is not a payroll account, so it is not on this page. The P&L Projection shows it
  as a separate line.
- Actuals are NetSuite's by posting period, so a late posting into a closed period appears on the next
  refresh.
- Deel is found by vendor name ("deel"); a Deel vendor named otherwise is missed. The vendors found are named
  in the Deel header and by `--dry-run`. Only LSports (subsidiary 3) is read.
- The headcount is HiBob's employees and their dates: a signed hire who is not in HiBob's employees yet does not
  count (an opening for them shows on the open positions line). Openings carry no company, so all show.
  `--dry-run` shows today's headcount next to the current month-end, and each event type with how it shows.
- Leaves (maternity) are counted only when HiBob's events have them; in October 2026 they had none.
