# Salary Projection

Payroll by GL account, month by month: Snowflake actuals for the current year, and a projection for the
rest of the year and the next one. Each payroll account is **permanent** (recurring pay and its burden) or
**temporary** (one-offs and seasonal payments), and the two are projected differently.

- Dev: `http://localhost:5176/salary-projection.html` (`npm run dev`)
- Standalone server: `http://<server>:8790/salary-projection.html`
- finance-it: **Business Tools → Salary Projection** (`/business-tools/salary-projection`), an iframe of
  `salary-projection.html`

Page code: `src/salary-projection/`. Data call: `GET /api/salary-projection` (`server/salary-projection.cjs`).

## How it works

**Source.** Snowflake `FCT_EXPENSE__FINANCE` joined to `DIM_GL_ACCOUNT__FINANCE`, the same tables and
filters as the other Snowflake payroll reads. The filters are LSports (`SUBSIDIARY_ID = 3`), booked from
NetSuite (`SOURCE = 'netsuite'`) and every `76xxxx` account, which is the P&L Projection's Payroll line.
Amounts are the plain `AMOUNT_ILS` / `AMOUNT_EUR` columns (`fetchPayrollByAccount` in `snowflake-api.cjs`).
It reads January of the previous year through December of the current year.

**Actual months.** They run through the **last closed month**: the latest month before the current one
whose permanent payroll is at least half the largest of the last 12 months. A month Snowflake has only
partly loaded is projected instead, and a warning says so. The current month is always projected as a
whole month, because its postings are partial.

**Forecast months**
| Group | Rule |
|---|---|
| Permanent | The last closed month, carried flat into every forecast month of both years. |
| Temporary | The same month of the previous year. The current year's forecast months repeat last year's actuals. The next year repeats the current year, actual or forecast. |
| Not classified | Any other `76xxxx` account with postings. It is projected like a temporary account and listed in a warning. |

The projection is made in **₪**, the currency payroll is paid in. Forecast **€** = ₪ ÷ the ILS/EUR ratio
of the last closed month's permanent payroll; the page shows the rate. Actual months show Snowflake's own €.

**Classification.** From finance's file `ProfitandLoss988.xlsx` (yellow = permanent, green = temporary),
held in `SALARY_GL_GROUPS` in `server/salary-projection.cjs`:

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
| 760018 Gross-up expenses | |
| 760025 Salaries payments during military reserve | |

To move an account, edit `SALARY_GL_GROUPS`, run the tests and deploy. Every listed account shows, even
when it is zero.

**The page**
- KPIs: payroll year to date, full-year payroll for each year, and the permanent monthly run-rate.
- The table: each group's accounts and subtotal, then the total payroll.
- Controls: ILS (default) or EUR, one year or both, Refresh, and Export (Excel, the table as shown).
- Hovering an account shows how it is projected.

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
  also mounts `/api/salary-projection` behind the new `salary_projection` role (step 1 below).

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

Snowflake settings are the server's usual `SNOWFLAKE_*` variables.

## Checking it

```bash
node scripts/test-salary-projection.cjs          # synthetic: classification, last closed month, run-rate, same month last year, €, cache, UI model
node scripts/salary-projection.cjs --dry-run     # real data
node scripts/salary-projection.cjs --reconcile   # real data: Snowflake vs NetSuite P&L, payroll accounts, closed months
```

`scripts/fixtures/salary-projection-sample.json` is a synthetic payload for UI checks
(`node scripts/test-salary-projection.cjs --write-fixture`).

## Limitations

- LSports only, like the other projection pages.
- Permanent accounts stay flat at the last closed month. Planned hires, leavers and raises are not added.
  The P&L Projection's payroll forecast includes them, so the two pages can differ in forecast months.
- Temporary accounts repeat last year as it was. A one-off that will not recur (for example a year with
  unusually high military reserve refunds) repeats in the next year too.
- Salaries CAPEX (950000) is not a payroll account, so it is not on this page. The P&L Projection shows it
  as a separate line.
- Actuals are Snowflake's. Older code notes that Snowflake once missed some one-off accounts (760017,
  760019). `--reconcile` shows any account and month where Snowflake and NetSuite differ.
