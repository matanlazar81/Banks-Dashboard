# Keep bank-classified fresh so closed-month reconciliation stays correct

**Related:** the forecast engine (`src/forecast/forecast-core.mjs`) now drives a **closed month's**
Salary/Vendors/Collections/Other from the **bank-classified feed (`bcm`)** when that month is complete,
so the month's closing equals the actual NS bank delta (see the "Closed-month reconciliation" block).
This note is about keeping `bcm` fresh.

## Where `bcm` comes from

- **Server nightly compute** (`scripts/net-cash-forecast-compute.cjs`): fetches `bcm` **live**
  (`ns.fetchBankClassifiedYearly(year)`), seed only as a fallback. So the **pushed `FORECAST_EUR`
  already reconciles every closed month** (June included) — **no action needed for the server.**
- **Dashboard** (`src/App.tsx`): tries `GET /api/ns-bank-classified-yearly`, and **falls back to the
  committed seed** `src/seeds/bank-classified-lsports-2026.json` when that route 404s. Production
  currently does **not** serve the route, so the dashboard uses the seed.

## The problem

The committed seed was generated **before June closed** (2026-07-01). Its June is incomplete —
`salary 0`, `collections ~590K` (vs ~5M), a phantom one-sided `reval −2.53M`. The engine's guard
(`bcmCashValid`, material salary) correctly makes **June fall back to the accrual feeds** on the
dashboard, while **Jan–May reconcile** (their seed months are complete). Net effect on the dashboard:
Jan–May closings now sit on the actual bank; **June is still ~€0.87M high** until the seed is refreshed
or the live route is served.

## Fix A — serve the live route in production (durable, recommended)

The shared API module already defines it: `server/api-routes.cjs` →
`GET /api/ns-bank-classified-yearly` (calls `ns.fetchBankClassifiedYearly` live). Have the parent
`finance-it-backend` mount the shared bank-dashboard API routes (see `docs/standalone-server.md` /
`docs/backend-bank-dashboard-api.ts`). Once served, the dashboard fetches **live** `bcm`, every closed
month reconciles, and the seed reverts to a cold-start-only bootstrap. This also removes the whole
class of "prod doesn't serve a new route" gaps (same root cause as the churn `quarterly` fix).

## Fix B — refresh the committed seed (stopgap, until Fix A ships)

Run the **canonical** generator on the server (it needs NS creds in `.env`), then commit the result:

```bash
node scripts/generate-bank-classified.cjs lsports 2026
# → writes src/seeds/bank-classified-lsports-2026.json (now with a complete June)
git add src/seeds/bank-classified-lsports-2026.json && git commit -m "chore(seed): refresh bank-classified 2026 (complete June)"
```

Use **only** this script — never hand-edit the seed. It is the same classifier the live route and the
server compute use, so the regenerated seed matches them exactly (no screen-vs-server drift).

## Verify

Dashboard hard-refresh → **June closing ≈ €5.37M** (the NS book-1 bank at Jun-30), and past
Salary/Vendors columns show **cash-paid** (bank) figures for the reconciled months.
