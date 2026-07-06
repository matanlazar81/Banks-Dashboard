# Backend patch — return quarterly MRR churn on `/api/sf-churn-analysis`

**Where:** `finance-it-backend/src/routes/bankDashboardApi.ts` (the production API host; external to
this repo). Recorded here so it survives a backend rebuild-from-scratch.

## Why

The cashflow forecast engine prefers the **latest-completed-quarter** churn run-rate: it uses
`sfChurnQuarterly` when present, else falls back to the **yearly** figure. The dashboard populates
`sfChurnQuarterly` from the `quarterly` field of the `/api/sf-churn-analysis` response
(`App.tsx` does `if (Array.isArray(churnR.quarterly)) setSfChurnQuarterly(churnR.quarterly)`).

The dev Vite server returns `quarterly`, but the **production** `bankDashboardApi.ts` route did not —
so on prod the dashboard fell back to the yearly churn (~€131K/mo) while the server-side compute
(`scripts/net-cash-forecast-compute.cjs`, which calls `sf.fetchQuarterlyChurnMRR()` directly) used the
quarterly (~€114.7K/mo). The dashboard and the pushed `FORECAST_EUR` therefore disagreed by ~€250K
(Dec €10.20M vs €10.45M) even though everything else matched. This aligns them.

## Fix

In the `router.get('/sf-churn-analysis', …)` handler, replace the single success line:

```ts
    const result = await sf.fetchChurnAnalysis();
    res.json({ data: result.yearly || result, recentMonthlyAvg: result.recentMonthlyAvg || 0 });
```

with (adds `quarterly`, guarded so a client without the method can't break the route):

```ts
    const result = await sf.fetchChurnAnalysis();
    // Also return quarterly MRR churn so the dashboard uses the latest-completed-quarter run-rate,
    // matching the server-side net-cash compute. Guarded: if unavailable, dashboard keeps yearly.
    let quarterly: any[] = [];
    try {
      const sfAny = sf as any;
      if (typeof sfAny.fetchQuarterlyChurnMRR === 'function') quarterly = await sfAny.fetchQuarterlyChurnMRR();
    } catch (qe: any) {
      logger.error(`[Banks API SF] Quarterly churn failed: ${qe.message}`);
    }
    res.json({ data: result.yearly || result, recentMonthlyAvg: result.recentMonthlyAvg || 0, quarterly });
```

Then rebuild + restart the backend:

```bash
cd /home/ubuntu/finance-it/backend && npm run build && pm2 restart finance-it-backend
```

## Effect

No frontend rebuild needed — `App.tsx` already reads `.quarterly`, so the dashboard switches to the
quarterly churn run-rate automatically and matches the server compute. Guarded: if the Snowflake
client lacks `fetchQuarterlyChurnMRR`, `quarterly: []` and the dashboard keeps using the yearly figure
(no breakage).

**Verified 2026-07-06:** after the patch + backend rebuild + a hard-refresh, the dashboard churn
header reads `Q2 2026: €114,683/mo` (per-month €114,683 × N), matching the server-side compute.
