# Forecast-core golden test

`src/forecast/forecast-core.mjs` is the single source of truth for the cashflow
forecast. It was extracted 1:1 from the `cashflowForecast` memo in `src/App.tsx`
so that BOTH the browser and the nightly server-side job (which has no browser)
compute the exact same numbers. The golden test guards that faithfulness — and
guards against future drift.

Run it any time:

```bash
node scripts/test-forecast-core.cjs
```

It has two modes.

## Smoke mode (always runs, no data needed)

A synthetic-but-representative `inputs` object exercises every branch — past
actuals, the current-month anchor + mid-month proration, future budget, the
"Last Actual" per-department projection, the pipeline methodology, cumulative
churn, currency-defense reval, and per-month scenario adjustments. It asserts
the engine returns 12 finite rows, the `opening + net + reval = closing`
cascade holds, opening chains from the prior closing, and the function is
deterministic. This proves the module loads and runs anywhere (CI, a fresh
clone) even with zero NetSuite/Snowflake access.

## Golden mode (the real €0-diff fidelity gate)

Golden mode compares the engine's output against numbers the **live dashboard**
actually displayed, month by month, and fails on any nonzero diff. It runs only
when a fixture exists at `scripts/fixtures/forecast-golden.json`:

```json
{ "inputs": { "...ForecastInputs, with `nowISO` (string) instead of `now`": true },
  "expected": [ /* the 12 ForecastRow objects the dashboard showed */ ] }
```

### How to capture the fixture

The memo exposes a capture hook. It is always on in a dev build; on the
production dashboard, opt in by appending `?fccapture=1` to the URL.

1. Open the dashboard on **Exit plan June26** with basis **Revenue: Pipeline**
   and **Salary: Last Actual** (the same basis the nightly job uses), and let it
   fully load. On production, add `?fccapture=1` to the URL (e.g.
   `…/bank-dashboard/?fccapture=1`) so the hook is active.
2. Open the browser console and run:

   ```js
   copy(JSON.stringify({
     inputs: { ...window.__fcInputs, now: undefined,
               nowISO: (window.__fcInputs.now || new Date()).toISOString() },
     expected: window.__fcRows,
   }));
   ```

   (`window.__fcInputs` / `window.__fcRows` are set by the memo in dev builds.)
3. Paste into `scripts/fixtures/forecast-golden.json`.
4. Run `node scripts/test-forecast-core.cjs`. Golden mode should report
   **"all 12 months match the captured dashboard values (€0 diff)"**.

Re-capture whenever the underlying data or the "Exit plan June26" scenario
changes materially; the fixture is a point-in-time lock, not a live feed.

> The fixture may contain internal finance figures — keep it out of any
> customer-facing artifact. It is git-ignored by default (see
> `scripts/fixtures/README.md`).
