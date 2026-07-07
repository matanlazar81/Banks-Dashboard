# Backend: `/api/dividend-distributions` (serves ALL bank-role users)

Powers the **engine-level dividend exclusion**: the shared forecast engine strips the shareholder
dividend distribution out of Vendors/Other so the operating closing rises by that amount — for
**everyone**, and in the **nightly pushed `FORECAST_EUR`** too (screen == server). The owner also gets a
`⚖ Reconcile vs NS` panel that bridges the operating closing to the live NetSuite bank using the
dividend as the reconciling line, plus a personal "show actual" toggle to view the with-dividend
figures (view-only — never the pushed number).

## What it returns

`GET /api/dividend-distributions?year=YYYY&subsidiary=N` →
```json
{ "byMonth": { "2026-06": { "distributionEUR": -500112, "whtEUR": -90019, "totalEUR": -590131,
                            "distributionILS": -2035000, "whtILS": -366000, "totalILS": -2401000 } },
  "total":   { "distributionEUR": -500112, "whtEUR": -90019, "totalEUR": -590131,
               "distributionILS": -2035000, "whtILS": -366000, "totalILS": -2401000 } }
```
Amounts are **signed bank deltas** (negative = cash out), **amounts only — no payee names / PII**.
`distributionEUR/ILS` = shareholder payments (VendPymt/BillPmt; these otherwise land in the **Vendors**
bucket). `whtEUR/ILS` = withholding-tax journals (these otherwise land in **Other**). EUR is book 1
(primary/consolidation), ILS is book 2 (local) — the engine shifts both closings, so the ILS side must
be served too (older prod builds that returned EUR only degrade gracefully: the EUR closing is correct,
the ILS closing lags by the dividend until the ILS fields are served).

## How the engine consumes it (screen == server)

`computeCashflowForecast(inputs)` (shared `src/forecast/forecast-core.mjs`) takes a `dividendExclusions`
input `{ byMonth: { 'YYYY-MM': { distributionEUR, whtEUR, distributionILS, whtILS } } }`. After the row
loop it strips the dividend from the payment month (`vendors -= distribution`, `other -= wht`, `net +=
total`) and adds the total back as a **cumulative offset** to `openingBalance`/`closingBalance` for that
month and every later month — applied to the FINAL rows so it survives the current-month re-anchor to the
real prior-month-end bank. No-op when the input is absent (backward compatible + golden-safe).

- **Dashboard** (`src/App.tsx`): fetches this endpoint for the active company/year (all users), maps it
  to `dividendExclusions`, and passes it into the canonical `excludedForecast` memo → the whole dashboard
  + the persisted/pushed forecast exclude the dividend. The owner-only `actualForecast` memo runs the
  engine WITHOUT the exclusion; a "show actual" toggle swaps the display to it (persist/roll-forward/
  budget-sync always read `excludedForecast`, so the toggle never changes the pushed number).
- **Nightly compute** (`scripts/net-cash-forecast-compute.cjs`): fetches `ns.fetchDividendDistributions(year)`
  and passes it as `dividendExclusions` to the same engine → the pushed `FORECAST_EUR` excludes the
  dividend. Same code path as the browser ⇒ screen == server.

## Detection

`fetchDividendDistributions(year)` (shared `netsuite-api.cjs`) queries Bank/CredCard txns (subsidiary,
posted) for **both** book 1 (EUR) and book 2 (ILS) whose **memo** matches
`LOWER(memo) LIKE '%div%distribution%' OR '%dividend%'`, grouped by month and tx type (Journal → WHT;
else → distribution). Verified against JE8990 ("Div distribution 8.6.2026", €590,447 declared): the 6
shareholder VendPymts (€500,112) + WHT journal JE9031 (€90,019) all carry that memo. The
`%div%distribution%` pattern needs "div" before "distribution", so it will **not** catch unrelated memos
like "Distribution sales".

## Gating — NONE (all bank-role users)

Dev (`server/api-routes.cjs`): the route is **un-gated** — it serves every bank-role user, because the
dividend is now excluded from the forecast for everyone (the engine consumes these amounts). It returns
per-month **amounts only** (no payee names / PII), so there is no data-secrecy reason to restrict it. (It
was owner-gated in PR #156 when it only fed the owner's reconciliation panel; the gate was removed once
the exclusion became shared.) The `⚖ Reconcile vs NS` panel and the "show actual" toggle remain
owner-only UI (client-side, via the `/api/whoami` email) — but that is view convenience, not data
protection.

## Production twin (finance-it-backend `bankDashboardApi.ts`)

Prod does not auto-register new bank-dashboard routes (same as `/ns-bank-classified-yearly`,
`/sf-churn-analysis`). Add the route **un-gated to bank-role** (NOT owner-only — everyone needs it now),
then rebuild + restart:

```ts
router.get('/dividend-distributions', bankRole, async (req: Request, res: Response) => {
  try {
    const sub = parseInt(req.query.subsidiary as string || '3') || 3;
    const year = parseInt(req.query.year as string || '') || new Date().getFullYear();
    const ns = await netsuiteService.getSubsidiaryClient(sub);
    if (!ns.fetchDividendDistributions) { res.json({ byMonth: {}, error: 'not available' }); return; }
    res.json(await ns.fetchDividendDistributions(year));
  } catch (e: any) {
    logger.error(`[NS API] Dividend distributions failed: ${e.message}`);
    res.json({ byMonth: {}, error: e.message });
  }
});
```

`fetchDividendDistributions` (with the ILS book-2 sum) ships in the shared `netsuite-api.cjs`, so a backend
**rebuild + restart** picks up the method; only the route above needs adding. **If the route is absent in
prod, the dashboard fetch 404s → `dividendExclusions` is null → the forecast is UNCHANGED (no exclusion)
and the closing does NOT rise.** So the +€590K only appears in the browser once this route is live; the
nightly `net-cash-forecast-compute.cjs` (which calls the NS client directly, no HTTP) excludes it
regardless. Deploy the route so the screen matches the pushed number.

## Verify

- Any bank-role user: `curl -s "http://localhost:3001/api/dividend-distributions?year=2026&subsidiary=3"`
  → the June row above (no 403).
- `node scripts/net-cash-forecast-compute.cjs --dry-run` → December closing ≈ €590K **higher** than
  before the exclusion (dividend stripped from June Vendors/Other).
- Dashboard (any user, once the prod route is live): June Vendors −€500K, Other −€90K, closing + KR5
  Dec-closing +€590K. Owner: the `⚖ Reconcile vs NS` panel shows Operating closing − NS bank ≈ €590,131;
  the "show actual (incl. dividend)" toggle reverts the owner's screen to the with-dividend figures (the
  pushed number is unchanged by the toggle). Non-owner: a small "Operating (excl. dividend)" badge.
