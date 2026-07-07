# Backend: owner-only `/api/dividend-distributions`

Powers the dashboard's **owner-only "⚖ Reconcile vs NS"** panel — it strips the dividend distribution
out of the operating closing and shows it as the reconciling line vs the NetSuite bank.

## What it returns

`GET /api/dividend-distributions?year=YYYY&subsidiary=N` →
```json
{ "byMonth": { "2026-06": { "distributionEUR": -500112, "whtEUR": -90019, "totalEUR": -590131 } },
  "total":   { "distributionEUR": -500112, "whtEUR": -90019, "totalEUR": -590131 } }
```
Amounts are **signed bank deltas** (negative = cash out). `distributionEUR` = shareholder payments
(VendPymt/BillPmt; these otherwise land in the **Vendors** bucket). `whtEUR` = withholding-tax journals
(these otherwise land in **Other**).

## Detection

The NS client method `fetchDividendDistributions(year)` (in the shared `netsuite-api.cjs`) queries
Bank/CredCard txns (subsidiary, EUR primary book, posted) whose **memo** matches
`LOWER(memo) LIKE '%div%distribution%' OR '%dividend%'`. Verified against JE8990 ("Div distribution
8.6.2026", €590,447 declared): the 6 shareholder VendPymts (€500,112) + WHT journal JE9031 (€90,019)
all carry that memo. The `%div%distribution%` pattern needs "div" before "distribution", so it will
**not** catch unrelated memos like "Distribution sales".

## Gating (owner only)

Dev (`server/api-routes.cjs`): gated with `getUserEmail(req)` + `canUserSync(email)` — the
`SYNC_ALLOWLIST` env (default `matan.l@lsports.eu`), the same enforcement as
`POST /api/sync-budget-targets`. Non-owners get **403**. The button is also hidden client-side via the
`/api/whoami` email, but the server gate is the real enforcement.

## Production twin (finance-it-backend `bankDashboardApi.ts`)

Prod does not auto-register new bank-dashboard routes (same as `/ns-bank-classified-yearly`,
`/sf-churn-analysis`). Add the route, gated to the owner, then rebuild + restart:

```ts
router.get('/dividend-distributions', bankRole, async (req: Request, res: Response) => {
  try {
    const email = getUserEmailFromReq(req);            // parent injects user; lowercase it
    if (!isOwnerAllowlisted(email)) { res.status(403).json({ error: 'forbidden', byMonth: {} }); return; }
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

`fetchDividendDistributions` ships in the shared `netsuite-api.cjs`, so a backend **rebuild + restart**
picks up the method; only the route above needs adding. If the route is absent in prod, the button's
fetch 404s and the panel simply shows €0 dividend (degrades gracefully).

## Verify

- As the owner: `curl -s "http://localhost:3001/api/dividend-distributions?year=2026&subsidiary=3"`
  (with the owner's `x-user-email`) → the June row above. As a non-owner → `403`.
- In the dashboard (owner): the **⚖ Reconcile vs NS** button appears; the panel shows Operating
  closing − NS bank ≈ €590,131 (the dividend) with a small residual.
