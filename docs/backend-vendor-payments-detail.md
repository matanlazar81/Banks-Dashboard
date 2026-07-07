# Backend: `/api/ns-vendor-payments-detail`

Powers the **"Bank cash reconciliation — vendor payments vs SF accrual (cash timing)"** plug-row
breakdown in the Vendors drilldown. Click a past month's Vendors cell, then click the plug row, and this
returns the month's bank vendor payments grouped by **bill month × category × vendor** — showing WHAT the
month's vendor cash paid for and WHEN those bills were accrued (the cash-timing gap: e.g. June cash
settling May bills).

## What it returns

`GET /api/ns-vendor-payments-detail?month=YYYY-MM&subsidiary=N` →
```json
{ "month": "2026-06",
  "rows": [
    { "billMonth": "2026-05", "category": "Cloud & Servers", "vendor": "V00102", "vendorName": "MATRIX -CLOUDZONE", "amountEUR": 502448 },
    { "billMonth": "2026-05", "category": "Collection Services", "vendor": "V00155", "vendorName": "STATSCORE SP. ZO.O.", "amountEUR": 504254 },
    { "billMonth": "2026-06", "category": "Collection Services", "vendor": "V00155", "vendorName": "STATSCORE SP. ZO.O.", "amountEUR": 370282 }
  ],
  "linkedTotal": 2100000 }
```
Rows are aggregated to `(billMonth, category, vendor)`. `category` is the NS account-prefix category
(`EXPENSE_CATEGORIES`, same map as `fetchPaymentsByCategory`). **Amounts + vendor names only — no
customer / end-bettor PII.**

## How it's computed

`fetchVendorPaymentsDetail(month)` (shared `netsuite-api.cjs`) mirrors `fetchPaymentsByCategory`'s join:
`previousTransactionLineLink` links each `VendPymt` made in `month` to the `VendBill` it settled, sums
the bill's expense lines by GL account (Expense/OthExpense/COGS, excluding `76%` payroll, `800%` FX,
`780502`), and groups by the bill's month + account→category + vendor. Shareholder **dividend vendors
(V001281–V001286) are excluded** so it matches the ex-dividend Vendors bucket.

**Caveat (intentional):** this is the *linked VendPymt→Bill cash* view. A partial bill payment attributes
the full bill expense to the payment month, and journal/direct payments (not bill-linked) aren't included.
So it **explains** the plug (the cash-timing composition) but does not foot to it to the euro — the UI
shows the un-linked remainder as a residual footnote.

## Gating — none (all bank-role users)

Un-gated, consistent with `/api/sf-vendor-breakdown` (the category breakdown that feeds the same modal).
Returns amounts + vendor names only.

## Production twin (finance-it-backend `bankDashboardApi.ts`)

Prod doesn't auto-register new bank-dashboard routes. Add it next to the other NS routes, then rebuild +
restart:

```ts
router.get('/ns-vendor-payments-detail', bankRole, async (req: Request, res: Response) => {
  try {
    const sub = parseInt(req.query.subsidiary as string || '3') || 3;
    const month = (req.query.month as string) || '';
    const ns = await netsuiteService.getSubsidiaryClient(sub);
    if (!ns.fetchVendorPaymentsDetail) { res.json({ month, rows: [], linkedTotal: 0, error: 'not available' }); return; }
    res.json(await ns.fetchVendorPaymentsDetail(month));
  } catch (e: any) {
    logger.error(`[NS API] Vendor payments detail failed: ${e.message}`);
    res.json({ month: '', rows: [], linkedTotal: 0, error: e.message });
  }
});
```

`fetchVendorPaymentsDetail` ships in the shared `netsuite-api.cjs`, so a backend **rebuild + restart**
picks up the method; only the route above needs adding. If the route is absent in prod, clicking the plug
row 404s and the sub-drilldown simply shows nothing (degrades gracefully).

## Verify

- `curl -s "http://localhost:3001/api/ns-vendor-payments-detail?month=2026-06&subsidiary=3"` → rows with
  `billMonth`/`category`/`vendor`; the prior-month-bill rows (May bills paid in June: MATRIX-CLOUDZONE +
  STATSCORE ≈ €1M) explain the June €1,032,157 cash-timing plug.
- Dashboard: June Vendors cell → the plug row is now clickable → grouped table by bill month → category →
  vendor; the Back button returns to the category list.
