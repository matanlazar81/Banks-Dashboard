// ============================================================================
// Splice into finance-it/backend/src/routes/bankDashboardApi.ts just before
// the final `export default router;`.
//
// Three new endpoints expose per-month NS GL totals so the cashflow Vendors
// and Inflows columns can reconcile to NS for past months:
//   GET /api/ns-vendor-actuals      — 6xxxx + 7xxxx ex 76xxx, per month (P&L)
//   GET /api/ns-revenue-actuals     — 4xxxx, per month (P&L, credit-positive)
//   GET /api/ns-customer-receipts   — Bank debits on CustPymt + CashSale
//                                     (actual cash deposits from customers,
//                                     includes prior-period AR catch-up)
//
// Both forward to functions added in bank-dashboard/netsuite-api.cjs
// (fetchVendorActuals and fetchRevenueActuals). The bank-dashboard module
// caches results to disk under data/ so subsequent calls are fast.
//
// ── SAFE SPLICE RECIPE ─────────────────────────────────────────────────────
// Use Python with rsplit so only the LAST `export default router;` gets the
// route prepended — never str.replace, which clobbers any matching text
// inside comments and produces duplicate route registrations.
//
//   python3 << 'EOF'
//   backend = '/home/ubuntu/finance-it/backend/src/routes/bankDashboardApi.ts'
//   patch   = '/home/ubuntu/finance-it/extra-apps/bank-dashboard/docs/backend-ns-actuals-routes.ts'
//   with open(backend) as f: src = f.read()
//   with open(patch)   as f: full = f.read()
//   route_code = full[full.find('// ── GET /api/ns-vendor-actuals'):].rstrip() + '\n\n'
//   if "router.get('/ns-vendor-actuals'" in src:
//       print('Routes already present — no change.')
//   else:
//       anchor = '\nexport default router;'
//       parts = src.rsplit(anchor, 1)
//       if len(parts) != 2: raise SystemExit('Anchor not found.')
//       open(backend, 'w').write(parts[0] + '\n' + route_code + anchor + parts[1])
//   EOF
//
// After splice:
//   cd /home/ubuntu/finance-it/backend
//   npm run build           # tsc — backend is started from dist/, must rebuild
//   pm2 restart finance-it-backend
//   curl -s -o /dev/null -w 'HTTP %{http_code}\n' \
//     http://localhost:3001/api/ns-vendor-actuals
//   curl -s -o /dev/null -w 'HTTP %{http_code}\n' \
//     http://localhost:3001/api/ns-revenue-actuals
//   # 401 = route registered + bankRole gating (expected from shell)
//   # 200 = route registered + no auth (test via authed browser/cookie)
//   # 404 = route NOT registered, debug the splice or mount prefix
// ============================================================================

// ── GET /api/ns-vendor-actuals — NS GL vendor totals per month (matches P&L) ──
router.get('/ns-vendor-actuals', bankRole, async (req: Request, res: Response) => {
  try {
    const sub = parseInt(req.query.subsidiary as string || '3') || 3;
    const ns = await netsuiteService.getSubsidiaryClient(sub);
    if (!ns.fetchVendorActuals) {
      res.json({ data: [], error: 'fetchVendorActuals not available on NS client' });
      return;
    }
    const data = await ns.fetchVendorActuals();
    res.json({ data });
  } catch (e: any) {
    logger.error(`[NS API] Vendor actuals failed: ${e.message}`);
    res.json({ data: [], error: e.message });
  }
});

// ── GET /api/ns-revenue-actuals — NS GL revenue totals per month (matches P&L) ──
router.get('/ns-revenue-actuals', bankRole, async (req: Request, res: Response) => {
  try {
    const sub = parseInt(req.query.subsidiary as string || '3') || 3;
    const ns = await netsuiteService.getSubsidiaryClient(sub);
    if (!ns.fetchRevenueActuals) {
      res.json({ data: [], error: 'fetchRevenueActuals not available on NS client' });
      return;
    }
    const data = await ns.fetchRevenueActuals();
    res.json({ data });
  } catch (e: any) {
    logger.error(`[NS API] Revenue actuals failed: ${e.message}`);
    res.json({ data: [], error: e.message });
  }
});

// ── GET /api/ns-customer-receipts — bank cash deposits from customers per month ──
// CustPymt + CashSale debits on Bank-type accounts. Includes AR catch-up from
// prior periods (e.g. a Dec invoice paid in Jan lands here in Jan).
router.get('/ns-customer-receipts', bankRole, async (req: Request, res: Response) => {
  try {
    const sub = parseInt(req.query.subsidiary as string || '3') || 3;
    const ns = await netsuiteService.getSubsidiaryClient(sub);
    if (!ns.fetchCustomerCashReceipts) {
      res.json({ data: {}, error: 'fetchCustomerCashReceipts not available on NS client' });
      return;
    }
    const data = await ns.fetchCustomerCashReceipts();
    res.json({ data });
  } catch (e: any) {
    logger.error(`[NS API] Customer receipts failed: ${e.message}`);
    res.json({ data: {}, error: e.message });
  }
});
