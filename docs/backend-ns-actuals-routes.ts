// ============================================================================
// Paste this block into finance-it/backend/src/routes/bankDashboardApi.ts
// just BEFORE `export default router;`.
//
// Two new endpoints expose per-month NS GL totals so the cashflow Vendors
// and Inflows columns can reconcile to the NS P&L for past months:
//   GET /api/ns-vendor-actuals     — 6xxxx + 7xxxx ex 76xxx, per month
//   GET /api/ns-revenue-actuals    — 4xxxx, per month (credit-positive)
//
// Both forward to functions added in bank-dashboard/netsuite-api.cjs
// (fetchVendorActuals and fetchRevenueActuals). The bank-dashboard module
// caches results to disk under data/ so subsequent calls are fast.
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
