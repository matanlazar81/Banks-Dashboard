// ============================================================================
// Paste into finance-it/backend/src/routes/bankDashboardApi.ts
// just BEFORE `export default router;`.
//
// Exposes the Column B pipeline revenue-projection methodology computed in
// bank-dashboard/snowflake-api.cjs (fetchPipelineMethodology). The frontend
// reads it via /api/sf-pipeline-methodology?year=YYYY and renders Columns A/B/D.
// ============================================================================

// ── GET /api/sf-pipeline-methodology — Column B revenue projection ──
router.get('/sf-pipeline-methodology', bankRole, async (req: Request, res: Response) => {
  try {
    const sf = snowflakeService.getClient();
    if (!sf || !sf.fetchPipelineMethodology) {
      res.json({ data: null, error: 'fetchPipelineMethodology not available' });
      return;
    }
    const year = parseInt((req.query.year as string) || '0', 10) || new Date().getFullYear();
    const data = await sf.fetchPipelineMethodology(year);
    res.json({ data });
  } catch (e: any) {
    logger.error(`[Banks API SF] Pipeline methodology failed: ${e.message}`);
    res.json({ data: null, error: e.message });
  }
});
