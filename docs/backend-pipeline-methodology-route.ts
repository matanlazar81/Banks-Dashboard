// ============================================================================
// Splice into finance-it/backend/src/routes/bankDashboardApi.ts just before
// the final `export default router;`.
//
// Exposes the Column B pipeline revenue-projection methodology computed in
// bank-dashboard/snowflake-api.cjs (fetchPipelineMethodology). The frontend
// reads it via /api/sf-pipeline-methodology?year=YYYY and renders Columns A/B/D.
//
// ── SAFE SPLICE RECIPE ─────────────────────────────────────────────────────
// Use Python with rsplit so only the LAST `export default router;` gets the
// route prepended — never str.replace, which clobbers any matching text
// inside comments and produces duplicate route registrations.
//
//   python3 << 'EOF'
//   backend = '/home/ubuntu/finance-it/backend/src/routes/bankDashboardApi.ts'
//   patch   = '/home/ubuntu/finance-it/extra-apps/bank-dashboard/docs/backend-pipeline-methodology-route.ts'
//   with open(backend) as f: src = f.read()
//   with open(patch)   as f: full = f.read()
//   route_code = full[full.find('// ── GET /api/sf-pipeline-methodology'):].rstrip() + '\n\n'
//   if "router.get('/sf-pipeline-methodology'" in src:
//       print('Route already present — no change.')
//   else:
//       anchor = '\nexport default router;'
//       parts = src.rsplit(anchor, 1)
//       if len(parts) != 2: raise SystemExit(f'Anchor not found.')
//       open(backend, 'w').write(parts[0] + '\n' + route_code + anchor + parts[1])
//   EOF
//
// After splice:
//   cd /home/ubuntu/finance-it/backend
//   npm run build           # tsc — backend is started from dist/, must rebuild
//   pm2 restart finance-it-backend
//   curl -s -o /dev/null -w 'HTTP %{http_code}\n' \
//     http://localhost:3001/api/sf-pipeline-methodology?year=2026
//   # 401 = route registered + bankRole gating (expected from shell)
//   # 200 = route registered + no auth (test via authed browser/cookie)
//   # 404 = route NOT registered, debug the splice or mount prefix
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
