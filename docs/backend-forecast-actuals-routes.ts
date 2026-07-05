// ============================================================================
// Three endpoints that are currently 404 in production finance-it-backend but
// exist in the dev vite server. Two of them feed ACTUALS into the cashflow
// forecast, so their absence makes the year-end number come out too high
// (missing vendor outflows + bank classification). The third lets the
// dashboard remember each user's last scenario (needed by the nightly headless
// refresh bot).
//
//   GET /api/ns-paid-vendors-yearly?subsidiary&year   → { accounts, months, grid, byMonth? }
//   GET /api/ns-bank-classified-yearly?subsidiary&year → { byMonth: { 'YYYY-MM': {...} } }
//   GET/PUT /api/user-pref                             → per-user prefs keyed by email
//
// The NS routes forward to functions already in bank-dashboard/netsuite-api.cjs
// (fetchPaidVendorsYearly, fetchBankClassifiedYearly). The frontend reads the
// RAW object (j.grid / j.accounts / j.byMonth), so return it unwrapped — do NOT
// nest under { data }.
//
// ── SAFE SPLICE RECIPE ─────────────────────────────────────────────────────
// Splice into finance-it/backend/src/routes/bankDashboardApi.ts just before the
// final `export default router;` (same file/pattern as backend-ns-actuals-routes.ts).
// Use rsplit so only the LAST occurrence is patched.
//
//   python3 << 'EOF'
//   backend = '/home/ubuntu/finance-it/backend/src/routes/bankDashboardApi.ts'
//   patch   = '/home/ubuntu/finance-it/extra-apps/bank-dashboard/docs/backend-forecast-actuals-routes.ts'
//   with open(backend) as f: src = f.read()
//   with open(patch)   as f: full = f.read()
//   route_code = full[full.find('// ── GET /api/ns-paid-vendors-yearly'):].rstrip() + '\n\n'
//   if "router.get('/ns-paid-vendors-yearly'" in src:
//       print('Routes already present — no change.')
//   else:
//       anchor = '\nexport default router;'
//       parts = src.rsplit(anchor, 1)
//       if len(parts) != 2: raise SystemExit('Anchor not found.')
//       open(backend, 'w').write(parts[0] + '\n' + route_code + anchor + parts[1])
//       print('Spliced 3 routes.')
//   EOF
//
// After splice:
//   cd /home/ubuntu/finance-it/backend && npm run build && pm2 restart finance-it-backend
//   for p in ns-paid-vendors-yearly ns-bank-classified-yearly user-pref; do
//     curl -s -o /dev/null -w "$p %{http_code}\n" "http://localhost:3001/api/$p"
//   done
//   # 401 = route registered + auth gating (expected from shell); 404 = splice/mount problem
//
// NOTE on user-pref email: this reads the signed-in user's email off req.user
// (passport). _userEmail() tries the common fields; if your session stores it
// elsewhere, adjust that one helper.
// ============================================================================

// ── GET /api/ns-paid-vendors-yearly — paid vendor bills for a year (month × GL) ──
// Feeds the forecast's Vendors actuals. Missing this understates outflows and
// inflates the year-end cash figure.
router.get('/ns-paid-vendors-yearly', bankRole, async (req: Request, res: Response) => {
  try {
    const sub = parseInt(req.query.subsidiary as string || '3') || 3;
    const year = parseInt(req.query.year as string || '') || new Date().getFullYear();
    const ns = await netsuiteService.getSubsidiaryClient(sub);
    if (!ns.fetchPaidVendorsYearly) {
      res.json({ accounts: [], months: [], grid: {}, error: 'fetchPaidVendorsYearly not available on NS client' });
      return;
    }
    const data = await ns.fetchPaidVendorsYearly(year);
    res.json(data); // RAW shape { accounts, months, grid } — frontend reads j.grid / j.accounts
  } catch (e: any) {
    logger.error(`[NS API] Paid vendors yearly failed: ${e.message}`);
    res.json({ accounts: [], months: [], grid: {}, error: e.message });
  }
});

// ── GET /api/ns-bank-classified-yearly — bank delta per month, classified ──
// Salary / Vendors / Collections / Reval / Other. Feeds the forecast's actuals side.
router.get('/ns-bank-classified-yearly', bankRole, async (req: Request, res: Response) => {
  try {
    const sub = parseInt(req.query.subsidiary as string || '3') || 3;
    const year = parseInt(req.query.year as string || '') || new Date().getFullYear();
    const ns = await netsuiteService.getSubsidiaryClient(sub);
    if (!ns.fetchBankClassifiedYearly) {
      res.json({ byMonth: {}, error: 'fetchBankClassifiedYearly not available on NS client' });
      return;
    }
    const data = await ns.fetchBankClassifiedYearly(year);
    res.json(data); // RAW shape { byMonth: {...} } — frontend reads j.byMonth
  } catch (e: any) {
    logger.error(`[NS API] Bank classified yearly failed: ${e.message}`);
    res.json({ byMonth: {}, error: e.message });
  }
});

// ── GET/PUT /api/user-pref — per-user preferences keyed by the signed-in email ──
// GET → { ok, data: { activeScenarioId?, activeSharedOwner?, ... } }
// PUT body { activeScenarioId?, activeSharedOwner? } → merges into the user's record.
// Stored as JSON on disk; the headless refresh bot relies on this to reopen its scenario.
const USER_PREF_FILE = process.env.USER_PREF_FILE
  || '/home/ubuntu/finance-it/extra-apps/bank-dashboard/data/user-prefs.json';
function _userEmail(req: Request): string {
  const u: any = (req as any).user || {};
  const raw = u.email || u.mail || (Array.isArray(u.emails) && u.emails[0] && (u.emails[0].value || u.emails[0])) || '';
  return String(raw).toLowerCase();
}
function _loadUserPrefs(): Record<string, any> {
  try { return JSON.parse(require('fs').readFileSync(USER_PREF_FILE, 'utf-8')); } catch { return {}; }
}
function _saveUserPrefs(p: Record<string, any>): void {
  const fs = require('fs'); const path = require('path');
  fs.mkdirSync(path.dirname(USER_PREF_FILE), { recursive: true });
  fs.writeFileSync(USER_PREF_FILE, JSON.stringify(p, null, 2));
}
router.get('/user-pref', bankRole, (req: Request, res: Response) => {
  const email = _userEmail(req);
  if (!email) { res.status(401).json({ ok: false, error: 'not authenticated' }); return; }
  res.json({ ok: true, data: _loadUserPrefs()[email] || {} });
});
router.put('/user-pref', bankRole, (req: Request, res: Response) => {
  const email = _userEmail(req);
  if (!email) { res.status(401).json({ ok: false, error: 'not authenticated' }); return; }
  try {
    const prefs = _loadUserPrefs();
    prefs[email] = { ...(prefs[email] || {}), ...(req.body || {}), updatedAt: new Date().toISOString() };
    _saveUserPrefs(prefs);
    res.json({ ok: true });
  } catch (e: any) {
    logger.error(`[user-pref] save failed: ${e.message}`);
    res.status(500).json({ ok: false, error: e.message });
  }
});
