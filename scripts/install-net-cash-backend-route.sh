#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Install the /api/net-cash-forecast route into finance-it-backend.
#
# This lets the Bank Dashboard (running in an iframe, bankRole-authed) persist the
# Exit-plan-June26 year-end forecast to
#   /home/ubuntu/finance-it/extra-apps/bank-dashboard/data/net-cash-forecast.json
# which the daily job (scripts/net-cash-snapshot.cjs) reads. Result: no more manual
# NET_CASH_FORECAST_EUR — the pushed forecast is always the live dashboard value.
#
# What it does (idempotent, backs up, verifies anchors first):
#   1. Adds '/api/net-cash-forecast' to csrfExemptIframePaths in src/index.ts
#      (the dashboard iframe can't send CSRF tokens, so POST routes must be listed).
#   2. Inserts GET + POST '/net-cash-forecast' handlers into the existing router in
#      src/routes/bankDashboardApi.ts (mounted at /api, guarded by bankRole).
#   3. Rebuilds the backend and restarts it via pm2.
#
# Run on the server:
#   bash /home/ubuntu/finance-it/extra-apps/bank-dashboard/scripts/install-net-cash-backend-route.sh
#
# Rollback: restore the *.netcash.bak files and rebuild/restart.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

BE="${FINANCE_IT_BACKEND:-/home/ubuntu/finance-it/backend}"
ROUTES="$BE/src/routes/bankDashboardApi.ts"
INDEX="$BE/src/index.ts"

echo "[install] finance-it-backend = $BE"
[ -f "$ROUTES" ] || { echo "[install] ABORT: routes file not found: $ROUTES"; exit 1; }
[ -f "$INDEX"  ] || { echo "[install] ABORT: index not found: $INDEX"; exit 1; }
grep -q '^export default router;' "$ROUTES" || { echo "[install] ABORT: 'export default router;' anchor missing in routes"; exit 1; }
grep -q "'/api/budget-snapshot-patch'," "$INDEX" || { echo "[install] ABORT: CSRF anchor missing in index.ts"; exit 1; }

# Backups (keep the first pre-install copy; don't clobber it on re-runs)
[ -f "$ROUTES.netcash.bak" ] || cp "$ROUTES" "$ROUTES.netcash.bak"
[ -f "$INDEX.netcash.bak"  ] || cp "$INDEX"  "$INDEX.netcash.bak"

# ── 1. CSRF exemption ──
if grep -q "'/api/net-cash-forecast'," "$INDEX"; then
  echo "[install] CSRF entry already present — skipping."
else
  sed -i "s#'/api/budget-snapshot-patch',#'/api/budget-snapshot-patch',\n    '/api/net-cash-forecast',#" "$INDEX"
  echo "[install] Added '/api/net-cash-forecast' to csrfExemptIframePaths."
fi

# ── 2. Route handlers ──
if grep -q "net-cash-forecast" "$ROUTES"; then
  echo "[install] Route already present — skipping."
else
  cat > /tmp/netcash-route.ts <<'ROUTE'

// ── Net Cash forecast snapshot (persisted from the dashboard for the daily Snowflake job) ──
const NET_CASH_FILE = '/home/ubuntu/finance-it/extra-apps/bank-dashboard/data/net-cash-forecast.json';
router.get('/net-cash-forecast', bankRole, (_req: Request, res: Response) => {
  try { res.json({ ok: true, data: JSON.parse(fs.readFileSync(NET_CASH_FILE, 'utf-8')) }); }
  catch { res.json({ ok: true, data: {} }); }
});
router.post('/net-cash-forecast', bankRole, (req: Request, res: Response) => {
  try {
    const b = req.body || {};
    const record = {
      date: b.date || new Date().toISOString().slice(0, 10),
      company: b.company || 'lsports',
      scenario: b.scenario || '',
      totalBankEur: Number(b.totalBankEur) || 0,
      totalBankIls: Number(b.totalBankIls) || 0,
      forecastEur: Number(b.forecastEur) || 0,
      forecastIls: Number(b.forecastIls) || 0,
      updatedAt: new Date().toISOString(),
    };
    fs.mkdirSync(path.dirname(NET_CASH_FILE), { recursive: true });
    fs.writeFileSync(NET_CASH_FILE, JSON.stringify(record, null, 2));
    res.json({ ok: true });
  } catch (e: any) {
    logger.error(`[net-cash-forecast] ${e && e.message ? e.message : e}`);
    res.status(500).json({ ok: false, error: 'net-cash error' });
  }
});
ROUTE
  awk '
    /^export default router;/ && !done {
      while ((getline line < "/tmp/netcash-route.ts") > 0) print line
      close("/tmp/netcash-route.ts")
      done = 1
    }
    { print }
  ' "$ROUTES" > "$ROUTES.tmp" && mv "$ROUTES.tmp" "$ROUTES"
  rm -f /tmp/netcash-route.ts
  echo "[install] Inserted GET/POST /net-cash-forecast handlers before 'export default router;'."
fi

# ── 3. Build + restart ──
echo "[install] Building backend (this compiles src → dist)..."
cd "$BE"
npm run build || npx tsc -p tsconfig.json
echo "[install] Restarting finance-it-backend..."
pm2 restart finance-it-backend
echo ""
echo "[install] DONE."
echo "[install] Next: open the dashboard on 'Exit plan June26', then check the file was written:"
echo "[install]   cat /home/ubuntu/finance-it/extra-apps/bank-dashboard/data/net-cash-forecast.json"
