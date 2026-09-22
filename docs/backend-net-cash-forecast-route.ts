// ─────────────────────────────────────────────────────────────────────────────
// finance-it-backend route: /api/net-cash-forecast
//
// WHY: In production, the bank-dashboard static frontend is served by
// finance-it-backend, and all /api/* calls hit finance-it-backend (not the vite
// dev server). The dashboard POSTs its LSports current-year bank total + year-end
// closing here so the daily Snowflake job (scripts/net-cash-snapshot.cjs on the
// server) can read it. This mirrors the dev implementation in vite.config.ts.
//
// HOW TO ADD:
//   1. Paste this route into finance-it-backend (Express).
//   2. Point NET_CASH_FILE at the SAME path the cron job reads. The cron defaults
//      to <bank-dashboard>/data/net-cash-forecast.json; set NET_CASH_SNAPSHOT_PATH
//      on the cron to match wherever this route writes, or write to that path here.
//   3. pm2 restart finance-it-backend
//
// The dashboard calls it fire-and-forget. POST must stay behind the app session
// AND an allowlist / admin check — any logged-in user writing this file can
// poison the nightly Snowflake NET_CASH_ACTUAL_AND_FORECAST row.
// ─────────────────────────────────────────────────────────────────────────────

import express from 'express';
import fs from 'fs';
import path from 'path';

const router = express.Router();

// IMPORTANT: keep this in sync with the cron's NET_CASH_SNAPSHOT_PATH.
const NET_CASH_FILE =
  process.env.NET_CASH_FILE ||
  '/home/ubuntu/finance-it/extra-apps/bank-dashboard/data/net-cash-forecast.json';

router.get('/api/net-cash-forecast', (_req, res) => {
  try {
    const data = JSON.parse(fs.readFileSync(NET_CASH_FILE, 'utf-8'));
    res.json({ ok: true, data });
  } catch {
    res.json({ ok: true, data: {} });
  }
});

function callerEmail(req: express.Request): string {
  const u: any = (req as any).user || {};
  return String(u.email || u.user || u.preferred_username || '').trim().toLowerCase();
}
function canPersistNetCash(req: express.Request): boolean {
  const token = (process.env.NET_CASH_WRITE_TOKEN || '').trim();
  const given = String(req.headers['x-net-cash-write-token'] || '');
  if (token && given && token === given) return true;
  const allow = (process.env.SYNC_ALLOWLIST || 'matan.l@lsports.eu')
    .split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
  const email = callerEmail(req);
  const admin = !!(req as any).user && ((req as any).user.isAdmin || (req as any).user.is_admin);
  return !!(email && allow.includes(email)) || admin;
}

router.post('/api/net-cash-forecast', (req, res) => {
  if (!canPersistNetCash(req)) {
    res.status(403).json({ ok: false, error: 'Not authorized to persist net-cash forecast' });
    return;
  }
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
    res.status(500).json({ ok: false, error: e?.message || 'net-cash error' });
  }
});

export default router;
