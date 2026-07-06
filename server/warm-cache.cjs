// ─────────────────────────────────────────────────────────────────────────────
// Keep-warm loop for the standalone server (server.cjs only — never in dev).
//
// On boot and every WARM_INTERVAL_MIN minutes it self-fetches the hot endpoints
// through the server's own HTTP interface with refresh=true, so:
//   • the shared cache (and consolidated-data's in-memory cache) stays fresh,
//   • every user gets instant, identical data — the Refresh button becomes a
//     "pull even sooner" convenience rather than a requirement,
//   • NetSuite serialization (queueNsCall) is respected because the requests go
//     through the exact same handlers as real traffic.
//
// Env knobs:
//   WARM_INTERVAL_MIN   minutes between sweeps (default 15; 0 disables the loop)
//   WARM_SUBSIDIARIES   comma list (default '3,6' — LSports + Statscore)
//   WARM_ENDPOINTS      comma list of paths to warm (overrides the default set;
//                       '{sub}' is replaced with each subsidiary id)
// ─────────────────────────────────────────────────────────────────────────────

const DEFAULT_ENDPOINTS = [
  '/api/bank-balance?subsidiary={sub}&refresh=true',
  '/api/bank-accounts?subsidiary={sub}&refresh=true',
  '/api/vendor-bills?subsidiary={sub}&refresh=true',
  '/api/salary-data?subsidiary={sub}&refresh=true',
  '/api/vendor-history?subsidiary={sub}&refresh=true',
  '/api/banks-collection-data?subsidiary={sub}&refresh=true',
  '/api/consolidated-data?refresh=true',
];

function buildUrls() {
  const subs = (process.env.WARM_SUBSIDIARIES || '3,6').split(',').map((s) => s.trim()).filter(Boolean);
  const eps = (process.env.WARM_ENDPOINTS || '').split(',').map((s) => s.trim()).filter(Boolean);
  const templates = eps.length ? eps : DEFAULT_ENDPOINTS;
  const urls = [];
  for (const t of templates) {
    if (t.includes('{sub}')) for (const sub of subs) urls.push(t.replaceAll('{sub}', sub));
    else urls.push(t);
  }
  return urls;
}

async function sweep(port) {
  const urls = buildUrls();
  const t0 = Date.now();
  console.log(`[warm] sweep started (${urls.length} endpoints)`);
  for (const u of urls) {
    // Sequential on purpose: the NS queue serializes anyway, and this keeps the
    // warm sweep from competing with live user requests for connections.
    try {
      const res = await fetch(`http://127.0.0.1:${port}${u}`);
      if (!res.ok) console.warn(`[warm] ${u} → HTTP ${res.status}`);
      else await res.arrayBuffer(); // drain
    } catch (e) {
      console.warn(`[warm] ${u} failed: ${e && e.message ? e.message : e}`);
    }
  }
  console.log(`[warm] sweep done in ${Math.round((Date.now() - t0) / 1000)}s`);
}

function startWarmCache(port) {
  const intervalMin = parseInt(process.env.WARM_INTERVAL_MIN ?? '15', 10);
  if (!intervalMin) { console.log('[warm] disabled (WARM_INTERVAL_MIN=0)'); return; }
  // First sweep shortly after boot (give the listener a moment), then on the interval.
  const kick = setTimeout(() => { sweep(port); }, 3000);
  if (kick.unref) kick.unref();
  const loop = setInterval(() => { sweep(port); }, intervalMin * 60 * 1000);
  if (loop.unref) loop.unref();
  console.log(`[warm] keep-warm loop armed: every ${intervalMin} min`);
}

module.exports = { startWarmCache, sweep };
