// ─────────────────────────────────────────────────────────────────────────────
// Banks-Dashboard API routes — the single home of every /api/* handler.
//
// Mounted by BOTH entries:
//   • vite.config.ts  (dev)  → registerApiRoutes(server.middlewares)
//   • server.cjs      (prod) → registerApiRoutes(app)   [express]
//
// Handlers are plain Node (req, res) middleware — no framework-specific APIs.
// Extracted verbatim from vite.config.ts (which now only mounts this module).
// All filesystem paths resolve against the repo root (ROOT), not this folder.
// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');
const { config: dotenvConfig } = require('dotenv');

const ROOT = path.resolve(__dirname, '..');
const IS_WINDOWS = process.platform === 'win32';
const PROJECT_DIR = IS_WINDOWS ? 'C:\\temp\\banks-dashboard' : ROOT;
dotenvConfig({ path: path.join(PROJECT_DIR, '.env') });

const nsClients                      = {};
let sfClient      = null;
function getSfClient() {
  // Same staleness-check pattern as getNsClient: if the in-memory client is missing methods we
  // expect from current snowflake-api.cjs, evict the require cache and rebuild so new functions
  // become visible without a full process restart.
  const expected           = ['fetchQuarterlyChurnMRR', 'fetchVendorsYearlyGrid'];
  const isStale = sfClient && expected.some(m => typeof sfClient[m] !== 'function');
  if (!sfClient || isStale) {
    try {
      const sfPath = path.resolve(ROOT, 'snowflake-api.cjs');
      delete require.cache[require.resolve(sfPath)];
      const { createSnowflakeClient } = require(sfPath);
      sfClient = createSnowflakeClient(process.env);
    } catch (e     ) { console.error('[SF] Failed:', e.message); return null; }
  }
  return sfClient;
}
function getNsClient(subsidiaryId         = 3) {
  // If the cached client is missing a method we expect from current netsuite-api.cjs,
  // the running Node process is holding an old require()'d copy from before the most
  // recent deploy. Evict the require cache and rebuild so the new methods become visible
  // without a full process restart.
  const expected           = ['fetchBankClassifiedYearly', 'fetchPaidVendorsYearly'];
  const cached = nsClients[subsidiaryId];
  const isStale = cached && expected.some(m => typeof cached[m] !== 'function');
  if (!cached || isStale) {
    const nsPath = require.resolve(ROOT + '/netsuite-api.cjs');
    delete require.cache[nsPath];
    const { createNetSuiteClient } = require(nsPath);
    nsClients[subsidiaryId] = createNetSuiteClient(process.env, subsidiaryId);
  }
  return nsClients[subsidiaryId];
}
// Helper: parse subsidiary from query string (default 3 = LSPORTS)
function getSubsidiary(req     )         {
  const url = new URL(req.url || '', 'http://localhost');
  return parseInt(url.searchParams.get('subsidiary') || '3') || 3;
}
// Helper: parse year from query string (default = current year)
function getYear(req     )         {
  const url = new URL(req.url || '', 'http://localhost');
  return parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
}

// User identity: trust an upstream-injected header (the finance-it parent app should set
// X-User-Email when proxying). DEV_USER_EMAIL is a local-dev fallback only.
function getUserEmail(req     )         {
  const h = req.headers || {};
  const raw = (h['x-user-email'] || h['x-forwarded-user'] || h['x-auth-user'] || process.env.DEV_USER_EMAIL || '').toString();
  return raw.trim().toLowerCase();
}

function getSyncAllowlist()           {
  return (process.env.SYNC_ALLOWLIST || 'matan.l@lsports.eu')
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function canUserSync(email        )          {
  if (!email) return false;
  return getSyncAllowlist().includes(email);
}

// NetSuite request queue — serialize all NS API calls to avoid 429 rate limits
let nsQueue               = Promise.resolve();
function queueNsCall   (fn                  )             {
  const next = nsQueue.then(() => fn(), () => fn());
  nsQueue = next.catch(() => {}); // prevent queue from breaking on errors
  return next;
}

// Server-side response cache — avoids re-hitting NS/SF between refreshes.
// TTL is env-tunable (CACHE_TTL_MIN, default 5 minutes). The cache also persists to
// data/api-cache.json (debounced) and hydrates from it at module load, so a restarted
// server serves the last known data instantly instead of starting cold.
const apiCache = new Map();
const CACHE_TTL = (parseInt(process.env.CACHE_TTL_MIN || '', 10) || 5) * 60 * 1000;
const CACHE_FILE = path.join(ROOT, 'data', 'api-cache.json');
try {
  const saved = JSON.parse(require('fs').readFileSync(CACHE_FILE, 'utf-8'));
  for (const [k, v] of Object.entries(saved)) if (v && typeof v.ts === 'number') apiCache.set(k, v);
  if (apiCache.size) console.log(`[cache] hydrated ${apiCache.size} entries from ${CACHE_FILE}`);
} catch { /* no persisted cache yet */ }
let _cacheSaveTimer = null;
function persistCacheSoon() {
  if (_cacheSaveTimer) return;
  _cacheSaveTimer = setTimeout(() => {
    _cacheSaveTimer = null;
    try {
      const fs = require('fs');
      fs.mkdirSync(path.dirname(CACHE_FILE), { recursive: true });
      fs.writeFileSync(CACHE_FILE, JSON.stringify(Object.fromEntries(apiCache)));
    } catch (e) { console.warn('[cache] persist failed:', e.message); }
  }, 5000);
  if (_cacheSaveTimer.unref) _cacheSaveTimer.unref();
}
function getCached(key) {
  const entry = apiCache.get(key);
  if (entry && Date.now() - entry.ts < CACHE_TTL) return entry.data;
  return null;
}
// Expired entries are still served while a background refresh runs (stale-while-revalidate).
function getStale(key) {
  const entry = apiCache.get(key);
  return entry ? entry.data : null;
}
function setCache(key, data) { apiCache.set(key, { data, ts: Date.now() }); persistCacheSoon(); }

function registerApiRoutes(app     ) {
  // connect (vite dev) and express (server.cjs) share the same use(route, handler) semantics.
  const use = (route        , handler     ) => app.use(route, handler);
      // ── GET /api/ns-config — expose NS account ID for register links ──
      use('/api/ns-config', (_req, res) => {
        const accountId = (process.env.NETSUITE_ACCOUNT_ID || '').replace(/_/g, '-').toLowerCase();
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ accountId }));
      });

      // Helper: cached NS endpoint. Fresh cache → serve it. Expired cache → serve the stale
      // copy immediately and refresh in the background (stale-while-revalidate), so users
      // never wait on NetSuite once the cache has been primed. refresh=true forces a
      // synchronous re-fetch. An in-flight guard prevents duplicate concurrent NS pulls.
      const nsInFlight = new Map();
      const cachedNsHandler = (path, fetchFn, fallback) => {
        use(path, async (req, res) => {
          const sub = getSubsidiary(req);
          const cacheKey = `${path}:${sub}`;
          const url = new URL(req.url || '', 'http://localhost');
          const forceRefresh = url.searchParams.get('refresh') === 'true';
          const send = (payload) => { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(payload)); };
          const refresh = () => {
            if (nsInFlight.has(cacheKey)) return nsInFlight.get(cacheKey);
            const p = (async () => {
              try {
                const ns = getNsClient(sub);
                const result = await fetchFn(ns, req);
                setCache(cacheKey, result);
                return result;
              } finally { nsInFlight.delete(cacheKey); }
            })();
            nsInFlight.set(cacheKey, p);
            return p;
          };
          if (!forceRefresh) {
            const cached = getCached(cacheKey);
            if (cached) { send(cached); return; }
            const stale = getStale(cacheKey);
            if (stale) {
              send(stale); // serve last-known data instantly …
              refresh().catch((e) => console.error(`[NS API] ${path} background refresh failed:`, e.message));
              return;      // … while the cache refreshes for the next request
            }
          }
          try {
            send(await refresh());
          } catch (e) {
            console.error(`[NS API] ${path} fetch failed:`, e.message);
            const stale = getStale(cacheKey);
            send(stale ? stale : { ...fallback, error: e.message });
          }
        });
      };

      // ── GET /api/bank-balance — daily bank balance (primary + local) ──
      cachedNsHandler('/api/bank-balance',
        async (ns) => await queueNsCall(() => ns.fetchBankBalance()),
        { openingBalance: 0, dailyBalances: [], currentBalance: 0 }
      );

      // ── GET /api/bank-accounts — per-account balances ──
      cachedNsHandler('/api/bank-accounts',
        async (ns) => ({ data: await queueNsCall(() => ns.fetchBankAccountList()), timestamp: new Date().toISOString() }),
        { data: [] }
      );

      // ── GET /api/vendor-bills — needed for cashflow forecast (outflows) ──
      cachedNsHandler('/api/vendor-bills',
        async (ns) => ({ data: await queueNsCall(() => ns.fetchVendorBills()), timestamp: new Date().toISOString() }),
        { data: [] }
      );

      // ── GET /api/salary-data — monthly payroll expenses ──
      cachedNsHandler('/api/salary-data',
        async (ns) => ({ data: await queueNsCall(() => ns.fetchSalaryData()), timestamp: new Date().toISOString() }),
        { data: [] }
      );

      // ── GET /api/vendor-history — paid vendor bills history (with JE expense fallback for non-VendBill subsidiaries) ──
      use('/api/vendor-history', async (req     , res     ) => {
        const ck = `vendor-history:${getSubsidiary(req)}`;
        const vhRefresh = (new URL(req.url || '', 'http://localhost')).searchParams.get('refresh') === 'true';
        const cv = vhRefresh ? null : getCached(ck);
        if (cv) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(cv)); return; }
        try {
          const sub = getSubsidiary(req);
          const ns = getNsClient(sub);
          const history = await queueNsCall(() => ns.fetchVendorPaymentHistory());
          // For subsidiaries with no vendor bills (e.g. Statscore uses JEs only),
          // fall back to expense account debits from JEs (excluding salary 76xx)
          const totalVendorAmt = history.reduce((s        , r     ) => s + (r.amountEUR || 0), 0);
          if (totalVendorAmt === 0) {
            try {
              // Clear stale cache entries with 0 amounts
              history.length = 0;
              // Query monthly expense totals by account from JEs
              const expRows = await queueNsCall(() => ns.suiteqlAll(`
                SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS mkey,
                       a.acctname AS vendor,
                       SUM(COALESCE(tal.debit, 0) - COALESCE(tal.credit, 0)) AS amount_eur
                FROM transactionaccountingline tal
                JOIN transaction t ON tal.transaction = t.id
                JOIN account a ON tal.account = a.id
                WHERE t.subsidiary = ${sub}
                  AND a.accttype IN ('Expense', 'OthExpense', 'COGS')
                  AND a.acctnumber NOT LIKE '76%'
                  AND tal.posting = 'T' AND tal.accountingbook = 1
                  AND t.trandate >= TO_DATE('2025-01-01', 'YYYY-MM-DD')
                GROUP BY TO_CHAR(t.trandate, 'YYYY-MM'), a.acctname
                HAVING SUM(COALESCE(tal.debit, 0) - COALESCE(tal.credit, 0)) > 0
                ORDER BY TO_CHAR(t.trandate, 'YYYY-MM')
              `));
              for (const r of expRows) {
                // Use 15th of month as paidDate for monthly aggregation
                history.push({
                  vendor: r.vendor || 'Unknown',
                  paidDate: (r.mkey || '') + '-15',
                  amountEUR: Math.round(parseFloat(r.amount_eur) || 0),
                  daysToPay: 0,
                });
              }
            } catch (e2     ) { console.error('[NS API] JE expense fallback failed:', e2.message); }
          }
          const resp = { data: history, timestamp: new Date().toISOString() };
          setCache(ck, resp);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(resp));
        } catch (e     ) {
          console.error('[NS API] Vendor history fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/banks-collection-data — actual paid invoices (for current month actuals) ──
      use('/api/banks-collection-data', async (req     , res     ) => {
        const url = new URL(req.url || '', `http://${req.headers.host}`);
        const forceRefresh = url.searchParams.get('refresh') === 'true';
        const ck2 = `collection-data:${getSubsidiary(req)}:${getYear(req)}`; // year is part of the SQL — must be part of the key
        const cv2 = forceRefresh ? null : getCached(ck2);
        if (cv2) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(cv2)); return; }
        try {
          const sub = getSubsidiary(req);
          const ns = getNsClient(sub);
          // Match the NS "Sales by Customer - Cash Basis" report: revenue recognized when paid.
          // - For paid invoices (status='B'/'X'): use closedate (when payment hit) as recognition date
          // - For cash sales / JEs / refunds touching Income: use trandate
          // - Skip open invoices (revenue not yet recognized on cash basis)
          const byMonth                         = {};
          try {
            const revenueRows = await queueNsCall(() => ns.suiteqlAll(`
              SELECT TO_CHAR(
                       CASE WHEN t.type = 'CustInvc' AND t.status IN ('B','X') THEN t.closedate
                            ELSE t.trandate END,
                       'YYYY-MM'
                     ) AS mkey,
                     SUM(COALESCE(tal.credit, 0)) - SUM(COALESCE(tal.debit, 0)) AS net_revenue
              FROM transactionaccountingline tal
              JOIN transaction t ON tal.transaction = t.id
              JOIN account a ON tal.account = a.id
              WHERE t.subsidiary = ${sub}
                AND a.accttype = 'Income'
                AND tal.posting = 'T'
                AND tal.accountingbook = 1
                AND (t.type <> 'CustInvc' OR t.status IN ('B','X'))
                AND CASE WHEN t.type = 'CustInvc' AND t.status IN ('B','X') THEN t.closedate
                         ELSE t.trandate END >= TO_DATE('${getYear(req)}-01-01', 'YYYY-MM-DD')
              GROUP BY TO_CHAR(
                       CASE WHEN t.type = 'CustInvc' AND t.status IN ('B','X') THEN t.closedate
                            ELSE t.trandate END,
                       'YYYY-MM')
              ORDER BY mkey
            `));
            for (const r of revenueRows) {
              if (r.mkey && parseFloat(r.net_revenue) > 0) {
                byMonth[r.mkey] = Math.round(parseFloat(r.net_revenue));
              }
            }
          } catch (e2     ) {
            console.error('[NS API] Income-credits query failed, falling back to invoice close-date:', e2.message);
            // Fallback: original CustInvc-by-closedate logic
            const data = await queueNsCall(() => ns.fetchCollectionData());
            for (const r of data) {
              if (r.dateClosed) {
                const parts = r.dateClosed.split('/');
                if (parts.length === 3) {
                  const m = `${parts[2]}-${parts[1].padStart(2, '0')}`;
                  byMonth[m] = (byMonth[m] || 0) + (r.amountEUR || 0);
                }
              }
            }
          }
          const resp2 = { data: byMonth, timestamp: new Date().toISOString() };
          setCache(ck2, resp2);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(resp2));
        } catch (e     ) {
          console.error('[NS API] Collection data fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: {}, error: e.message }));
        }
      });

      // ── GET /api/ar-forecast — AR collection forecast (SO due dates) ──
      cachedNsHandler('/api/ar-forecast', async (ns) => {
        const soItems = await queueNsCall(() => ns.fetchAgingData());
        const forecast = soItems
          .filter((r     ) => r.type === 'Sales Order' && r.soDueDate)
          .map((r     ) => ({ customer: r.customer, amountEUR: r.amountUnbilledEUR || r.amountEUR, dueDate: r.soDueDate }));
        return { data: forecast, timestamp: new Date().toISOString() };
      }, { data: [] });

      // ── GET /api/expense-categories — monthly expenses by category ──
      cachedNsHandler('/api/expense-categories',
        async (ns) => ({ data: await queueNsCall(() => ns.fetchPaymentsByCategory()), timestamp: new Date().toISOString() }),
        { data: { byMonth: {}, categories: [], monthlyTotals: {} } }
      );

      // ── GET /api/ns-bank-accounts-asof — bank balances as of a specific date ──
      use('/api/ns-bank-accounts-asof', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const ns = getNsClient(parseInt(url.searchParams.get('subsidiary') || '3') || 3);
          const asOf = url.searchParams.get('date');
          if (!ns || !asOf) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ data: [] })); return; }
          const accounts = await queueNsCall(() => ns.fetchBankAccountListAsOf(asOf));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: accounts }));
        } catch (e     ) {
          console.error('[NS API] Bank accounts as-of failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/ns-bank-classified-yearly — bank-side delta per month, classified into Salary/Vendors/Collections/Reval/Other
      use('/api/ns-bank-classified-yearly', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const sub = parseInt(url.searchParams.get('subsidiary') || '3') || 3;
          const year = parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
          const ns = getNsClient(sub);
          if (!ns || !ns.fetchBankClassifiedYearly) {
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ byMonth: {}, error: 'endpoint not available' }));
            return;
          }
          const data = await queueNsCall(() => ns.fetchBankClassifiedYearly(year));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(data));
        } catch (e     ) {
          console.error('[NS API] Bank classified yearly failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ byMonth: {}, error: e.message }));
        }
      });

      // ── GET /api/dividend-distributions — OWNER ONLY. Dividend cash by month (memo-based), so the
      // owner's Closing↔NS reconciliation panel can strip the distribution out of the operating view
      // and show it as the reconciling line. Gated to the SYNC allowlist via the upstream x-user-email
      // header (same enforcement as POST /api/sync-budget-targets); non-owners get 403.
      use('/api/dividend-distributions', async (req, res) => {
        try {
          const email = getUserEmail(req);
          if (!canUserSync(email)) {
            res.statusCode = 403;
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ error: 'forbidden', byMonth: {} }));
            return;
          }
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const sub = parseInt(url.searchParams.get('subsidiary') || '3') || 3;
          const year = parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
          const ns = getNsClient(sub);
          if (!ns || !ns.fetchDividendDistributions) {
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ byMonth: {}, error: 'endpoint not available' }));
            return;
          }
          const data = await queueNsCall(() => ns.fetchDividendDistributions(year));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(data));
        } catch (e     ) {
          console.error('[NS API] Dividend distributions failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ byMonth: {}, error: e.message }));
        }
      });

      // ── GET /api/ns-paid-vendors-yearly — NS paid vendor bills for a year, grouped by month + GL account
      use('/api/ns-paid-vendors-yearly', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const sub = parseInt(url.searchParams.get('subsidiary') || '3') || 3;
          const year = parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
          const ns = getNsClient(sub);
          if (!ns || !ns.fetchPaidVendorsYearly) {
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ accounts: [], months: [], grid: {}, error: 'endpoint not available' }));
            return;
          }
          const data = await queueNsCall(() => ns.fetchPaidVendorsYearly(year));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(data));
        } catch (e     ) {
          console.error('[NS API] Paid vendors yearly failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ accounts: [], months: [], grid: {}, error: e.message }));
        }
      });

      // ── GET /api/ns-vendor-bills — vendor bills for specific account + month ──
      use('/api/ns-vendor-bills', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const ns = getNsClient(parseInt(url.searchParams.get('subsidiary') || '3') || 3);
          const accountId = url.searchParams.get('accountId');
          const month = url.searchParams.get('month');
          if (!ns || !accountId || !month) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ data: [] })); return; }
          const paidOnly = url.searchParams.get('paidOnly') === '1';
          const result = await queueNsCall(() => ns.fetchVendorBillsByAccount(accountId, month, { paidOnly }));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: result.bills || [], nsAcctId: result.nsAcctId || null, queryError: result.queryError || null }));
        } catch (e     ) {
          console.error('[NS API] Vendor bills fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── DEBUG: Raw NS SuiteQL query ──
      use('/api/debug-ns-sql', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const sql = url.searchParams.get('sql') || '';
          const sub = parseInt(url.searchParams.get('subsidiary') || '6') || 6;
          const ns = getNsClient(sub);
          if (!sql) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ error: 'no sql' })); return; }
          const result = await queueNsCall(() => ns.suiteql(sql));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ items: result.items || [], count: (result.items || []).length }));
        } catch (e     ) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── DEBUG: Check override table columns ──
      use('/api/debug-overrides', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ error: 'no sf client' })); return; }
          const rows = await sf.query(`SELECT * FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE WHERE source IN ('future_cost_override','future_cost_increment') LIMIT 10`);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ count: rows.length, columns: rows.length > 0 ? Object.keys(rows[0]) : [], sample: rows }));
        } catch (e     ) { res.end(JSON.stringify({ error: e.message })); }
      });

      // ── Snowflake: Budget + Revenue ──
      use('/api/sf-budget', async (_req, res) => {
        try {
          const sf = getSfClient();
          const yr = getYear(_req);
          if (!sf) { res.end(JSON.stringify({ data: { byMonth: {}, totalByMonth: {}, overrides: [] } })); return; }
          const [data, overrides] = await Promise.all([
            sf.fetchBudgetByCategory(yr),
            sf.fetchBudgetOverrides().catch(() => []),
          ]);
          // Apply overrides from FCT_EXPENSE (already have month + category per row)
          const appliedOverrides        = [];
          for (const ov of overrides) {
            const mKey = ov.month; // already "YYYY-MM" format
            const category = ov.category || `Acct ${(ov.account || '').substring(0, 3)}`;
            if (!mKey || mKey < `${yr}-01`) continue;
            // Skip payroll overrides — those are applied in sf-salary-budget endpoint
            if (category === 'Payroll') continue;
            if (!data.byMonth[mKey]) data.byMonth[mKey] = {};
            if (!data.totalByMonth[mKey]) data.totalByMonth[mKey] = { eur: 0, ils: 0 };
            const oldVal = data.byMonth[mKey][category] || 0;
            if (ov.mode === 'Override') {
              data.byMonth[mKey][category] = ov.amountEUR;
              const diff = ov.amountEUR - oldVal;
              data.totalByMonth[mKey].eur += diff;
            } else {
              // Increment
              data.byMonth[mKey][category] = oldVal + ov.amountEUR;
              data.totalByMonth[mKey].eur += ov.amountEUR;
            }
            appliedOverrides.push({ ...ov, mKey, category, oldVal, newVal: data.byMonth[mKey][category] });
          }
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: { ...data, overrides: appliedOverrides } }));
        } catch (e     ) { res.end(JSON.stringify({ data: { byMonth: {}, totalByMonth: {}, overrides: [] }, error: e.message })); }
      });

      // ── Budget targets list / edit ─────────────────────────────────────────
      // GET  /api/budget-targets?year=YYYY&subsidiary=N → list rows
      // PUT  /api/budget-targets                       → set override (auth: any logged-in user)
      // GET  /api/budget-target-edits?since=ISO        → audit log since a watermark (for notifications)
      // ─────────────────────────────────────────────────────────────────────
      use('/api/budget-targets', async (req, res) => {
        res.setHeader('Content-Type', 'application/json');
        try {
          const dbPath = require.resolve(ROOT + '/db.cjs');
          delete require.cache[dbPath];
          const { getDb } = require(dbPath);
          const db = getDb();
          const method = (req.method || 'GET').toUpperCase();
          const url = new URL(req.url || '', 'http://localhost');

          if (method === 'GET') {
            const year = parseInt(url.searchParams.get('year') || '0', 10);
            const subsidiary = parseInt(url.searchParams.get('subsidiary') || '3', 10);
            if (!year) { res.statusCode = 400; res.end(JSON.stringify({ ok: false, error: 'year required' })); return; }
            const raw = db.prepare(`
              SELECT FISCAL_YEAR, DEPARTMENT, LOCATION, CURRENCY, ACCOUNT_NUMBER, ACCOUNT_NAME,
                     NETSUITE_INTERNAL_NUMBER, CATEGORY,
                     SOURCE_AMOUNT_ILS, MONTHLY_SOURCE_ILS,
                     USER_OVERRIDE_AMOUNT_ILS, USER_OVERRIDE_PCT, ANNUAL_BUDGET_TARGET_AMOUNT,
                     SUBSIDIARY_ID, USER_EDITED_BY, USER_EDITED_AT, SOURCE_SYNCED_AT
              FROM FCT_BUDGET_TARGET_BY_DEPT_ACCT
              WHERE FISCAL_YEAR = ? AND SUBSIDIARY_ID = ?
              ORDER BY DEPARTMENT, ACCOUNT_NUMBER
            `).all(year, subsidiary);
            const rows = raw.map((r     ) => ({
              ...r,
              MONTHLY_SOURCE_ILS: r.MONTHLY_SOURCE_ILS ? (() => { try { return JSON.parse(r.MONTHLY_SOURCE_ILS); } catch { return null; } })() : null,
            }));
            res.end(JSON.stringify({ ok: true, rows }));
            return;
          }

          if (method === 'PUT') {
            const email = getUserEmail(req);
            if (!email) { res.statusCode = 401; res.end(JSON.stringify({ ok: false, error: 'Not authenticated' })); return; }
            let body = '';
            for await (const chunk of req) body += chunk;
            const p = JSON.parse(body || '{}');
            const { fiscalYear, subsidiaryId, department, location, accountNumber, currency } = p;
            if (!fiscalYear || !subsidiaryId || !accountNumber) {
              res.statusCode = 400;
              res.end(JSON.stringify({ ok: false, error: 'fiscalYear, subsidiaryId, accountNumber required' }));
              return;
            }
            // Validate field/value pairs. Only USER_OVERRIDE_AMOUNT_ILS and USER_OVERRIDE_PCT are writable.
            const allowed                                  = {
              USER_OVERRIDE_AMOUNT_ILS: 'real',
              USER_OVERRIDE_PCT: 'real',
            };
            const updates                                                     = [];
            const current = db.prepare(`
              SELECT USER_OVERRIDE_AMOUNT_ILS, USER_OVERRIDE_PCT FROM FCT_BUDGET_TARGET_BY_DEPT_ACCT
              WHERE FISCAL_YEAR=? AND SUBSIDIARY_ID=? AND DEPARTMENT=? AND LOCATION=? AND ACCOUNT_NUMBER=? AND CURRENCY=?
            `).get(fiscalYear, subsidiaryId, department, location, accountNumber, currency);
            if (!current) {
              res.statusCode = 404;
              res.end(JSON.stringify({ ok: false, error: 'Row not found' }));
              return;
            }
            for (const [field] of Object.entries(allowed)) {
              if (field in p) {
                const nv = p[field] === null || p[field] === '' || p[field] === undefined ? null : Number(p[field]);
                const cv = (current       )[field];
                if (nv !== cv) updates.push({ field, oldVal: cv, newVal: nv });
              }
            }
            if (updates.length === 0) { res.end(JSON.stringify({ ok: true, changes: 0 })); return; }

            const editAt = new Date().toISOString().slice(0, 19).replace('T', ' ');
            const logStmt = db.prepare(`
              INSERT INTO BUDGET_TARGET_EDIT_LOG
                (EDITED_AT, EDITED_BY, FISCAL_YEAR, SUBSIDIARY_ID, DEPARTMENT, LOCATION, ACCOUNT_NUMBER, CURRENCY, FIELD_NAME, OLD_VALUE, NEW_VALUE)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);
            const txn = db.transaction(() => {
              for (const u of updates) {
                db.prepare(`
                  UPDATE FCT_BUDGET_TARGET_BY_DEPT_ACCT
                  SET ${u.field} = ?, USER_EDITED_BY = ?, USER_EDITED_AT = ?
                  WHERE FISCAL_YEAR=? AND SUBSIDIARY_ID=? AND DEPARTMENT=? AND LOCATION=? AND ACCOUNT_NUMBER=? AND CURRENCY=?
                `).run(u.newVal, email, editAt, fiscalYear, subsidiaryId, department, location, accountNumber, currency);
                logStmt.run(
                  editAt, email, fiscalYear, subsidiaryId, department, location, accountNumber, currency,
                  u.field,
                  u.oldVal == null ? null : String(u.oldVal),
                  u.newVal == null ? null : String(u.newVal),
                );
              }
            });
            txn();
            res.end(JSON.stringify({ ok: true, changes: updates.length, editedBy: email, editedAt: editAt }));
            return;
          }

          res.statusCode = 405;
          res.end(JSON.stringify({ ok: false, error: 'Method not allowed' }));
        } catch (e     ) {
          res.statusCode = 500;
          res.end(JSON.stringify({ ok: false, error: e?.message || String(e) }));
        }
      });

      use('/api/budget-target-edits', (req, res) => {
        res.setHeader('Content-Type', 'application/json');
        try {
          const dbPath = require.resolve(ROOT + '/db.cjs');
          delete require.cache[dbPath];
          const { getDb } = require(dbPath);
          const db = getDb();
          const url = new URL(req.url || '', 'http://localhost');
          const since = url.searchParams.get('since') || '1970-01-01 00:00:00';
          const subsidiary = parseInt(url.searchParams.get('subsidiary') || '0', 10);
          const year = parseInt(url.searchParams.get('year') || '0', 10);
          const conds = ['EDITED_AT > ?'];
          const args        = [since.replace('T', ' ').slice(0, 19)];
          if (subsidiary) { conds.push('SUBSIDIARY_ID = ?'); args.push(subsidiary); }
          if (year) { conds.push('FISCAL_YEAR = ?'); args.push(year); }
          const edits = db.prepare(`
            SELECT ID, EDITED_AT, EDITED_BY, FISCAL_YEAR, SUBSIDIARY_ID, DEPARTMENT, LOCATION,
                   ACCOUNT_NUMBER, CURRENCY, FIELD_NAME, OLD_VALUE, NEW_VALUE
            FROM BUDGET_TARGET_EDIT_LOG
            WHERE ${conds.join(' AND ')}
            ORDER BY EDITED_AT DESC
            LIMIT 200
          `).all(...args);
          const me = getUserEmail(req);
          res.end(JSON.stringify({ ok: true, edits, viewerEmail: me, now: new Date().toISOString() }));
        } catch (e     ) {
          res.statusCode = 500;
          res.end(JSON.stringify({ ok: false, error: e?.message || String(e) }));
        }
      });

      // ── GET /api/whoami — current user + permission flags for budget-targets sync ──
      use('/api/whoami', (req, res) => {
        const email = getUserEmail(req);
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ email, canSync: canUserSync(email) }));
      });

      // ── GET /api/fx-rate — server-side EUR→ILS proxy ──
      // The dashboard's "Get live rate" button used to call the ECB (Frankfurter) API
      // straight from the browser. A Content-Security-Policy was blocking it
      // ("fetch blocked"). Calling from the server avoids the browser policy entirely.
      // Primary: Frankfurter (ECB ref rates). Backup: open.er-api.com (no key).
      use('/api/fx-rate', async (req, res) => {
        res.setHeader('Content-Type', 'application/json');
        try {
          const url = new URL(req.url || '', 'http://localhost');
          const from = (url.searchParams.get('from') || 'EUR').toUpperCase();
          const to = (url.searchParams.get('to') || 'ILS').toUpperCase();
          const fetchJson = async (u        ) => {
            const r = await fetch(u, { signal: AbortSignal.timeout(5000) }       );
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json();
          };
          let rate                    , date                    , source                    ;
          try {
            const d      = await fetchJson(`https://api.frankfurter.app/latest?from=${from}&to=${to}`);
            rate = d?.rates?.[to]; date = d?.date; source = 'ECB (Frankfurter)';
          } catch {
            const d      = await fetchJson(`https://open.er-api.com/v6/latest/${from}`);
            rate = d?.rates?.[to]; date = (d?.time_last_update_utc || '').slice(0, 16); source = 'open.er-api.com';
          }
          if (!rate || !isFinite(rate)) { res.statusCode = 502; res.end(JSON.stringify({ ok: false, error: 'no rate' })); return; }
          res.end(JSON.stringify({ ok: true, from, to, rate: Math.round(rate * 1000) / 1000, date, source }));
        } catch (e     ) {
          res.statusCode = 500;
          res.end(JSON.stringify({ ok: false, error: e?.message || 'fx fetch failed' }));
        }
      });

      // ── GET /api/ns-fx-rate — latest EUR→ILS rate from NetSuite ──
      // Sidesteps the production CSP that blocks external FX services: NetSuite is on the
      // same allowlist as the rest of /api/* calls. Returns "ILS per 1 EUR" (the rate the
      // dashboard's eurIlsRatio expects), date, and which NS source it came from.
      use('/api/ns-fx-rate', async (req, res) => {
        res.setHeader('Content-Type', 'application/json');
        try {
          const url = new URL(req.url || '', 'http://localhost');
          const from = (url.searchParams.get('from') || 'EUR').toUpperCase();
          const to = (url.searchParams.get('to') || 'ILS').toUpperCase();
          const sub = parseInt(url.searchParams.get('subsidiary') || '3', 10) || 3;
          const ns = getNsClient(sub);
          if (!ns?.fetchLatestFxRate) { res.statusCode = 501; res.end(JSON.stringify({ ok: false, error: 'fetchLatestFxRate not available — NS client needs refresh' })); return; }
          const data = await ns.fetchLatestFxRate(from, to);
          if (!data) { res.statusCode = 404; res.end(JSON.stringify({ ok: false, error: `no NetSuite rate for ${from}→${to}` })); return; }
          res.end(JSON.stringify({ ok: true, from, to, rate: Math.round(data.rate * 1000) / 1000, date: data.date, source: data.source }));
        } catch (e     ) {
          res.statusCode = 500;
          res.end(JSON.stringify({ ok: false, error: e?.message || 'NS fx fetch failed' }));
        }
      });

      // ── POST /api/sync-budget-targets — refresh FCT_BUDGET_TARGET_BY_DEPT_ACCT
      // for the (subsidiary, year) currently shown on the dashboard.
      // Gated to SYNC_ALLOWLIST (defaults to matan.l@lsports.eu). User overrides
      // (USER_OVERRIDE_AMOUNT_ILS / USER_OVERRIDE_PCT) are preserved silently. ──
      use('/api/sync-budget-targets', async (req, res) => {
        res.setHeader('Content-Type', 'application/json');
        if ((req.method || 'GET').toUpperCase() !== 'POST') {
          res.statusCode = 405;
          res.end(JSON.stringify({ ok: false, error: 'Method not allowed; use POST' }));
          return;
        }
        const email = getUserEmail(req);
        if (!canUserSync(email)) {
          res.statusCode = 403;
          res.end(JSON.stringify({ ok: false, error: 'Not authorized to sync. Contact the dashboard owner.' }));
          return;
        }
        try {
          const url = new URL(req.url || '', 'http://localhost');
          const subsidiary = parseInt(url.searchParams.get('subsidiary') || '3', 10);
          const yearsParam = url.searchParams.get('year') || url.searchParams.get('years') || '';
          const years = yearsParam
            .split(',')
            .map((y) => parseInt(y.trim(), 10))
            .filter((y) => Number.isFinite(y));
          if (years.length === 0) {
            res.statusCode = 400;
            res.end(JSON.stringify({ ok: false, error: 'year query param required, e.g. ?year=2026' }));
            return;
          }
          const sfPath = require.resolve(ROOT + '/scripts/populate-budget-targets.cjs');
          delete require.cache[sfPath];
          const { populateBudgetTargets } = require(sfPath);
          const result = await populateBudgetTargets({ subsidiary, years, env: process.env });
          res.end(JSON.stringify({ ok: true, triggeredBy: email, ...result }));
        } catch (e     ) {
          res.statusCode = 500;
          res.end(JSON.stringify({ ok: false, error: e?.message || String(e) }));
        }
      });

      use('/api/sf-revenue', async (_req, res) => {
        try {
          const sf = getSfClient();
          const yr = getYear(_req);
          if (!sf) { res.end(JSON.stringify({ data: { budget: {}, actuals: {} } })); return; }
          const data = await sf.fetchRevenueProjection(yr);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) { res.end(JSON.stringify({ data: { budget: {}, actuals: {} }, error: e.message })); }
      });

      // ── Snowflake: Monthly actuals split (salary/vendors) ──
      use('/api/sf-actuals-split', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: {} })); return; }
          const data = await sf.fetchMonthlyActualsSplit();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) { res.end(JSON.stringify({ data: {}, error: e.message })); }
      });

      // ── Snowflake: Full expense export for data comparison ──
      use('/api/sf-expense-export', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const year = parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
          if (!sf) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchExpenseExport(year);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data, count: data.length }));
        } catch (e     ) {
          console.error('[SF] Expense export failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── Snowflake: Vendor expenses yearly grid (months × GL account, accrual via FCT_EXPENSE) ──
      use('/api/sf-vendors-yearly', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const sub = parseInt(url.searchParams.get('subsidiary') || '3') || 3;
          const year = parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
          const sf = getSfClient();
          if (!sf || !sf.fetchVendorsYearlyGrid) {
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ accounts: [], months: [], grid: {}, error: 'endpoint not available' }));
            return;
          }
          const data = await sf.fetchVendorsYearlyGrid(year, sub);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(data));
        } catch (e     ) {
          console.error('[SF API] Vendors yearly failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ accounts: [], months: [], grid: {}, error: e.message }));
        }
      });

      // ── Snowflake: Vendor breakdown for a month ──
      use('/api/sf-vendor-breakdown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          if (!sf || !month) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchVendorBreakdown(month);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) { res.end(JSON.stringify({ data: [], error: e.message })); }
      });

      // ── GET /api/sf-discover — list tables in a schema ──
      use('/api/sf-discover', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const schema = url.searchParams.get('schema') || 'FINANCE';
          if (!sf) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.listTables('DL_PRODUCTION', schema);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-query — run arbitrary Snowflake SQL (dev only) ──
      use('/api/sf-query', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const sql = url.searchParams.get('sql') || '';
          if (!sf || !sql) { res.end(JSON.stringify({ data: [], error: 'Missing sql param' })); return; }
          const data = await sf.fetchFinancialData(sql);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-conversion — Salesforce conversion rate analysis ──
      use('/api/sf-conversion', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: null })); return; }
          const data = await sf.fetchConversionAnalysis();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Conversion analysis failed:', e.message);
          res.end(JSON.stringify({ data: null, error: e.message }));
        }
      });

      // ── GET /api/sf-won-opps — won opportunity details by year and type ──
      use('/api/sf-won-opps', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const year = url.searchParams.get('year');
          const type = url.searchParams.get('type') || 'new'; // 'new' or 'upgrades'
          if (!sf || !year) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchWonOpportunitiesDetail(year, type);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Won opps detail failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-pipeline — open opportunities not closed-won ──
      use('/api/sf-pipeline', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchOpenPipeline(getYear(_req));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Pipeline fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-revenue-paid — monthly revenue from FCT_MONTHLY_REVENUE__SUBSET_PAID ──
      use('/api/sf-revenue-paid', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: {} })); return; }
          const data = await sf.fetchMonthlyRevenuePaid(getYear(_req));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Revenue paid fetch failed:', e.message);
          res.end(JSON.stringify({ data: {}, error: e.message }));
        }
      });

      // ── GET /api/sf-revenue-breakdown — per-customer revenue for a month ──
      use('/api/sf-revenue-breakdown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          const unpaidOnly = url.searchParams.get('unpaidOnly') === '1';
          if (!sf || !month) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchRevenueBreakdown(month, unpaidOnly);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Revenue breakdown fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-yoy-revenue — YoY revenue comparison for OKRs ──
      use('/api/sf-yoy-revenue', async (req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({})); return; }
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const asOfDate = url.searchParams.get('asOfDate') || '';
          const result = await sf.fetchYoYRevenue(asOfDate || undefined);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(result));
        } catch (e     ) {
          console.error('[SF] YoY revenue fetch failed:', e.message);
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── GET /api/sf-churn-analysis — yearly churn rate, lost revenue, and quarterly MRR churn.
      // Quarterly data is piggy-backed on this endpoint (instead of a separate /api/sf-churn-quarterly
      // route) because the parent finance-it server only routes the endpoints it knows about, and
      // extending an existing routed endpoint is the cheapest way to get new SF data through.
      use('/api/sf-churn-analysis', async (req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: [], quarterly: [] })); return; }
          const [result, quarterly] = await Promise.all([
            sf.fetchChurnAnalysis(),
            sf.fetchQuarterlyChurnMRR ? sf.fetchQuarterlyChurnMRR().catch((e     ) => { console.error('[SF] Quarterly churn (inline) failed:', e.message); return []; }) : Promise.resolve([]),
          ]);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: result.yearly, recentMonthlyAvg: result.recentMonthlyAvg, quarterly }));
        } catch (e     ) {
          console.error('[SF] Churn analysis fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], quarterly: [], error: e.message }));
        }
      });

      // ── GET /api/sf-churn-quarterly — MRR churn grouped by quarter (DIM_OPPORTUNITY). ──
      use('/api/sf-churn-quarterly', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf || !sf.fetchQuarterlyChurnMRR) {
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ data: [], error: 'endpoint not available' }));
            return;
          }
          const data = await sf.fetchQuarterlyChurnMRR();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Quarterly churn fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-churn-drilldown — individual churned customers for a year ──
      use('/api/sf-churn-drilldown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const year = url.searchParams.get('year');
          if (!sf || !year) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchChurnDrilldown(year);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Churn drilldown fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-headcount-by-dept — headcount per department with avg salary ──
      use('/api/sf-headcount-by-dept', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchHeadcountByDepartment();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-headcount-lever-detail — all events for a lever type (full year) ──
      // NOTE: must be registered before sf-headcount-events (prefix matching)
      use('/api/sf-headcount-lever-detail', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const eventType = url.searchParams.get('eventType');
          const eventSubType = url.searchParams.get('eventSubType');
          const fromMonth = url.searchParams.get('fromMonth');
          if (!sf || !eventType || !eventSubType) { res.end(JSON.stringify({ data: [] })); return; }
          const yr = parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
          const data = await sf.fetchHeadcountLeverDetail(eventType, eventSubType, fromMonth, yr);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Headcount lever detail fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-headcount-events — HiBob lever data for salary projection ──
      use('/api/sf-headcount-events', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          if (!sf || !month) { res.end(JSON.stringify({ data: { events: [], cumulative: [], baseline: {} } })); return; }
          const yr = parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
          const data = await sf.fetchHeadcountEvents(month, yr);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Headcount events fetch failed:', e.message);
          res.end(JSON.stringify({ data: { events: [], cumulative: [], baseline: {} }, error: e.message }));
        }
      });

      // ── GET /api/sf-salary-budget-breakdown — per-account salary budget for a month ──
      // NOTE: must be registered before sf-salary-budget and sf-salary-breakdown (prefix matching)
      use('/api/sf-salary-budget-breakdown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          if (!sf || !month) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchSalaryBudgetBreakdown(month);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] Salary budget breakdown fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-salary-budget — Snowflake salary budget per month ──
      // NOTE: must be registered before sf-salary-breakdown (prefix matching)
      use('/api/sf-salary-budget', async (_req, res) => {
        try {
          const sf = getSfClient();
          const yr = getYear(_req);
          if (!sf) { res.end(JSON.stringify({ data: {} })); return; }
          const [data, overrides] = await Promise.all([
            sf.fetchSalaryBudget(yr),
            sf.fetchBudgetOverrides().catch(() => []),
          ]);
          // Apply payroll overrides only — new format has month + category per row
          let payrollAccounts = new Set        ();
          try {
            const payrollRows = await sf.query(`SELECT DISTINCT GL_ACCOUNT_NUMBER FROM DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT WHERE IS_PAYROLL = TRUE`);
            payrollAccounts = new Set(payrollRows.map((r     ) => r.GL_ACCOUNT_NUMBER));
          } catch (_) {}
          const appliedOverrides        = [];
          for (const ov of overrides) {
            if (!payrollAccounts.has(ov.account)) continue; // only payroll overrides
            const mKey = ov.month;
            if (!mKey || mKey < `${yr}-01`) continue;
            if (!data[mKey]) data[mKey] = { eur: 0, ils: 0 };
            const oldVal = data[mKey].eur;
            if (ov.mode === 'Override') {
              data[mKey].eur = ov.amountEUR;
            } else {
              data[mKey].eur += ov.amountEUR;
            }
            appliedOverrides.push({ ...ov, mKey, oldVal, newVal: data[mKey].eur });
          }
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data, overrides: appliedOverrides }));
        } catch (e     ) {
          console.error('[SF] Salary budget fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: {}, error: e.message }));
        }
      });

      // ── GET /api/sf-salary-actuals-by-dept — actual payroll by dept (recurring accounts only) ──
      use('/api/sf-salary-actuals-by-dept', async (_req, res) => {
        try {
          const sf = getSfClient();
          const yr = getYear(_req);
          if (!sf) { res.end(JSON.stringify({ byMonth: {}, lastActualMonth: '' })); return; }
          const data = await sf.fetchSalaryActualsByDept(yr);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(data));
        } catch (e     ) {
          console.error('[SF] Salary actuals by dept fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ byMonth: {}, lastActualMonth: '', error: e.message }));
        }
      });

      // ── GET /api/sf-monthly-hc-impact — cumulative headcount event impact per month ──
      use('/api/sf-monthly-hc-impact', async (_req, res) => {
        try {
          const sf = getSfClient();
          const yr = getYear(_req);
          const url = new URL(_req.url || '', 'http://localhost');
          const lastActual = url.searchParams.get('lastActual') || '';
          if (!sf) { res.end(JSON.stringify({})); return; }
          const data = await sf.fetchMonthlyHCImpact(yr, lastActual);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(data));
        } catch (e     ) {
          console.error('[SF] Monthly HC impact fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── Currency defense budget (account 800029 from NetSuite) ──
      use('/api/sf-finance-budget', async (_req, res) => {
        try {
          const ns3 = getNsClient(3);
          if (!ns3) { res.end(JSON.stringify({ data: {} })); return; }
          const data = await ns3.fetchCurrencyDefenseBudget();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[NS] Currency defense budget fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: {}, error: e.message }));
        }
      });

      // ── ARR/MRR: current run-rate + daily history ──
      const arrSnapshotPath = path.resolve(ROOT, 'data', 'arr-history.json');
      function loadArrHistory()        {
        try { return JSON.parse(fs.readFileSync(arrSnapshotPath, 'utf-8')); } catch { return []; }
      }
      function saveArrSnapshot(entry     ) {
        const dir = path.dirname(arrSnapshotPath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        const history = loadArrHistory();
        // Replace today's entry if exists, else append
        const today = new Date().toISOString().slice(0, 10);
        const idx = history.findIndex((h     ) => h.date === today);
        if (idx >= 0) history[idx] = entry; else history.push(entry);
        fs.writeFileSync(arrSnapshotPath, JSON.stringify(history, null, 2));
      }
      use('/api/arr-current', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ data: null })); return; }
          const data = await sf.fetchCurrentARR();
          // Auto-snapshot
          const today = new Date().toISOString().slice(0, 10);
          saveArrSnapshot({ date: today, ...data });
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) {
          console.error('[SF] ARR fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: null, error: e.message }));
        }
      });
      use('/api/arr-history', async (_req, res) => {
        try {
          const history = loadArrHistory();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: history }));
        } catch (e     ) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── Snowflake: Salary breakdown for a month ──
      use('/api/sf-salary-breakdown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          if (!sf || !month) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchSalaryBreakdown(month);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) { res.end(JSON.stringify({ data: [], error: e.message })); }
      });

      // ── GET /api/ns-budget — NS budget data (for subsidiaries without Snowflake) ──
      use('/api/ns-budget', async (req, res) => {
        try {
          const ns = getNsClient(getSubsidiary(req));
          if (!ns.fetchNSBudget) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ byMonth: {} })); return; }
          const data = await queueNsCall(() => ns.fetchNSBudget());
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(data));
        } catch (e     ) {
          console.error('[NS API] NS budget fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ byMonth: {}, error: e.message }));
        }
      });

      // ── GET /api/ns-salary-breakdown — NS salary actuals + budget detail by account for a month ──
      use('/api/ns-salary-breakdown', async (req, res) => {
        try {
          const sub = getSubsidiary(req);
          const ns = getNsClient(sub);
          const url = new URL(req.url || '', 'http://localhost');
          const month = url.searchParams.get('month') || '';
          if (!month) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ actuals: [], budget: [] })); return; }
          const [y, m] = month.split('-');
          const startDate = `${y}-${m}-01`;
          const endDay = new Date(parseInt(y), parseInt(m), 0).getDate();
          const endDate = `${y}-${m}-${String(endDay).padStart(2, '0')}`;
          // Actuals: salary by account for the month — fetch EUR (book=1) and ILS (book=2)
          // in parallel, then zip on acctnumber so the modal can render both columns.
          const accountQuery = (book        ) => queueNsCall(() => ns.suiteqlAll(`
            SELECT a.acctnumber, a.acctname,
                   SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) AS amount
            FROM transactionaccountingline tal
            JOIN transaction t ON tal.transaction = t.id
            JOIN account a ON tal.account = a.id
            WHERE t.subsidiary = ${sub}
              AND tal.posting = 'T' AND tal.accountingbook = ${book}
              AND a.acctnumber LIKE '76%'
              AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
              AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
            GROUP BY a.acctnumber, a.acctname
            HAVING SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) <> 0
            ORDER BY a.acctnumber
          `));
          const [eurRows, ilsRows] = await Promise.all([accountQuery(1), accountQuery(2)]);
          const ilsByAccount                         = {};
          for (const r of ilsRows         ) {
            if (r.acctnumber) ilsByAccount[r.acctnumber] = Math.round(parseFloat(r.amount) || 0);
          }
          const actuals = (eurRows         ).map((r     ) => ({
            account: r.acctnumber || '',
            name: r.acctname || '',
            amountEUR: Math.round(parseFloat(r.amount) || 0),
            amountILS: ilsByAccount[r.acctnumber] || 0,
          }));
          // Budget: salary accounts from budgetsmachine for this period
          const budRows = await queueNsCall(() => ns.suiteqlAll(`
            SELECT a.acctnumber, a.acctname, bm.amount
            FROM budgetsmachine bm
            JOIN budgets b ON bm.budget = b.id
            JOIN accountingperiod ap ON bm.period = ap.id
            JOIN account a ON b.account = a.id
            WHERE b.subsidiary = ${sub}
              AND a.acctnumber LIKE '76%'
              AND b.category = 5
              AND b.accountingbook = 1
              AND ap.isyear = 'F' AND ap.isquarter = 'F'
              AND ap.periodname LIKE '%${y}'
              AND ap.periodname LIKE '${['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][parseInt(m)-1]}%'
            ORDER BY a.acctnumber
          `));
          const budget = budRows.map((r     ) => ({
            account: r.acctnumber || '',
            name: r.acctname || '',
            amountEUR: Math.round(parseFloat(r.amount) || 0),
          })).filter((r     ) => r.amountEUR !== 0);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ actuals, budget }));
        } catch (e     ) {
          console.error('[NS API] NS salary breakdown failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ actuals: [], budget: [], error: e.message }));
        }
      });

      // ── GET /api/consolidated-data — combined data for both subsidiaries + I/C elimination ──
      let consolidatedCache                                                               = null;
      use('/api/consolidated-data', async (_req, res) => {
        try {
          const url = new URL(_req.url || '', 'http://localhost');
          const forceRefresh = url.searchParams.get('refresh') === 'true';
          const lsYear = parseInt(url.searchParams.get('lsYear') || '') || getYear(_req);
          const stYear = parseInt(url.searchParams.get('stYear') || '') || getYear(_req);
          const cacheKey = `${lsYear}-${stYear}`;
          // Serve from cache if available and not a forced refresh
          if (!forceRefresh && consolidatedCache && consolidatedCache.cacheKey === cacheKey) {
            console.log(`[Consolidated] Serving from cache (age: ${Math.round((Date.now() - consolidatedCache.timestamp) / 1000)}s)`);
            res.setHeader('Content-Type', 'application/json');
            res.end(consolidatedCache.data);
            return;
          }
          console.log(`[Consolidated] ${forceRefresh ? 'Forced refresh' : 'No cache'} — fetching fresh data (LS:${lsYear}, ST:${stYear})...`);
          const ns3 = getNsClient(3); // LSports
          const ns6 = getNsClient(6); // Statscore
          const sf = getSfClient();
          const fsM = await import('fs');
          const budgetDirC = path.resolve(ROOT, 'data', 'budgets');
          const consolCurrentYear = new Date().getFullYear();

          // Helper to load snapshot for a subsidiary
          const loadSnapshot = (yr        , co        ) => {
            const fp = path.resolve(budgetDirC, `${yr}-${co}.json`);
            if (fsM.existsSync(fp)) return JSON.parse(fsM.readFileSync(fp, 'utf-8'));
            return null;
          };

          // Load from snapshot if not current year, else from live NS
          let bankBalance3     , bankAccounts3       , salary3       , vendorHistory3       , collections3       , nsBudget3     , monthlyReval3     ;
          let bankBalance6     , bankAccounts6       , salary6       , vendorHistory6       , collections6       , nsBudget6     , monthlyReval6     ;

          const lsSnap = lsYear !== consolCurrentYear ? loadSnapshot(lsYear, 'lsports') : null;
          const stSnap = stYear !== consolCurrentYear ? loadSnapshot(stYear, 'statscore') : null;

          if (lsSnap) {
            console.log(`[Consolidated] Using LS snapshot for ${lsYear}`);
            // Use projectedDecClosing as opening balance (not raw bankBalance which is from the source year)
            const lsOpenBal = lsSnap.projectedDecClosing || lsSnap.bankBalance?.openingBalance || 0;
            bankBalance3 = { openingBalance: lsOpenBal, dailyBalances: [], currentBalance: lsOpenBal };
            bankAccounts3 = []; salary3 = lsSnap.salary || []; vendorHistory3 = lsSnap.vendorHistory || [];
            collections3 = []; // raw collection records not needed — we pass collByMonth separately
            nsBudget3 = lsSnap.nsBudget || { byMonth: {} };
            monthlyReval3 = lsSnap.monthlyReval || { byMonth: {}, preYear: { eur: 0, ils: 0 } };
          } else {
            [bankBalance3, bankAccounts3, salary3, vendorHistory3, collections3, nsBudget3, monthlyReval3] = await Promise.all([
              queueNsCall(() => ns3.fetchBankBalance()).catch((e     ) => ({ openingBalance: 0, dailyBalances: [], currentBalance: 0, error: e.message })),
              queueNsCall(() => ns3.fetchBankAccountList()).catch(() => []),
              queueNsCall(() => ns3.fetchSalaryData()).catch(() => []),
              queueNsCall(() => ns3.fetchVendorPaymentHistory()).catch(() => []),
              queueNsCall(() => ns3.fetchCollectionData()).catch(() => []),
              ns3.fetchNSBudget ? queueNsCall(() => ns3.fetchNSBudget()).catch(() => ({ byMonth: {} })) : Promise.resolve({ byMonth: {} }),
              queueNsCall(() => ns3.fetchMonthlyRevaluation()).catch(() => ({ byMonth: {}, preYear: { eur: 0, ils: 0 } })),
            ]);
          }

          if (stSnap) {
            console.log(`[Consolidated] Using ST snapshot for ${stYear}`);
            const stOpenBal = stSnap.projectedDecClosing || stSnap.bankBalance?.openingBalance || 0;
            bankBalance6 = { openingBalance: stOpenBal, dailyBalances: [], currentBalance: stOpenBal };
            bankAccounts6 = []; salary6 = stSnap.salary || []; vendorHistory6 = stSnap.vendorHistory || [];
            collections6 = [];
            nsBudget6 = stSnap.nsBudget || { byMonth: {} };
            monthlyReval6 = stSnap.monthlyReval || { byMonth: {}, preYear: { eur: 0, ils: 0 } };
          } else {
            [bankBalance6, bankAccounts6, salary6, vendorHistory6, collections6, nsBudget6, monthlyReval6] = await Promise.all([
              queueNsCall(() => ns6.fetchBankBalance()).catch((e     ) => ({ openingBalance: 0, dailyBalances: [], currentBalance: 0, error: e.message })),
              queueNsCall(() => ns6.fetchBankAccountList()).catch(() => []),
              queueNsCall(() => ns6.fetchSalaryData()).catch(() => []),
              queueNsCall(() => ns6.fetchVendorPaymentHistory()).catch(() => []),
              queueNsCall(() => ns6.fetchCollectionData()).catch(() => []),
              ns6.fetchNSBudget ? queueNsCall(() => ns6.fetchNSBudget()).catch(() => ({ byMonth: {} })) : Promise.resolve({ byMonth: {} }),
              queueNsCall(() => ns6.fetchMonthlyRevaluation()).catch(() => ({ byMonth: {}, preYear: { eur: 0, ils: 0 } })),
            ]);
          }

          // Statscore vendor history fallback + collections (skip for snapshots — already populated)
          let collByMonth6                         = stSnap?.collections || {};
          if (!stSnap) {
            const totalVendor6 = vendorHistory6.reduce((s        , r     ) => s + (r.amountEUR || 0), 0);
            if (totalVendor6 === 0) {
              try {
                vendorHistory6.length = 0;
                const expRows = await queueNsCall(() => ns6.suiteqlAll(`
                  SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS mkey,
                         a.acctname AS vendor,
                         SUM(COALESCE(tal.debit, 0) - COALESCE(tal.credit, 0)) AS amount_eur
                  FROM transactionaccountingline tal
                  JOIN transaction t ON tal.transaction = t.id
                  JOIN account a ON tal.account = a.id
                  WHERE t.subsidiary = 6
                    AND a.accttype IN ('Expense', 'OthExpense', 'COGS')
                    AND a.acctnumber NOT LIKE '76%'
                    AND tal.posting = 'T' AND tal.accountingbook = 1
                    AND t.trandate >= TO_DATE('2025-01-01', 'YYYY-MM-DD')
                  GROUP BY TO_CHAR(t.trandate, 'YYYY-MM'), a.acctname
                  HAVING SUM(COALESCE(tal.debit, 0) - COALESCE(tal.credit, 0)) > 0
                  ORDER BY TO_CHAR(t.trandate, 'YYYY-MM')
                `));
                for (const r of expRows) {
                  vendorHistory6.push({ vendor: r.vendor || 'Unknown', paidDate: (r.mkey || '') + '-15', amountEUR: Math.round(parseFloat(r.amount_eur) || 0), daysToPay: 0 });
                }
              } catch (e2     ) { console.error('[Consolidated] Statscore JE expense fallback failed:', e2.message); }
            }

            // Statscore collections fallback: if no invoices, use Income account credits
            for (const r of collections6) {
              if (r.dateClosed) {
                const parts = r.dateClosed.split('/');
                if (parts.length === 3) {
                  const m = `${parts[2]}-${parts[1].padStart(2, '0')}`;
                  collByMonth6[m] = (collByMonth6[m] || 0) + (r.amountEUR || 0);
                }
              }
            }
            if (Object.keys(collByMonth6).length === 0) {
              try {
                const revenueRows = await queueNsCall(() => ns6.suiteqlAll(`
                  SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS mkey,
                         SUM(COALESCE(tal.credit, 0)) - SUM(COALESCE(tal.debit, 0)) AS net_revenue
                  FROM transactionaccountingline tal
                  JOIN transaction t ON tal.transaction = t.id
                  JOIN account a ON tal.account = a.id
                  WHERE t.subsidiary = 6
                    AND a.accttype = 'Income'
                    AND tal.posting = 'T' AND tal.accountingbook = 1
                    AND t.trandate >= TO_DATE('${stYear}-01-01', 'YYYY-MM-DD')
                  GROUP BY TO_CHAR(t.trandate, 'YYYY-MM')
                  ORDER BY TO_CHAR(t.trandate, 'YYYY-MM')
                `));
                for (const r of revenueRows) {
                  if (r.mkey && parseFloat(r.net_revenue) > 0) collByMonth6[r.mkey] = Math.round(parseFloat(r.net_revenue));
                }
              } catch (e2     ) { console.error('[Consolidated] Statscore revenue fallback failed:', e2.message); }
            }
          }

          // LSports collections grouped by month
          const collByMonth3                         = lsSnap?.collections || {};
          if (!lsSnap) {
            for (const r of collections3) {
              if (r.dateClosed) {
                const parts = r.dateClosed.split('/');
                if (parts.length === 3) {
                  const m = `${parts[2]}-${parts[1].padStart(2, '0')}`;
                  collByMonth3[m] = (collByMonth3[m] || 0) + (r.amountEUR || 0);
                }
              }
            }
          }

          // Snowflake data (LSports only) — use snapshot if available
          let sfBudgetData = { totalByMonth: {} }       ;
          let sfRevenueData = {}       ;
          let sfActualsSplitData = {}       ;
          let sfSalaryBudgetData = {}       ;
          let sfRevenuePaidData = {}       ;
          let sfFinanceBudgetData = {}       ;
          // Always fetch currency defense budget fresh from NS (not in snapshot — was Snowflake before, always empty)
          try {
            const ns3ForDef = getNsClient(3);
            if (ns3ForDef) sfFinanceBudgetData = await ns3ForDef.fetchCurrencyDefenseBudget().catch(() => ({}));
          } catch {}
          if (lsSnap) {
            sfBudgetData = lsSnap.sfBudget || { totalByMonth: {} };
            sfRevenueData = lsSnap.sfRevenue || {};
            sfActualsSplitData = lsSnap.sfActualsSplit || {};
            sfSalaryBudgetData = lsSnap.sfSalaryBudget || {};
            sfRevenuePaidData = lsSnap.sfRevenuePaid || {};
          } else if (sf) {
            try {
              const ns3ForFinBud = getNsClient(3);
              const [bud, rev, split, salBud, revPaid, finBud] = await Promise.all([
                sf.fetchBudgetByCategory(lsYear).catch(() => ({ byMonth: {}, totalByMonth: {} })),
                sf.fetchRevenueProjection(lsYear).catch(() => ({ budget: {}, actuals: {} })),
                sf.fetchMonthlyActualsSplit().catch(() => ({})),
                sf.fetchSalaryBudget(lsYear).catch(() => ({})),
                sf.fetchMonthlyRevenuePaid(lsYear).catch(() => ({})),
                ns3ForFinBud ? ns3ForFinBud.fetchCurrencyDefenseBudget().catch(() => ({})) : Promise.resolve({}),
              ]);
              // Apply budget overrides
              const overrides = await sf.fetchBudgetOverrides().catch(() => []);
              let payrollAccounts = new Set        ();
              try {
                const payrollRows = await sf.query(`SELECT DISTINCT GL_ACCOUNT_NUMBER FROM DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT WHERE IS_PAYROLL = TRUE`);
                payrollAccounts = new Set(payrollRows.map((r     ) => r.GL_ACCOUNT_NUMBER));
              } catch (_) {}
              for (const ov of overrides) {
                const mKey = ov.month;
                const category = ov.category || `Acct ${(ov.account || '').substring(0, 3)}`;
                if (!mKey || mKey < `${lsYear}-01`) continue;
                if (category === 'Payroll') continue;
                if (!bud.byMonth[mKey]) bud.byMonth[mKey] = {};
                if (!bud.totalByMonth[mKey]) bud.totalByMonth[mKey] = { eur: 0, ils: 0 };
                const oldVal = bud.byMonth[mKey][category] || 0;
                if (ov.mode === 'Override') {
                  bud.byMonth[mKey][category] = ov.amountEUR;
                  bud.totalByMonth[mKey].eur += (ov.amountEUR - oldVal);
                } else {
                  bud.byMonth[mKey][category] = oldVal + ov.amountEUR;
                  bud.totalByMonth[mKey].eur += ov.amountEUR;
                }
              }
              // Apply salary overrides
              for (const ov of overrides) {
                if (!payrollAccounts.has(ov.account)) continue;
                const mKey = ov.month;
                if (!mKey || mKey < `${lsYear}-01`) continue;
                if (!salBud[mKey]) salBud[mKey] = { eur: 0, ils: 0 };
                if (ov.mode === 'Override') { salBud[mKey].eur = ov.amountEUR; }
                else { salBud[mKey].eur += ov.amountEUR; }
              }
              sfBudgetData = bud;
              sfRevenueData = rev;
              sfActualsSplitData = split;
              sfSalaryBudgetData = salBud;
              sfRevenuePaidData = revPaid;
              sfFinanceBudgetData = finBud;
            } catch (e     ) { console.error('[Consolidated] SF data fetch failed:', e.message); }
          }

          // ── I/C Elimination: actual amounts from xElimination subsidiary (5) ──
          const icRevenueAccts = ['400017', '400020', '400022', '400023', '400025'];
          const icExpenseAccts = ['620005', '620012', '745002', '800007', '650003', '650005', '650006', '650008'];
          const icAllAccts = [...icRevenueAccts, ...icExpenseAccts];

          let actualICRows        = [];
          try {
            // Query xElimination (subsidiary 5) via ns6 (Statscore token has access)
            actualICRows = await queueNsCall(() => ns6.suiteqlAll(`
              SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS mkey,
                     a.acctnumber, a.acctname, a.accttype,
                     SUM(COALESCE(tal.debit,0)) as debit,
                     SUM(COALESCE(tal.credit,0)) as credit
              FROM transactionaccountingline tal
              JOIN transaction t ON tal.transaction = t.id
              JOIN account a ON tal.account = a.id
              WHERE t.subsidiary = 5
                AND tal.posting = 'T'
                AND tal.accountingbook = 1
                AND t.trandate >= TO_DATE('${getYear(_req)}-01-01','YYYY-MM-DD')
              GROUP BY TO_CHAR(t.trandate, 'YYYY-MM'), a.acctnumber, a.acctname, a.accttype
              ORDER BY TO_CHAR(t.trandate, 'YYYY-MM'), a.acctnumber
            `)).catch((e     ) => { console.error('[Consolidated] xElim IC query failed:', e.message); return []; });
          } catch (e     ) { console.error('[Consolidated] I/C elimination query failed:', e.message); }
          console.log(`[Consolidated] xElimination IC: ${actualICRows.length} rows`);

          // Process actual I/C by month from xElimination
          // Revenue accounts: debit = revenue elimination (reduces collections)
          // Expense accounts: credit = expense elimination (reduces vendors)
          const actualByMonth                                                                       = {};
          for (const row of actualICRows) {
            const mk = row.mkey;
            if (!mk) continue;
            if (!actualByMonth[mk]) actualByMonth[mk] = { revenue: 0, expense: 0, details: [] };
            const debit = parseFloat(row.debit) || 0;
            const credit = parseFloat(row.credit) || 0;
            const isRevenue = row.accttype === 'Income' || icRevenueAccts.includes(row.acctnumber);
            const net = isRevenue ? debit : credit; // xElim: revenue debits eliminate income, expense credits eliminate costs
            actualByMonth[mk].details.push({ acctnumber: row.acctnumber, acctname: row.acctname, accttype: row.accttype, debit, credit, net: Math.round(net) });
            if (isRevenue) {
              actualByMonth[mk].revenue += Math.round(net);
            } else {
              actualByMonth[mk].expense += Math.round(net);
            }
          }
          console.log('[Consolidated] IC actualByMonth:', Object.entries(actualByMonth).map(([k, v]) => `${k}: rev=${v.revenue} exp=${v.expense}`).join(', '));

          // Project future I/C elimination from Statscore budget (IC accounts)
          // Query ST budget for IC-related accounts: 400020 (IC revenue), 620004 (IT license), 620010 (Streaming)
          const projectedByMonth                                                                                       = {};
          try {
            const yearLookup = await queueNsCall(() => ns6.suiteqlAll(`
              SELECT id FROM accountingperiod WHERE periodname = 'FY ${new Date().getFullYear()}' AND isyear = 'T'
            `));
            const yearPeriodId = yearLookup?.[0]?.id;
            if (yearPeriodId) {
              const icBudgetRows = await queueNsCall(() => ns6.suiteqlAll(`
                SELECT bm.amount, ap.periodname, a.acctnumber, a.acctname, a.accttype
                FROM budgetsmachine bm
                JOIN budgets b ON bm.budget = b.id
                JOIN accountingperiod ap ON bm.period = ap.id
                JOIN account a ON b.account = a.id
                WHERE b.subsidiary = 6
                  AND b.year = ${yearPeriodId}
                  AND b.accountingbook = 1
                  AND (a.acctnumber = '400020' OR a.acctnumber = '620004' OR a.acctnumber = '620010')
                  AND ap.isyear = 'F' AND ap.isquarter = 'F'
                ORDER BY ap.id, a.acctnumber
              `));
              console.log(`[Consolidated] ST IC budget: ${icBudgetRows.length} rows`);

              const budgetMonthMap                         = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
              };
              const icBudgetByMonth                                                                       = {};
              for (const row of icBudgetRows) {
                const pn = (row.periodname || '').trim();
                const parts = pn.split(' ');
                if (parts.length !== 2) continue;
                const mk = `${parts[1]}-${budgetMonthMap[parts[0]] || '01'}`;
                if (!icBudgetByMonth[mk]) icBudgetByMonth[mk] = { revenue: 0, expense: 0, details: [] };
                const amount = Math.round(parseFloat(row.amount) || 0);
                icBudgetByMonth[mk].details.push({ acctnumber: row.acctnumber, acctname: row.acctname, accttype: row.accttype, amount, projected: true });
                if (row.accttype === 'Income' || row.acctnumber === '400020') {
                  icBudgetByMonth[mk].revenue += amount;
                } else {
                  icBudgetByMonth[mk].expense += amount;
                }
              }

              // For months without xElim actuals: use ST budget IC amounts
              // Revenue side = 400020 budget, Expense side = revenue (IC nets to zero)
              const nowDt = new Date();
              const currentMKey = `${nowDt.getFullYear()}-${String(nowDt.getMonth() + 1).padStart(2, '0')}`;
              for (let mi = 0; mi < 12; mi++) {
                const dt = new Date(nowDt.getFullYear(), mi, 1);
                const mk = `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}`;
                if (!actualByMonth[mk] && mk >= currentMKey && icBudgetByMonth[mk]) {
                  const bud = icBudgetByMonth[mk];
                  // IC elimination: revenue and expense should be equal (nets to zero)
                  // Use budget revenue (400020) as the base, expense = revenue for balanced elimination
                  const icRevenue = bud.revenue;
                  const icExpenseBudget = bud.expense; // 620004 + 620010
                  // Remaining expense (650xxx accounts) = revenue - budgeted expenses, to balance
                  const remainingExpense = icRevenue - icExpenseBudget;
                  projectedByMonth[mk] = {
                    revenue: icRevenue,
                    expense: icRevenue, // balanced: expense always equals revenue in IC elimination
                    details: [
                      ...bud.details,
                      ...(remainingExpense > 0 ? [{ acctnumber: '650xxx', acctname: 'Other IC Services (Statscore)', accttype: 'Expense', amount: remainingExpense, projected: true }] : []),
                    ],
                    source: 'ST budget',
                  };
                }
              }
              console.log('[Consolidated] IC projected from ST budget:', Object.entries(projectedByMonth).map(([k, v]) => `${k}: rev=${v.revenue} exp=${v.expense}`).join(', '));
            }
          } catch (e     ) { console.error('[Consolidated] IC budget projection failed:', e.message); }

          const responseJson = JSON.stringify({
            lsports: {
              bankBalance: bankBalance3,
              bankAccounts: bankAccounts3,
              salary: salary3,
              vendorHistory: vendorHistory3,
              collections: collByMonth3,
              nsBudget: nsBudget3,
              monthlyReval: monthlyReval3,
              sfBudget: sfBudgetData,
              sfRevenue: sfRevenueData,
              sfActualsSplit: sfActualsSplitData,
              sfSalaryBudget: sfSalaryBudgetData,
              sfFinanceBudget: sfFinanceBudgetData,
              sfRevenuePaid: sfRevenuePaidData,
            },
            statscore: {
              bankBalance: bankBalance6,
              bankAccounts: bankAccounts6,
              salary: salary6,
              vendorHistory: vendorHistory6,
              collections: collByMonth6,
              nsBudget: nsBudget6,
              monthlyReval: monthlyReval6,
            },
            elimination: {
              actualByMonth,
              projectedByMonth,
            },
          });
          consolidatedCache = { data: responseJson, timestamp: Date.now(), cacheKey };
          console.log('[Consolidated] Data cached successfully');
          res.setHeader('Content-Type', 'application/json');
          res.end(responseJson);
        } catch (e     ) {
          console.error('[Consolidated] Fatal error:', e.message);
          res.statusCode = 500;
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── GET /api/consolidated-account-breakdown — account-level detail for consolidated drilldown ──
      use('/api/consolidated-account-breakdown', async (req, res) => {
        try {
          const ns3 = getNsClient(3); // LSports
          const ns6 = getNsClient(6); // Statscore
          const url = new URL(req.url || '', 'http://localhost');
          const month = url.searchParams.get('month') || '';
          const type = url.searchParams.get('type') || ''; // salary, vendors, collections
          if (!month || !type) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ ls: [], st: [] })); return; }
          const [y, m] = month.split('-');
          const startDate = `${y}-${m}-01`;
          const endDay = new Date(parseInt(y), parseInt(m), 0).getDate();
          const endDate = `${y}-${m}-${String(endDay).padStart(2, '0')}`;

          let lsRows        = [];
          let stRows        = [];

          if (type === 'salary') {
            // Salary: 76xx accounts
            const q = (sub        ) => `
              SELECT a.acctnumber, a.acctname,
                     SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) AS amount_eur
              FROM transactionaccountingline tal
              JOIN transaction t ON tal.transaction = t.id
              JOIN account a ON tal.account = a.id
              WHERE t.subsidiary = ${sub}
                AND tal.posting = 'T' AND tal.accountingbook = 1
                AND a.acctnumber LIKE '76%'
                AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
                AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
              GROUP BY a.acctnumber, a.acctname
              HAVING SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) <> 0
              ORDER BY a.acctnumber`;
            [lsRows, stRows] = await Promise.all([
              queueNsCall(() => ns3.suiteqlAll(q(3))).catch((e     ) => { console.error('[Account Breakdown] LS salary query failed:', e.message); return []; }),
              queueNsCall(() => ns6.suiteqlAll(q(6))).catch((e     ) => { console.error('[Account Breakdown] ST salary query failed:', e.message); return []; }),
            ]);
          } else if (type === 'vendors') {
            // Vendors: Expense/OthExpense/COGS accounts, NOT salary (not 76xx)
            const q = (sub        ) => `
              SELECT a.acctnumber, a.acctname,
                     SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) AS amount_eur
              FROM transactionaccountingline tal
              JOIN transaction t ON tal.transaction = t.id
              JOIN account a ON tal.account = a.id
              WHERE t.subsidiary = ${sub}
                AND tal.posting = 'T' AND tal.accountingbook = 1
                AND a.accttype IN ('Expense', 'OthExpense', 'COGS')
                AND a.acctnumber NOT LIKE '76%'
                AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
                AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
              GROUP BY a.acctnumber, a.acctname
              HAVING SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) <> 0
              ORDER BY ABS(SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0))) DESC`;
            [lsRows, stRows] = await Promise.all([
              queueNsCall(() => ns3.suiteqlAll(q(3))).catch((e     ) => { console.error('[Account Breakdown] LS vendors query failed:', e.message); return []; }),
              queueNsCall(() => ns6.suiteqlAll(q(6))).catch((e     ) => { console.error('[Account Breakdown] ST vendors query failed:', e.message); return []; }),
            ]);
          } else if (type === 'collections') {
            // Collections: Income accounts (credits)
            const q = (sub        ) => `
              SELECT a.acctnumber, a.acctname,
                     SUM(COALESCE(tal.credit,0)) - SUM(COALESCE(tal.debit,0)) AS amount_eur
              FROM transactionaccountingline tal
              JOIN transaction t ON tal.transaction = t.id
              JOIN account a ON tal.account = a.id
              WHERE t.subsidiary = ${sub}
                AND tal.posting = 'T' AND tal.accountingbook = 1
                AND a.accttype = 'Income'
                AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
                AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
              GROUP BY a.acctnumber, a.acctname
              HAVING SUM(COALESCE(tal.credit,0)) - SUM(COALESCE(tal.debit,0)) <> 0
              ORDER BY ABS(SUM(COALESCE(tal.credit,0)) - SUM(COALESCE(tal.debit,0))) DESC`;
            [lsRows, stRows] = await Promise.all([
              queueNsCall(() => ns3.suiteqlAll(q(3))).catch((e     ) => { console.error('[Account Breakdown] LS collections query failed:', e.message); return []; }),
              queueNsCall(() => ns6.suiteqlAll(q(6))).catch((e     ) => { console.error('[Account Breakdown] ST collections query failed:', e.message); return []; }),
            ]);
          }

          const fmt = (rows       ) => rows.map((r     ) => ({
            account: r.acctnumber || '',
            name: r.acctname || '',
            amount: Math.round(parseFloat(r.amount_eur) || 0),
          }));

          console.log(`[Account Breakdown] type=${type} month=${month} ls=${lsRows.length} rows, st=${stRows.length} rows`);
          if (stRows.length === 0) console.log(`[Account Breakdown] ST returned 0 rows for ${type} ${month}`);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ ls: fmt(lsRows), st: fmt(stRows) }));
        } catch (e     ) {
          console.error('[NS API] Consolidated account breakdown failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ ls: [], st: [], error: e.message }));
        }
      });

      // ── GET /api/monthly-reval — FX revaluation impact per month from NS ──
      cachedNsHandler('/api/monthly-reval',
        async (ns) => ({ data: await queueNsCall(() => ns.fetchMonthlyRevaluation()), timestamp: new Date().toISOString() }),
        { data: { byMonth: {}, preYear: { eur: 0, ils: 0 } } }
      );

      // ── Snowflake: Budget category detail (departmental breakdown) ──
      use('/api/sf-budget-detail', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          const category = url.searchParams.get('category');
          if (!sf || !month || !category) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchBudgetCategoryDetail(month, category);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e     ) { res.end(JSON.stringify({ data: [], error: e.message })); }
      });

      // ── Scenarios CRUD Storage ──
      const scenariosPath = path.resolve(ROOT, 'data', 'scenarios.json');
      const fs = require('fs');

      const loadScenarios = ()                                                                                                                        => {
        try { return JSON.parse(fs.readFileSync(scenariosPath, 'utf-8')); } catch { return []; }
      };
      const saveScenarios = (scenarios       ) => {
        const dir = path.dirname(scenariosPath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(scenariosPath, JSON.stringify(scenarios, null, 2));
      };

      // ── /api/user-pref — per-user preferences (keyed by email) ──
      // Persists across devices so Lital's last scenario follows her wherever she signs in.
      // GET → { activeScenarioId, activeSharedOwner } | PUT body { activeScenarioId?, activeSharedOwner? }.
      const userPrefPath = path.resolve(ROOT, 'data', 'user-prefs.json');
      const loadPrefs = ()                      => {
        try { return JSON.parse(fs.readFileSync(userPrefPath, 'utf-8')); } catch { return {}; }
      };
      const savePrefs = (p                     ) => {
        const dir = path.dirname(userPrefPath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(userPrefPath, JSON.stringify(p, null, 2));
      };
      use('/api/user-pref', async (req     , res     ) => {
        res.setHeader('Content-Type', 'application/json');
        try {
          const email = (getUserEmail(req) || '').toLowerCase();
          if (!email) { res.statusCode = 401; res.end(JSON.stringify({ ok: false, error: 'not authenticated' })); return; }
          const method = (req.method || 'GET').toUpperCase();
          const prefs = loadPrefs();
          if (method === 'GET') {
            res.end(JSON.stringify({ ok: true, data: prefs[email] || {} }));
            return;
          }
          if (method === 'PUT') {
            let body = '';
            for await (const chunk of req) body += chunk;
            const patch = JSON.parse(body || '{}');
            prefs[email] = { ...(prefs[email] || {}), ...patch, updatedAt: new Date().toISOString() };
            savePrefs(prefs);
            res.end(JSON.stringify({ ok: true }));
            return;
          }
          res.statusCode = 405; res.end(JSON.stringify({ ok: false, error: 'method not allowed' }));
        } catch (e     ) {
          res.statusCode = 500; res.end(JSON.stringify({ ok: false, error: e?.message || 'pref error' }));
        }
      });

      // ── /api/net-cash-forecast — latest net-cash snapshot persisted from the dashboard ──
      // The dashboard POSTs its LSports current-year bank total + year-end (Dec) closing here.
      // The server cron scripts/net-cash-snapshot.cjs reads this file nightly and inserts one
      // row into RAW.LANDING_FINANCE.NET_CASH_ACTUAL_AND_FORECAST.
      // GET → { data: {...} } | POST body { date, company, totalBankEur, totalBankIls, forecastEur, forecastIls }
      const netCashPath = path.resolve(ROOT, 'data', 'net-cash-forecast.json');
      use('/api/net-cash-forecast', async (req     , res     ) => {
        res.setHeader('Content-Type', 'application/json');
        try {
          const method = (req.method || 'GET').toUpperCase();
          if (method === 'GET') {
            let data = {};
            try { data = JSON.parse(fs.readFileSync(netCashPath, 'utf-8')); } catch {}
            res.end(JSON.stringify({ ok: true, data }));
            return;
          }
          if (method === 'POST') {
            let body = '';
            for await (const chunk of req) body += chunk;
            const b = JSON.parse(body || '{}');
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
            const dir = path.dirname(netCashPath);
            if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
            fs.writeFileSync(netCashPath, JSON.stringify(record, null, 2));
            res.end(JSON.stringify({ ok: true }));
            return;
          }
          res.statusCode = 405; res.end(JSON.stringify({ ok: false, error: 'method not allowed' }));
        } catch (e     ) {
          res.statusCode = 500; res.end(JSON.stringify({ ok: false, error: e?.message || 'net-cash error' }));
        }
      });

      // GET /api/bank-dashboard-users — stub
      use('/api/bank-dashboard-users', (_req     , res     ) => {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ data: [] }));
      });

      // ── GET /api/budget-years — list available budget years per company ──
      use('/api/budget-years', async (_req     , res     ) => {
        try {
          const fs = await import('fs');
          const budgetDir = path.resolve(ROOT, 'data', 'budgets');
          if (!fs.existsSync(budgetDir)) fs.mkdirSync(budgetDir, { recursive: true });
          const files = fs.readdirSync(budgetDir).filter((f        ) => f.endsWith('.json'));
          const currentYear = new Date().getFullYear();

          // Auto-migrate old format: 2027.json → 2027-lsports.json + 2027-statscore.json
          for (const f of files) {
            const m = f.match(/^(\d{4})\.json$/);
            if (m) {
              const yr = m[1];
              const oldPath = path.resolve(budgetDir, f);
              const data = JSON.parse(fs.readFileSync(oldPath, 'utf-8'));
              // Split into per-company files
              const lsData = { sourceYear: data.sourceYear, targetYear: data.targetYear, createdAt: data.createdAt, status: data.status, sfBudget: data.sfBudget, sfSalaryBudget: data.sfSalaryBudget, sfFinanceBudget: data.sfFinanceBudget || {}, sfRevenue: data.sfRevenue, sfActualsSplit: data.sfActualsSplit, sfRevenuePaid: data.sfRevenuePaid, nsBudget: data.nsBudget3 || { byMonth: {} } };
              const stData = { sourceYear: data.sourceYear, targetYear: data.targetYear, createdAt: data.createdAt, status: data.status, nsBudget: data.nsBudget6 || { byMonth: {} } };
              fs.writeFileSync(path.resolve(budgetDir, `${yr}-lsports.json`), JSON.stringify(lsData, null, 2));
              fs.writeFileSync(path.resolve(budgetDir, `${yr}-statscore.json`), JSON.stringify(stData, null, 2));
              fs.unlinkSync(oldPath);
              console.log(`[Budget] Migrated ${f} → ${yr}-lsports.json + ${yr}-statscore.json`);
            }
          }

          // Parse per-company files: {year}-{company}.json
          const freshFiles = fs.readdirSync(budgetDir).filter((f        ) => f.endsWith('.json'));
          const byCompany                           = { lsports: [currentYear], statscore: [currentYear] };
          for (const f of freshFiles) {
            const m2 = f.match(/^(\d{4})-(lsports|statscore)\.json$/);
            if (m2) {
              const yr = parseInt(m2[1]);
              const co = m2[2];
              if (!byCompany[co]) byCompany[co] = [currentYear];
              if (!byCompany[co].includes(yr)) byCompany[co].push(yr);
            }
          }
          byCompany.lsports.sort();
          byCompany.statscore.sort();

          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ byCompany, currentYear }));
        } catch (e     ) {
          const currentYear = new Date().getFullYear();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ byCompany: { lsports: [currentYear], statscore: [currentYear] }, currentYear, error: e.message }));
        }
      });

      // ── /api/budget-snapshot-patch — update specific fields in existing snapshot ──
      use('/api/budget-snapshot-patch', async (req     , res     ) => {
        if (req.method !== 'POST') { res.statusCode = 405; res.end('Method not allowed'); return; }
        const fs = await import('fs');
        const budgetDir = path.resolve(ROOT, 'data', 'budgets');
        res.setHeader('Content-Type', 'application/json');
        let body = '';
        req.on('data', (chunk     ) => { body += chunk; });
        req.on('end', () => {
          try {
            const { year, company, projectedDecClosing } = JSON.parse(body);
            if (!year || !company) { res.end(JSON.stringify({ error: 'year and company required' })); return; }
            const filePath = path.resolve(budgetDir, `${year}-${company}.json`);
            if (!fs.existsSync(filePath)) { res.end(JSON.stringify({ error: 'snapshot not found' })); return; }
            const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
            if (projectedDecClosing !== undefined) data.projectedDecClosing = projectedDecClosing;
            data.lastPatchedAt = new Date().toISOString();
            fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
            console.log(`[Budget] Patched ${year}-${company}: projectedDecClosing=€${projectedDecClosing?.toLocaleString()}`);
            res.end(JSON.stringify({ success: true }));
          } catch (e     ) { res.end(JSON.stringify({ error: e.message })); }
        });
      });

      // ── /api/budget-snapshot — per-company roll forward, read, delete ──
      use('/api/budget-snapshot', async (req     , res     ) => {
        const fs = await import('fs');
        const budgetDir = path.resolve(ROOT, 'data', 'budgets');
        if (!fs.existsSync(budgetDir)) fs.mkdirSync(budgetDir, { recursive: true });
        res.setHeader('Content-Type', 'application/json');
        const url = new URL(req.url || '', 'http://localhost');

        // ── DELETE: remove a per-company snapshot ──
        if (req.method === 'DELETE') {
          try {
            const yr = parseInt(url.searchParams.get('year') || '');
            const company = url.searchParams.get('company') || '';
            if (!yr || !company) { res.end(JSON.stringify({ error: 'year and company required' })); return; }
            const filePath = path.resolve(budgetDir, `${yr}-${company}.json`);
            if (fs.existsSync(filePath)) {
              fs.unlinkSync(filePath);
              console.log(`[Budget] Deleted snapshot: ${yr}-${company}.json`);
            }
            res.end(JSON.stringify({ success: true }));
          } catch (e     ) { res.end(JSON.stringify({ error: e.message })); }
          return;
        }

        // ── GET: read a per-company snapshot ──
        if (req.method !== 'POST') {
          try {
            const yr = parseInt(url.searchParams.get('year') || '');
            const company = url.searchParams.get('company') || '';
            if (!yr || !company) { res.end(JSON.stringify({ error: 'year and company required' })); return; }
            const filePath = path.resolve(budgetDir, `${yr}-${company}.json`);
            if (!fs.existsSync(filePath)) { res.end(JSON.stringify({ exists: false })); return; }
            const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
            res.end(JSON.stringify({ exists: true, data }));
          } catch (e     ) { res.end(JSON.stringify({ error: e.message })); }
          return;
        }

        // ── POST: create per-company snapshot (roll forward) ──
        try {
          let body = '';
          req.on('data', (chunk     ) => { body += chunk; });
          req.on('end', async () => {
            try {
              const { sourceYear, targetYear, company, clientDecClosing } = JSON.parse(body);
              if (!sourceYear || !targetYear || !company) { res.end(JSON.stringify({ error: 'sourceYear, targetYear, and company required' })); return; }
              // When clientDecClosing is not provided (e.g. refresh from source year),
              // preserve the existing snapshot's projectedDecClosing so the opening balance stays correct.
              // The server-side cashflow calc omits pipeline/churn/unpaid carry, so its estimate diverges.
              let existingDecClosing                    ;
              if (!clientDecClosing) {
                const existingPath = path.resolve(budgetDir, `${targetYear}-${company}.json`);
                if (fs.existsSync(existingPath)) {
                  try {
                    const existing = JSON.parse(fs.readFileSync(existingPath, 'utf-8'));
                    if (existing.projectedDecClosing) {
                      existingDecClosing = existing.projectedDecClosing;
                      console.log(`[Budget] Preserving existing projected Dec closing: €${existingDecClosing .toLocaleString()}`);
                    }
                  } catch {}
                }
              }
              console.log(`[Budget] Rolling forward ${company} ${sourceYear} → ${targetYear}...${clientDecClosing ? ` (client Dec closing: €${Math.round(clientDecClosing).toLocaleString()})` : ''}`);

              // Remap any YYYY-MM key to targetYear-MM (handles data from any source year)
              const remapMonths = (obj                     ) => {
                const result                      = {};
                for (const [key, val] of Object.entries(obj)) {
                  const newKey = key.replace(/^\d{4}(-\d{2})$/, `${targetYear}$1`);
                  result[newKey] = val;
                }
                return result;
              };
              // Helper: get value from object by month index (0-11), ignoring the year in keys
              const getByMonthIdx = (obj                     , mi        ) => {
                const mm = String(mi + 1).padStart(2, '0');
                for (const key of Object.keys(obj)) {
                  if (key.endsWith(`-${mm}`)) return obj[key];
                }
                return undefined;
              };

              if (company === 'lsports') {
                const sf = getSfClient();
                const ns3 = getNsClient(3);
                const [sfBudget, sfSalaryBudget, sfFinanceBudget, sfRevenue, sfActualsSplit, sfRevenuePaid, sfPipeline, sfConversion] = await Promise.all([
                  sf ? sf.fetchBudgetByCategory(sourceYear).catch(() => ({ byMonth: {}, totalByMonth: {} })) : { byMonth: {}, totalByMonth: {} },
                  sf ? sf.fetchSalaryBudget(sourceYear).catch(() => ({})) : {},
                  ns3 ? ns3.fetchCurrencyDefenseBudget().catch(() => ({})) : {},
                  sf ? sf.fetchRevenueProjection(sourceYear).catch(() => ({ budget: {}, actuals: {}, targets: {} })) : { budget: {}, actuals: {}, targets: {} },
                  sf ? sf.fetchMonthlyActualsSplit().catch(() => ({})) : {},
                  sf ? sf.fetchMonthlyRevenuePaid(sourceYear).catch(() => ({})) : {},
                  sf ? sf.fetchOpenPipeline(sourceYear).catch(() => []) : [],
                  sf ? sf.fetchConversionAnalysis().catch(() => ({ yearly: [], stages: [], customers: [], projection: [] })) : { yearly: [], stages: [], customers: [], projection: [] },
                ]);
                let nsBudget3 = { byMonth: {} }       ;
                try { nsBudget3 = await queueNsCall(() => ns3.fetchNsBudget()); } catch {}
                // Fetch bank balance + salary + vendor history + collections for opening balance
                let bankBalance = { openingBalance: 0, dailyBalances: [], currentBalance: 0 }       ;
                let salary        = [];
                let vendorHistory        = [];
                let collections                         = {};
                let monthlyReval = { byMonth: {}, preYear: { eur: 0, ils: 0 } }       ;
                try { bankBalance = await queueNsCall(() => ns3.fetchBankBalance()); } catch {}
                try { salary = await queueNsCall(() => ns3.fetchSalaryData()); } catch {}
                try { vendorHistory = await queueNsCall(() => ns3.fetchVendorPaymentHistory()); } catch {}
                try {
                  const collData = await queueNsCall(() => ns3.fetchCollectionData());
                  for (const r of collData) {
                    if (r.dateClosed) {
                      const parts = r.dateClosed.split('/');
                      if (parts.length === 3) {
                        const m = `${parts[2]}-${parts[1].padStart(2, '0')}`;
                        collections[m] = (collections[m] || 0) + (r.amountEUR || 0);
                      }
                    }
                  }
                } catch {}
                try { monthlyReval = await queueNsCall(() => ns3.fetchMonthlyRevaluation()); } catch {}

                // ── Compute projected Dec closing balance (server-side cashflow) ──
                // Uses month-index lookups to handle data keyed to any year (SF data may use different years)
                const now = new Date();
                const curMonthIdx = now.getMonth(); // 0-based
                let runBal = (bankBalance.openingBalance || 0) + (monthlyReval.preYear?.eur || 0);
                const anchorBalance = bankBalance.currentBalance || 0; // actual current bank balance
                let lastSal = 0;
                let lastColl = 0;
                // Group salary by month index
                const salByIdx                         = {};
                for (const s of salary) {
                  if (s.month && s.amountEUR > 0) {
                    const m = parseInt(s.month.split('-')[1]) - 1;
                    if (!isNaN(m)) salByIdx[m] = s.amountEUR;
                  }
                }
                // Group vendor history by month index (sourceYear only)
                const vendByIdx                         = {};
                for (const v of vendorHistory) {
                  if (v.paidDate?.startsWith(`${sourceYear}`)) {
                    const m = parseInt(v.paidDate.substring(5, 7)) - 1;
                    if (!isNaN(m)) vendByIdx[m] = (vendByIdx[m] || 0) + (v.amountEUR || 0);
                  }
                }
                // Group collections by month index (sourceYear only)
                const collByIdx                         = {};
                for (const [k, v] of Object.entries(collections)) {
                  if (k.startsWith(`${sourceYear}`)) {
                    const m = parseInt(k.substring(5, 7)) - 1;
                    if (!isNaN(m)) collByIdx[m] = v          ;
                  }
                }
                for (let mi = 0; mi < 12; mi++) {
                  const isPast = mi < curMonthIdx;
                  const isCurrent = mi === curMonthIdx;
                  // Anchor to actual current bank balance at current month (like frontend does)
                  if (isCurrent && anchorBalance > 0) { runBal = anchorBalance; }
                  const mKey = `${sourceYear}-${String(mi + 1).padStart(2, '0')}`;
                  // Salary: actuals (past) → SF split → SF salary budget → last known
                  let sal = 0;
                  const sfSplitSal = getByMonthIdx(sfActualsSplit, mi);
                  const sfSalBud = getByMonthIdx(sfSalaryBudget, mi);
                  if (isPast && sfSplitSal?.salary > 0) sal = sfSplitSal.salary;
                  else if (isPast && salByIdx[mi] > 0) sal = salByIdx[mi];
                  else if (sfSalBud?.eur > 0) sal = sfSalBud.eur;
                  else if (nsBudget3.byMonth?.[mKey]?.salary > 0) sal = nsBudget3.byMonth[mKey].salary;
                  else sal = lastSal;
                  if (sal > 0) lastSal = sal;
                  // Vendors: actuals (past) → SF budget → NS budget
                  let vend = 0;
                  const sfSplitVend = getByMonthIdx(sfActualsSplit, mi);
                  const sfBudTotal = getByMonthIdx(sfBudget.totalByMonth || {}, mi);
                  if (isPast && sfSplitVend?.vendors > 0) vend = sfSplitVend.vendors;
                  else if (isPast && vendByIdx[mi] > 0) vend = vendByIdx[mi];
                  else if (sfBudTotal?.eur > 0) vend = sfBudTotal.eur;
                  else if (nsBudget3.byMonth?.[mKey]?.vendors) vend = nsBudget3.byMonth[mKey].vendors;
                  // Collections: actuals (past) → SF revenuePaid → SF revenue budget → NS revenue
                  let coll = 0;
                  const revPaid = getByMonthIdx(sfRevenuePaid, mi);
                  const revBud = getByMonthIdx(sfRevenue.budget || {}, mi);
                  if (isPast && collByIdx[mi] > 0) coll = collByIdx[mi];
                  else if (revPaid?.revenue > 0) coll = revPaid.revenue;
                  else if (revBud?.eur > 0) coll = revBud.eur;
                  else if (nsBudget3.byMonth?.[mKey]?.revenue > 0) coll = nsBudget3.byMonth[mKey].revenue;
                  if (coll > 0) lastColl = coll;
                  // Reval
                  const revalEntry = getByMonthIdx(monthlyReval.byMonth || {}, mi);
                  const reval = revalEntry?.eur || 0;
                  runBal += coll - sal - vend + reval;
                }
                const serverDecClosing = Math.round(runBal);
                const projectedDecClosing = clientDecClosing ? Math.round(clientDecClosing) : (existingDecClosing || serverDecClosing);
                const lastMonthInflow = lastColl;
                console.log(`[Budget] LS projected Dec ${sourceYear} closing: €${projectedDecClosing.toLocaleString()} (server: €${serverDecClosing.toLocaleString()}${existingDecClosing ? `, existing: €${existingDecClosing.toLocaleString()}` : ''}), last inflow: €${lastMonthInflow.toLocaleString()}`);

                const snapshot = {
                  sourceYear, targetYear, company: 'lsports', createdAt: new Date().toISOString(), status: 'draft',
                  projectedDecClosing,
                  lastMonthInflow,
                  // Vendor budget: carry 2026 monthly values to 2027 (same month mapping)
                  sfBudget: { byMonth: remapMonths(sfBudget.byMonth || {}), totalByMonth: remapMonths(sfBudget.totalByMonth || {}) },
                  // Salary baseline: flat fallback = avg of last 3 source-year monthly totals
                  // (Oct-Dec). The frontend OVERRIDES this on snapshot load by fetching source-year
                  // per-department actuals (last 3 actual months, Mar-May) and using the per-dept
                  // average sum. The flat value here is the fallback when per-dept actuals can't
                  // be fetched (no SF or empty response).
                  sfSalaryBudget: (() => {
                    const octSal = getByMonthIdx(sfSalaryBudget, 9)?.eur || salByIdx[9] || lastSal;
                    const novSal = getByMonthIdx(sfSalaryBudget, 10)?.eur || salByIdx[10] || lastSal;
                    const decSal = getByMonthIdx(sfSalaryBudget, 11)?.eur || salByIdx[11] || lastSal;
                    const avgSal = Math.round((octSal + novSal + decSal) / 3) || lastSal;
                    const flat                                  = {};
                    for (let m = 1; m <= 12; m++) flat[`${targetYear}-${String(m).padStart(2, '0')}`] = { eur: avgSal };
                    return flat;
                  })(),
                  // Use avg of last 3 months (Oct-Dec) inflow as flat baseline for all target year months
                  sfRevenue: (() => {
                    const oct = getByMonthIdx(sfRevenuePaid, 9)?.revenue || getByMonthIdx(sfRevenue.budget || {}, 9)?.eur || 0;
                    const nov = getByMonthIdx(sfRevenuePaid, 10)?.revenue || getByMonthIdx(sfRevenue.budget || {}, 10)?.eur || 0;
                    const dec = getByMonthIdx(sfRevenuePaid, 11)?.revenue || getByMonthIdx(sfRevenue.budget || {}, 11)?.eur || 0;
                    const avg3m = Math.round((oct + nov + dec) / 3) || lastMonthInflow;
                    console.log(`[Budget] LS inflow baseline: avg(Oct €${oct.toLocaleString()}, Nov €${nov.toLocaleString()}, Dec €${dec.toLocaleString()}) = €${avg3m.toLocaleString()}`);
                    const flat                                  = {};
                    for (let m = 1; m <= 12; m++) flat[`${targetYear}-${String(m).padStart(2, '0')}`] = { eur: avg3m };
                    return { budget: flat, targets: remapMonths(sfRevenue.targets || {}) };
                  })(),
                  sfActualsSplit: remapMonths(sfActualsSplit),
                  sfRevenuePaid: (() => {
                    const oct = getByMonthIdx(sfRevenuePaid, 9)?.revenue || 0;
                    const nov = getByMonthIdx(sfRevenuePaid, 10)?.revenue || 0;
                    const dec = getByMonthIdx(sfRevenuePaid, 11)?.revenue || 0;
                    const avgRev = Math.round((oct + nov + dec) / 3) || lastMonthInflow;
                    const avgCust = Math.round(((getByMonthIdx(sfRevenuePaid, 9)?.customers || 0) + (getByMonthIdx(sfRevenuePaid, 10)?.customers || 0) + (getByMonthIdx(sfRevenuePaid, 11)?.customers || 0)) / 3);
                    const flat                      = {};
                    for (let m = 1; m <= 12; m++) flat[`${targetYear}-${String(m).padStart(2, '0')}`] = { revenue: avgRev, paid: avgRev, unpaid: 0, customers: avgCust };
                    return flat;
                  })(),
                  sfFinanceBudget: remapMonths(sfFinanceBudget || {}),
                  nsBudget: { byMonth: remapMonths(nsBudget3.byMonth || {}) },
                  sfPipeline: sfPipeline,
                  sfConversion: sfConversion,
                  bankBalance,
                  salary: salary.map((s     ) => ({ ...s, month: s.month?.replace(/^\d{4}/, `${targetYear}`) })),
                  vendorHistory: vendorHistory.map((v     ) => ({ ...v, paidDate: v.paidDate?.replace(/^\d{4}/, `${targetYear}`) })),
                  collections: remapMonths(collections),
                  monthlyReval: { byMonth: remapMonths(monthlyReval.byMonth || {}), preYear: { eur: 0, ils: 0 } },
                };
                fs.writeFileSync(path.resolve(budgetDir, `${targetYear}-lsports.json`), JSON.stringify(snapshot, null, 2));
              } else {
                // Statscore
                const ns6 = getNsClient(6);
                let nsBudget6 = { byMonth: {} }       ;
                try { nsBudget6 = await queueNsCall(() => ns6.fetchNsBudget()); } catch {}
                let bankBalance6 = { openingBalance: 0, dailyBalances: [], currentBalance: 0 }       ;
                let salary6        = [];
                let vendorHistory6        = [];
                let collections6                         = {};
                let monthlyReval6 = { byMonth: {}, preYear: { eur: 0, ils: 0 } }       ;
                try { bankBalance6 = await queueNsCall(() => ns6.fetchBankBalance()); } catch {}
                try { salary6 = await queueNsCall(() => ns6.fetchSalaryData()); } catch {}
                try { vendorHistory6 = await queueNsCall(() => ns6.fetchVendorPaymentHistory()); } catch {}
                try {
                  const collData6 = await queueNsCall(() => ns6.fetchCollectionData());
                  for (const r of collData6) {
                    if (r.dateClosed) {
                      const parts = r.dateClosed.split('/');
                      if (parts.length === 3) {
                        const m = `${parts[2]}-${parts[1].padStart(2, '0')}`;
                        collections6[m] = (collections6[m] || 0) + (r.amountEUR || 0);
                      }
                    }
                  }
                } catch {}
                try { monthlyReval6 = await queueNsCall(() => ns6.fetchMonthlyRevaluation()); } catch {}

                // ── Compute projected Dec closing balance for Statscore ──
                const curMonthIdx6 = new Date().getMonth();
                let runBal6 = (bankBalance6.openingBalance || 0) + (monthlyReval6.preYear?.eur || 0);
                const anchorBalance6 = bankBalance6.currentBalance || 0;
                let lastSal6 = 0; let lastColl6 = 0;
                const salByIdx6                         = {};
                for (const s of salary6) {
                  if (s.month && s.amountEUR > 0) { const m = parseInt(s.month.split('-')[1]) - 1; if (!isNaN(m)) salByIdx6[m] = s.amountEUR; }
                }
                const vendByIdx6                         = {};
                for (const v of vendorHistory6) {
                  if (v.paidDate?.startsWith(`${sourceYear}`)) { const m = parseInt(v.paidDate.substring(5, 7)) - 1; if (!isNaN(m)) vendByIdx6[m] = (vendByIdx6[m] || 0) + (v.amountEUR || 0); }
                }
                const collByIdx6                         = {};
                for (const [k, v] of Object.entries(collections6)) {
                  if (k.startsWith(`${sourceYear}`)) { const m = parseInt(k.substring(5, 7)) - 1; if (!isNaN(m)) collByIdx6[m] = v          ; }
                }
                // ── Compute avg baselines from last 3 months with actual data ──
                const avgLast3 = (byIdx                        ) => {
                  const months = Object.keys(byIdx).map(Number).filter(m => byIdx[m] > 0).sort((a, b) => b - a);
                  const last3 = months.slice(0, 3);
                  if (last3.length === 0) return 0;
                  return Math.round(last3.reduce((s, m) => s + byIdx[m], 0) / last3.length);
                };
                const avgColl6 = avgLast3(collByIdx6);
                const avgVend6 = avgLast3(vendByIdx6);
                const avgSal6 = avgLast3(salByIdx6);
                console.log(`[Budget] ST avg(last 3m): inflow €${avgColl6.toLocaleString()}, vendors €${avgVend6.toLocaleString()}, salary €${avgSal6.toLocaleString()}`);

                for (let mi = 0; mi < 12; mi++) {
                  const isPast = mi < curMonthIdx6;
                  const isCurrent6 = mi === curMonthIdx6;
                  if (isCurrent6 && anchorBalance6 > 0) { runBal6 = anchorBalance6; }
                  const mKey = `${sourceYear}-${String(mi + 1).padStart(2, '0')}`;
                  let sal = 0;
                  if (isPast && salByIdx6[mi] > 0) sal = salByIdx6[mi];
                  else if (nsBudget6.byMonth?.[mKey]?.salary > 0) sal = nsBudget6.byMonth[mKey].salary;
                  else sal = lastSal6 || avgSal6;
                  if (sal > 0) lastSal6 = sal;
                  let vend = isPast && vendByIdx6[mi] > 0 ? vendByIdx6[mi] : (nsBudget6.byMonth?.[mKey]?.vendors || avgVend6);
                  let coll = isPast && collByIdx6[mi] > 0 ? collByIdx6[mi] : (nsBudget6.byMonth?.[mKey]?.revenue || avgColl6);
                  if (coll > 0) lastColl6 = coll;
                  const revalEntry = getByMonthIdx(monthlyReval6.byMonth || {}, mi);
                  const reval = revalEntry?.eur || 0;
                  runBal6 += coll - sal - vend + reval;
                }
                const serverDecClosing6 = Math.round(runBal6);
                const projectedDecClosing6 = clientDecClosing ? Math.round(clientDecClosing) : (existingDecClosing || serverDecClosing6);
                console.log(`[Budget] ST projected Dec ${sourceYear} closing: €${projectedDecClosing6.toLocaleString()} (server: €${serverDecClosing6.toLocaleString()}${existingDecClosing ? `, existing: €${existingDecClosing.toLocaleString()}` : ''}), last inflow: €${(lastColl6 || avgColl6).toLocaleString()}`);

                // ── Build flat nsBudget with avg(Oct-Dec) baselines for all 12 months ──
                const nsBudgetFlat6                      = {};
                for (let m = 1; m <= 12; m++) {
                  const mk = `${targetYear}-${String(m).padStart(2, '0')}`;
                  const existing = nsBudget6.byMonth?.[`${sourceYear}-${String(m).padStart(2, '0')}`] || {};
                  nsBudgetFlat6[mk] = {
                    revenue: existing.revenue || avgColl6,
                    salary: existing.salary || avgSal6,
                    vendors: existing.vendors || avgVend6,
                  };
                }

                // ── Build flat collections for target year ──
                const collectionsFlat6                         = {};
                for (let m = 1; m <= 12; m++) {
                  const mk = `${targetYear}-${String(m).padStart(2, '0')}`;
                  const srcMk = `${sourceYear}-${String(m).padStart(2, '0')}`;
                  collectionsFlat6[mk] = collections6[srcMk] || avgColl6;
                }

                const snapshot = {
                  sourceYear, targetYear, company: 'statscore', createdAt: new Date().toISOString(), status: 'draft',
                  projectedDecClosing: projectedDecClosing6,
                  lastMonthInflow: lastColl6 || avgColl6,
                  nsBudget: { byMonth: nsBudgetFlat6 },
                  bankBalance: bankBalance6,
                  salary: salary6.map((s     ) => ({ ...s, month: s.month?.replace(/^\d{4}/, `${targetYear}`) })),
                  vendorHistory: vendorHistory6.map((v     ) => ({ ...v, paidDate: v.paidDate?.replace(/^\d{4}/, `${targetYear}`) })),
                  collections: collectionsFlat6,
                  monthlyReval: { byMonth: remapMonths(monthlyReval6.byMonth || {}), preYear: { eur: 0, ils: 0 } },
                };
                fs.writeFileSync(path.resolve(budgetDir, `${targetYear}-statscore.json`), JSON.stringify(snapshot, null, 2));
              }
              console.log(`[Budget] Snapshot saved: data/budgets/${targetYear}-${company}.json`);
              res.end(JSON.stringify({ success: true, targetYear, company, status: 'draft' }));
            } catch (e     ) {
              console.error('[Budget] Snapshot creation failed:', e.message);
              res.end(JSON.stringify({ error: e.message }));
            }
          });
        } catch (e     ) { res.end(JSON.stringify({ error: e.message })); }
      });

      // /api/scenarios — CRUD
      use('/api/scenarios', async (req     , res     ) => {
        const url = new URL(req.url || '', 'http://localhost');
        const pathParts = url.pathname.replace(/^\/api\/scenarios\/?/, '').split('/').filter(Boolean);
        // pathParts: [] | [id] | [id, 'shares'] | [id, 'share'] | [id, 'share', email]
        res.setHeader('Content-Type', 'application/json');

        try {
          // GET /api/scenarios — list all
          if (req.method === 'GET' && pathParts.length === 0) {
            const scenarios = loadScenarios();
            res.end(JSON.stringify({ data: scenarios, shared: [], viewerEmail: 'admin@cloudpay.net' }));
            return;
          }

          // POST /api/scenarios — create/save
          if (req.method === 'POST' && pathParts.length === 0) {
            let body = '';
            for await (const chunk of req) body += chunk;
            const { id, name, data, company } = JSON.parse(body);
            const scenarios = loadScenarios();
            const now = new Date().toISOString();
            const existing = scenarios.findIndex((s     ) => s.id === id);
            if (existing >= 0) {
              scenarios[existing] = { ...scenarios[existing], name: name || scenarios[existing].name, data: data || scenarios[existing].data, updatedAt: now, ...(company ? { company } : {}) };
            } else {
              scenarios.push({ id, name, createdAt: now, updatedAt: now, data, ownerEmail: 'admin@cloudpay.net', company: company || 'lsports' });
            }
            saveScenarios(scenarios);
            res.end(JSON.stringify({ ok: true }));
            return;
          }

          // PUT /api/scenarios/:id — update
          if (req.method === 'PUT' && pathParts.length === 1) {
            let body = '';
            for await (const chunk of req) body += chunk;
            const updates = JSON.parse(body);
            const scenarios = loadScenarios();
            const idx = scenarios.findIndex((s     ) => s.id === pathParts[0]);
            if (idx >= 0) {
              if (updates.name !== undefined) scenarios[idx].name = updates.name;
              if (updates.data !== undefined) scenarios[idx].data = updates.data;
              scenarios[idx].updatedAt = new Date().toISOString();
              saveScenarios(scenarios);
              res.end(JSON.stringify({ ok: true }));
            } else {
              res.statusCode = 404;
              res.end(JSON.stringify({ error: 'Scenario not found' }));
            }
            return;
          }

          // DELETE /api/scenarios/:id — delete
          if (req.method === 'DELETE' && pathParts.length === 1) {
            const scenarios = loadScenarios();
            const filtered = scenarios.filter((s     ) => s.id !== pathParts[0]);
            saveScenarios(filtered);
            res.end(JSON.stringify({ ok: true }));
            return;
          }

          // GET /api/scenarios/:id/shares — stub
          if (req.method === 'GET' && pathParts.length === 2 && pathParts[1] === 'shares') {
            res.end(JSON.stringify({ data: [] }));
            return;
          }

          // POST /api/scenarios/:id/share — stub
          if (req.method === 'POST' && pathParts.length === 2 && pathParts[1] === 'share') {
            let body = '';
            for await (const chunk of req) body += chunk;
            res.end(JSON.stringify({ ok: true }));
            return;
          }

          // DELETE /api/scenarios/:id/share/:email — stub
          if (req.method === 'DELETE' && pathParts.length === 3 && pathParts[1] === 'share') {
            res.end(JSON.stringify({ ok: true }));
            return;
          }

          // Fallback
          res.statusCode = 404;
          res.end(JSON.stringify({ error: 'Not found' }));
        } catch (e     ) {
          console.error('[Scenarios API] Error:', e.message);
          res.statusCode = 500;
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── Chat History Storage ──
      const chatHistoryPath = path.resolve(ROOT, 'chat-history.json');

      const loadChatHistory = ()                                                                                         => {
        try { return JSON.parse(fs.readFileSync(chatHistoryPath, 'utf-8')); } catch { return []; }
      };
      const saveChatHistory = (history       ) => {
        fs.writeFileSync(chatHistoryPath, JSON.stringify(history, null, 2));
      };

      // GET /api/chat-history — list all conversations
      use('/api/chat-history', async (req, res) => {
        if (req.method === 'GET') {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(loadChatHistory()));
          return;
        }
        if (req.method === 'POST') {
          let body = '';
          for await (const chunk of req) body += chunk;
          const { action, id, title, messages } = JSON.parse(body);
          const history = loadChatHistory();
          if (action === 'save') {
            const existing = history.find(h => h.id === id);
            if (existing) {
              existing.messages = messages;
              existing.title = title || existing.title;
              existing.updatedAt = new Date().toISOString();
            } else {
              history.unshift({ id, title: title || 'New Chat', messages, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
            }
            saveChatHistory(history);
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ ok: true }));
          } else if (action === 'delete') {
            saveChatHistory(history.filter(h => h.id !== id));
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ ok: true }));
          } else {
            res.end(JSON.stringify({ ok: false }));
          }
          return;
        }
        res.statusCode = 405; res.end('Method not allowed');
      });

      // ── POST /api/chat — AI assistant powered by Claude ──
      use('/api/chat', async (req, res) => {
        if (req.method !== 'POST') { res.statusCode = 405; res.end('Method not allowed'); return; }
        try {
          let body = '';
          for await (const chunk of req) body += chunk;
          const { messages, dashboardContext } = JSON.parse(body);
          const apiKey = process.env.ANTHROPIC_API_KEY;
          if (!apiKey) { res.statusCode = 500; res.end(JSON.stringify({ error: 'ANTHROPIC_API_KEY not set' })); return; }

          const systemPrompt = `You are a senior financial analyst AI assistant embedded in a Banks Dashboard for CloudPay.
You have access to the following real-time dashboard data:

${dashboardContext || 'No dashboard context provided.'}

Your capabilities:
1. **Answer questions** about the financial data shown on the dashboard — bank balances, cashflow, salary, vendors, collections, revenue, churn, pipeline, OKRs, etc.
2. **Create scenarios** — the user can ask "what if we reduce vendor spend by 10%?" or "what if we hire 5 more people?" and you model the impact on cashflow, runway, and net position.
3. **Recommend changes** — suggest optimizations to departments, salaries, vendors, collections timing, etc. based on the data.
4. **Explain trends** — interpret MoM or YoY changes, flag anomalies, and provide executive-level insights.

Rules:
- Always use EUR as the primary currency unless asked otherwise.
- Format numbers with € and thousands separators (e.g., €1,234,567).
- Be concise but thorough. Use bullet points for clarity.
- When creating scenarios, show the baseline vs. scenario comparison.
- If you don't have enough data to answer, say so clearly.
- Reference specific months, categories, or accounts when relevant.

CRITICAL — When the user asks you to CREATE A SCENARIO (e.g. "what if we cut R&D by 20%", "reduce hiring"):
You MUST be GRANULAR — adjust at department level, not just a blanket %. Use the department salary data provided to make specific, realistic recommendations.

After your analysis text, you MUST include EXACTLY this JSON block at the very end using TRIPLE backticks:

\`\`\`scenario
{"name":"Release 40 Playmakers Aug-Dec","headcountAdj":{"2026-08":{"Playmakers":-40}},"salaryDeptAdj":{"2026-08":{"Playmakers":-29}},"vendorCatAdj":{},"collPctByMonth":{},"salaryAdjPctByMonth":{},"pipelineMinProb":100}
\`\`\`

Supported fields:
- "name": short scenario name string
- "headcountAdj": HEADCOUNT adjustments per department per month. Object keyed by "YYYY-MM", each containing department name → delta people count. E.g. {"2026-08":{"Playmakers":-40}} means release 40 Playmakers starting August. Cascades forward (set once, applies to remaining months). ALWAYS include this when the user mentions releasing/hiring people.
- "salaryAdjPctByMonth": object with month keys "0"-"11" (Jan=0, Dec=11). Values are OVERALL % change to salary. Rarely used — prefer salaryDeptAdj for department-level.
- "salaryDeptAdj": DEPARTMENT-LEVEL salary % adjustments. Object keyed by "YYYY-MM", each containing department name → % adjustment. MUST match headcountAdj: pct = round(-N * (deptBudget/headcount) / deptBudget * 100) = round(-N/headcount * 100). E.g. releasing 40 from 139 Playmakers = round(-40/139*100) = -29%. Cascades forward. Department names must EXACTLY match the HEADCOUNT BY DEPARTMENT data.
- "vendorCatAdj": VENDOR CATEGORY adjustments. Object keyed by "YYYY-MM", each containing category name → % adjustment. Category names must EXACTLY match the VENDOR EXPENSES BY CATEGORY data. Cascades forward. HR-related categories (Welfare, Training) auto-adjust from headcount — no need to set manually.
- "collPctByMonth": object with month keys "0"-"11". Values are collection % where 100=normal. 80=20% less revenue.
- "pipelineMinProb": number 0-100. Pipeline inclusion threshold. 100=exclude pipeline.

CRITICAL RULES FOR HEADCOUNT SCENARIOS:
- When user says "release N people from Department X": ALWAYS calculate headcountAdj AND salaryDeptAdj together.
- Formula: salaryDeptAdj % = round(-N / departmentHeadcount * 100). Use the HEADCOUNT BY DEPARTMENT data.
- ONLY adjust the SPECIFIC department the user mentions. Do NOT touch other departments.
- Set the adjustment for the STARTING month only — it cascades forward automatically.
- For phased reductions (e.g. "over 120 days"): set increasing headcountAdj per month. E.g. month1: -10, month2: -25, month3: -35, month4: -40.
- HR vendor categories (Welfare, Training, Recruiting) auto-adjust proportionally — no need to set them manually.

IMPORTANT RULES:
- ALWAYS be granular: use "salaryDeptAdj" for salary cuts (not salaryAdjPctByMonth) and "vendorCatAdj" for vendor cuts.
- Use EXACT department and category names from the dashboard data.
- Show your math clearly: "Playmakers has 139 people, releasing 40 = -29% salary reduction".
The scenario will be auto-saved to the dashboard's Scenarios dropdown.

ADJUSTING EXISTING SCENARIOS:
If the user asks to modify, adjust, or refine an existing scenario (e.g. "reduce outsourcing more", "increase the cut to 20%", "make it more aggressive"), look at the ACTIVE SCENARIO data in the dashboard context. Build on top of the current adjustments — don't start from scratch. Merge your new recommendations with the existing scenario values. For example, if the current scenario has salaryDeptAdj for R&D at -10%, and the user says "cut R&D more", you might change it to -15%. Always show what changed from the previous scenario.`;

          const apiRes = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'x-api-key': apiKey,
              'anthropic-version': '2023-06-01',
            },
            body: JSON.stringify({
              model: 'claude-sonnet-4-20250514',
              max_tokens: 2048,
              system: systemPrompt,
              messages: messages.map((m     ) => ({ role: m.role, content: m.content })),
            }),
          });
          const response = await apiRes.json();
          if (response.error) throw new Error(response.error.message);

          const text = response.content.map((b     ) => b.type === 'text' ? b.text : '').join('');
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ reply: text }));
        } catch (e     ) {
          console.error('[Chat] Error:', e.message);
          res.statusCode = 500;
          res.end(JSON.stringify({ error: e.message }));
        }
      });
}

module.exports = { registerApiRoutes, apiCache, getCached, setCache, getNsClient, getSfClient, queueNsCall };
