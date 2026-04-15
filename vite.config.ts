import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'
import { execSync } from 'child_process'
import { config as dotenvConfig } from 'dotenv'

const IS_WINDOWS = process.platform === 'win32';
const PROJECT_DIR = IS_WINDOWS ? 'C:\\temp\\banks-dashboard' : __dirname;
dotenvConfig({ path: path.join(PROJECT_DIR, '.env') });

const nsClients: Record<number, any> = {};
let sfClient: any = null;
function getSfClient() {
  if (!sfClient) {
    try {
      const sfPath = path.resolve(__dirname, 'snowflake-api.cjs');
      delete require.cache[require.resolve(sfPath)]; // bust cache to pick up file changes
      const { createSnowflakeClient } = require(sfPath);
      sfClient = createSnowflakeClient(process.env);
    } catch (e: any) { console.error('[SF] Failed:', e.message); return null; }
  }
  return sfClient;
}
function getNsClient(subsidiaryId: number = 3) {
  if (!nsClients[subsidiaryId]) {
    const { createNetSuiteClient } = require('./netsuite-api.cjs');
    nsClients[subsidiaryId] = createNetSuiteClient(process.env, subsidiaryId);
  }
  return nsClients[subsidiaryId];
}
// Helper: parse subsidiary from query string (default 3 = LSPORTS)
function getSubsidiary(req: any): number {
  const url = new URL(req.url || '', 'http://localhost');
  return parseInt(url.searchParams.get('subsidiary') || '3') || 3;
}
// Helper: parse year from query string (default = current year)
function getYear(req: any): number {
  const url = new URL(req.url || '', 'http://localhost');
  return parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
}

// NetSuite request queue — serialize all NS API calls to avoid 429 rate limits
let nsQueue: Promise<any> = Promise.resolve();
function queueNsCall<T>(fn: () => Promise<T>): Promise<T> {
  const next = nsQueue.then(() => fn(), () => fn());
  nsQueue = next.catch(() => {}); // prevent queue from breaking on errors
  return next;
}

// Server-side response cache — avoids re-hitting NS/SF for 5 minutes
const apiCache = new Map<string, { data: any; ts: number }>();
const CACHE_TTL = 5 * 60 * 1000; // 5 min
function getCached(key: string): any | null {
  const entry = apiCache.get(key);
  if (entry && Date.now() - entry.ts < CACHE_TTL) return entry.data;
  return null;
}
function setCache(key: string, data: any) { apiCache.set(key, { data, ts: Date.now() }); }

function banksPlugin(): Plugin {
  return {
    name: 'banks-api',
    configureServer(server) {
      // ── GET /api/ns-config — expose NS account ID for register links ──
      server.middlewares.use('/api/ns-config', (_req, res) => {
        const accountId = (process.env.NETSUITE_ACCOUNT_ID || '').replace(/_/g, '-').toLowerCase();
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ accountId }));
      });

      // Helper: cached NS endpoint — returns cached response if fresh, else fetches and caches
      const cachedNsHandler = (path: string, fetchFn: (ns: any, req: any) => Promise<any>, fallback: any) => {
        server.middlewares.use(path, async (req: any, res: any) => {
          const sub = getSubsidiary(req);
          const cacheKey = `${path}:${sub}`;
          // Check for refresh=true query param to bypass cache
          const url = new URL(req.url || '', 'http://localhost');
          const forceRefresh = url.searchParams.get('refresh') === 'true';
          if (!forceRefresh) {
            const cached = getCached(cacheKey);
            if (cached) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(cached)); return; }
          }
          try {
            const ns = getNsClient(sub);
            const result = await fetchFn(ns, req);
            setCache(cacheKey, result);
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify(result));
          } catch (e: any) {
            console.error(`[NS API] ${path} fetch failed:`, e.message);
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ ...fallback, error: e.message }));
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
      server.middlewares.use('/api/vendor-history', async (req: any, res: any) => {
        const ck = `vendor-history:${getSubsidiary(req)}`; const cv = getCached(ck);
        if (cv) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(cv)); return; }
        try {
          const sub = getSubsidiary(req);
          const ns = getNsClient(sub);
          const history = await queueNsCall(() => ns.fetchVendorPaymentHistory());
          // For subsidiaries with no vendor bills (e.g. Statscore uses JEs only),
          // fall back to expense account debits from JEs (excluding salary 76xx)
          const totalVendorAmt = history.reduce((s: number, r: any) => s + (r.amountEUR || 0), 0);
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
            } catch (e2: any) { console.error('[NS API] JE expense fallback failed:', e2.message); }
          }
          const resp = { data: history, timestamp: new Date().toISOString() };
          setCache(ck, resp);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(resp));
        } catch (e: any) {
          console.error('[NS API] Vendor history fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/banks-collection-data — actual paid invoices (for current month actuals) ──
      server.middlewares.use('/api/banks-collection-data', async (req: any, res: any) => {
        const ck2 = `collection-data:${getSubsidiary(req)}`; const cv2 = getCached(ck2);
        if (cv2) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(cv2)); return; }
        try {
          const sub = getSubsidiary(req);
          const ns = getNsClient(sub);
          const data = await queueNsCall(() => ns.fetchCollectionData());
          // Group by month
          const byMonth: Record<string, number> = {};
          for (const r of data) {
            if (r.dateClosed) {
              const parts = r.dateClosed.split('/');
              if (parts.length === 3) {
                const m = `${parts[2]}-${parts[1].padStart(2, '0')}`;
                byMonth[m] = (byMonth[m] || 0) + (r.amountEUR || 0);
              }
            }
          }
          // For non-invoice subsidiaries (e.g. Statscore with JEs only), if no invoice collections found,
          // fall back to Income account credits (net revenue recognized) from transactionaccountingline
          if (Object.keys(byMonth).length === 0) {
            try {
              const revenueRows = await queueNsCall(() => ns.suiteqlAll(`
                SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS mkey,
                       SUM(COALESCE(tal.credit, 0)) - SUM(COALESCE(tal.debit, 0)) AS net_revenue
                FROM transactionaccountingline tal
                JOIN transaction t ON tal.transaction = t.id
                JOIN account a ON tal.account = a.id
                WHERE t.subsidiary = ${sub}
                  AND a.accttype = 'Income'
                  AND tal.posting = 'T'
                  AND tal.accountingbook = 1
                  AND t.trandate >= TO_DATE('${getYear(req)}-01-01', 'YYYY-MM-DD')
                GROUP BY TO_CHAR(t.trandate, 'YYYY-MM')
                ORDER BY TO_CHAR(t.trandate, 'YYYY-MM')
              `));
              for (const r of revenueRows) {
                if (r.mkey && parseFloat(r.net_revenue) > 0) {
                  byMonth[r.mkey] = Math.round(parseFloat(r.net_revenue));
                }
              }
            } catch (e2: any) { console.error('[NS API] Revenue fallback failed:', e2.message); }
          }
          const resp2 = { data: byMonth, timestamp: new Date().toISOString() };
          setCache(ck2, resp2);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(resp2));
        } catch (e: any) {
          console.error('[NS API] Collection data fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: {}, error: e.message }));
        }
      });

      // ── GET /api/ar-forecast — AR collection forecast (SO due dates) ──
      cachedNsHandler('/api/ar-forecast', async (ns) => {
        const soItems = await queueNsCall(() => ns.fetchAgingData());
        const forecast = soItems
          .filter((r: any) => r.type === 'Sales Order' && r.soDueDate)
          .map((r: any) => ({ customer: r.customer, amountEUR: r.amountUnbilledEUR || r.amountEUR, dueDate: r.soDueDate }));
        return { data: forecast, timestamp: new Date().toISOString() };
      }, { data: [] });

      // ── GET /api/expense-categories — monthly expenses by category ──
      cachedNsHandler('/api/expense-categories',
        async (ns) => ({ data: await queueNsCall(() => ns.fetchPaymentsByCategory()), timestamp: new Date().toISOString() }),
        { data: { byMonth: {}, categories: [], monthlyTotals: {} } }
      );

      // ── GET /api/ns-bank-accounts-asof — bank balances as of a specific date ──
      server.middlewares.use('/api/ns-bank-accounts-asof', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const ns = getNsClient(parseInt(url.searchParams.get('subsidiary') || '3') || 3);
          const asOf = url.searchParams.get('date');
          if (!ns || !asOf) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ data: [] })); return; }
          const accounts = await queueNsCall(() => ns.fetchBankAccountListAsOf(asOf));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: accounts }));
        } catch (e: any) {
          console.error('[NS API] Bank accounts as-of failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/ns-vendor-bills — vendor bills for specific account + month ──
      server.middlewares.use('/api/ns-vendor-bills', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const ns = getNsClient(parseInt(url.searchParams.get('subsidiary') || '3') || 3);
          const accountId = url.searchParams.get('accountId');
          const month = url.searchParams.get('month');
          if (!ns || !accountId || !month) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ data: [] })); return; }
          const result = await queueNsCall(() => ns.fetchVendorBillsByAccount(accountId, month));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: result.bills || [], nsAcctId: result.nsAcctId || null, queryError: result.queryError || null }));
        } catch (e: any) {
          console.error('[NS API] Vendor bills fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── DEBUG: Raw NS SuiteQL query ──
      server.middlewares.use('/api/debug-ns-sql', async (req, res) => {
        try {
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const sql = url.searchParams.get('sql') || '';
          const sub = parseInt(url.searchParams.get('subsidiary') || '6') || 6;
          const ns = getNsClient(sub);
          if (!sql) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ error: 'no sql' })); return; }
          const result = await queueNsCall(() => ns.suiteql(sql));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ items: result.items || [], count: (result.items || []).length }));
        } catch (e: any) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── DEBUG: Check override table columns ──
      server.middlewares.use('/api/debug-overrides', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ error: 'no sf client' })); return; }
          const rows = await sf.query(`SELECT * FROM DL_PRODUCTION.FINANCE.FCT_EXPENSE WHERE source IN ('future_cost_override','future_cost_increment') LIMIT 10`);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ count: rows.length, columns: rows.length > 0 ? Object.keys(rows[0]) : [], sample: rows }));
        } catch (e: any) { res.end(JSON.stringify({ error: e.message })); }
      });

      // ── Snowflake: Budget + Revenue ──
      server.middlewares.use('/api/sf-budget', async (_req, res) => {
        try {
          const sf = getSfClient();
          const yr = getYear(_req);
          if (!sf) { res.end(JSON.stringify({ data: { byMonth: {}, totalByMonth: {}, overrides: [] } })); return; }
          const [data, overrides] = await Promise.all([
            sf.fetchBudgetByCategory(yr),
            sf.fetchBudgetOverrides().catch(() => []),
          ]);
          // Apply overrides from FCT_EXPENSE (already have month + category per row)
          const appliedOverrides: any[] = [];
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
        } catch (e: any) { res.end(JSON.stringify({ data: { byMonth: {}, totalByMonth: {}, overrides: [] }, error: e.message })); }
      });

      server.middlewares.use('/api/sf-revenue', async (_req, res) => {
        try {
          const sf = getSfClient();
          const yr = getYear(_req);
          if (!sf) { res.end(JSON.stringify({ data: { budget: {}, actuals: {} } })); return; }
          const data = await sf.fetchRevenueProjection(yr);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) { res.end(JSON.stringify({ data: { budget: {}, actuals: {} }, error: e.message })); }
      });

      // ── Snowflake: Monthly actuals split (salary/vendors) ──
      server.middlewares.use('/api/sf-actuals-split', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: {} })); return; }
          const data = await sf.fetchMonthlyActualsSplit();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) { res.end(JSON.stringify({ data: {}, error: e.message })); }
      });

      // ── Snowflake: Vendor breakdown for a month ──
      server.middlewares.use('/api/sf-vendor-breakdown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          if (!sf || !month) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchVendorBreakdown(month);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) { res.end(JSON.stringify({ data: [], error: e.message })); }
      });

      // ── GET /api/sf-discover — list tables in a schema ──
      server.middlewares.use('/api/sf-discover', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const schema = url.searchParams.get('schema') || 'FINANCE';
          if (!sf) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.listTables('DL_PRODUCTION', schema);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-query — run arbitrary Snowflake SQL (dev only) ──
      server.middlewares.use('/api/sf-query', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const sql = url.searchParams.get('sql') || '';
          if (!sf || !sql) { res.end(JSON.stringify({ data: [], error: 'Missing sql param' })); return; }
          const data = await sf.fetchFinancialData(sql);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-conversion — Salesforce conversion rate analysis ──
      server.middlewares.use('/api/sf-conversion', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: null })); return; }
          const data = await sf.fetchConversionAnalysis();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[SF] Conversion analysis failed:', e.message);
          res.end(JSON.stringify({ data: null, error: e.message }));
        }
      });

      // ── GET /api/sf-won-opps — won opportunity details by year and type ──
      server.middlewares.use('/api/sf-won-opps', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const year = url.searchParams.get('year');
          const type = url.searchParams.get('type') || 'new'; // 'new' or 'upgrades'
          if (!sf || !year) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchWonOpportunitiesDetail(year, type);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[SF] Won opps detail failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-pipeline — open opportunities not closed-won ──
      server.middlewares.use('/api/sf-pipeline', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchOpenPipeline(getYear(_req));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[SF] Pipeline fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-revenue-paid — monthly revenue from FCT_MONTHLY_REVENUE__SUBSET_PAID ──
      server.middlewares.use('/api/sf-revenue-paid', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: {} })); return; }
          const data = await sf.fetchMonthlyRevenuePaid(getYear(_req));
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[SF] Revenue paid fetch failed:', e.message);
          res.end(JSON.stringify({ data: {}, error: e.message }));
        }
      });

      // ── GET /api/sf-revenue-breakdown — per-customer revenue for a month ──
      server.middlewares.use('/api/sf-revenue-breakdown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          const unpaidOnly = url.searchParams.get('unpaidOnly') === '1';
          if (!sf || !month) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchRevenueBreakdown(month, unpaidOnly);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[SF] Revenue breakdown fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-yoy-revenue — YoY revenue comparison for OKRs ──
      server.middlewares.use('/api/sf-yoy-revenue', async (req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({})); return; }
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const asOfDate = url.searchParams.get('asOfDate') || '';
          const result = await sf.fetchYoYRevenue(asOfDate || undefined);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(result));
        } catch (e: any) {
          console.error('[SF] YoY revenue fetch failed:', e.message);
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── GET /api/sf-churn-analysis — yearly churn rate and lost revenue ──
      server.middlewares.use('/api/sf-churn-analysis', async (req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.end(JSON.stringify({ data: [] })); return; }
          const result = await sf.fetchChurnAnalysis();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: result.yearly, recentMonthlyAvg: result.recentMonthlyAvg }));
        } catch (e: any) {
          console.error('[SF] Churn analysis fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-churn-drilldown — individual churned customers for a year ──
      server.middlewares.use('/api/sf-churn-drilldown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const year = url.searchParams.get('year');
          if (!sf || !year) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchChurnDrilldown(year);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[SF] Churn drilldown fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-headcount-by-dept — headcount per department with avg salary ──
      server.middlewares.use('/api/sf-headcount-by-dept', async (_req, res) => {
        try {
          const sf = getSfClient();
          if (!sf) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchHeadcountByDepartment();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-headcount-lever-detail — all events for a lever type (full year) ──
      // NOTE: must be registered before sf-headcount-events (prefix matching)
      server.middlewares.use('/api/sf-headcount-lever-detail', async (req, res) => {
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
        } catch (e: any) {
          console.error('[SF] Headcount lever detail fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-headcount-events — HiBob lever data for salary projection ──
      server.middlewares.use('/api/sf-headcount-events', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          if (!sf || !month) { res.end(JSON.stringify({ data: { events: [], cumulative: [], baseline: {} } })); return; }
          const yr = parseInt(url.searchParams.get('year') || '') || new Date().getFullYear();
          const data = await sf.fetchHeadcountEvents(month, yr);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[SF] Headcount events fetch failed:', e.message);
          res.end(JSON.stringify({ data: { events: [], cumulative: [], baseline: {} }, error: e.message }));
        }
      });

      // ── GET /api/sf-salary-budget-breakdown — per-account salary budget for a month ──
      // NOTE: must be registered before sf-salary-budget and sf-salary-breakdown (prefix matching)
      server.middlewares.use('/api/sf-salary-budget-breakdown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          if (!sf || !month) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchSalaryBudgetBreakdown(month);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[SF] Salary budget breakdown fetch failed:', e.message);
          res.end(JSON.stringify({ data: [], error: e.message }));
        }
      });

      // ── GET /api/sf-salary-budget — Snowflake salary budget per month ──
      // NOTE: must be registered before sf-salary-breakdown (prefix matching)
      server.middlewares.use('/api/sf-salary-budget', async (_req, res) => {
        try {
          const sf = getSfClient();
          const yr = getYear(_req);
          if (!sf) { res.end(JSON.stringify({ data: {} })); return; }
          const [data, overrides] = await Promise.all([
            sf.fetchSalaryBudget(yr),
            sf.fetchBudgetOverrides().catch(() => []),
          ]);
          // Apply payroll overrides only — new format has month + category per row
          let payrollAccounts = new Set<string>();
          try {
            const payrollRows = await sf.query(`SELECT DISTINCT GL_ACCOUNT_NUMBER FROM DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT WHERE IS_PAYROLL = TRUE`);
            payrollAccounts = new Set(payrollRows.map((r: any) => r.GL_ACCOUNT_NUMBER));
          } catch (_) {}
          const appliedOverrides: any[] = [];
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
        } catch (e: any) {
          console.error('[SF] Salary budget fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: {}, error: e.message }));
        }
      });

      // ── Currency defense budget (account 800029 from NetSuite) ──
      server.middlewares.use('/api/sf-finance-budget', async (_req, res) => {
        try {
          const ns3 = getNsClient(3);
          if (!ns3) { res.end(JSON.stringify({ data: {} })); return; }
          const data = await ns3.fetchCurrencyDefenseBudget();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) {
          console.error('[NS] Currency defense budget fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data: {}, error: e.message }));
        }
      });

      // ── Snowflake: Salary breakdown for a month ──
      server.middlewares.use('/api/sf-salary-breakdown', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          if (!sf || !month) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchSalaryBreakdown(month);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) { res.end(JSON.stringify({ data: [], error: e.message })); }
      });

      // ── GET /api/ns-budget — NS budget data (for subsidiaries without Snowflake) ──
      server.middlewares.use('/api/ns-budget', async (req, res) => {
        try {
          const ns = getNsClient(getSubsidiary(req));
          if (!ns.fetchNSBudget) { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ byMonth: {} })); return; }
          const data = await queueNsCall(() => ns.fetchNSBudget());
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(data));
        } catch (e: any) {
          console.error('[NS API] NS budget fetch failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ byMonth: {}, error: e.message }));
        }
      });

      // ── GET /api/ns-salary-breakdown — NS salary actuals + budget detail by account for a month ──
      server.middlewares.use('/api/ns-salary-breakdown', async (req, res) => {
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
          // Actuals: salary by account for the month
          const actRows = await queueNsCall(() => ns.suiteqlAll(`
            SELECT a.acctnumber, a.acctname,
                   SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) AS amount_eur
            FROM transactionaccountingline tal
            JOIN transaction t ON tal.transaction = t.id
            JOIN account a ON tal.account = a.id
            WHERE t.subsidiary = ${sub}
              AND tal.posting = 'T' AND tal.accountingbook = 1
              AND a.acctnumber LIKE '76%'
              AND a.acctnumber NOT IN ('760038', '760023')
              AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
              AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
            GROUP BY a.acctnumber, a.acctname
            HAVING SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) <> 0
            ORDER BY a.acctnumber
          `));
          const actuals = actRows.map((r: any) => ({
            account: r.acctnumber || '',
            name: r.acctname || '',
            amountEUR: Math.round(parseFloat(r.amount_eur) || 0),
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
          const budget = budRows.map((r: any) => ({
            account: r.acctnumber || '',
            name: r.acctname || '',
            amountEUR: Math.round(parseFloat(r.amount) || 0),
          })).filter((r: any) => r.amountEUR !== 0);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ actuals, budget }));
        } catch (e: any) {
          console.error('[NS API] NS salary breakdown failed:', e.message);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ actuals: [], budget: [], error: e.message }));
        }
      });

      // ── GET /api/consolidated-data — combined data for both subsidiaries + I/C elimination ──
      let consolidatedCache: { data: string; timestamp: number; cacheKey: string } | null = null;
      server.middlewares.use('/api/consolidated-data', async (_req, res) => {
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
          const budgetDirC = path.resolve(__dirname, 'data', 'budgets');
          const consolCurrentYear = new Date().getFullYear();

          // Helper to load snapshot for a subsidiary
          const loadSnapshot = (yr: number, co: string) => {
            const fp = path.resolve(budgetDirC, `${yr}-${co}.json`);
            if (fsM.existsSync(fp)) return JSON.parse(fsM.readFileSync(fp, 'utf-8'));
            return null;
          };

          // Load from snapshot if not current year, else from live NS
          let bankBalance3: any, bankAccounts3: any[], salary3: any[], vendorHistory3: any[], collections3: any[], nsBudget3: any, monthlyReval3: any;
          let bankBalance6: any, bankAccounts6: any[], salary6: any[], vendorHistory6: any[], collections6: any[], nsBudget6: any, monthlyReval6: any;

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
              queueNsCall(() => ns3.fetchBankBalance()).catch((e: any) => ({ openingBalance: 0, dailyBalances: [], currentBalance: 0, error: e.message })),
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
              queueNsCall(() => ns6.fetchBankBalance()).catch((e: any) => ({ openingBalance: 0, dailyBalances: [], currentBalance: 0, error: e.message })),
              queueNsCall(() => ns6.fetchBankAccountList()).catch(() => []),
              queueNsCall(() => ns6.fetchSalaryData()).catch(() => []),
              queueNsCall(() => ns6.fetchVendorPaymentHistory()).catch(() => []),
              queueNsCall(() => ns6.fetchCollectionData()).catch(() => []),
              ns6.fetchNSBudget ? queueNsCall(() => ns6.fetchNSBudget()).catch(() => ({ byMonth: {} })) : Promise.resolve({ byMonth: {} }),
              queueNsCall(() => ns6.fetchMonthlyRevaluation()).catch(() => ({ byMonth: {}, preYear: { eur: 0, ils: 0 } })),
            ]);
          }

          // Statscore vendor history fallback + collections (skip for snapshots — already populated)
          let collByMonth6: Record<string, number> = stSnap?.collections || {};
          if (!stSnap) {
            const totalVendor6 = vendorHistory6.reduce((s: number, r: any) => s + (r.amountEUR || 0), 0);
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
              } catch (e2: any) { console.error('[Consolidated] Statscore JE expense fallback failed:', e2.message); }
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
              } catch (e2: any) { console.error('[Consolidated] Statscore revenue fallback failed:', e2.message); }
            }
          }

          // LSports collections grouped by month
          const collByMonth3: Record<string, number> = lsSnap?.collections || {};
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
          let sfBudgetData = { totalByMonth: {} } as any;
          let sfRevenueData = {} as any;
          let sfActualsSplitData = {} as any;
          let sfSalaryBudgetData = {} as any;
          let sfRevenuePaidData = {} as any;
          let sfFinanceBudgetData = {} as any;
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
              let payrollAccounts = new Set<string>();
              try {
                const payrollRows = await sf.query(`SELECT DISTINCT GL_ACCOUNT_NUMBER FROM DL_PRODUCTION.FINANCE.DIM_GL_ACCOUNT WHERE IS_PAYROLL = TRUE`);
                payrollAccounts = new Set(payrollRows.map((r: any) => r.GL_ACCOUNT_NUMBER));
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
            } catch (e: any) { console.error('[Consolidated] SF data fetch failed:', e.message); }
          }

          // ── I/C Elimination: actual amounts from xElimination subsidiary (5) ──
          const icRevenueAccts = ['400017', '400020', '400022', '400023', '400025'];
          const icExpenseAccts = ['620005', '620012', '745002', '800007', '650003', '650005', '650006', '650008'];
          const icAllAccts = [...icRevenueAccts, ...icExpenseAccts];

          let actualICRows: any[] = [];
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
            `)).catch((e: any) => { console.error('[Consolidated] xElim IC query failed:', e.message); return []; });
          } catch (e: any) { console.error('[Consolidated] I/C elimination query failed:', e.message); }
          console.log(`[Consolidated] xElimination IC: ${actualICRows.length} rows`);

          // Process actual I/C by month from xElimination
          // Revenue accounts: debit = revenue elimination (reduces collections)
          // Expense accounts: credit = expense elimination (reduces vendors)
          const actualByMonth: Record<string, { revenue: number; expense: number; details: any[] }> = {};
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
          const projectedByMonth: Record<string, { revenue: number; expense: number; details: any[]; source: string }> = {};
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

              const budgetMonthMap: Record<string, string> = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
              };
              const icBudgetByMonth: Record<string, { revenue: number; expense: number; details: any[] }> = {};
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
          } catch (e: any) { console.error('[Consolidated] IC budget projection failed:', e.message); }

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
        } catch (e: any) {
          console.error('[Consolidated] Fatal error:', e.message);
          res.statusCode = 500;
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── GET /api/consolidated-account-breakdown — account-level detail for consolidated drilldown ──
      server.middlewares.use('/api/consolidated-account-breakdown', async (req, res) => {
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

          let lsRows: any[] = [];
          let stRows: any[] = [];

          if (type === 'salary') {
            // Salary: 76xx accounts
            const q = (sub: number) => `
              SELECT a.acctnumber, a.acctname,
                     SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) AS amount_eur
              FROM transactionaccountingline tal
              JOIN transaction t ON tal.transaction = t.id
              JOIN account a ON tal.account = a.id
              WHERE t.subsidiary = ${sub}
                AND tal.posting = 'T' AND tal.accountingbook = 1
                AND a.acctnumber LIKE '76%'
                AND a.acctnumber NOT IN ('760038', '760023')
                AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
                AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
              GROUP BY a.acctnumber, a.acctname
              HAVING SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) <> 0
              ORDER BY a.acctnumber`;
            [lsRows, stRows] = await Promise.all([
              queueNsCall(() => ns3.suiteqlAll(q(3))).catch((e: any) => { console.error('[Account Breakdown] LS salary query failed:', e.message); return []; }),
              queueNsCall(() => ns6.suiteqlAll(q(6))).catch((e: any) => { console.error('[Account Breakdown] ST salary query failed:', e.message); return []; }),
            ]);
          } else if (type === 'vendors') {
            // Vendors: Expense/OthExpense/COGS accounts, NOT salary (not 76xx)
            const q = (sub: number) => `
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
              queueNsCall(() => ns3.suiteqlAll(q(3))).catch((e: any) => { console.error('[Account Breakdown] LS vendors query failed:', e.message); return []; }),
              queueNsCall(() => ns6.suiteqlAll(q(6))).catch((e: any) => { console.error('[Account Breakdown] ST vendors query failed:', e.message); return []; }),
            ]);
          } else if (type === 'collections') {
            // Collections: Income accounts (credits)
            const q = (sub: number) => `
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
              queueNsCall(() => ns3.suiteqlAll(q(3))).catch((e: any) => { console.error('[Account Breakdown] LS collections query failed:', e.message); return []; }),
              queueNsCall(() => ns6.suiteqlAll(q(6))).catch((e: any) => { console.error('[Account Breakdown] ST collections query failed:', e.message); return []; }),
            ]);
          }

          const fmt = (rows: any[]) => rows.map((r: any) => ({
            account: r.acctnumber || '',
            name: r.acctname || '',
            amount: Math.round(parseFloat(r.amount_eur) || 0),
          }));

          console.log(`[Account Breakdown] type=${type} month=${month} ls=${lsRows.length} rows, st=${stRows.length} rows`);
          if (stRows.length === 0) console.log(`[Account Breakdown] ST returned 0 rows for ${type} ${month}`);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ ls: fmt(lsRows), st: fmt(stRows) }));
        } catch (e: any) {
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
      server.middlewares.use('/api/sf-budget-detail', async (req, res) => {
        try {
          const sf = getSfClient();
          const url = new URL(req.url || '', `http://${req.headers.host}`);
          const month = url.searchParams.get('month');
          const category = url.searchParams.get('category');
          if (!sf || !month || !category) { res.end(JSON.stringify({ data: [] })); return; }
          const data = await sf.fetchBudgetCategoryDetail(month, category);
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ data }));
        } catch (e: any) { res.end(JSON.stringify({ data: [], error: e.message })); }
      });

      // ── Scenarios CRUD Storage ──
      const scenariosPath = path.resolve(__dirname, 'data', 'scenarios.json');
      const fs = require('fs');

      const loadScenarios = (): { id: string; name: string; createdAt: string; updatedAt: string; data: any; ownerEmail: string; company?: string }[] => {
        try { return JSON.parse(fs.readFileSync(scenariosPath, 'utf-8')); } catch { return []; }
      };
      const saveScenarios = (scenarios: any[]) => {
        const dir = path.dirname(scenariosPath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(scenariosPath, JSON.stringify(scenarios, null, 2));
      };

      // GET /api/bank-dashboard-users — stub
      server.middlewares.use('/api/bank-dashboard-users', (_req: any, res: any) => {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ data: [] }));
      });

      // ── GET /api/budget-years — list available budget years per company ──
      server.middlewares.use('/api/budget-years', async (_req: any, res: any) => {
        try {
          const fs = await import('fs');
          const budgetDir = path.resolve(__dirname, 'data', 'budgets');
          if (!fs.existsSync(budgetDir)) fs.mkdirSync(budgetDir, { recursive: true });
          const files = fs.readdirSync(budgetDir).filter((f: string) => f.endsWith('.json'));
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
          const freshFiles = fs.readdirSync(budgetDir).filter((f: string) => f.endsWith('.json'));
          const byCompany: Record<string, number[]> = { lsports: [currentYear], statscore: [currentYear] };
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
        } catch (e: any) {
          const currentYear = new Date().getFullYear();
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ byCompany: { lsports: [currentYear], statscore: [currentYear] }, currentYear, error: e.message }));
        }
      });

      // ── /api/budget-snapshot-patch — update specific fields in existing snapshot ──
      server.middlewares.use('/api/budget-snapshot-patch', async (req: any, res: any) => {
        if (req.method !== 'POST') { res.statusCode = 405; res.end('Method not allowed'); return; }
        const fs = await import('fs');
        const budgetDir = path.resolve(__dirname, 'data', 'budgets');
        res.setHeader('Content-Type', 'application/json');
        let body = '';
        req.on('data', (chunk: any) => { body += chunk; });
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
          } catch (e: any) { res.end(JSON.stringify({ error: e.message })); }
        });
      });

      // ── /api/budget-snapshot — per-company roll forward, read, delete ──
      server.middlewares.use('/api/budget-snapshot', async (req: any, res: any) => {
        const fs = await import('fs');
        const budgetDir = path.resolve(__dirname, 'data', 'budgets');
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
          } catch (e: any) { res.end(JSON.stringify({ error: e.message })); }
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
          } catch (e: any) { res.end(JSON.stringify({ error: e.message })); }
          return;
        }

        // ── POST: create per-company snapshot (roll forward) ──
        try {
          let body = '';
          req.on('data', (chunk: any) => { body += chunk; });
          req.on('end', async () => {
            try {
              const { sourceYear, targetYear, company, clientDecClosing } = JSON.parse(body);
              if (!sourceYear || !targetYear || !company) { res.end(JSON.stringify({ error: 'sourceYear, targetYear, and company required' })); return; }
              // When clientDecClosing is not provided (e.g. refresh from source year),
              // preserve the existing snapshot's projectedDecClosing so the opening balance stays correct.
              // The server-side cashflow calc omits pipeline/churn/unpaid carry, so its estimate diverges.
              let existingDecClosing: number | undefined;
              if (!clientDecClosing) {
                const existingPath = path.resolve(budgetDir, `${targetYear}-${company}.json`);
                if (fs.existsSync(existingPath)) {
                  try {
                    const existing = JSON.parse(fs.readFileSync(existingPath, 'utf-8'));
                    if (existing.projectedDecClosing) {
                      existingDecClosing = existing.projectedDecClosing;
                      console.log(`[Budget] Preserving existing projected Dec closing: €${existingDecClosing!.toLocaleString()}`);
                    }
                  } catch {}
                }
              }
              console.log(`[Budget] Rolling forward ${company} ${sourceYear} → ${targetYear}...${clientDecClosing ? ` (client Dec closing: €${Math.round(clientDecClosing).toLocaleString()})` : ''}`);

              // Remap any YYYY-MM key to targetYear-MM (handles data from any source year)
              const remapMonths = (obj: Record<string, any>) => {
                const result: Record<string, any> = {};
                for (const [key, val] of Object.entries(obj)) {
                  const newKey = key.replace(/^\d{4}(-\d{2})$/, `${targetYear}$1`);
                  result[newKey] = val;
                }
                return result;
              };
              // Helper: get value from object by month index (0-11), ignoring the year in keys
              const getByMonthIdx = (obj: Record<string, any>, mi: number) => {
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
                let nsBudget3 = { byMonth: {} } as any;
                try { nsBudget3 = await queueNsCall(() => ns3.fetchNsBudget()); } catch {}
                // Fetch bank balance + salary + vendor history + collections for opening balance
                let bankBalance = { openingBalance: 0, dailyBalances: [], currentBalance: 0 } as any;
                let salary: any[] = [];
                let vendorHistory: any[] = [];
                let collections: Record<string, number> = {};
                let monthlyReval = { byMonth: {}, preYear: { eur: 0, ils: 0 } } as any;
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
                const salByIdx: Record<number, number> = {};
                for (const s of salary) {
                  if (s.month && s.amountEUR > 0) {
                    const m = parseInt(s.month.split('-')[1]) - 1;
                    if (!isNaN(m)) salByIdx[m] = s.amountEUR;
                  }
                }
                // Group vendor history by month index (sourceYear only)
                const vendByIdx: Record<number, number> = {};
                for (const v of vendorHistory) {
                  if (v.paidDate?.startsWith(`${sourceYear}`)) {
                    const m = parseInt(v.paidDate.substring(5, 7)) - 1;
                    if (!isNaN(m)) vendByIdx[m] = (vendByIdx[m] || 0) + (v.amountEUR || 0);
                  }
                }
                // Group collections by month index (sourceYear only)
                const collByIdx: Record<number, number> = {};
                for (const [k, v] of Object.entries(collections)) {
                  if (k.startsWith(`${sourceYear}`)) {
                    const m = parseInt(k.substring(5, 7)) - 1;
                    if (!isNaN(m)) collByIdx[m] = v as number;
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
                  // Salary budget: AVG of last 3 months (Oct-Dec) as flat baseline for all target year months
                  sfSalaryBudget: (() => {
                    const octSal = getByMonthIdx(sfSalaryBudget, 9)?.eur || salByIdx[9] || lastSal;
                    const novSal = getByMonthIdx(sfSalaryBudget, 10)?.eur || salByIdx[10] || lastSal;
                    const decSal = getByMonthIdx(sfSalaryBudget, 11)?.eur || salByIdx[11] || lastSal;
                    const avgSal = Math.round((octSal + novSal + decSal) / 3) || lastSal;
                    console.log(`[Budget] LS salary baseline: avg(Oct €${octSal.toLocaleString()}, Nov €${novSal.toLocaleString()}, Dec €${decSal.toLocaleString()}) = €${avgSal.toLocaleString()}`);
                    const flat: Record<string, { eur: number }> = {};
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
                    const flat: Record<string, { eur: number }> = {};
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
                    const flat: Record<string, any> = {};
                    for (let m = 1; m <= 12; m++) flat[`${targetYear}-${String(m).padStart(2, '0')}`] = { revenue: avgRev, paid: avgRev, unpaid: 0, customers: avgCust };
                    return flat;
                  })(),
                  sfFinanceBudget: remapMonths(sfFinanceBudget || {}),
                  nsBudget: { byMonth: remapMonths(nsBudget3.byMonth || {}) },
                  sfPipeline: sfPipeline,
                  sfConversion: sfConversion,
                  bankBalance,
                  salary: salary.map((s: any) => ({ ...s, month: s.month?.replace(/^\d{4}/, `${targetYear}`) })),
                  vendorHistory: vendorHistory.map((v: any) => ({ ...v, paidDate: v.paidDate?.replace(/^\d{4}/, `${targetYear}`) })),
                  collections: remapMonths(collections),
                  monthlyReval: { byMonth: remapMonths(monthlyReval.byMonth || {}), preYear: { eur: 0, ils: 0 } },
                };
                fs.writeFileSync(path.resolve(budgetDir, `${targetYear}-lsports.json`), JSON.stringify(snapshot, null, 2));
              } else {
                // Statscore
                const ns6 = getNsClient(6);
                let nsBudget6 = { byMonth: {} } as any;
                try { nsBudget6 = await queueNsCall(() => ns6.fetchNsBudget()); } catch {}
                let bankBalance6 = { openingBalance: 0, dailyBalances: [], currentBalance: 0 } as any;
                let salary6: any[] = [];
                let vendorHistory6: any[] = [];
                let collections6: Record<string, number> = {};
                let monthlyReval6 = { byMonth: {}, preYear: { eur: 0, ils: 0 } } as any;
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
                const salByIdx6: Record<number, number> = {};
                for (const s of salary6) {
                  if (s.month && s.amountEUR > 0) { const m = parseInt(s.month.split('-')[1]) - 1; if (!isNaN(m)) salByIdx6[m] = s.amountEUR; }
                }
                const vendByIdx6: Record<number, number> = {};
                for (const v of vendorHistory6) {
                  if (v.paidDate?.startsWith(`${sourceYear}`)) { const m = parseInt(v.paidDate.substring(5, 7)) - 1; if (!isNaN(m)) vendByIdx6[m] = (vendByIdx6[m] || 0) + (v.amountEUR || 0); }
                }
                const collByIdx6: Record<number, number> = {};
                for (const [k, v] of Object.entries(collections6)) {
                  if (k.startsWith(`${sourceYear}`)) { const m = parseInt(k.substring(5, 7)) - 1; if (!isNaN(m)) collByIdx6[m] = v as number; }
                }
                // ── Compute avg baselines from last 3 months with actual data ──
                const avgLast3 = (byIdx: Record<number, number>) => {
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
                const nsBudgetFlat6: Record<string, any> = {};
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
                const collectionsFlat6: Record<string, number> = {};
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
                  salary: salary6.map((s: any) => ({ ...s, month: s.month?.replace(/^\d{4}/, `${targetYear}`) })),
                  vendorHistory: vendorHistory6.map((v: any) => ({ ...v, paidDate: v.paidDate?.replace(/^\d{4}/, `${targetYear}`) })),
                  collections: collectionsFlat6,
                  monthlyReval: { byMonth: remapMonths(monthlyReval6.byMonth || {}), preYear: { eur: 0, ils: 0 } },
                };
                fs.writeFileSync(path.resolve(budgetDir, `${targetYear}-statscore.json`), JSON.stringify(snapshot, null, 2));
              }
              console.log(`[Budget] Snapshot saved: data/budgets/${targetYear}-${company}.json`);
              res.end(JSON.stringify({ success: true, targetYear, company, status: 'draft' }));
            } catch (e: any) {
              console.error('[Budget] Snapshot creation failed:', e.message);
              res.end(JSON.stringify({ error: e.message }));
            }
          });
        } catch (e: any) { res.end(JSON.stringify({ error: e.message })); }
      });

      // /api/scenarios — CRUD
      server.middlewares.use('/api/scenarios', async (req: any, res: any) => {
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
            const existing = scenarios.findIndex((s: any) => s.id === id);
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
            const idx = scenarios.findIndex((s: any) => s.id === pathParts[0]);
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
            const filtered = scenarios.filter((s: any) => s.id !== pathParts[0]);
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
        } catch (e: any) {
          console.error('[Scenarios API] Error:', e.message);
          res.statusCode = 500;
          res.end(JSON.stringify({ error: e.message }));
        }
      });

      // ── Chat History Storage ──
      const chatHistoryPath = path.resolve(__dirname, 'chat-history.json');

      const loadChatHistory = (): { id: string; title: string; messages: any[]; createdAt: string; updatedAt: string }[] => {
        try { return JSON.parse(fs.readFileSync(chatHistoryPath, 'utf-8')); } catch { return []; }
      };
      const saveChatHistory = (history: any[]) => {
        fs.writeFileSync(chatHistoryPath, JSON.stringify(history, null, 2));
      };

      // GET /api/chat-history — list all conversations
      server.middlewares.use('/api/chat-history', async (req, res) => {
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
      server.middlewares.use('/api/chat', async (req, res) => {
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
              messages: messages.map((m: any) => ({ role: m.role, content: m.content })),
            }),
          });
          const response = await apiRes.json();
          if (response.error) throw new Error(response.error.message);

          const text = response.content.map((b: any) => b.type === 'text' ? b.text : '').join('');
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ reply: text }));
        } catch (e: any) {
          console.error('[Chat] Error:', e.message);
          res.statusCode = 500;
          res.end(JSON.stringify({ error: e.message }));
        }
      });
    },
  };
}

const gitHash = (() => { try { return execSync('git rev-parse --short HEAD', { cwd: __dirname }).toString().trim(); } catch { return 'unknown'; } })();

export default defineConfig({
  root: path.resolve(__dirname),
  plugins: [react(), tailwindcss(), banksPlugin()],
  define: { '__GIT_HASH__': JSON.stringify(gitHash) },
  resolve: { alias: { 'xlsx': 'xlsx-js-style' } },
  build: { chunkSizeWarningLimit: 2000 },
  server: { port: 5176 },
})
