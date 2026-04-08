/**
 * NetSuite API module — TBA OAuth 1.0 + SuiteQL
 * Used by vite.config.ts server middleware
 */
const crypto = require('crypto');

const EXCHANGE_RATES = { EUR: 1.0, USD: 0.92, ILS: 0.24 };

function createNetSuiteClient(env, subsidiaryId = 3) {
  const ACCOUNT_ID = env.NETSUITE_ACCOUNT_ID;
  // Support per-subsidiary credentials: NETSUITE_CONSUMER_KEY_6, NETSUITE_TOKEN_ID_6, etc.
  const CONSUMER_KEY = env[`NETSUITE_CONSUMER_KEY_${subsidiaryId}`] || env.NETSUITE_CONSUMER_KEY;
  const CONSUMER_SECRET = env[`NETSUITE_CONSUMER_SECRET_${subsidiaryId}`] || env.NETSUITE_CONSUMER_SECRET;
  const TOKEN_ID = env[`NETSUITE_TOKEN_ID_${subsidiaryId}`] || env.NETSUITE_TOKEN_ID;
  const TOKEN_SECRET = env[`NETSUITE_TOKEN_SECRET_${subsidiaryId}`] || env.NETSUITE_TOKEN_SECRET;
  const BASE_URL = `https://${ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql`;

  function genAuth(method, url) {
    const ts = Math.floor(Date.now() / 1000).toString();
    const nonce = crypto.randomBytes(16).toString('hex');
    const params = {
      oauth_consumer_key: CONSUMER_KEY, oauth_token: TOKEN_ID,
      oauth_nonce: nonce, oauth_timestamp: ts,
      oauth_signature_method: 'HMAC-SHA256', oauth_version: '1.0',
    };
    const ps = Object.keys(params).sort().map(k => encodeURIComponent(k) + '=' + encodeURIComponent(params[k])).join('&');
    const bs = [method.toUpperCase(), encodeURIComponent(url), encodeURIComponent(ps)].join('&');
    const sk = encodeURIComponent(CONSUMER_SECRET) + '&' + encodeURIComponent(TOKEN_SECRET);
    params.oauth_signature = crypto.createHmac('sha256', sk).update(bs).digest('base64');
    return 'OAuth realm="' + ACCOUNT_ID + '", ' + Object.keys(params).sort().map(k => k + '="' + encodeURIComponent(params[k]) + '"').join(', ');
  }

  async function suiteql(query, timeoutMs = 60000) {
    const auth = genAuth('POST', BASE_URL);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const resp = await fetch(BASE_URL, {
      method: 'POST',
      headers: { 'Authorization': auth, 'Content-Type': 'application/json', 'Prefer': 'transient' },
      body: JSON.stringify({ q: query }),
      signal: controller.signal,
    });
    clearTimeout(timer);
    if (!resp.ok) {
      const err = await resp.text();
      throw new Error(`SuiteQL ${resp.status}: ${err.substring(0, 300)}`);
    }
    return resp.json();
  }

  // Paginated SuiteQL — fetches all results
  async function suiteqlAll(query, pageSize = 1000) {
    let allItems = [];
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const pagedQuery = query.replace(/FETCH FIRST \d+ ROWS ONLY/i, '') + ` OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY`;
      const auth = genAuth('POST', BASE_URL);
      const resp = await fetch(BASE_URL, {
        method: 'POST',
        headers: { 'Authorization': auth, 'Content-Type': 'application/json', 'Prefer': 'transient' },
        body: JSON.stringify({ q: pagedQuery }),
      });
      if (!resp.ok) {
        const err = await resp.text();
        throw new Error(`SuiteQL ${resp.status}: ${err.substring(0, 300)}`);
      }
      const result = await resp.json();
      allItems = allItems.concat(result.items || []);
      hasMore = result.hasMore === true;
      offset += pageSize;
    }
    return allItems;
  }

  /** Fetch aging data — SuiteQL matching saved search 1887 filters */
  async function fetchAgingData() {
    console.log('[NS API] Fetching aging data via SuiteQL...');

    // Query 1: Pending Billing Sales Orders
    // Join transactionLine with mainline='T' to get header-level data and avoid duplicates
    // Filter on header status to exclude Closed (H), Billed (G), Cancelled (C)
    const soItems = await suiteqlAll(`
      SELECT t.id, t.tranid, c.companyname, t.type,
             t.foreigntotal as amount_foreign,
             tl.foreignamount as line_amount,
             t.foreigntotal as amount_base,
             (t.foreigntotal - NVL(t.foreignamountpaid, 0)) as amount_remaining,
             ROUND(SYSDATE - t.trandate) as days_open,
             t.status,
             TO_CHAR(t.trandate, 'DD/MM/YYYY') as tran_date,
             cur.symbol as currency,
             TO_CHAR(t.startdate, 'DD/MM/YYYY') as start_date,
             TO_CHAR(t.enddate, 'DD/MM/YYYY') as end_date,
             TO_CHAR(t.custbody_so_due_date, 'DD/MM/YYYY') as so_due_date,
             t.exchangerate as xrate
      FROM transaction t
      JOIN transactionLine tl ON tl.transaction = t.id AND tl.mainline = 'T'
      LEFT JOIN customer c ON t.entity = c.id
      LEFT JOIN currency cur ON t.currency = cur.id
      WHERE t.type = 'SalesOrd'
        AND t.status IN ('F')
        AND t.subsidiary = ${subsidiaryId}
        AND t.trandate >= TO_DATE('2026-01-01', 'YYYY-MM-DD')
      ORDER BY ROUND(SYSDATE - t.trandate) DESC
    `);

    // Query 2: Open + Pending Approval Invoices
    const invItems = await suiteqlAll(`
      SELECT t.id, t.tranid, c.companyname, t.type,
             t.foreigntotal as amount_foreign,
             tl.foreignamount as line_amount,
             t.foreigntotal as amount_base,
             (t.foreigntotal - NVL(t.foreignamountpaid, 0)) as amount_remaining,
             ROUND(SYSDATE - t.trandate) as days_open,
             t.status,
             TO_CHAR(t.trandate, 'DD/MM/YYYY') as tran_date,
             cur.symbol as currency,
             TO_CHAR(t.startdate, 'DD/MM/YYYY') as start_date,
             TO_CHAR(t.enddate, 'DD/MM/YYYY') as end_date,
             TO_CHAR(t.custbody_so_due_date, 'DD/MM/YYYY') as so_due_date,
             t.exchangerate as xrate
      FROM transaction t
      JOIN transactionLine tl ON tl.transaction = t.id AND tl.mainline = 'T'
      LEFT JOIN customer c ON t.entity = c.id
      LEFT JOIN currency cur ON t.currency = cur.id
      WHERE t.type = 'CustInvc'
        AND t.status IN ('A', 'D')
        AND t.subsidiary = ${subsidiaryId}
        AND t.trandate >= TO_DATE('2026-01-01', 'YYYY-MM-DD')
      ORDER BY ROUND(SYSDATE - t.trandate) DESC
    `);

    const soStatusMap = { 'F': 'Pending Billing', 'E': 'Pending Approval' };
    const invStatusMap = { 'A': 'Open', 'D': 'Pending Approval' };

    function mapRecord(r, type, statusMap) {
      const cur = r.currency || 'EUR';
      // Use NetSuite's own exchange rate per transaction (not hardcoded)
      const xrate = parseFloat(r.xrate) || 1;
      const amountForeign = parseFloat(r.amount_foreign) || 0;  // in transaction currency
      const unbilledForeign = parseFloat(r.line_amount) || amountForeign;
      // Use remaining amount (unbilled for SOs, unpaid for invoices) when available
      const remainingForeign = parseFloat(r.amount_remaining);
      // Convert to EUR using NetSuite's rate
      const amountEUR = Math.round(amountForeign * xrate * 100) / 100;
      const unbilledEUR = Math.round(unbilledForeign * xrate * 100) / 100;
      // If remaining amount is available, use it; otherwise fall back to line amount (SOs) or total (invoices)
      const remainingEUR = !isNaN(remainingForeign) ? Math.round(remainingForeign * xrate * 100) / 100 : null;
      const sd = r.start_date || '';
      const ed = r.end_date || '';
      const cleanName = (r.companyname || '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');

      return {
        internalId: parseInt(r.id) || 0,
        documentNumber: r.tranid || '',
        customer: cleanName,
        type,
        amount: amountEUR,
        amountEUR,
        amountUnbilled: remainingEUR !== null ? remainingEUR : (type === 'Sales Order' ? unbilledEUR : amountEUR),
        amountUnbilledEUR: remainingEUR !== null ? remainingEUR : (type === 'Sales Order' ? unbilledEUR : amountEUR),
        originalAmount: !isNaN(remainingForeign) ? remainingForeign : amountForeign,
        days: parseInt(r.days_open) || 0,
        status: statusMap[r.status] || r.status,
        date: r.tran_date || '',
        currency: cur,
        dateClosed: '',
        startDate: sd,
        endDate: ed,
        billingPeriod: sd && ed ? `${sd} to ${ed}` : '',
        soDueDate: r.so_due_date || '',
      };
    }

    const allRecords = [
      ...soItems.map(r => mapRecord(r, 'Sales Order', soStatusMap)),
      ...invItems.map(r => mapRecord(r, 'Invoice', invStatusMap)),
    ];

    // Sort by days descending
    allRecords.sort((a, b) => b.days - a.days);

    console.log(`[NS API] Fetched ${soItems.length} SOs + ${invItems.length} invoices = ${allRecords.length} total`);
    return allRecords;
  }

  /** Fetch collection data — invoices (saved search 1916 equivalent) */
  async function fetchCollectionData() {
    console.log(`[NS API] fetchCollectionData called for subsidiary=${subsidiaryId}`);
    const items = await suiteqlAll(`
      SELECT t.id, t.tranid, c.companyname, t.type, t.foreigntotal,
             (t.foreigntotal - NVL(t.foreignamountpaid, 0)) as amount_remaining,
             ROUND(SYSDATE - t.trandate) as days_open, t.status,
             TO_CHAR(t.trandate, 'DD/MM/YYYY') as tran_date,
             TO_CHAR(t.closedate, 'DD/MM/YYYY') as close_date,
             cur.symbol as currency,
             TO_CHAR(t.startdate, 'DD/MM/YYYY') as start_date,
             TO_CHAR(t.enddate, 'DD/MM/YYYY') as end_date
      FROM transaction t
      LEFT JOIN customer c ON t.entity = c.id
      LEFT JOIN currency cur ON t.currency = cur.id
      WHERE t.type = 'CustInvc'
        AND t.subsidiary = ${subsidiaryId}
        AND t.trandate >= TO_DATE('2026-01-01', 'YYYY-MM-DD')
        AND t.entity NOT IN (3819, 11063)
      ORDER BY t.trandate
    `);

    // Status mapping: A=Open, B=Paid In Full, etc.
    const statusMap = { 'A': 'Open', 'B': 'Paid In Full', 'V': 'Rejected', 'E': 'Pending Approval', 'X': 'Fully Applied' };

    return items.map(r => {
      const cur = r.currency || 'EUR';
      const rate = EXCHANGE_RATES[cur] || 1;
      const amount = parseFloat(r.amount_remaining) || parseFloat(r.foreigntotal) || 0;
      return {
        dateClosed: r.close_date || '',
        amountEUR: Math.round(amount * rate * 100) / 100,
        currency: cur,
        type: 'Invoice',
        status: statusMap[r.status] || r.status,
        customer: (r.companyname || '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&'),
        documentNumber: r.tranid || '',
        date: r.tran_date || '',
      };
    });
  }

  /** Build collection.json structure from raw collection data */
  function buildCollectionJson(collectionRecords) {
    const dailyByMonth = {};
    const openByMonth = {};
    const paidByMonth = {};
    let totalPaid = 0;

    collectionRecords.forEach(r => {
      if (r.status === 'Paid In Full' && r.dateClosed) {
        const [dd, mm, yy] = r.dateClosed.split('/').map(Number);
        const monthKey = `${yy}-${String(mm).padStart(2, '0')}`;
        if (!dailyByMonth[monthKey]) dailyByMonth[monthKey] = {};
        dailyByMonth[monthKey][dd] = (dailyByMonth[monthKey][dd] || 0) + r.amountEUR;
        paidByMonth[monthKey] = (paidByMonth[monthKey] || 0) + r.amountEUR;
        totalPaid += r.amountEUR;
      }

      if (r.status === 'Open' || r.status === 'Pending Approval') {
        if (r.date) {
          const [, mm, yy] = r.date.split('/').map(Number);
          const monthKey = `${yy}-${String(mm).padStart(2, '0')}`;
          openByMonth[monthKey] = (openByMonth[monthKey] || 0) + r.amountEUR;
        }
      }
    });

    // Round all values
    for (const m of Object.keys(dailyByMonth)) {
      for (const d of Object.keys(dailyByMonth[m])) {
        dailyByMonth[m][d] = Math.round(dailyByMonth[m][d]);
      }
    }
    for (const m of Object.keys(paidByMonth)) paidByMonth[m] = Math.round(paidByMonth[m]);
    for (const m of Object.keys(openByMonth)) openByMonth[m] = Math.round(openByMonth[m]);

    return { dailyByMonth, openByMonth, paidByMonth, totalPaid: Math.round(totalPaid) };
  }

  /** Detect per-client payment anomalies based on SO issuance → invoice payment gap */
  async function fetchClientAnomalies(agingData) {
    // Get last 4 paid invoices per client (ranked by close date)
    const history = await suiteqlAll(`
      SELECT companyname,
             COUNT(*) as paid_count,
             ROUND(AVG(days_to_pay)) as avg_so_to_payment,
             ROUND(MAX(days_to_pay)) as max_so_to_payment,
             ROUND(MIN(days_to_pay)) as min_so_to_payment
      FROM (
        SELECT c.companyname,
               inv.closedate - so.trandate as days_to_pay,
               ROW_NUMBER() OVER (PARTITION BY c.companyname ORDER BY inv.closedate DESC) as rn
        FROM transaction inv
        JOIN transactionLine tl ON tl.transaction = inv.id AND tl.mainline = 'T'
        JOIN transaction so ON tl.createdfrom = so.id AND so.type = 'SalesOrd'
        LEFT JOIN customer c ON inv.entity = c.id
        WHERE inv.type = 'CustInvc' AND inv.status = 'B'
          AND inv.subsidiary = ${subsidiaryId}
          AND inv.closedate IS NOT NULL
          AND inv.entity NOT IN (3819, 11063)
      )
      WHERE rn <= 4
      GROUP BY companyname
      HAVING COUNT(*) >= 1
    `);

    // Group current open SOs/invoices by client
    const openByClient = {};
    agingData.forEach(r => {
      if (!openByClient[r.customer]) openByClient[r.customer] = { items: [], totalEUR: 0, maxDays: 0 };
      openByClient[r.customer].items.push(r);
      openByClient[r.customer].totalEUR += r.amountUnbilledEUR;
      openByClient[r.customer].maxDays = Math.max(openByClient[r.customer].maxDays, r.days);
    });

    // Find anomalies: current SO age exceeds historical SO→payment pattern
    const anomalies = [];
    history.forEach(h => {
      const name = (h.companyname || '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
      const open = openByClient[name];
      if (!open) return;

      const avgCycle = parseInt(h.avg_so_to_payment) || 0;
      const maxCycle = parseInt(h.max_so_to_payment) || 0;
      const paidCount = parseInt(h.paid_count) || 0;

      // Anomaly detection — multiple sensitivity levels
      const overdueDays = open.maxDays - avgCycle;
      const beyondMax = open.maxDays - maxCycle;

      // Trigger anomaly if ANY of these conditions met:
      // 1. Current age exceeds average cycle by 7+ days
      // 2. Current age exceeds max cycle (even by 1 day)
      // 3. Current age > 30 days and > average + 5 days
      const isAnomaly = (
        overdueDays >= 7 ||           // 7+ days beyond average
        beyondMax > 0 ||              // exceeded worst historical payment
        (open.maxDays > 30 && overdueDays >= 5)  // older than 30d and 5+ beyond avg
      );

      if (isAnomaly && open.maxDays > 7) {  // Minimum 7 days old
        // Severity based on how far beyond average
        let severity = 'low';
        if (overdueDays > 30 || beyondMax > 14) severity = 'high';
        else if (overdueDays > 14 || beyondMax > 7) severity = 'medium';

        anomalies.push({
          customer: name,
          avgCycleDays: avgCycle,
          maxCycleDays: maxCycle,
          currentMaxDays: open.maxDays,
          overdueDays,
          beyondMax,
          openItems: open.items.length,
          openAmountEUR: Math.round(open.totalEUR),
          paidInvoices: paidCount,
          severity,
        });
      }
    });

    // Also flag clients with NO payment history but open items > 30 days
    const clientsWithHistory = new Set(history.map(h => (h.companyname || '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&')));
    Object.entries(openByClient).forEach(([name, open]) => {
      if (clientsWithHistory.has(name)) return; // Already checked above
      if (open.maxDays > 30) {
        anomalies.push({
          customer: name,
          avgCycleDays: 0,
          maxCycleDays: 0,
          currentMaxDays: open.maxDays,
          overdueDays: open.maxDays,
          beyondMax: 0,
          openItems: open.items.length,
          openAmountEUR: Math.round(open.totalEUR),
          paidInvoices: 0,
          severity: open.maxDays > 45 ? 'high' : open.maxDays > 30 ? 'medium' : 'low',
          noHistory: true,
        });
      }
    });

    // Calculate global average SO issuance → payment days
    const globalAvg = history.reduce((s, h) => {
      const avg = parseInt(h.avg_so_to_payment) || 0;
      const count = parseInt(h.paid_count) || 0;
      return { totalDays: s.totalDays + (avg * count), totalCount: s.totalCount + count };
    }, { totalDays: 0, totalCount: 0 });
    const avgDaysToPayment = globalAvg.totalCount > 0 ? Math.round(globalAvg.totalDays / globalAvg.totalCount) : 0;

    return {
      anomalies: anomalies.sort((a, b) => b.overdueDays - a.overdueDays),
      avgDaysToPayment,
      paidInvoiceCount: globalAvg.totalCount,
    };
  }

  // Fetch ALL SOs by billing period (includes billed/closed)
  async function fetchAllSOsByBillingPeriod() {
    const query = `
      SELECT
        t.id,
        t.tranid AS doc_number,
        BUILTIN.DF(t.entity) AS customer,
        t.foreigntotal AS amount,
        BUILTIN.DF(t.status) AS status_text,
        TO_CHAR(t.trandate, 'DD/MM/YYYY') AS trandate,
        BUILTIN.DF(t.currency) AS currency,
        TO_CHAR(t.startdate, 'DD/MM/YYYY') AS start_date,
        TO_CHAR(t.enddate, 'DD/MM/YYYY') AS end_date,
        EXTRACT(MONTH FROM t.startdate) AS period_month,
        EXTRACT(YEAR FROM t.startdate) AS period_year
      FROM transaction t
      WHERE t.type = 'SalesOrd'
        AND t.mainline = 'T'
        AND t.subsidiary = ${subsidiaryId}
        AND t.startdate >= TO_DATE('2025-01-01', 'YYYY-MM-DD')
      ORDER BY t.startdate DESC
    `;
    const rows = await suiteqlAll(query);

    // Group by billing period month
    const byPeriod = {};
    rows.forEach(r => {
      if (!r.period_month || !r.period_year) return;
      const key = `${r.period_year}-${String(r.period_month).padStart(2, '0')}`;
      if (!byPeriod[key]) byPeriod[key] = { count: 0, total: 0, statuses: {} };
      byPeriod[key].count++;
      byPeriod[key].total += parseFloat(r.amount) || 0;
      const st = r.status_text || 'Unknown';
      byPeriod[key].statuses[st] = (byPeriod[key].statuses[st] || 0) + 1;
    });

    return { totalRecords: rows.length, byPeriod };
  }

  /** Fetch monthly revenue from GL — accounts under 400000, subsidiary 3, book 1 */
  async function fetchRevenueData() {
    console.log('[NS API] Fetching revenue data from GL...');
    const REVENUE_ACCOUNTS = [54, 784, 785, 786, 787, 788, 789, 790, 791, 907, 911, 925, 963, 1078, 1079, 1080, 1081, 1082, 1083, 1089, 386];
    const accountIds = REVENUE_ACCOUNTS.join(', ');

    // 2025 is closed — hardcoded to avoid slow GL queries
    const REVENUE_2025 = {
      '2025-01': 5430388, '2025-02': 3255606, '2025-03': 3785220,
      '2025-04': 3680433, '2025-05': 3936884, '2025-06': 4801214,
      '2025-07': 4964882, '2025-08': 4380851, '2025-09': 4398932,
      '2025-10': 4272920, '2025-11': 5050977, '2025-12': 4418666,
    };

    // For 2026: cache closed months, only query current month live
    const now = new Date();
    const currentMonth = now.getMonth() + 1; // 1-based
    const currentYear = now.getFullYear();

    // Read cached 2026 months
    const cachePath = require('path').join(__dirname, 'data', `revenue-cache-sub${subsidiaryId}.json`);
    let cached2026 = {};
    try {
      if (require('fs').existsSync(cachePath)) {
        cached2026 = JSON.parse(require('fs').readFileSync(cachePath, 'utf-8'));
      }
    } catch {}

    // Determine which months need querying (current month + any missing closed months)
    const monthsToQuery = [];
    for (let m = 1; m <= currentMonth; m++) {
      const key = `2026-${String(m).padStart(2, '0')}`;
      // Always query current month; query past months if not cached
      if (m === currentMonth || !cached2026[key]) {
        monthsToQuery.push(m);
      }
    }

    if (monthsToQuery.length > 0) {
      const minMonth = Math.min(...monthsToQuery);
      const result = await suiteql(`
        SELECT EXTRACT(MONTH FROM t.trandate) AS month, 2026 AS year,
               SUM(tal.credit) - SUM(NVL(tal.debit, 0)) AS net_revenue
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        WHERE tal.account IN (${accountIds})
          AND t.subsidiary = ${subsidiaryId}
          AND t.trandate >= TO_DATE('2026-${String(minMonth).padStart(2, '0')}-01', 'YYYY-MM-DD')
          AND tal.posting = 'T'
          AND tal.accountingbook = 1
        GROUP BY EXTRACT(MONTH FROM t.trandate)
        ORDER BY month
      `, 90000);

      (result.items || []).forEach(r => {
        const key = `2026-${String(r.month).padStart(2, '0')}`;
        cached2026[key] = Math.round(parseFloat(r.net_revenue) || 0);
      });

      // Save cache (closed months persist, current month gets refreshed next time)
      try {
        const { mkdirSync, existsSync: ex, writeFileSync: wf } = require('fs');
        const dir = require('path').join(__dirname, 'data');
        if (!ex(dir)) mkdirSync(dir, { recursive: true });
        wf(cachePath, JSON.stringify(cached2026, null, 2));
      } catch {}
    }

    // Merge all data
    const byYearMonth = { ...REVENUE_2025, ...cached2026 };

    console.log(`[NS API] Revenue data: ${Object.keys(byYearMonth).length} months`);
    return byYearMonth;
  }

  /** Fetch MRR — Sales Orders raised per month (2026+) excluding I/C and Closed */
  async function fetchMRRData() {
    const items = await suiteqlAll(`
      SELECT d.month, d.year, COUNT(*) AS so_count, SUM(d.foreigntotal) AS total_amount
      FROM (
        SELECT DISTINCT t.id, EXTRACT(MONTH FROM t.trandate) AS month,
               EXTRACT(YEAR FROM t.trandate) AS year, t.foreigntotal
        FROM transaction t
        WHERE t.type = 'SalesOrd'
          AND t.subsidiary = ${subsidiaryId}
          AND t.trandate >= TO_DATE('2026-01-01', 'YYYY-MM-DD')
          AND t.entity NOT IN (3819, 11063)
          AND t.status IN ('F', 'G')
      ) d
      GROUP BY d.month, d.year
      ORDER BY d.year, d.month
    `);
    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return items.map(r => ({
      month: monthNames[(parseInt(r.month) || 1) - 1] + ' ' + r.year,
      monthKey: `${r.year}-${String(r.month).padStart(2, '0')}`,
      count: parseInt(r.so_count) || 0,
      amount: Math.round(parseFloat(r.total_amount) || 0),
    }));
  }

  /** Fetch daily bank balance for chart — both primary (EUR) and local (ILS) books */
  async function fetchBankBalance() {
    async function fetchBookBalance(bookId) {
      const openingResult = await suiteql(`
        SELECT SUM(tal.amount) AS opening_balance
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = ${bookId}
          AND t.type NOT IN ('FxReval')
          AND t.trandate < TO_DATE('2026-01-01', 'YYYY-MM-DD')
      `);
      const openingBalance = parseFloat(openingResult?.items?.[0]?.opening_balance) || 0;

      const movements = await suiteqlAll(`
        SELECT TO_CHAR(t.trandate, 'YYYY-MM-DD') AS dt,
               SUM(tal.amount) AS daily_movement
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = ${bookId}
          AND t.type NOT IN ('FxReval')
          AND t.trandate >= TO_DATE('2026-01-01', 'YYYY-MM-DD')
          AND t.trandate <= SYSDATE
        GROUP BY TO_CHAR(t.trandate, 'YYYY-MM-DD')
        ORDER BY dt
      `);

      let runningBalance = openingBalance;
      const dailyBalances = movements.map(m => {
        runningBalance += parseFloat(m.daily_movement) || 0;
        return {
          date: m.dt,
          balance: Math.round(runningBalance),
          movement: Math.round(parseFloat(m.daily_movement) || 0),
        };
      });

      // Use unrounded running total for currentBalance to avoid accumulated rounding errors
      const currentBalance = Math.round(runningBalance);
      return { openingBalance: Math.round(openingBalance), dailyBalances, currentBalance };
    }

    const [primary, local] = await Promise.all([
      fetchBookBalance(1),
      fetchBookBalance(2),
    ]);

    // Get FxReval impact — check if current month has been revalued
    let revalData = { lastRevalDate: '', lastRevalImpact: 0, unrevalSince: '', estimatedMissing: 0 };
    try {
      const revals = await suiteqlAll(`
        SELECT TO_CHAR(t.trandate, 'YYYY-MM-DD') AS dt,
               SUM(tal.amount) AS reval_impact,
               COUNT(DISTINCT t.id) AS txn_count
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = 1
          AND t.type = 'FxReval'
          AND t.trandate >= ADD_MONTHS(SYSDATE, -3)
        GROUP BY TO_CHAR(t.trandate, 'YYYY-MM-DD')
        ORDER BY dt DESC
      `);

      if (revals.length > 0) {
        // Find the latest month-end reval (positive amount, not a reversal)
        const monthEndRevals = revals.filter(r => parseFloat(r.reval_impact) > 0);
        if (monthEndRevals.length > 0) {
          const lastReval = monthEndRevals[0];
          revalData.lastRevalDate = lastReval.dt;
          revalData.lastRevalImpact = Math.round(parseFloat(lastReval.reval_impact));

          // Check if current month has been revalued
          const today = new Date();
          const lastRevalMonth = new Date(lastReval.dt).getMonth();
          const currentMonth = today.getMonth();
          if (lastRevalMonth < currentMonth || (lastRevalMonth === 11 && currentMonth === 0)) {
            revalData.unrevalSince = lastReval.dt;
            revalData.estimatedMissing = revalData.lastRevalImpact; // Use last reval as estimate
          }
        }
      }
    } catch (e) { console.error('FxReval check failed:', e.message); }

    // Get FxReval for local book (ILS) too
    let revalDataLocal = { lastRevalDate: '', lastRevalImpact: 0, unrevalSince: '', estimatedMissing: 0 };
    try {
      const revalsLocal = await suiteqlAll(`
        SELECT TO_CHAR(t.trandate, 'YYYY-MM-DD') AS dt,
               SUM(tal.amount) AS reval_impact
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = 2
          AND t.type = 'FxReval'
          AND t.trandate >= ADD_MONTHS(SYSDATE, -3)
        GROUP BY TO_CHAR(t.trandate, 'YYYY-MM-DD')
        ORDER BY dt DESC
      `);
      if (revalsLocal.length > 0) {
        const monthEndRevals = revalsLocal.filter(r => parseFloat(r.reval_impact) > 0);
        if (monthEndRevals.length > 0) {
          const lastReval = monthEndRevals[0];
          revalDataLocal.lastRevalDate = lastReval.dt;
          revalDataLocal.lastRevalImpact = Math.round(parseFloat(lastReval.reval_impact));
          const today = new Date();
          const lastRevalMonth = new Date(lastReval.dt).getMonth();
          if (lastRevalMonth < today.getMonth() || (lastRevalMonth === 11 && today.getMonth() === 0)) {
            revalDataLocal.unrevalSince = lastReval.dt;
            revalDataLocal.estimatedMissing = revalDataLocal.lastRevalImpact;
          }
        }
      }
    } catch (e) { console.error('FxReval local check failed:', e.message); }

    // Apply reval adjustments to both books
    function applyRevalAdjustment(book, reval) {
      const adjusted = { ...book };
      if (reval.estimatedMissing && adjusted.dailyBalances.length > 0) {
        const revalDate = new Date(reval.unrevalSince);
        adjusted.dailyBalances = adjusted.dailyBalances.map(d => {
          const dayDate = new Date(d.date);
          if (dayDate > revalDate) {
            return { ...d, adjustedBalance: d.balance + reval.estimatedMissing };
          }
          return { ...d, adjustedBalance: d.balance };
        });
        adjusted.adjustedCurrentBalance = adjusted.currentBalance + reval.estimatedMissing;
      }
      return adjusted;
    }

    const adjustedPrimary = applyRevalAdjustment(primary, revalData);
    const adjustedLocal = applyRevalAdjustment(local, revalDataLocal);

    return {
      primary: { ...adjustedPrimary, currency: 'EUR', label: 'Primary Book (EUR)' },
      local: { ...adjustedLocal, currency: 'ILS', label: 'Local Book (ILS)' },
      revaluation: revalData,
      revaluationLocal: revalDataLocal,
      openingBalance: primary.openingBalance,
      dailyBalances: primary.dailyBalances,
      currentBalance: primary.currentBalance,
    };
  }

  // ── Accounts Payable: Open vendor bills ──
  async function fetchVendorBills() {
    console.log('[NS API] Fetching open vendor bills...');

    // Get bill headers
    const items = await suiteqlAll(`
      SELECT t.id, t.tranid, v.companyname AS vendor,
             t.foreigntotal AS amount, t.exchangerate AS xrate,
             ROUND(SYSDATE - t.trandate) AS days_open,
             t.status,
             TO_CHAR(t.trandate, 'YYYY-MM-DD') AS tran_date,
             TO_CHAR(t.duedate, 'YYYY-MM-DD') AS due_date,
             cur.symbol AS currency
      FROM transaction t
      JOIN transactionLine tl ON tl.transaction = t.id AND tl.mainline = 'T'
      LEFT JOIN vendor v ON t.entity = v.id
      LEFT JOIN currency cur ON t.currency = cur.id
      WHERE t.type = 'VendBill'
        AND t.status IN ('A')
        AND t.subsidiary = ${subsidiaryId}
      ORDER BY ROUND(SYSDATE - t.trandate) DESC
    `);

    // Get ILS amounts from local book (book 2) for these bills
    let ilsMap = {};
    if (items.length > 0) {
      try {
        const ids = items.map(r => r.id).join(',');
        const ilsRows = await suiteqlAll(`
          SELECT tal.transaction AS txn_id, SUM(tal.amount) AS amount_ils
          FROM transactionaccountingline tal
          JOIN account a ON tal.account = a.id
          WHERE tal.transaction IN (${ids})
            AND tal.accountingbook = 2
            AND tal.posting = 'T'
            AND a.accttype = 'AcctPay'
          GROUP BY tal.transaction
        `);
        for (const r of ilsRows) {
          ilsMap[r.txn_id] = Math.round(parseFloat(r.amount_ils) || 0);
        }
      } catch (e) { console.error('[NS API] ILS amounts fetch failed:', e.message); }
    }

    console.log(`[NS API] Vendor bills: ${items.length} open bills`);
    return items.map(r => {
      const cur = r.currency || 'EUR';
      const xrate = parseFloat(r.xrate) || 1;
      const amount = parseFloat(r.amount) || 0;
      const amountEUR = Math.round(amount * xrate * 100) / 100;
      const vendor = (r.vendor || '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
      return {
        internalId: r.id,
        documentNumber: r.tranid,
        vendor,
        amount: Math.abs(amount),
        amountEUR: Math.abs(amountEUR),
        amountILS: Math.abs(ilsMap[r.id] || 0),
        daysOpen: parseInt(r.days_open) || 0,
        status: r.status === 'A' ? 'Open' : r.status,
        tranDate: r.tran_date,
        dueDate: r.due_date,
        currency: cur,
      };
    });
  }

  // ── Accounts Payable: Paid vendor bills history (for forecasting) ──
  async function fetchVendorPaymentHistory() {
    console.log('[NS API] Fetching vendor payment history...');
    const fs = require('fs');
    const pathMod = require('path');
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const twoYearsAgo = now.getFullYear() - 2;

    // ── 1. Monthly aggregated totals — with cache ──
    const cachePath = pathMod.join(__dirname, 'data', `vendor-history-cache-sub${subsidiaryId}.json`);
    let cached = {};
    try { if (fs.existsSync(cachePath)) cached = JSON.parse(fs.readFileSync(cachePath, 'utf-8')); } catch {}

    // Determine which months need querying
    const monthsNeeded = [];
    for (let y = twoYearsAgo; y <= now.getFullYear(); y++) {
      const maxM = y === now.getFullYear() ? now.getMonth() + 1 : 12;
      for (let m = 1; m <= maxM; m++) {
        const key = `${y}-${String(m).padStart(2, '0')}`;
        if (key === currentMonth || !cached[key]) monthsNeeded.push(key);
      }
    }

    if (monthsNeeded.length > 0) {
      const minMonth = monthsNeeded[0];
      console.log(`[NS API] Vendor history: querying ${monthsNeeded.length} months from ${minMonth} (${Object.keys(cached).length} cached)`);

      const monthlyTotals = await suiteqlAll(`
        SELECT TO_CHAR(t.closedate, 'YYYY-MM') AS paid_month,
               COUNT(*) AS bill_count,
               SUM(ABS(t.foreigntotal * t.exchangerate)) AS total_eur,
               ROUND(AVG(t.closedate - t.trandate)) AS avg_days_to_pay
        FROM transaction t
        JOIN transactionLine tl ON tl.transaction = t.id AND tl.mainline = 'T'
        WHERE t.type = 'VendBill'
          AND t.status = 'B'
          AND t.subsidiary = ${subsidiaryId}
          AND t.closedate >= TO_DATE('${minMonth}-01', 'YYYY-MM-DD')
          AND t.closedate <= SYSDATE
        GROUP BY TO_CHAR(t.closedate, 'YYYY-MM')
        ORDER BY paid_month
      `);

      // Store queried months in cache — including months with 0 bills (to avoid re-querying)
      const queriedSet = new Set(monthlyTotals.map(mt => mt.paid_month));
      for (const mt of monthlyTotals) {
        cached[mt.paid_month] = {
          totalEUR: Math.round(parseFloat(mt.total_eur) || 0),
          billCount: parseInt(mt.bill_count) || 0,
          avgDaysToPay: parseInt(mt.avg_days_to_pay) || 0,
        };
      }
      // Mark months with no results as zero so they won't be re-queried
      for (const key of monthsNeeded) {
        if (!queriedSet.has(key) && key !== currentMonth) {
          cached[key] = { totalEUR: 0, billCount: 0, avgDaysToPay: 0 };
        }
      }

      // Save cache
      try {
        const dir = pathMod.join(__dirname, 'data');
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(cachePath, JSON.stringify(cached, null, 2));
      } catch {}
    } else {
      console.log(`[NS API] Vendor history: all ${Object.keys(cached).length} months from cache`);
    }

    // ── 2. Individual recent bills (last 6 months) — always live for drilldowns ──
    const sixMonthsAgo = new Date();
    sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
    const sixMonthStr = `${sixMonthsAgo.getFullYear()}-${String(sixMonthsAgo.getMonth() + 1).padStart(2, '0')}-01`;

    const items = await suiteqlAll(`
      SELECT v.companyname AS vendor,
             TO_CHAR(t.trandate, 'YYYY-MM-DD') AS bill_date,
             TO_CHAR(t.closedate, 'YYYY-MM-DD') AS paid_date,
             t.foreigntotal AS amount, t.exchangerate AS xrate,
             ROUND(t.closedate - t.trandate) AS days_to_pay,
             ROUND(t.closedate - t.duedate) AS days_past_due,
             cur.symbol AS currency
      FROM transaction t
      JOIN transactionLine tl ON tl.transaction = t.id AND tl.mainline = 'T'
      LEFT JOIN vendor v ON t.entity = v.id
      LEFT JOIN currency cur ON t.currency = cur.id
      WHERE t.type = 'VendBill'
        AND t.status = 'B'
        AND t.subsidiary = ${subsidiaryId}
        AND t.closedate >= TO_DATE('${sixMonthStr}', 'YYYY-MM-DD')
      ORDER BY t.closedate DESC
    `);
    console.log(`[NS API] Vendor history: ${Object.keys(cached).length} months (cached), ${items.length} recent detail bills`);

    // Build result: detail records + synthetic records from cached months not covered by detail
    const detailRecords = items.map(r => {
      const cur = r.currency || 'EUR';
      const xrate = parseFloat(r.xrate) || 1;
      const amount = parseFloat(r.amount) || 0;
      const vendor = (r.vendor || '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
      return {
        vendor, billDate: r.bill_date, paidDate: r.paid_date,
        amount: Math.abs(amount), amountEUR: Math.abs(Math.round(amount * xrate * 100) / 100),
        daysToPay: parseInt(r.days_to_pay) || 0, daysPastDue: parseInt(r.days_past_due) || 0, currency: cur,
      };
    });

    const detailMonths = new Set(detailRecords.map(r => r.paidDate?.slice(0, 7)).filter(Boolean));
    for (const [month, v] of Object.entries(cached)) {
      if (!detailMonths.has(month)) {
        detailRecords.push({
          vendor: '__MONTHLY_AGGREGATE__',
          billDate: `${month}-15`, paidDate: `${month}-15`,
          amount: v.totalEUR, amountEUR: v.totalEUR,
          daysToPay: v.avgDaysToPay, daysPastDue: 0, currency: 'EUR',
        });
      }
    }

    return detailRecords;
  }

  // ── Banks: Per-account balances for both books ──
  async function fetchBankAccountList() {
    console.log('[NS API] Fetching bank account list...');
    async function fetchBookAccounts(bookId) {
      return await suiteqlAll(`
        SELECT a.id, a.acctname, a.acctnumber,
               SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) AS balance
        FROM transactionaccountingline tal
        JOIN transaction t ON t.id = tal.transaction
        JOIN account a ON a.id = tal.account
        WHERE a.accttype = 'Bank'
          AND t.subsidiary = ${subsidiaryId}
          AND tal.accountingbook = ${bookId}
          AND tal.posting = 'T'
        GROUP BY a.id, a.acctname, a.acctnumber
        ORDER BY a.acctname
      `);
    }

    const [primaryAccounts, localAccounts] = await Promise.all([
      fetchBookAccounts(1),
      fetchBookAccounts(2),
    ]);

    // Merge by account id
    const accountMap = {};
    for (const a of primaryAccounts) {
      accountMap[a.id] = {
        id: a.id,
        name: (a.acctname || '').replace(/&amp;/g, '&'),
        number: a.acctnumber || '',
        primaryBalance: Math.round(parseFloat(a.balance) || 0),
        localBalance: 0,
      };
    }
    for (const a of localAccounts) {
      if (!accountMap[a.id]) {
        accountMap[a.id] = {
          id: a.id,
          name: (a.acctname || '').replace(/&amp;/g, '&'),
          number: a.acctnumber || '',
          primaryBalance: 0,
          localBalance: 0,
        };
      }
      accountMap[a.id].localBalance = Math.round(parseFloat(a.balance) || 0);
    }

    const accounts = Object.values(accountMap).sort((a, b) => a.name.localeCompare(b.name));
    console.log(`[NS API] Bank accounts: ${accounts.length} accounts`);
    return accounts;
  }

  // Fetch bank account balances as of a specific date (end of day)
  async function fetchBankAccountListAsOf(asOfDate) {
    console.log(`[NS API] Fetching bank account list as of ${asOfDate}...`);
    async function fetchBookAccounts(bookId) {
      const q = `
        SELECT a.id, a.acctname, a.acctnumber,
               SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) AS balance
        FROM transactionaccountingline tal
        JOIN transaction t ON t.id = tal.transaction
        JOIN account a ON a.id = tal.account
        WHERE a.accttype = 'Bank'
          AND t.subsidiary = ${subsidiaryId}
          AND tal.accountingbook = ${bookId}
          AND tal.posting = 'T'
          AND t.trandate <= TO_DATE('${asOfDate}', 'YYYY-MM-DD')
        GROUP BY a.id, a.acctname, a.acctnumber
        ORDER BY a.acctname
      `;
      console.log(`[NS API] Bank as-of query book ${bookId} starting...`);
      const result = await suiteql(q, 120000);
      const items = result.items || [];
      console.log(`[NS API] Bank as-of query book ${bookId} returned ${items.length} rows`);
      return items;
    }

    const [primaryAccounts, localAccounts] = await Promise.all([
      fetchBookAccounts(1),
      fetchBookAccounts(2),
    ]);

    const accountMap = {};
    for (const a of primaryAccounts) {
      accountMap[a.id] = {
        id: a.id,
        name: (a.acctname || '').replace(/&amp;/g, '&'),
        number: a.acctnumber || '',
        primaryBalance: Math.round(parseFloat(a.balance) || 0),
        localBalance: 0,
      };
    }
    for (const a of localAccounts) {
      if (!accountMap[a.id]) {
        accountMap[a.id] = {
          id: a.id,
          name: (a.acctname || '').replace(/&amp;/g, '&'),
          number: a.acctnumber || '',
          primaryBalance: 0,
          localBalance: 0,
        };
      }
      accountMap[a.id].localBalance = Math.round(parseFloat(a.balance) || 0);
    }

    const accounts = Object.values(accountMap).sort((a, b) => a.name.localeCompare(b.name));
    console.log(`[NS API] Bank accounts as of ${asOfDate}: ${accounts.length} accounts`);
    return accounts;
  }

  // ── Monthly salary/payroll expenses (accounts 76xxxx) — with cache ──
  async function fetchSalaryData() {
    console.log('[NS API] Fetching salary data...');
    const fs = require('fs');
    const pathMod = require('path');
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const prevYear = now.getFullYear() - 1;

    // Read cache
    const cachePath = pathMod.join(__dirname, 'data', `salary-cache-sub${subsidiaryId}.json`);
    let cached = {};
    try { if (fs.existsSync(cachePath)) cached = JSON.parse(fs.readFileSync(cachePath, 'utf-8')); } catch {}

    // Determine which months need querying: current month + any missing past months
    const monthsNeeded = [];
    for (let y = prevYear; y <= now.getFullYear(); y++) {
      const maxM = y === now.getFullYear() ? now.getMonth() + 1 : 12;
      for (let m = 1; m <= maxM; m++) {
        const key = `${y}-${String(m).padStart(2, '0')}`;
        if (key === currentMonth || !cached[key]) monthsNeeded.push(key);
      }
    }

    if (monthsNeeded.length > 0) {
      const minMonth = monthsNeeded[0];
      console.log(`[NS API] Salary: querying ${monthsNeeded.length} months from ${minMonth} (${Object.keys(cached).length} cached)`);

      const eurRows = await suiteqlAll(`
        SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS month,
               SUM(tal.debit) - SUM(tal.credit) AS amount
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = 1
          AND a.acctnumber LIKE '76%'
          AND a.acctnumber NOT IN ('760038', '760023')
          AND t.trandate >= TO_DATE('${minMonth}-01', 'YYYY-MM-DD')
          AND t.trandate <= SYSDATE
        GROUP BY TO_CHAR(t.trandate, 'YYYY-MM')
        ORDER BY month
      `);
      const ilsRows = await suiteqlAll(`
        SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS month,
               SUM(tal.debit) - SUM(tal.credit) AS amount
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = 2
          AND a.acctnumber LIKE '76%'
          AND a.acctnumber NOT IN ('760038', '760023')
          AND t.trandate >= TO_DATE('${minMonth}-01', 'YYYY-MM-DD')
          AND t.trandate <= SYSDATE
        GROUP BY TO_CHAR(t.trandate, 'YYYY-MM')
        ORDER BY month
      `);
      const ilsMap = {};
      for (const r of ilsRows) ilsMap[r.month] = Math.round(parseFloat(r.amount) || 0);
      for (const r of eurRows) {
        if (r.amount != null) {
          cached[r.month] = { amountEUR: Math.round(parseFloat(r.amount) || 0), amountILS: ilsMap[r.month] || 0 };
        }
      }

      // Save cache
      try {
        const dir = pathMod.join(__dirname, 'data');
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(cachePath, JSON.stringify(cached, null, 2));
      } catch {}
    } else {
      console.log(`[NS API] Salary: all ${Object.keys(cached).length} months from cache`);
    }

    // Convert cache to array sorted by month
    const data = Object.entries(cached)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([month, v]) => ({ month, amountEUR: v.amountEUR, amountILS: v.amountILS }));

    console.log(`[NS API] Salary data: ${data.length} months`);
    return data;
  }

  // ── Monthly bank cashflow (excluding internal transfers and FX reval) ──
  // Uses net-per-transaction approach: for each transaction, compute the net
  // impact on bank accounts. If debit ≈ credit (inter-bank transfer), it's
  // excluded by the HAVING clause. Only genuine external flows remain.
  async function fetchCashflowHistory() {
    console.log('[NS API] Fetching cashflow history...');
    const prevYear = new Date().getFullYear() - 1;

    const rows = await suiteqlAll(`
      SELECT month,
             SUM(CASE WHEN net_bank < 0 THEN ABS(net_bank) ELSE 0 END) AS total_outflow,
             SUM(CASE WHEN net_bank > 0 THEN net_bank ELSE 0 END) AS total_inflow
      FROM (
        SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS month,
               t.id,
               SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) AS net_bank
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = 1
          AND t.type NOT IN ('Transfer', 'FxReval')
          AND t.trandate >= TO_DATE('${prevYear}-01-01', 'YYYY-MM-DD')
          AND t.trandate <= SYSDATE
        GROUP BY TO_CHAR(t.trandate, 'YYYY-MM'), t.id
        HAVING ABS(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) > 1
      )
      GROUP BY month
      ORDER BY month
    `);

    const data = rows.map(r => ({
      month: r.month,
      outflow: Math.round(parseFloat(r.total_outflow) || 0),
      inflow: Math.round(parseFloat(r.total_inflow) || 0),
      net: Math.round((parseFloat(r.total_inflow) || 0) - (parseFloat(r.total_outflow) || 0)),
    }));

    console.log(`[NS API] Cashflow history: ${data.length} months`);
    return data;
  }

  // ── Monthly expense breakdown by category (for vendor projection) ──
  const EXPENSE_CATEGORIES = {
    '640': 'Cloud & Servers', '650': 'Collection Services', '745': 'Sales Commissions',
    '660': 'Professional Services', '620': 'Software', '700': 'Travel', '710': 'Office & Facilities',
    '780': 'Employee Benefits', '600': 'Marketing', '610': 'Marketing', '630': 'HR Activities',
    '670': 'Communications', '680': 'Training', '685': 'Training', '730': 'Insurance', '750': 'Events & PR',
  };

  async function fetchExpenseCategoryData() {
    console.log('[NS API] Fetching expense category data...');
    const fs = require('fs');
    const pathMod = require('path');
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const startYear = now.getFullYear() - 1;

    // Cache
    const cachePath = pathMod.join(__dirname, 'data', `expense-category-cache-sub${subsidiaryId}.json`);
    let cached = {};
    try { if (fs.existsSync(cachePath)) cached = JSON.parse(fs.readFileSync(cachePath, 'utf-8')); } catch {}

    // Determine months to query
    const monthsNeeded = [];
    for (let y = startYear; y <= now.getFullYear(); y++) {
      const maxM = y === now.getFullYear() ? now.getMonth() + 1 : 12;
      for (let m = 1; m <= maxM; m++) {
        const key = `${y}-${String(m).padStart(2, '0')}`;
        if (key === currentMonth || !cached[key]) monthsNeeded.push(key);
      }
    }

    if (monthsNeeded.length > 0) {
      const minMonth = monthsNeeded[0];
      console.log(`[NS API] Expenses: querying ${monthsNeeded.length} months from ${minMonth} (${Object.keys(cached).length} cached)`);

      const rows = await suiteqlAll(`
        SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS month,
               a.acctnumber, a.acctname,
               ROUND(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) AS amount_eur
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = 1
          AND a.accttype IN ('Expense', 'OthExpense', 'COGS')
          AND a.acctnumber NOT LIKE '76%'
          AND a.acctnumber NOT LIKE '800%'
          AND a.acctnumber NOT IN ('780502')
          AND t.trandate >= TO_DATE('${minMonth}-01', 'YYYY-MM-DD')
          AND t.trandate <= SYSDATE
        GROUP BY TO_CHAR(t.trandate, 'YYYY-MM'), a.acctnumber, a.acctname
        HAVING ABS(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) > 10
        ORDER BY month, a.acctnumber
      `);

      // Clear months being re-queried (especially current month) to avoid accumulation
      for (const key of monthsNeeded) cached[key] = {};

      // Group by month → category
      for (const r of rows) {
        const month = r.month;
        if (!cached[month]) cached[month] = {};
        const prefix = r.acctnumber.substring(0, 3);
        const category = EXPENSE_CATEGORIES[prefix] || `Other (${prefix})`;
        cached[month][category] = (cached[month][category] || 0) + Math.round(parseFloat(r.amount_eur) || 0);
      }
      // Mark empty months
      for (const key of monthsNeeded) {
        if (!cached[key] && key !== currentMonth) cached[key] = {};
      }

      try {
        const dir = pathMod.join(__dirname, 'data');
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(cachePath, JSON.stringify(cached, null, 2));
      } catch {}
    } else {
      console.log(`[NS API] Expenses: all ${Object.keys(cached).length} months from cache`);
    }

    // Build structured output
    const allCategories = new Set();
    const monthlyTotals = {};
    for (const [month, cats] of Object.entries(cached)) {
      monthlyTotals[month] = 0;
      for (const [cat, amt] of Object.entries(cats)) {
        allCategories.add(cat);
        monthlyTotals[month] += amt;
      }
    }
    // Sort categories by total amount descending
    const catTotals = {};
    for (const [, cats] of Object.entries(cached)) {
      for (const [cat, amt] of Object.entries(cats)) catTotals[cat] = (catTotals[cat] || 0) + amt;
    }
    const categories = [...allCategories].sort((a, b) => (catTotals[b] || 0) - (catTotals[a] || 0));

    console.log(`[NS API] Expense data: ${Object.keys(cached).length} months, ${categories.length} categories`);
    return { byMonth: cached, categories, monthlyTotals, categoryMapping: EXPENSE_CATEGORIES };
  }

  // ── Payment-based expense categories: VendPymt → VendBill → expense accounts ──
  // This traces actual bank payments back to the bills they paid, then to expense categories
  async function fetchPaymentsByCategory() {
    console.log('[NS API] Fetching payment-based expense categories...');
    const fs = require('fs');
    const pathMod = require('path');
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const startYear = now.getFullYear() - 1;

    // Cache
    const cachePath = pathMod.join(__dirname, 'data', `payment-category-cache-sub${subsidiaryId}.json`);
    let cached = {};
    try { if (fs.existsSync(cachePath)) cached = JSON.parse(fs.readFileSync(cachePath, 'utf-8')); } catch {}

    // Determine months to query
    const monthsNeeded = [];
    for (let y = startYear; y <= now.getFullYear(); y++) {
      const maxM = y === now.getFullYear() ? now.getMonth() + 1 : 12;
      for (let m = 1; m <= maxM; m++) {
        const key = `${y}-${String(m).padStart(2, '0')}`;
        if (key === currentMonth || !cached[key]) monthsNeeded.push(key);
      }
    }

    if (monthsNeeded.length > 0) {
      const minMonth = monthsNeeded[0];
      console.log(`[NS API] Payments: querying ${monthsNeeded.length} months from ${minMonth} (${Object.keys(cached).length} cached)`);

      // Clear months being re-queried to avoid accumulation
      for (const key of monthsNeeded) cached[key] = {};

      // Use previousTransactionLineLink for exact VendPymt→VendBill linking (no duplication)
      // Group by: payment month + bill + account, then aggregate by month+category
      const rows = await suiteqlAll(`
        SELECT payment_month, acctnumber, acctname,
               ROUND(SUM(bill_expense)) AS expense_eur
        FROM (
          SELECT TO_CHAR(vp.trandate, 'YYYY-MM') AS payment_month,
                 vb.id AS bill_id, a.acctnumber, a.acctname,
                 SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) AS bill_expense
          FROM previousTransactionLineLink ptll
          JOIN transaction vp ON ptll.nextdoc = vp.id
          JOIN transaction vb ON ptll.previousdoc = vb.id
          JOIN transactionaccountingline tal ON tal.transaction = vb.id
            AND tal.posting = 'T' AND tal.accountingbook = 1
          JOIN account a ON tal.account = a.id
            AND a.accttype IN ('Expense', 'OthExpense', 'COGS')
            AND a.acctnumber NOT LIKE '76%'
            AND a.acctnumber NOT LIKE '800%'
            AND a.acctnumber NOT IN ('780502')
          WHERE ptll.nexttype = 'VendPymt' AND ptll.previoustype = 'VendBill'
            AND ptll.linktype = 'Payment'
            AND vp.subsidiary = ${subsidiaryId}
            AND vp.trandate >= TO_DATE('${minMonth}-01', 'YYYY-MM-DD')
            AND vp.trandate <= SYSDATE
          GROUP BY TO_CHAR(vp.trandate, 'YYYY-MM'), vb.id, a.acctnumber, a.acctname
        )
        GROUP BY payment_month, acctnumber, acctname
        HAVING ABS(SUM(bill_expense)) > 10
        ORDER BY payment_month, acctnumber
      `);

      // Group by month → category
      for (const r of rows) {
        const month = r.payment_month;
        if (!cached[month]) cached[month] = {};
        const prefix = r.acctnumber.substring(0, 3);
        const category = EXPENSE_CATEGORIES[prefix] || `Other (${prefix})`;
        cached[month][category] = (cached[month][category] || 0) + Math.round(parseFloat(r.expense_eur) || 0);
      }

      // Save cache
      try {
        const dir = pathMod.join(__dirname, 'data');
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(cachePath, JSON.stringify(cached, null, 2));
      } catch {}
    } else {
      console.log(`[NS API] Payments: all ${Object.keys(cached).length} months from cache`);
    }

    // Build structured output (same format as fetchExpenseCategoryData)
    const allCategories = new Set();
    const monthlyTotals = {};
    for (const [month, cats] of Object.entries(cached)) {
      monthlyTotals[month] = 0;
      for (const [cat, amt] of Object.entries(cats)) {
        allCategories.add(cat);
        monthlyTotals[month] += amt;
      }
    }
    const catTotals = {};
    for (const [, cats] of Object.entries(cached)) {
      for (const [cat, amt] of Object.entries(cats)) catTotals[cat] = (catTotals[cat] || 0) + amt;
    }
    const categories = [...allCategories].sort((a, b) => (catTotals[b] || 0) - (catTotals[a] || 0));

    console.log(`[NS API] Payment data: ${Object.keys(cached).length} months, ${categories.length} categories`);
    return { byMonth: cached, categories, monthlyTotals, categoryMapping: EXPENSE_CATEGORIES };
  }

  // ── Cashflow breakdown by transaction type for a specific month ──
  async function fetchCashflowBreakdown(month) {
    console.log(`[NS API] Fetching cashflow breakdown for ${month}...`);
    const startDate = `${month}-01`;
    const endDay = new Date(parseInt(month.split('-')[0]), parseInt(month.split('-')[1]), 0).getDate();
    const endDate = `${month}-${endDay}`;

    // EUR (book 1) and ILS (book 2) in parallel
    const buildQuery = (bookId) => `
      SELECT txn_type,
             SUM(CASE WHEN net_bank > 0 THEN net_bank ELSE 0 END) AS inflow,
             SUM(CASE WHEN net_bank < 0 THEN ABS(net_bank) ELSE 0 END) AS outflow,
             COUNT(*) AS txn_count
      FROM (
        SELECT t.type AS txn_type, t.id,
               SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) AS net_bank
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = ${bookId}
          AND t.type NOT IN ('Transfer', 'FxReval')
          AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
          AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
        GROUP BY t.type, t.id
        HAVING ABS(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) > 1
      )
      GROUP BY txn_type
      ORDER BY outflow DESC
    `;

    const [eurRows, ilsRows] = await Promise.all([
      suiteqlAll(buildQuery(1)),
      suiteqlAll(buildQuery(2)),
    ]);

    const ilsMap = {};
    for (const r of ilsRows) {
      ilsMap[r.txn_type] = { inflow: Math.round(parseFloat(r.inflow) || 0), outflow: Math.round(parseFloat(r.outflow) || 0) };
    }

    const TYPE_LABELS = {
      'VendPymt': 'Vendor Payments', 'Journal': 'Journals (Salary, Tax, etc.)',
      'CustPymt': 'Customer Payments', 'Deposit': 'Deposits', 'Check': 'Checks',
      'CustInvc': 'Customer Invoices', 'ExpRept': 'Expense Reports',
    };

    return eurRows.map(r => ({
      type: r.txn_type,
      label: TYPE_LABELS[r.txn_type] || r.txn_type,
      inflow: Math.round(parseFloat(r.inflow) || 0),
      outflow: Math.round(parseFloat(r.outflow) || 0),
      inflowILS: ilsMap[r.txn_type]?.inflow || 0,
      outflowILS: ilsMap[r.txn_type]?.outflow || 0,
      count: parseInt(r.txn_count) || 0,
    }));
  }

  // ── Individual transactions for a cashflow breakdown row ──
  async function fetchCashflowTransactions(month, txnType, direction) {
    console.log(`[NS API] Fetching cashflow transactions: ${month} / ${txnType} / ${direction}...`);
    const startDate = `${month}-01`;
    const endDay = new Date(parseInt(month.split('-')[0]), parseInt(month.split('-')[1]), 0).getDate();
    const endDate = `${month}-${endDay}`;

    const rows = await suiteqlAll(`
      SELECT t.id, t.tranid, t.type AS txn_type,
             TO_CHAR(t.trandate, 'DD/MM/YYYY') AS tran_date,
             t.memo,
             COALESCE(v.companyname, e2.entityid, '') AS entity_name,
             SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) AS net_bank
      FROM transactionaccountingline tal
      JOIN transaction t ON tal.transaction = t.id
      JOIN account a ON tal.account = a.id
      LEFT JOIN vendor v ON t.entity = v.id AND t.type IN ('VendPymt', 'VendBill')
      LEFT JOIN entity e2 ON t.entity = e2.id AND t.type NOT IN ('VendPymt', 'VendBill')
      WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
        AND tal.posting = 'T' AND tal.accountingbook = 1
        AND t.type NOT IN ('Transfer', 'FxReval')
        AND t.type = '${txnType}'
        AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
        AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
      GROUP BY t.id, t.tranid, t.type, TO_CHAR(t.trandate, 'DD/MM/YYYY'), t.memo,
               COALESCE(v.companyname, e2.entityid, '')
      HAVING ABS(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) > 1
      ORDER BY ABS(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) DESC
    `);

    const NS_ACCOUNT = '11069058';
    const txnUrls = {
      'VendPymt': (id) => `https://${NS_ACCOUNT}.app.netsuite.com/app/accounting/transactions/vendpymt.nl?id=${id}`,
      'CustPymt': (id) => `https://${NS_ACCOUNT}.app.netsuite.com/app/accounting/transactions/custpymt.nl?id=${id}`,
      'Journal': (id) => `https://${NS_ACCOUNT}.app.netsuite.com/app/accounting/transactions/journal.nl?id=${id}`,
      'CustRfnd': (id) => `https://${NS_ACCOUNT}.app.netsuite.com/app/accounting/transactions/custrefund.nl?id=${id}`,
    };
    const urlFn = txnUrls[txnType] || ((id) => `https://${NS_ACCOUNT}.app.netsuite.com/app/accounting/transactions/transaction.nl?id=${id}`);

    // Get ILS amounts (book 2) for the same transactions
    const txnIds = rows.map(r => r.id).filter(Boolean);
    const ilsMap = {};
    if (txnIds.length > 0) {
      // Batch in chunks of 200 to avoid query length limits
      for (let c = 0; c < txnIds.length; c += 200) {
        const chunk = txnIds.slice(c, c + 200);
        const ilsRows = await suiteqlAll(`
          SELECT t.id, SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) AS net_bank_ils
          FROM transactionaccountingline tal
          JOIN transaction t ON tal.transaction = t.id
          JOIN account a ON tal.account = a.id
          WHERE a.accttype = 'Bank' AND tal.posting = 'T' AND tal.accountingbook = 2
            AND t.id IN (${chunk.join(',')})
          GROUP BY t.id
        `);
        for (const r of ilsRows) ilsMap[String(r.id)] = Math.round(parseFloat(r.net_bank_ils) || 0);
      }
    }

    const result = rows
      .filter(r => {
        const net = parseFloat(r.net_bank) || 0;
        if (direction === 'inflow') return net > 0;
        if (direction === 'outflow') return net < 0;
        return true;
      })
      .map(r => ({
        id: r.id,
        tranId: r.tranid || '',
        date: r.tran_date || '',
        entity: (r.entity_name || '').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>'),
        memo: (r.memo || '').substring(0, 80),
        amount: Math.abs(Math.round(parseFloat(r.net_bank) || 0)),
        amountILS: Math.abs(ilsMap[String(r.id)] || 0),
        url: urlFn(r.id),
      }));

    console.log(`[NS API] Cashflow transactions: ${result.length} rows`);
    return result;
  }

  // ── Expense transactions for a specific category + month ──
  async function fetchExpenseTransactions(month, category) {
    console.log(`[NS API] Fetching payment transactions: ${month} / ${category}...`);
    const prefixes = Object.entries(EXPENSE_CATEGORIES)
      .filter(([, cat]) => cat === category)
      .map(([prefix]) => prefix);
    if (prefixes.length === 0) return [];

    const prefixConditions = prefixes.map(p => `a.acctnumber LIKE '${p}%'`).join(' OR ');
    const startDate = `${month}-01`;
    const endDay = new Date(parseInt(month.split('-')[0]), parseInt(month.split('-')[1]), 0).getDate();
    const endDate = `${month}-${endDay}`;
    const NS_ACCOUNT = '11069058';

    // Group by BILL (not payment) — each bill appears once with its expense amount for this category
    // Join via previousTransactionLineLink to get only bills paid in this month
    const rows = await suiteqlAll(`
      SELECT vb.id AS bill_id, vb.tranid AS bill_ref,
             TO_CHAR(vp.trandate, 'DD/MM/YYYY') AS pay_date,
             v.companyname AS vendor,
             a.acctnumber, a.acctname,
             ROUND(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) AS amount_eur
      FROM previousTransactionLineLink ptll
      JOIN transaction vp ON ptll.nextdoc = vp.id
      JOIN transaction vb ON ptll.previousdoc = vb.id
      LEFT JOIN vendor v ON vb.entity = v.id
      LEFT JOIN transactionaccountingline tal ON tal.transaction = vb.id
        AND tal.posting = 'T' AND tal.accountingbook = 1
      LEFT JOIN account a ON tal.account = a.id
        AND a.accttype IN ('Expense', 'OthExpense', 'COGS')
      WHERE ptll.nexttype = 'VendPymt' AND ptll.previoustype = 'VendBill'
        AND ptll.linktype = 'Payment'
        AND vp.subsidiary = ${subsidiaryId}
        AND vp.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
        AND vp.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
        AND (${prefixConditions})
      GROUP BY vb.id, vb.tranid, TO_CHAR(vp.trandate, 'DD/MM/YYYY'), v.companyname, a.acctnumber, a.acctname
      HAVING ABS(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) > 10
      ORDER BY SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) DESC
      FETCH FIRST 100 ROWS ONLY
    `);

    console.log(`[NS API] Payment transactions: ${rows.length} rows for ${category} in ${month}`);
    return rows.map(r => ({
      id: r.bill_id,
      tranId: r.bill_ref || '',
      type: 'VendBill',
      date: r.pay_date || '',
      account: `${r.acctnumber || ''} ${r.acctname || ''}`.trim(),
      entity: (r.vendor || '').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>'),
      amount: Math.abs(Math.round(parseFloat(r.amount_eur) || 0)),
      url: `https://${NS_ACCOUNT}.app.netsuite.com/app/accounting/transactions/vendbill.nl?id=${r.bill_id}`,
    }));
  }

  // ── Salary breakdown by account for a specific month ──
  async function fetchSalaryBreakdown(month) {
    console.log(`[NS API] Fetching salary breakdown for ${month}...`);
    const startDate = `${month}-01`;
    const endDay = new Date(parseInt(month.split('-')[0]), parseInt(month.split('-')[1]), 0).getDate();
    const endDate = `${month}-${endDay}`;

    const [eurRows, ilsRows] = await Promise.all([
      suiteqlAll(`
        SELECT a.acctnumber, a.acctname,
               ROUND(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) AS amount
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = 1
          AND a.acctnumber LIKE '76%'
          AND a.acctnumber NOT IN ('760038', '760023')
          AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
          AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
        GROUP BY a.acctnumber, a.acctname
        HAVING ABS(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) > 10
        ORDER BY SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0)) DESC
      `),
      suiteqlAll(`
        SELECT a.acctnumber,
               ROUND(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) AS amount
        FROM transactionaccountingline tal
        JOIN transaction t ON tal.transaction = t.id
        JOIN account a ON tal.account = a.id
        WHERE t.subsidiary = ${subsidiaryId}
          AND tal.posting = 'T' AND tal.accountingbook = 2
          AND a.acctnumber LIKE '76%'
          AND a.acctnumber NOT IN ('760038', '760023')
          AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
          AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
        GROUP BY a.acctnumber
        HAVING ABS(SUM(COALESCE(tal.debit, 0)) - SUM(COALESCE(tal.credit, 0))) > 10
      `)
    ]);

    const ilsMap = {};
    for (const r of ilsRows) ilsMap[r.acctnumber] = Math.round(parseFloat(r.amount) || 0);

    console.log(`[NS API] Salary breakdown: ${eurRows.length} accounts for ${month}`);
    return eurRows.map(r => ({
      account: r.acctnumber,
      name: (r.acctname || '').replace(/&amp;/g, '&'),
      amountEUR: Math.round(parseFloat(r.amount) || 0),
      amountILS: ilsMap[r.acctnumber] || 0,
    }));
  }

  // ── Invoice-based vendor payment projection ──
  // Each bill is counted ONCE and placed in the correct payment month based on vendor rules
  const PAYMENT_RULES = {
    'MATRIX -CLOUDZONE': { type: 'fixed_day', monthsAfter: 2, dayOfMonth: 10 },
    'Mivtach Simon': { type: 'fixed_day', monthsAfter: 1, dayOfMonth: 14 },
    'mivtach simon': { type: 'fixed_day', monthsAfter: 1, dayOfMonth: 14 },
    'Mivtach Simon - pension': { type: 'fixed_day', monthsAfter: 1, dayOfMonth: 14 },
  };
  const DEFAULT_PAYMENT_DAYS = 8;

  function computePaymentDate(invoiceDate, vendorName) {
    const rule = PAYMENT_RULES[vendorName] || PAYMENT_RULES[vendorName?.toLowerCase()];
    if (rule && rule.type === 'fixed_day') {
      const d = new Date(invoiceDate);
      d.setMonth(d.getMonth() + rule.monthsAfter);
      d.setDate(rule.dayOfMonth);
      return d;
    }
    // Default: invoice date + 8 days
    const d = new Date(invoiceDate);
    d.setDate(d.getDate() + DEFAULT_PAYMENT_DAYS);
    return d;
  }

  async function fetchInvoiceBasedProjection() {
    console.log('[NS API] Building invoice-based vendor projection...');
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    // 1. Get open (unpaid) vendor bills — these are future payments
    const openBills = await fetchVendorBills();

    // 2. Get already-paid bills this month (closedate in current month)
    const paidThisMonth = await suiteqlAll(`
      SELECT t.id AS internalid, t.tranid AS doc_number,
             v.companyname AS vendor,
             ROUND(ABS(t.foreigntotal * t.exchangerate)) AS amount_eur,
             ROUND(ABS(t.foreigntotal * t.exchangerate) * 3.68) AS amount_ils,
             TO_CHAR(t.trandate, 'YYYY-MM-DD') AS tran_date,
             TO_CHAR(t.closedate, 'YYYY-MM-DD') AS close_date,
             a.acctnumber, a.acctname
      FROM transaction t
      JOIN transactionLine tl ON tl.transaction = t.id AND tl.mainline = 'T'
      LEFT JOIN vendor v ON t.entity = v.id
      LEFT JOIN transactionaccountingline tal ON tal.transaction = t.id
        AND tal.posting = 'T' AND tal.accountingbook = 1
      LEFT JOIN account a ON tal.account = a.id
        AND a.accttype IN ('Expense', 'OthExpense', 'COGS')
        AND a.acctnumber NOT LIKE '76%' AND a.acctnumber NOT LIKE '800%' AND a.acctnumber NOT IN ('780502')
      WHERE t.type = 'VendBill' AND t.status = 'B' AND t.subsidiary = ${subsidiaryId}
        AND t.closedate >= TO_DATE('${currentMonth}-01', 'YYYY-MM-DD')
        AND t.closedate <= SYSDATE
        AND a.acctnumber IS NOT NULL
      GROUP BY t.id, t.tranid, v.companyname, t.foreigntotal, t.exchangerate,
               t.trandate, t.closedate, a.acctnumber, a.acctname
    `);

    // 3. Apply payment rules to open bills → assign to payment months
    const billsByMonth = {};  // { "2026-04": { known: [...], knownTotal: 0 } }

    // Add already-paid bills to current month
    const paidBillIds = new Set();
    for (const b of paidThisMonth) {
      if (!billsByMonth[currentMonth]) billsByMonth[currentMonth] = { known: [], knownTotal: 0, paidTotal: 0 };
      const prefix = (b.acctnumber || '').substring(0, 3);
      const category = EXPENSE_CATEGORIES[prefix] || `Other (${prefix})`;
      const amt = Math.round(parseFloat(b.amount_eur) || 0);
      if (!paidBillIds.has(b.internalid)) {
        billsByMonth[currentMonth].known.push({
          id: b.internalid, doc: b.doc_number, vendor: (b.vendor || '').replace(/&amp;/g, '&'),
          amount: amt, invoiceDate: b.tran_date, paymentDate: b.close_date,
          category, status: 'paid',
        });
        billsByMonth[currentMonth].paidTotal = (billsByMonth[currentMonth].paidTotal || 0) + amt;
        paidBillIds.add(b.internalid);
      }
    }

    // Add open bills with computed payment dates
    for (const b of openBills) {
      const invoiceDate = b.tranDate ? new Date(b.tranDate) : new Date();
      const payDate = computePaymentDate(invoiceDate, b.vendor);
      const payMonth = `${payDate.getFullYear()}-${String(payDate.getMonth() + 1).padStart(2, '0')}`;

      if (!billsByMonth[payMonth]) billsByMonth[payMonth] = { known: [], knownTotal: 0, paidTotal: 0 };
      billsByMonth[payMonth].known.push({
        id: b.internalId, doc: b.documentNumber, vendor: b.vendor,
        amount: b.amountEUR, amountILS: b.amountILS,
        invoiceDate: b.tranDate, paymentDate: payDate.toISOString().slice(0, 10),
        category: 'Open Bill', status: 'pending',
      });
      billsByMonth[payMonth].knownTotal = (billsByMonth[payMonth].knownTotal || 0) + b.amountEUR;
    }

    // 4. Compute totals per month
    for (const [, data] of Object.entries(billsByMonth)) {
      data.knownTotal = data.known.reduce((s, b) => s + b.amount, 0);
    }

    // 5. Get historical category averages for supplementing far-future months
    const paymentCategoryData = await fetchPaymentsByCategory();
    const histAvg = {};
    const completedMonths = Object.keys(paymentCategoryData.byMonth)
      .filter(m => m <= currentMonth).sort().slice(-3);
    if (completedMonths.length > 0) {
      for (const m of completedMonths) {
        const total = Object.values(paymentCategoryData.byMonth[m] || {}).reduce((s, v) => s + v, 0);
        histAvg[m] = total;
      }
    }
    const avgMonthly = Object.values(histAvg).length > 0
      ? Math.round(Object.values(histAvg).reduce((s, v) => s + v, 0) / Object.values(histAvg).length)
      : 0;

    console.log(`[NS API] Invoice projection: ${openBills.length} open bills, ${paidThisMonth.length} paid this month, avg monthly: €${avgMonthly}`);

    return {
      billsByMonth,
      avgMonthly,
      paymentRules: PAYMENT_RULES,
      categoryData: paymentCategoryData,
    };
  }

  // ── Monthly FX Revaluation impact on bank accounts ──
  // Returns the net reval impact per month (EUR book 1, ILS book 2)
  // Plus the cumulative reval before Jan 1 of the current year (for opening balance adjustment)
  async function fetchMonthlyRevaluation() {
    console.log('[NS API] Fetching monthly FX revaluation data...');
    const now = new Date();
    const startYear = now.getFullYear();

    // Cumulative reval before Jan 1 of current year (to adjust opening balance)
    const preYearEur = await suiteql(`
      SELECT SUM(tal.amount) AS reval_total
      FROM transactionaccountingline tal
      JOIN transaction t ON tal.transaction = t.id
      JOIN account a ON tal.account = a.id
      WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
        AND tal.posting = 'T' AND tal.accountingbook = 1
        AND t.type = 'FxReval'
        AND t.trandate < TO_DATE('${startYear}-01-01', 'YYYY-MM-DD')
    `);
    const preYearIls = await suiteql(`
      SELECT SUM(tal.amount) AS reval_total
      FROM transactionaccountingline tal
      JOIN transaction t ON tal.transaction = t.id
      JOIN account a ON tal.account = a.id
      WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
        AND tal.posting = 'T' AND tal.accountingbook = 2
        AND t.type = 'FxReval'
        AND t.trandate < TO_DATE('${startYear}-01-01', 'YYYY-MM-DD')
    `);

    // Monthly reval for current year — EUR (accounting book 1)
    // Also fetch min/max trandate per month to detect if we have both beginning & end revals
    const eurRows = await suiteqlAll(`
      SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS month,
             SUM(tal.amount) AS reval_net,
             MIN(t.trandate) AS first_date,
             MAX(t.trandate) AS last_date,
             COUNT(DISTINCT t.trandate) AS date_count
      FROM transactionaccountingline tal
      JOIN transaction t ON tal.transaction = t.id
      JOIN account a ON tal.account = a.id
      WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
        AND tal.posting = 'T' AND tal.accountingbook = 1
        AND t.type = 'FxReval'
        AND t.trandate >= TO_DATE('${startYear}-01-01', 'YYYY-MM-DD')
        AND t.trandate <= SYSDATE
      GROUP BY TO_CHAR(t.trandate, 'YYYY-MM')
      ORDER BY month
    `);

    // Monthly reval for current year — ILS (accounting book 2)
    const ilsRows = await suiteqlAll(`
      SELECT TO_CHAR(t.trandate, 'YYYY-MM') AS month,
             SUM(tal.amount) AS reval_net,
             MIN(t.trandate) AS first_date,
             MAX(t.trandate) AS last_date,
             COUNT(DISTINCT t.trandate) AS date_count
      FROM transactionaccountingline tal
      JOIN transaction t ON tal.transaction = t.id
      JOIN account a ON tal.account = a.id
      WHERE a.accttype = 'Bank' AND t.subsidiary = ${subsidiaryId}
        AND tal.posting = 'T' AND tal.accountingbook = 2
        AND t.type = 'FxReval'
        AND t.trandate >= TO_DATE('${startYear}-01-01', 'YYYY-MM-DD')
        AND t.trandate <= SYSDATE
      GROUP BY TO_CHAR(t.trandate, 'YYYY-MM')
      ORDER BY month
    `);

    const byMonth = {};
    for (const r of eurRows) {
      // hasBothEnds: true if reval transactions exist on at least 2 distinct dates in the month
      // (beginning-of-month reversal + end-of-month new reval)
      const dateCount = parseInt(r.date_count) || 0;
      byMonth[r.month] = { eur: Math.round(parseFloat(r.reval_net) || 0), ils: 0, hasBothEnds: dateCount >= 2 };
    }
    for (const r of ilsRows) {
      const dateCount = parseInt(r.date_count) || 0;
      if (!byMonth[r.month]) byMonth[r.month] = { eur: 0, ils: 0, hasBothEnds: dateCount >= 2 };
      else byMonth[r.month].ils = Math.round(parseFloat(r.reval_net) || 0);
      // If ILS has both ends but EUR didn't, upgrade the flag
      if (dateCount >= 2) byMonth[r.month].hasBothEnds = true;
    }

    const preYear = {
      eur: Math.round(parseFloat(preYearEur?.items?.[0]?.reval_total) || 0),
      ils: Math.round(parseFloat(preYearIls?.items?.[0]?.reval_total) || 0),
    };

    console.log(`[NS API] Monthly reval data: ${Object.keys(byMonth).length} months, pre-year reval: EUR ${preYear.eur}, ILS ${preYear.ils}`);
    return { byMonth, preYear };
  }

  // ── Fetch vendor bills for a specific account and month ──
  async function fetchVendorBillsByAccount(accountIdOrNumber, month) {
    const [y, m] = month.split('-');
    const startDate = `${y}-${m}-01`;
    const lastDay = new Date(parseInt(y), parseInt(m), 0).getDate();
    const endDate = `${y}-${m}-${String(lastDay).padStart(2, '0')}`;
    const safeAcct = String(accountIdOrNumber).replace(/'/g, "''");
    console.log(`[NS API] Fetching transactions for account ${accountIdOrNumber}, ${startDate} to ${endDate}...`);
    // Resolve account number to NS internal ID for register links
    let nsAcctId = null;
    try {
      const lookupResult = await suiteql(`SELECT a.id, a.acctnumber FROM account a WHERE a.acctnumber = '${safeAcct}' FETCH FIRST 1 ROWS ONLY`);
      const acctLookup = lookupResult.items || [];
      nsAcctId = acctLookup.length > 0 ? acctLookup[0].id : null;
      console.log(`[NS API] Account ${accountIdOrNumber} → NS id ${nsAcctId}`);
    } catch (lookupErr) {
      console.log(`[NS API] Account lookup failed: ${lookupErr.message}`);
    }
    let rows;
    try {
      rows = await suiteqlAll(`SELECT t.id AS bill_id, t.tranid AS bill_number, t.trandate, BUILTIN.DF(t.entity) AS vendor_name, t.memo, tal.amount AS amount, t.currency, t.status, t.type AS tran_type FROM transactionaccountingline tal JOIN transaction t ON tal.transaction = t.id JOIN account a ON tal.account = a.id WHERE a.acctnumber = '${safeAcct}' AND t.subsidiary = ${subsidiaryId} AND tal.accountingbook = 1 AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD') AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD') ORDER BY t.trandate, t.id`);
    } catch (queryErr) {
      console.log(`[NS API] Transaction query failed: ${queryErr.message}`);
      rows = [];
      // Return early with error info for debugging
      return { bills: [], nsAcctId, queryError: queryErr.message };
    }
    console.log(`[NS API] Found ${rows.length} transaction lines for account ${accountIdOrNumber}`);
    const bills = rows.map(r => {
      const typeMap = { VendBill: 'vendbill', VendCred: 'vendcred', Journal: 'journal', ExpRept: 'exprept', VendPymt: 'vendpymt', Check: 'check', CustPymt: 'custpymt', CashSale: 'cashsale', InvTrnfr: 'invtrnfr', CardChrg: 'cardchrg', CustCred: 'custcred', CustInvc: 'custinvc', ItemRcpt: 'itemrcpt' };
      const urlType = typeMap[r.tran_type] || (r.tran_type ? r.tran_type.toLowerCase() : 'transaction');
      return {
        billId: r.bill_id,
        billNumber: r.bill_number,
        date: r.trandate,
        vendor: (r.vendor_name || '').trim(),
        memo: r.memo,
        amount: Math.round((r.amount || 0) * 100) / 100,
        currency: r.currency,
        status: r.status,
        tranType: r.tran_type,
        nsUrlType: urlType,
      };
    });
    return { bills, nsAcctId };
  }

  // ── NS Budget data (for subsidiaries without Snowflake) ──
  // Queries the NS budget table (amount1..amount12 per account row)
  // Uses category 5 (Budget Board Approval) — the approved budget
  async function fetchNSBudget() {
    console.log(`[NS API] Fetching NS budget for subsidiary ${subsidiaryId}...`);
    const now = new Date();
    const year = now.getFullYear();

    // First resolve the year period ID for the current fiscal year
    const yearLookup = await suiteql(`
      SELECT id FROM accountingperiod
      WHERE periodname = 'FY ${year}' AND isyear = 'T'
      FETCH FIRST 1 ROWS ONLY
    `);
    const yearPeriodId = yearLookup?.items?.[0]?.id;
    if (!yearPeriodId) {
      console.log(`[NS API] No accounting period found for FY ${year}`);
      return { byMonth: {} };
    }
    console.log(`[NS API] FY ${year} period ID: ${yearPeriodId}`);

    // Use budgetsmachine table for per-month budget amounts (joined with budgets for account info)
    const rows = await suiteqlAll(`
      SELECT bm.amount, ap.periodname, a.acctnumber, a.acctname, a.accttype
      FROM budgetsmachine bm
      JOIN budgets b ON bm.budget = b.id
      JOIN accountingperiod ap ON bm.period = ap.id
      JOIN account a ON b.account = a.id
      WHERE b.subsidiary = ${subsidiaryId}
        AND b.year = ${yearPeriodId}
        AND b.category = 5
        AND b.accountingbook = 1
        AND ap.isyear = 'F' AND ap.isquarter = 'F'
      ORDER BY ap.id, a.acctnumber
    `);

    console.log(`[NS API] NS budget (budgetsmachine): ${rows.length} monthly rows for subsidiary ${subsidiaryId}`);

    // Map period names to month keys: "Jan 2026" -> "2026-01"
    const monthMap = { 'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                       'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12' };

    const byMonth = {};
    for (const r of rows) {
      const periodName = r.periodname || '';
      const parts = periodName.split(' ');
      const mm = monthMap[parts[0]];
      const yy = parts[1];
      if (!mm || !yy) continue;
      const mKey = `${yy}-${mm}`;

      const acctNum = r.acctnumber || '';
      const acctType = r.accttype || '';
      const prefix = acctNum.substring(0, 3);
      const isSalary = acctNum.startsWith('76');
      const isRevenue = acctType === 'Income';
      const isExpense = !isSalary && !isRevenue && ['Expense', 'OthExpense', 'COGS'].includes(acctType);
      const amount = Math.round(parseFloat(r.amount) || 0);
      if (amount === 0) continue;

      if (!byMonth[mKey]) byMonth[mKey] = { vendors: 0, salary: 0, revenue: 0, categories: {} };

      if (isSalary) {
        byMonth[mKey].salary += amount;
      } else if (isRevenue) {
        byMonth[mKey].revenue += Math.abs(amount);
      } else if (isExpense) {
        byMonth[mKey].vendors += amount;
        const category = EXPENSE_CATEGORIES[prefix] || `Other (${prefix})`;
        byMonth[mKey].categories[category] = (byMonth[mKey].categories[category] || 0) + amount;
      }
    }

    // Log summary
    const months = Object.keys(byMonth).sort();
    if (months.length > 0) {
      const sample = byMonth[months[0]];
      console.log(`[NS API] NS budget: ${months.length} months, sample ${months[0]}: salary=${sample.salary}, vendors=${sample.vendors}, revenue=${sample.revenue}, categories=${Object.keys(sample.categories).length}`);
    } else {
      console.log(`[NS API] NS budget: no monthly data found`);
    }
    return { byMonth };
  }

  return { suiteql, suiteqlAll, fetchAgingData, fetchCollectionData, buildCollectionJson, fetchClientAnomalies, fetchAllSOsByBillingPeriod, fetchRevenueData, fetchMRRData, fetchBankBalance, fetchVendorBills, fetchVendorPaymentHistory, fetchBankAccountList, fetchBankAccountListAsOf, fetchSalaryData, fetchCashflowHistory, fetchExpenseCategoryData, fetchPaymentsByCategory, fetchCashflowBreakdown, fetchCashflowTransactions, fetchExpenseTransactions, fetchSalaryBreakdown, fetchInvoiceBasedProjection, fetchMonthlyRevaluation, fetchVendorBillsByAccount, fetchNSBudget };
}


module.exports = { createNetSuiteClient };
