// ============================================================================
// finance-it backend patch — /api/ns-salary-breakdown route
//
// This route PRE-EXISTS in finance-it/backend/src/routes/bankDashboardApi.ts.
// It was patched in-place during the cashflow-accuracy work. This file records
// the FINAL state so the change is reproducible after a clean finance-it
// checkout (Pull & Build does NOT rebuild the backend, so the live patch
// survives day-to-day, but a fresh git deploy of finance-it would drop it).
//
// Two fixes vs the original:
//   1. Query BOTH accounting books (1=EUR, 2=ILS) and zip by acctnumber, so the
//      modal's per-account ILS column shows real values instead of "NaN ₪".
//   2. REMOVE the `NOT IN ('760038','760023')` exclusion. 760023 is the
//      Military reserve refund (a negative credit). Excluding it overstated
//      payroll; including it makes the breakdown total equal the NS P&L
//      "Total - 760000 - Payroll" line exactly. The exclusion is a
//      one-time/irregular filter that belongs ONLY on the projection baseline,
//      never on the actuals path.
//
// ── APPLY (idempotent) ──────────────────────────────────────────────────────
//   python3 - <<'EOF'
//   path = '/home/ubuntu/finance-it/backend/src/routes/bankDashboardApi.ts'
//   with open(path) as f: src = f.read()
//
//   # If the old single-book actRows block is still present, replace it.
//   old = """    const actRows: any[] = await queueNsCall(() => ns.suiteqlAll(`
//       SELECT a.acctnumber, a.acctname,
//              SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) AS amount_eur
//       FROM transactionaccountingline tal
//       JOIN transaction t ON tal.transaction = t.id
//       JOIN account a ON tal.account = a.id
//       WHERE t.subsidiary = ${sub}
//         AND tal.posting = 'T' AND tal.accountingbook = 1
//         AND a.acctnumber LIKE '76%'
//         AND a.acctnumber NOT IN ('760038', '760023')
//         AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
//         AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
//       GROUP BY a.acctnumber, a.acctname
//       HAVING SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) <> 0
//       ORDER BY a.acctnumber
//     `));
//     const actuals = actRows.map((r: any) => ({ account: r.acctnumber || '', name: r.acctname || '', amountEUR: Math.round(parseFloat(r.amount_eur) || 0) }));"""
//
//   new = open('/home/ubuntu/finance-it/extra-apps/bank-dashboard/docs/backend-salary-breakdown-route.ts').read()
//   new = new[new.index('// >>> BEGIN ROUTE BODY'):new.index('// <<< END ROUTE BODY')]
//   new = '\n'.join(l[3:] if l.startswith('// ') else l[2:] if l.startswith('//') else l for l in new.splitlines()[1:-1])
//
//   if 'accountingbook = ${book}' in src and "NOT IN ('760038', '760023')" not in src:
//       print('Already patched.')
//   elif old in src:
//       open(path,'w').write(src.replace(old, new.strip()))
//       print('Patched OK.')
//   else:
//       print('Block not found — apply manually from the route body below.')
//   EOF
//
//   cd /home/ubuntu/finance-it/backend && npm run build && pm2 restart finance-it-backend
// ============================================================================

// >>> BEGIN ROUTE BODY (the actuals query block inside /ns-salary-breakdown)
//     // Fetch EUR (book=1) and ILS (book=2) in parallel, zip by acctnumber so the
//     // modal shows both currency columns instead of NaN for ILS. No account
//     // exclusion — actuals must reflect every 76xxx that posted, incl. 760023
//     // (Military reserve refund), so the total equals NS P&L Total - 760000.
//     const accountQuery = (book: number) => queueNsCall(() => ns.suiteqlAll(`
//       SELECT a.acctnumber, a.acctname,
//              SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) AS amount
//       FROM transactionaccountingline tal
//       JOIN transaction t ON tal.transaction = t.id
//       JOIN account a ON tal.account = a.id
//       WHERE t.subsidiary = ${sub}
//         AND tal.posting = 'T' AND tal.accountingbook = ${book}
//         AND a.acctnumber LIKE '76%'
//         AND t.trandate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
//         AND t.trandate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
//       GROUP BY a.acctnumber, a.acctname
//       HAVING SUM(COALESCE(tal.debit,0)) - SUM(COALESCE(tal.credit,0)) <> 0
//       ORDER BY a.acctnumber
//     `));
//     const [eurRows, ilsRows] = (await Promise.all([accountQuery(1), accountQuery(2)])) as [any[], any[]];
//     const ilsByAccount: Record<string, number> = {};
//     for (const r of ilsRows) {
//       if (r.acctnumber) ilsByAccount[r.acctnumber] = Math.round(parseFloat(r.amount) || 0);
//     }
//     const actuals = eurRows.map((r: any) => ({
//       account: r.acctnumber || '',
//       name: r.acctname || '',
//       amountEUR: Math.round(parseFloat(r.amount) || 0),
//       amountILS: ilsByAccount[r.acctnumber] || 0,
//     }));
// <<< END ROUTE BODY
