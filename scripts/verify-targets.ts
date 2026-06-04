// Verify the budget_target_by_dept_acct table against the dashboard's accrual view.
//
// PURPOSE
//   After a Sync, prove that Targets equals the dashboard 100% per account /
//   month / department, in BOTH currencies. This is read-only (Postgres only).
//
// HOW TO RUN (on the finance-it server, from the backend dir, like the diag scripts):
//   cd /home/ubuntu/finance-it/backend
//   cp /home/ubuntu/finance-it/extra-apps/bank-dashboard/scripts/verify-targets.ts .
//   /home/ubuntu/finance-it/node_modules/.bin/tsx verify-targets.ts            # LSports 2026
//   /home/ubuntu/finance-it/node_modules/.bin/tsx verify-targets.ts 3 2026     # explicit sub/year
//
// WHAT IT CHECKS
//   1. Grand total + payroll/vendor split, in ILS and native EUR.
//   2. Per-month bucket totals (salary / vendors), ILS and EUR — compare these
//      to the dashboard's SALARY and VENDORS columns for each month.
//   3. Native-EUR health: flags rows where monthly_source_eur is missing (row
//      synced before PR-H — needs a re-Sync) or where the implied EUR/ILS rate
//      per month falls outside a sane band (3.0–4.5), which would indicate a
//      conversion problem rather than a true Snowflake rate.
//   4. A per-department breakdown for one closed month (default January) at the
//      Gross Salaries account (760001), to diff line-for-line against the
//      salary modal's "Actual (Snowflake)" rows.

import * as dotenv from 'dotenv';
dotenv.config({ path: '/home/ubuntu/finance-it/backend/.env' });

const SUB = Number(process.argv[2]) || 3;
const YEAR = Number(process.argv[3]) || 2026;
const CHECK_MONTH = '01'; // closed month to break down by department

const MONTHS = ['01','02','03','04','05','06','07','08','09','10','11','12'];
const sum = (o: Record<string, number> | null | undefined): number =>
  o ? Object.values(o).reduce((s, v) => s + (Number(v) || 0), 0) : 0;
const f = (n: number) => Math.round(n).toLocaleString('en-GB');

async function main() {
  const db: any = await import('./src/config/database');
  const query = db.query || (db.pool && db.pool.query.bind(db.pool)) || db.default?.query;
  if (!query) throw new Error('no query fn exported from ./src/config/database');

  const res = await query(
    `SELECT department, location, account_number, account_name, currency,
            monthly_source_ils, monthly_source_eur,
            (account_number LIKE '76%') AS is_payroll
     FROM budget_target_by_dept_acct
     WHERE subsidiary_id = $1 AND fiscal_year = $2`,
    [SUB, YEAR]
  );
  const rows = res.rows;
  console.log(`\nbudget_target_by_dept_acct — subsidiary ${SUB}, FY ${YEAR}: ${rows.length} rows\n`);

  // 1 + 2: totals and per-month bucket split
  let payIls = 0, payEur = 0, venIls = 0, venEur = 0;
  const monIls: Record<string, { sal: number; ven: number }> = {};
  const monEur: Record<string, { sal: number; ven: number }> = {};
  for (const m of MONTHS) { monIls[m] = { sal: 0, ven: 0 }; monEur[m] = { sal: 0, ven: 0 }; }

  let missingEur = 0;
  const rateFlags: string[] = [];

  for (const r of rows) {
    const mi = r.monthly_source_ils || {};
    const me = r.monthly_source_eur || {};
    const isPay = r.is_payroll;
    if (isPay) { payIls += sum(mi); payEur += sum(me); }
    else       { venIls += sum(mi); venEur += sum(me); }
    for (const m of MONTHS) {
      const ils = Number(mi[m]) || 0;
      const eur = Number(me[m]) || 0;
      if (isPay) { monIls[m].sal += ils; monEur[m].sal += eur; }
      else       { monIls[m].ven += ils; monEur[m].ven += eur; }
      // native-EUR health
      if (ils !== 0 && eur === 0) missingEur++;
      if (ils !== 0 && eur !== 0) {
        const rate = ils / eur;
        if (rate < 3.0 || rate > 4.5) {
          if (rateFlags.length < 10) rateFlags.push(`${r.department}/${r.account_number}/${m}: rate ${rate.toFixed(3)} (ILS ${f(ils)} / EUR ${f(eur)})`);
        }
      }
    }
  }

  console.log('=== Totals ===');
  console.log(`  Payroll : ILS ${f(payIls).padStart(14)}   EUR ${f(payEur).padStart(13)}`);
  console.log(`  Vendors : ILS ${f(venIls).padStart(14)}   EUR ${f(venEur).padStart(13)}`);
  console.log(`  GRAND   : ILS ${f(payIls + venIls).padStart(14)}   EUR ${f(payEur + venEur).padStart(13)}`);

  console.log('\n=== Per-month bucket totals (compare to dashboard SALARY / VENDORS columns) ===');
  console.log('Month        Salary ILS     Salary EUR     Vendor ILS     Vendor EUR');
  for (const m of MONTHS) {
    console.log(`  ${YEAR}-${m}  ${f(monIls[m].sal).padStart(12)}  ${f(monEur[m].sal).padStart(12)}  ${f(monIls[m].ven).padStart(12)}  ${f(monEur[m].ven).padStart(12)}`);
  }

  console.log('\n=== Native-EUR health (PR-H) ===');
  console.log(`  Cells with ILS but no EUR (need re-Sync): ${missingEur}`);
  if (rateFlags.length) {
    console.log(`  Out-of-band EUR/ILS rates (first ${rateFlags.length}):`);
    for (const fl of rateFlags) console.log(`    ${fl}`);
  } else {
    console.log('  All implied EUR/ILS rates within 3.0–4.5 ✓');
  }

  // 4: per-department breakdown for one closed month at Gross Salaries (760001)
  console.log(`\n=== ${YEAR}-${CHECK_MONTH} · account 760001 (Gross Salaries) by department ===`);
  console.log('  (diff these against the Salary modal "Actual (Snowflake)" rows)');
  console.log('Department          EUR            ILS');
  const dept760 = rows
    .filter((r: any) => r.account_number === '760001')
    .map((r: any) => ({
      dept: r.department,
      eur: Number((r.monthly_source_eur || {})[CHECK_MONTH]) || 0,
      ils: Number((r.monthly_source_ils || {})[CHECK_MONTH]) || 0,
    }))
    .sort((a: any, b: any) => b.eur - a.eur);
  let de = 0, di = 0;
  for (const d of dept760) { de += d.eur; di += d.ils; console.log(`  ${String(d.dept).padEnd(16)} ${f(d.eur).padStart(12)}  ${f(d.ils).padStart(12)}`); }
  console.log(`  ${'TOTAL'.padEnd(16)} ${f(de).padStart(12)}  ${f(di).padStart(12)}`);
  console.log('');
}

main().catch(e => { console.error(e); process.exit(1); });
