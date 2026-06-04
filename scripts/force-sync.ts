// CLI bypass for the dashboard's Sync 2026 button.
//
// Runs populateBudgetTargets directly against Postgres + Snowflake, with NO
// scenario adjustments (clean baseline = Snowflake actuals + budget). Use this
// to confirm Targets matches the dashboard's "Actual (Snowflake)" view per
// account / month / department without having to click the button.
//
// USAGE (on the finance-it server):
//   cd /home/ubuntu/finance-it/backend
//   cp ../extra-apps/bank-dashboard/scripts/force-sync.ts .
//   /home/ubuntu/finance-it/node_modules/.bin/tsx force-sync.ts            # LSports 2026
//   /home/ubuntu/finance-it/node_modules/.bin/tsx force-sync.ts 3 2026     # explicit sub/year
//
// Requires the PR-K snippet (populateBudgetTargets + ensureBudgetTablesExist
// re-exported) already spliced into backend/src/routes/bankDashboardApi.ts
// and the backend rebuilt.

import * as dotenv from 'dotenv';
dotenv.config({ path: '/home/ubuntu/finance-it/backend/.env' });

const SUB = Number(process.argv[2]) || 3;
const YEAR = Number(process.argv[3]) || 2026;

async function main() {
  // Import the route file — its module top-level kicks off ensureBudgetTablesExist().
  // The exported populateBudgetTargets reuses the same Postgres pool + Snowflake client.
  const mod: any = await import('./src/routes/bankDashboardApi');
  if (typeof mod.populateBudgetTargets !== 'function') {
    throw new Error('populateBudgetTargets not exported from bankDashboardApi. Re-splice the latest snippet (PR-K).');
  }
  if (typeof mod.ensureBudgetTablesExist === 'function') {
    await mod.ensureBudgetTablesExist();
  }
  console.log(`Running populateBudgetTargets({ subsidiary: ${SUB}, years: [${YEAR}] })...`);
  const t0 = Date.now();
  const result = await mod.populateBudgetTargets({ subsidiary: SUB, years: [YEAR] });
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`Done in ${elapsed}s.`);
  console.log(JSON.stringify(result, null, 2));
  if (result?.overlayError) {
    console.error('\n!! Overlay error surfaced:', result.overlayError);
    process.exit(2);
  }
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
