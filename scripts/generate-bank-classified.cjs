// Regenerate the static bank-classified JSON seed for a (company, year).
//
// Usage:  node scripts/generate-bank-classified.cjs <company> <year>
// Example: node scripts/generate-bank-classified.cjs lsports 2026
//
// Output: src/seeds/bank-classified-<company>-<year>.json
//
// Why this exists: the parent finance-it server does not auto-register new bank-dashboard
// API routes, so /api/ns-bank-classified-yearly is unreachable in production. Until that
// is fixed, App.tsx imports this JSON at build time and uses it as the bank-classified
// data source. Re-run this script and commit the result whenever you want fresh numbers.

const path = require('path');
const fs = require('fs');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const { createNetSuiteClient } = require('../netsuite-api.cjs');

const COMPANY_TO_SUBSIDIARY = { lsports: 3, statscore: 6 };

async function main() {
  const company = (process.argv[2] || 'lsports').toLowerCase();
  const year = parseInt(process.argv[3] || String(new Date().getFullYear()));
  const subsidiary = COMPANY_TO_SUBSIDIARY[company];
  if (!subsidiary) {
    console.error(`Unknown company "${company}". Allowed: ${Object.keys(COMPANY_TO_SUBSIDIARY).join(', ')}`);
    process.exit(1);
  }
  if (!year || year < 2000 || year > 2100) {
    console.error(`Invalid year "${process.argv[3]}"`);
    process.exit(1);
  }

  console.log(`Generating bank-classified data for ${company} (subsidiary ${subsidiary}), year ${year}...`);
  const ns = createNetSuiteClient(process.env, subsidiary);
  if (!ns.fetchBankClassifiedYearly) {
    console.error('netsuite-api.cjs does not export fetchBankClassifiedYearly — check the file is up to date.');
    process.exit(1);
  }

  const t0 = Date.now();
  const data = await ns.fetchBankClassifiedYearly(year);
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  const monthCount = Object.keys(data.byMonth || {}).length;
  console.log(`Fetched ${monthCount} months in ${elapsed}s`);

  const outDir = path.join(__dirname, '..', 'src', 'seeds');
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `bank-classified-${company}-${year}.json`);
  const payload = {
    subsidiary,
    company,
    year,
    generatedAt: new Date().toISOString(),
    note: 'Static seed for bank-classified buckets. Refresh by running scripts/generate-bank-classified.cjs.',
    byMonth: data.byMonth || {},
  };
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2));
  console.log(`Wrote ${outPath}`);
}

main().catch(err => { console.error(err); process.exit(1); });
