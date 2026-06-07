// Diagnostic for the Column B pipeline calibration factor.
// Computes factor=39.1331 (live) vs expected ~0.8381 — find the root cause.
//
//   node scripts/diagnose-calibration.cjs
//
// Dumps:
//   - FCT_MONTHLY_REVENUE columns (to spot price_update / shared flags on MR side)
//   - DIM_OPPORTUNITY columns (to find alt amount fields like AMOUNT_EUR)
//   - Currency mix on both tables
//   - Numerator / denominator raw values for prior year
//   - MR records per (opportunity, month) — fanout check
//   - Sample 10 closed-won opps showing OPPORTUNITY_AMOUNT vs Σ MR_AMOUNT

const path = require('path');
const fs = require('fs');
const { createSnowflakeClient } = require('../snowflake-api.cjs');

// Walk up the tree collecting candidate .env files, then pick whichever one
// actually contains SNOWFLAKE_ACCOUNT. bank-dashboard sits under extra-apps/
// in the finance-it tree and the Snowflake creds live in backend/.env, while
// the repo root carries a different .env with only a few keys.
function loadEnv() {
  const candidates = [];
  let dir = __dirname;
  for (let i = 0; i < 6; i++) {
    for (const name of ['backend/.env', '.env']) {
      const p = path.join(dir, name);
      if (fs.existsSync(p)) candidates.push(p);
    }
    dir = path.dirname(dir);
  }
  for (const p of candidates) {
    const txt = fs.readFileSync(p, 'utf-8');
    if (/^\s*SNOWFLAKE_ACCOUNT\s*=/m.test(txt)) {
      require('dotenv').config({ path: p, override: true });
      return { path: p, hasSnowflake: true };
    }
  }
  if (candidates.length) { require('dotenv').config({ path: candidates[0], override: true }); return { path: candidates[0], hasSnowflake: false }; }
  return null;
}
const env = loadEnv();
console.log(`[diag] env loaded from: ${env ? env.path : '(none found)'} (snowflake=${env?.hasSnowflake ? 'yes' : 'no'})`);

(async () => {
  const sf = createSnowflakeClient(process.env);
  if (!sf) { console.error('Snowflake client unavailable (check key path / env)'); process.exit(1); }
  const q = sf.query.bind(sf);

  const yr = new Date().getFullYear();
  const priorYr = yr - 1;

  console.log(`\n══════════════════════════════════════════════════════════════`);
  console.log(`  Calibration diagnostic — prior year = ${priorYr}`);
  console.log(`══════════════════════════════════════════════════════════════\n`);

  // ── FCT_MONTHLY_REVENUE columns ───────────────────────────────────────────
  console.log('▸ FCT_MONTHLY_REVENUE columns:');
  const mrCols = await q(`
    SELECT COLUMN_NAME, DATA_TYPE FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'FCT_MONTHLY_REVENUE'
    ORDER BY ORDINAL_POSITION
  `);
  console.log(mrCols.map(r => `  - ${r.COLUMN_NAME} (${r.DATA_TYPE})`).join('\n'));

  // ── DIM_OPPORTUNITY columns (amount + currency suspects) ──────────────────
  console.log('\n▸ DIM_OPPORTUNITY columns matching AMOUNT / CURRENCY / REVENUE / MRR / EUR:');
  const oppCols = await q(`
    SELECT COLUMN_NAME, DATA_TYPE FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'DIM_OPPORTUNITY'
      AND (COLUMN_NAME ILIKE '%AMOUNT%' OR COLUMN_NAME ILIKE '%CURRENCY%'
        OR COLUMN_NAME ILIKE '%REVENUE%' OR COLUMN_NAME ILIKE '%MRR%'
        OR COLUMN_NAME ILIKE '%EUR%' OR COLUMN_NAME ILIKE '%PRICE%')
    ORDER BY ORDINAL_POSITION
  `);
  console.log(oppCols.map(r => `  - ${r.COLUMN_NAME} (${r.DATA_TYPE})`).join('\n'));

  // ── Currency mix in DIM_OPPORTUNITY for prior-year closed-won ─────────────
  console.log(`\n▸ DIM_OPPORTUNITY currency mix for ${priorYr} closed-won:`);
  const oppCurCol = oppCols.find(r => /^CURRENCY/i.test(r.COLUMN_NAME))?.COLUMN_NAME
                 || oppCols.find(r => /CURRENCY/i.test(r.COLUMN_NAME))?.COLUMN_NAME;
  if (oppCurCol) {
    const oppCur = await q(`
      SELECT ${oppCurCol} AS CCY, COUNT(*) AS N, SUM(ROUND(OPPORTUNITY_AMOUNT)) AS SUM_AMT
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
      WHERE IS_OPPORTUNITY_WON = TRUE
        AND EXTRACT(YEAR FROM CLOSED_WON_DATE) = ${priorYr}
        AND OPPORTUNITY_AMOUNT > 0
      GROUP BY 1 ORDER BY SUM_AMT DESC
    `);
    console.log(oppCur.map(r => `  ${r.CCY}: n=${r.N}  Σamount=${r.SUM_AMT}`).join('\n'));
  } else {
    console.log('  (no CURRENCY column on DIM_OPPORTUNITY)');
  }

  // ── Currency mix in FCT_MONTHLY_REVENUE for prior-year MR ─────────────────
  console.log(`\n▸ FCT_MONTHLY_REVENUE currency mix for ${priorYr}:`);
  const mrCur = await q(`
    SELECT CURRENCY AS CCY, COUNT(*) AS N, SUM(ROUND(MR_AMOUNT)) AS SUM_AMT
    FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE
    WHERE EXTRACT(YEAR FROM CAL_MONTH_START_DATE) = ${priorYr}
    GROUP BY 1 ORDER BY SUM_AMT DESC
  `);
  console.log(mrCur.map(r => `  ${r.CCY}: n=${r.N}  Σamount=${r.SUM_AMT}`).join('\n'));

  // ── Raw numerator + denominator (current SQL) ─────────────────────────────
  console.log(`\n▸ Calibration components (current SQL, EUR-only MR):`);
  const denomRows = await q(`
    SELECT SUM(ROUND(OPPORTUNITY_AMOUNT) * (12 - (EXTRACT(MONTH FROM CLOSED_WON_DATE) - 1))) AS DENOM,
           COUNT(*) AS N_OPPS,
           SUM(ROUND(OPPORTUNITY_AMOUNT)) AS SUM_AMOUNT
    FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
    WHERE IS_OPPORTUNITY_WON = TRUE
      AND EXTRACT(YEAR FROM CLOSED_WON_DATE) = ${priorYr}
      AND OPPORTUNITY_AMOUNT > 0
      AND COALESCE(IS_OPPORTUNITY_REVENUE_SHARED, FALSE) = FALSE
      AND COALESCE(IS_PRICE_UPDATE, FALSE) = FALSE
  `);
  console.log(`  denominator = ${denomRows[0]?.DENOM}  (opps=${denomRows[0]?.N_OPPS}, Σamount=${denomRows[0]?.SUM_AMOUNT})`);

  const numRows = await q(`
    SELECT SUM(ROUND(mr.MR_AMOUNT)) AS NUMER, COUNT(*) AS N_MR
    FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE mr
    JOIN DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY o ON mr.OPPORTUNITY_ID = o.OPPORTUNITY_ID
    WHERE EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${priorYr}
      AND o.IS_OPPORTUNITY_WON = TRUE
      AND EXTRACT(YEAR FROM o.CLOSED_WON_DATE) = ${priorYr}
      AND COALESCE(o.IS_OPPORTUNITY_REVENUE_SHARED, FALSE) = FALSE
      AND COALESCE(o.IS_PRICE_UPDATE, FALSE) = FALSE
      AND COALESCE(mr.MR_AMOUNT, 0) <> 0
      AND mr.CURRENCY = 'EUR'
  `);
  console.log(`  numerator   = ${numRows[0]?.NUMER}  (mr-rows=${numRows[0]?.N_MR})`);
  const dn = Number(denomRows[0]?.DENOM) || 0;
  const nm = Number(numRows[0]?.NUMER) || 0;
  console.log(`  factor      = ${dn > 0 ? (nm / dn).toFixed(4) : 'n/a'}`);

  // ── MR fanout per (opp, month) — if >1 record per pair, numerator inflates
  console.log(`\n▸ MR record fanout — distinct (opp, month) pairs vs total rows for ${priorYr}:`);
  const fanout = await q(`
    SELECT COUNT(*) AS TOTAL_ROWS,
           COUNT(DISTINCT mr.OPPORTUNITY_ID || '|' || TO_VARCHAR(mr.CAL_MONTH_START_DATE,'YYYY-MM')) AS DISTINCT_PAIRS,
           COUNT(DISTINCT mr.OPPORTUNITY_ID) AS DISTINCT_OPPS
    FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE mr
    JOIN DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY o ON mr.OPPORTUNITY_ID = o.OPPORTUNITY_ID
    WHERE EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${priorYr}
      AND o.IS_OPPORTUNITY_WON = TRUE
      AND EXTRACT(YEAR FROM o.CLOSED_WON_DATE) = ${priorYr}
      AND mr.CURRENCY = 'EUR'
      AND COALESCE(mr.MR_AMOUNT, 0) <> 0
  `);
  const fr = fanout[0] || {};
  console.log(`  total mr rows   = ${fr.TOTAL_ROWS}`);
  console.log(`  distinct pairs  = ${fr.DISTINCT_PAIRS}`);
  console.log(`  distinct opps   = ${fr.DISTINCT_OPPS}`);
  console.log(`  fanout/pair     = ${(Number(fr.TOTAL_ROWS)/Number(fr.DISTINCT_PAIRS)).toFixed(2)}x`);

  // ── Sample: pick 5 top closed-won opps and compare amount vs Σ MR ─────────
  console.log(`\n▸ Top 5 prior-year closed-won opps — OPPORTUNITY_AMOUNT vs Σ MR_AMOUNT:`);
  const samples = await q(`
    WITH s AS (
      SELECT o.OPPORTUNITY_ID, o.OPPORTUNITY_AMOUNT, o.CLOSED_WON_DATE,
             ${oppCurCol ? `o.${oppCurCol} AS OPP_CCY,` : `'?' AS OPP_CCY,`}
             COUNT(mr.OPPORTUNITY_ID) AS N_MR,
             SUM(ROUND(mr.MR_AMOUNT)) AS SUM_MR_EUR,
             COUNT(DISTINCT TO_VARCHAR(mr.CAL_MONTH_START_DATE,'YYYY-MM')) AS DISTINCT_MONTHS
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY o
      LEFT JOIN DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE mr
        ON o.OPPORTUNITY_ID = mr.OPPORTUNITY_ID
        AND EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${priorYr}
        AND mr.CURRENCY = 'EUR'
      WHERE o.IS_OPPORTUNITY_WON = TRUE
        AND EXTRACT(YEAR FROM o.CLOSED_WON_DATE) = ${priorYr}
        AND o.OPPORTUNITY_AMOUNT > 0
      GROUP BY 1, 2, 3, 4
    )
    SELECT * FROM s ORDER BY OPPORTUNITY_AMOUNT DESC LIMIT 5
  `);
  for (const r of samples) {
    console.log(`  ${r.OPPORTUNITY_ID}  amount=${r.OPPORTUNITY_AMOUNT} ${r.OPP_CCY}  close=${String(r.CLOSED_WON_DATE).substring(0,10)}  Σmr=${r.SUM_MR_EUR}  n_mr=${r.N_MR}  months=${r.DISTINCT_MONTHS}`);
  }

  // ── Probe: is there an EUR-converted opp amount field? ────────────────────
  console.log(`\n▸ Probe: do DIM_OPPORTUNITY EUR-amount candidates exist?`);
  const candCols = ['OPPORTUNITY_AMOUNT_EUR','AMOUNT_EUR','OPPORTUNITY_AMOUNT_CONVERTED','AMOUNT_CONVERTED','ARR_EUR','MRR_EUR','MRR','ARR'];
  for (const c of candCols) {
    const present = oppCols.find(r => r.COLUMN_NAME === c);
    if (present) console.log(`  ✓ ${c} EXISTS (${present.DATA_TYPE})`);
  }

  // ── Probe: DIM_OPPORTUNITY SCD-2 fanout (multiple rows per OPPORTUNITY_ID?)
  console.log(`\n▸ DIM_OPPORTUNITY row uniqueness:`);
  const scd = await q(`
    SELECT COUNT(*) AS TOTAL_ROWS, COUNT(DISTINCT OPPORTUNITY_ID) AS DISTINCT_OPPS
    FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY
    WHERE IS_OPPORTUNITY_WON = TRUE
      AND EXTRACT(YEAR FROM CLOSED_WON_DATE) = ${priorYr}
  `);
  console.log(`  total rows for ${priorYr} closed-won = ${scd[0]?.TOTAL_ROWS}`);
  console.log(`  distinct OPPORTUNITY_ID            = ${scd[0]?.DISTINCT_OPPS}`);
  console.log(`  SCD fanout                          = ${(Number(scd[0]?.TOTAL_ROWS)/Number(scd[0]?.DISTINCT_OPPS)).toFixed(2)}x`);

  // ── Probe: FCT_MONTHLY_REVENUE__SUBSET_PAID columns (curated EUR view) ────
  console.log(`\n▸ FCT_MONTHLY_REVENUE__SUBSET_PAID columns:`);
  const subsetCols = await q(`
    SELECT COLUMN_NAME, DATA_TYPE FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'FINANCE' AND TABLE_NAME = 'FCT_MONTHLY_REVENUE__SUBSET_PAID'
    ORDER BY ORDINAL_POSITION
  `).catch((e) => { console.log(`  (probe failed: ${e.message})`); return []; });
  console.log(subsetCols.map(r => `  - ${r.COLUMN_NAME} (${r.DATA_TYPE})`).join('\n'));

  // If SUBSET_PAID has OPPORTUNITY_ID + REVENUE_AMOUNT_EUR, recompute factor using it
  const subsetHasOpp = subsetCols.some(c => c.COLUMN_NAME === 'OPPORTUNITY_ID' || c.COLUMN_NAME === 'SRC_OPPORTUNITY_ID');
  if (subsetHasOpp) {
    const oppCol = subsetCols.find(c => c.COLUMN_NAME === 'OPPORTUNITY_ID')?.COLUMN_NAME || 'SRC_OPPORTUNITY_ID';
    console.log(`\n▸ Calibration recomputed using SUBSET_PAID + ${oppCol}:`);
    const altNum = await q(`
      SELECT SUM(ROUND(mr.REVENUE_AMOUNT_EUR)) AS NUMER, COUNT(*) AS N_MR
      FROM DL_PRODUCTION.FINANCE.FCT_MONTHLY_REVENUE__SUBSET_PAID mr
      JOIN DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY o ON mr.${oppCol} = o.OPPORTUNITY_ID
      WHERE EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${priorYr}
        AND o.IS_OPPORTUNITY_WON = TRUE
        AND EXTRACT(YEAR FROM o.CLOSED_WON_DATE) = ${priorYr}
        AND COALESCE(o.IS_OPPORTUNITY_REVENUE_SHARED, FALSE) = FALSE
        AND COALESCE(o.IS_PRICE_UPDATE, FALSE) = FALSE
    `).catch((e) => { console.log(`  (alt numer failed: ${e.message})`); return []; });
    const altN = Number(altNum[0]?.NUMER) || 0;
    console.log(`  alt-numer = ${altN}  (n=${altNum[0]?.N_MR})`);
    console.log(`  alt-factor = ${dn > 0 ? (altN / dn).toFixed(4) : 'n/a'}`);
  }

  console.log('\nDone.');
  process.exit(0);
})().catch(e => { console.error('ERROR:', e); process.exit(1); });
