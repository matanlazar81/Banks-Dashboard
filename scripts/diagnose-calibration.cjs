// Diagnostic for the Column B pipeline calibration factor.
// Recomputes numerator / denominator / factor against the current MR source and
// checks for the failure modes that historically distorted it (SCD fanout,
// record fanout per opp/month, opp-amount vs Σ MR mismatch).
//
//   node scripts/diagnose-calibration.cjs
//
// MR source: DL_PRODUCTION.CONSUMER_HUB__BANKS_DASHBOARD.
//            FCT_OPPORTUNITY_MONTHLY_REVENUE__SCD_DAILY__BANKS_DASHBOARD
// (migrated 2026-07 from the retired FINANCE.FCT_MONTHLY_REVENUE* tables). The
// new table is EUR-native — amount = REVENUE_AMOUNT_EUR, no CURRENCY column —
// and exposes IS_INTEGRATION_MONTH, which the calibration numerator excludes.
//
// Dumps:
//   - MR source columns + DIM_OPPORTUNITY amount/currency columns
//   - Numerator / denominator raw values for prior year
//   - MR records per (opportunity, month) — fanout check
//   - Sample 5 closed-won opps showing OPPORTUNITY_AMOUNT vs Σ REVENUE_AMOUNT_EUR

const path = require('path');
const fs = require('fs');
const { createSnowflakeClient } = require('../snowflake-api.cjs');

const MR_TABLE = 'DL_PRODUCTION.CONSUMER_HUB__BANKS_DASHBOARD.FCT_OPPORTUNITY_MONTHLY_REVENUE__SCD_DAILY__BANKS_DASHBOARD';
const MR_SCHEMA = 'CONSUMER_HUB__BANKS_DASHBOARD';
const MR_NAME = 'FCT_OPPORTUNITY_MONTHLY_REVENUE__SCD_DAILY__BANKS_DASHBOARD';
// Integration-month exclusion applied to the numerator (matches snowflake-api.cjs).
const MR_WHERE = `AND COALESCE(mr.REVENUE_AMOUNT_EUR, 0) <> 0
      AND COALESCE(mr.IS_INTEGRATION_MONTH, FALSE) = FALSE`;

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

  // ── MR source columns ─────────────────────────────────────────────────────
  console.log(`▸ ${MR_NAME} columns:`);
  const mrCols = await q(`
    SELECT COLUMN_NAME, DATA_TYPE FROM DL_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '${MR_SCHEMA}' AND TABLE_NAME = '${MR_NAME}'
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

  // ── Raw numerator + denominator (current SQL) ─────────────────────────────
  console.log(`\n▸ Calibration components (current SQL, EUR-native MR):`);
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
    SELECT SUM(ROUND(mr.REVENUE_AMOUNT_EUR)) AS NUMER, COUNT(*) AS N_MR
    FROM ${MR_TABLE} mr
    JOIN DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY o ON mr.OPPORTUNITY_ID = o.OPPORTUNITY_ID
    WHERE EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${priorYr}
      AND o.IS_OPPORTUNITY_WON = TRUE
      AND EXTRACT(YEAR FROM o.CLOSED_WON_DATE) = ${priorYr}
      AND COALESCE(o.IS_OPPORTUNITY_REVENUE_SHARED, FALSE) = FALSE
      AND COALESCE(o.IS_PRICE_UPDATE, FALSE) = FALSE
      ${MR_WHERE}
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
    FROM ${MR_TABLE} mr
    JOIN DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY o ON mr.OPPORTUNITY_ID = o.OPPORTUNITY_ID
    WHERE EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${priorYr}
      AND o.IS_OPPORTUNITY_WON = TRUE
      AND EXTRACT(YEAR FROM o.CLOSED_WON_DATE) = ${priorYr}
      ${MR_WHERE}
  `);
  const fr = fanout[0] || {};
  console.log(`  total mr rows   = ${fr.TOTAL_ROWS}`);
  console.log(`  distinct pairs  = ${fr.DISTINCT_PAIRS}`);
  console.log(`  distinct opps   = ${fr.DISTINCT_OPPS}`);
  console.log(`  fanout/pair     = ${(Number(fr.TOTAL_ROWS)/Number(fr.DISTINCT_PAIRS)).toFixed(2)}x`);

  // ── Sample: pick 5 top closed-won opps and compare amount vs Σ MR ─────────
  console.log(`\n▸ Top 5 prior-year closed-won opps — OPPORTUNITY_AMOUNT vs Σ REVENUE_AMOUNT_EUR:`);
  const samples = await q(`
    WITH s AS (
      SELECT o.OPPORTUNITY_ID, o.OPPORTUNITY_AMOUNT, o.CLOSED_WON_DATE,
             ${oppCurCol ? `o.${oppCurCol} AS OPP_CCY,` : `'?' AS OPP_CCY,`}
             COUNT(mr.OPPORTUNITY_ID) AS N_MR,
             SUM(ROUND(mr.REVENUE_AMOUNT_EUR)) AS SUM_MR_EUR,
             COUNT(DISTINCT TO_VARCHAR(mr.CAL_MONTH_START_DATE,'YYYY-MM')) AS DISTINCT_MONTHS
      FROM DL_PRODUCTION.FINANCE.DIM_OPPORTUNITY o
      LEFT JOIN ${MR_TABLE} mr
        ON o.OPPORTUNITY_ID = mr.OPPORTUNITY_ID
        AND EXTRACT(YEAR FROM mr.CAL_MONTH_START_DATE) = ${priorYr}
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

  console.log('\nDone.');
  process.exit(0);
})().catch(e => { console.error('ERROR:', e); process.exit(1); });
