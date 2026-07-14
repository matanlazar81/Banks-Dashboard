# SF Revenue Migration — Phase 0 Capture (before/after parity)

Goal: capture every detailed number **before** the revenue-table migration so we can prove nothing
changed afterward except the known impact zones. Run these while the **old** tables are still
queryable (they are frozen at 2026-06-23 and will be dropped).

Two tools, both read-only. Neither writes to Snowflake or mutates app state.

---

## 1. Source parity (old vs new, one run) — `scripts/verify-mr-migration.cjs`

Runs on the server (needs the Snowflake creds in `backend/.env`, same as `diagnose-calibration.cjs`).
Queries the OLD `FINANCE` tables and the NEW `CONSUMER_HUB__BANKS_DASHBOARD` table in the same run and
diffs them at the source.

```bash
node scripts/verify-mr-migration.cjs                 # year=2026, asof=today
node scripts/verify-mr-migration.cjs --year=2026 --asof=2026-06-30 --months=2026-05,2026-06,2025-06
```

Outputs (under `data/migration-snapshots/`):
- `mr-parity-<ts>.json` — machine-readable full diff
- `mr-parity-<ts>.md` — reviewer report: flagged (>1%) diffs with **old→new, Δ, Δ%, driver (GAP #),
  and the named widget where you'd see it**, plus the calibration factor old→new and the GAP-3
  integration-filter decision.

Expected: 2025 and Jan–May 2026 near-exact; June/July 2026 diffs are legitimate SF corrections after
the freeze (GAP 4); projection diffs are GAP 5/6. If the report says **"NEW TABLE NOT READABLE"**, the
role needs a read grant on `CONSUMER_HUB__BANKS_DASHBOARD` — request it and re-run.

---

## 2. End-to-end dashboard snapshot — `scripts/capture-dashboard-snapshot.cjs`

Snapshots every load-path API response so the whole dashboard (not just the source tables) can be
compared. Run **before** against prod (old code/tables) and **after** against localhost (new code),
at **both** asOf modes.

```bash
# BEFORE (prod, old) — prod serves on :8790
node scripts/capture-dashboard-snapshot.cjs --label=before --base=http://localhost:8790 --asof=2026-06-30
node scripts/capture-dashboard-snapshot.cjs --label=before --base=http://localhost:8790 --asof=live

# AFTER (localhost, new) — dev serves on :5176
node scripts/capture-dashboard-snapshot.cjs --label=after --base=http://localhost:5176 --asof=2026-06-30
node scripts/capture-dashboard-snapshot.cjs --label=after --base=http://localhost:5176 --asof=live
```

Writes one JSON per endpoint to `data/migration-snapshots/<label>/<asof>/`. No auth header is needed
for read endpoints. Pin `--asof=2026-06-30` for a reproducible forecast; `--asof=live` matches what's
on screen today (capture before/after the same day for live).

### Layer 2 — client-computed numbers (not in any API response)
The cashflow forecast rows (REVENUE / TOTAL INFLOWS / CLOSING), KR5, the bridge revenue rows, the YoY
card, and the pipeline factor are computed in the browser. Capture them per snapshot:

1. Open `<base>/?fccapture=1&asOf=2026-06-30` (omit `&asOf=` for live).
2. In the browser console: `copy(JSON.stringify(window.__fcRows))` and save as
   `data/migration-snapshots/<label>/<asof>/__fcRows.json`.
3. Jot the on-screen numbers next to it: KR5 "Dec Closing (forecast)" + net growth, the Budget Bridge
   revenue row (Budget/Actual/Variance), the OKR YoY card, the Pipeline `×N.NN` label + methodology
   Column B, and the department revenue budget-vs-actual card.

### Diff two snapshots (offline)
```bash
node scripts/capture-dashboard-snapshot.cjs --diff=before,after --asof=2026-06-30
node scripts/capture-dashboard-snapshot.cjs --diff=before,after --asof=live
```
Prints changed endpoints split into **expected** (revenue impact zones: `sf-revenue`,
`sf-revenue-paid`, `sf-yoy-revenue`, `sf-pipeline-methodology`, `sf-revenue-breakdown`) and
**SHOULD-BE-UNCHANGED** (everything else — any entry here is a regression to investigate).

---

## Sign-off gate

The migration's table-reference code changes do **not** deploy to prod until: (1) the parity report
is reviewed and every flagged diff carries an accepted driver, and (2) the before/after diff shows
changes only in the named impact zones.
