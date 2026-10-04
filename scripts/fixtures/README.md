# scripts/fixtures

Drop `forecast-golden.json` here to enable the golden (€0-diff) mode of
`scripts/test-forecast-core.cjs`. See `docs/forecast-core-golden-test.md` for
how to capture it from a running dashboard.

`forecast-golden.json` is **git-ignored** — it can contain internal finance
figures, so it must not be committed or shared. The test runs fine without it
(smoke mode still exercises every branch).

`cash-projection-sample.json` is a **synthetic** `/api/cash-projection` payload
(made-up numbers, not LSports data) used for New Bank Dashboard UI checks and
screenshots. Regenerate it with `node scripts/test-cash-projection.cjs --write-fixture`.

`pnl-projection-sample.json` is a **synthetic** `/api/pnl-projection` payload (made-up
numbers, not LSports data) used for P&L Projection UI checks and screenshots.
Regenerate it with `node scripts/test-pnl-projection.cjs --write-fixture`.
