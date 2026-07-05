# scripts/fixtures

Drop `forecast-golden.json` here to enable the golden (€0-diff) mode of
`scripts/test-forecast-core.cjs`. See `docs/forecast-core-golden-test.md` for
how to capture it from a running dashboard.

`forecast-golden.json` is **git-ignored** — it can contain internal finance
figures, so it must not be committed or shared. The test runs fine without it
(smoke mode still exercises every branch).
