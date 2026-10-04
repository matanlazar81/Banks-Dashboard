// ─────────────────────────────────────────────────────────────────────────────
// Shared input stage of the projections (New Bank Dashboard and P&L Projection).
//
// Both pages run the same engine on the same inputs: the nightly job's gatherInputs(), the official
// plan and the roll-forward extras. Pulling them costs ~16 NetSuite feeds, so one pull serves both:
// a computation that starts while another one is gathering joins it, and a finished pull is reused
// for PROJECTION_INPUTS_REUSE_MIN (default 5; 0 = never reuse) within the same month.
// ─────────────────────────────────────────────────────────────────────────────
const MIN = 60 * 1000;
const pad2 = (n) => String(n).padStart(2, '0');

// Route every client method through `queue` (when given) and record which ones failed. Failures are
// rethrown so gatherInputs' own fallbacks still apply; the names surface as data warnings.
function wrapClient(client, label, failures, queue) {
  return new Proxy(client, {
    get(target, prop) {
      const value = target[prop];
      if (typeof value !== 'function') return value;
      return (...args) => new Promise((resolve) => {
        const run = () => value(...args);
        resolve(queue ? queue(run) : run());
      }).catch((e) => {
        failures.push(`${label}.${String(prop)}`);
        throw e;
      });
    },
  });
}

// Extra Snowflake reads the roll-forward needs beyond gatherInputs(): the plain budgets (gatherInputs
// merges overrides into its copies) and the Oct–Dec payroll budget by department.
async function fetchRollForwardExtras(sf, year) {
  const settle = (p) => p.then((v) => v, () => null);
  const [rawSfBudget, rawSfSalaryBudget, ...breakdowns] = await Promise.all([
    settle(sf.fetchBudgetByCategory(year)),
    settle(sf.fetchSalaryBudget(year)),
    ...[10, 11, 12].map((m) => settle(sf.fetchSalaryBudgetBreakdown(`${year}-${pad2(m)}`))),
  ]);
  return { rawSfBudget, rawSfSalaryBudget, breakdowns };
}

function reuseWindowMs() {
  const v = parseFloat(process.env.PROJECTION_INPUTS_REUSE_MIN || '');
  return (Number.isFinite(v) && v >= 0 ? v : 5) * MIN;
}

// One memo per compute module: production shares the required singleton; tests that pass their own
// stub module never see each other's inputs.
const memos = new WeakMap();

async function gather({ now, cmp, nsClient, sfClient, queueNsCall, scenarioName }) {
  const failures = [];
  const ns = wrapClient(nsClient, 'ns', failures, queueNsCall);
  const sf = wrapClient(sfClient, 'sf', failures, null);
  const Y = now.getFullYear();
  const scenarioP = cmp.loadScenarioDataAsync(scenarioName);
  const { inputs, meta } = await cmp.gatherInputs(ns, sf, Y);
  const extras = await fetchRollForwardExtras(sf, Y);
  const scenario = await scenarioP;
  return { inputs, meta, extras, scenario, failures };
}

/**
 * The engine inputs for `now`'s year. opts: { now, cmp, nsClient, sfClient, queueNsCall, scenarioName, reuseMs? }.
 * Resolves { inputs, meta, extras, scenario, failures }. Callers must treat the result as read-only:
 * it can be shared with the other page's computation.
 */
function loadProjectionInputs(opts) {
  const { now, cmp } = opts;
  const key = `${now.getFullYear()}-${pad2(now.getMonth() + 1)}|${opts.scenarioName}`;
  const reuseMs = opts.reuseMs ?? reuseWindowMs();
  const memo = memos.get(cmp);
  const t = Date.now();
  if (memo && memo.key === key && (memo.pending || t - memo.doneAt < reuseMs)) return memo.p;
  const entry = { key, pending: true, doneAt: 0, p: null };
  entry.p = gather(opts);
  memos.set(cmp, entry);
  entry.p.then(
    () => { entry.pending = false; entry.doneAt = Date.now(); },
    () => { if (memos.get(cmp) === entry) memos.delete(cmp); },
  );
  return entry.p;
}

module.exports = { loadProjectionInputs, wrapClient, fetchRollForwardExtras };
