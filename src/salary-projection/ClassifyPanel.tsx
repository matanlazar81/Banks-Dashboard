// Side panel to choose, per payroll account, how it is projected: Permanent (the last closed month carried
// forward), Temporary (the same month last year) or Actuals only (not projected). Every change shows in the
// table at once; Save shares the classification with everyone who opens the page.
import { Loader2, RotateCcw, X } from 'lucide-react';
import { METHOD_LABEL, SALARY_METHODS } from '../forecast/salary-projection.mjs';
import { Segmented } from '../new-dashboard/PageParts.tsx';
import type { ClassificationState } from './classification.ts';
import type { SalaryAccount, SalaryMethod } from './types.ts';

const OPTION_TITLE: Record<SalaryMethod, string> = {
  'run-rate': 'Every forecast month carries the last closed month',
  'last-year': 'The rest of this year repeats the same month of last year; next year is 0',
  none: 'Actual months only; forecast months are 0',
};
const OPTIONS = SALARY_METHODS.map((m) => ({ value: m, label: METHOD_LABEL[m], title: OPTION_TITLE[m] }));

interface Props {
  /** The accounts with the classification the page shows (saved choices plus unsaved edits). */
  accounts: SalaryAccount[];
  state: ClassificationState;
  runRate: string;
  onClose: () => void;
}

export default function ClassifyPanel({ accounts, state, runRate, onClose }: Props) {
  const saved = state.updatedAt
    ? `Saved ${new Date(state.updatedAt).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })}${state.updatedBy ? ` by ${state.updatedBy}` : ''}`
    : "Finance's default classification (nothing saved yet)";

  return (
    <aside
      role="dialog"
      aria-label="Permanent or temporary"
      className="fixed inset-y-0 right-0 z-40 flex w-full max-w-md flex-col border-l border-slate-200 bg-white shadow-2xl"
    >
      <div className="flex items-start justify-between gap-2 border-b border-slate-200 px-4 py-3">
        <div>
          <h2 className="text-sm font-semibold text-slate-900">Permanent or temporary</h2>
          <p className="mt-0.5 text-xs leading-relaxed text-slate-500">
            Permanent: {runRate} carried into every forecast month. Temporary: the rest of this year repeats the same
            month of last year, and next year is 0. Actuals only: not projected. The table changes as you pick; Save applies it for everyone.
          </p>
        </div>
        <button type="button" onClick={onClose} aria-label="Close" className="rounded p-1 text-slate-500 hover:bg-slate-100 hover:text-slate-800">
          <X size={16} />
        </button>
      </div>

      <ul className="flex-1 divide-y divide-slate-100 overflow-y-auto">
        {accounts.map((a) => {
          const unclassified = a.group === 'other';
          const differs = a.defaultMethod !== null && a.method !== a.defaultMethod;
          return (
            <li key={a.acct} className="px-4 py-2.5">
              <div className="mb-1.5 flex items-baseline justify-between gap-2">
                <span className="text-xs font-medium text-slate-800">{a.acct} - {a.name}</span>
                {unclassified && <span className="shrink-0 text-[11px] font-medium text-amber-700">Not classified</span>}
                {differs && <span className="shrink-0 text-[11px] text-slate-500">Default: {METHOD_LABEL[a.defaultMethod as SalaryMethod]}</span>}
              </div>
              <Segmented
                label={`${a.acct} ${a.name}`}
                value={a.method}
                options={OPTIONS}
                onChange={(m) => state.choose(a, m)}
              />
            </li>
          );
        })}
      </ul>

      <div className="space-y-2 border-t border-slate-200 px-4 py-3">
        {state.error && <p className="text-xs text-rose-700">{state.error}</p>}
        {!state.available && <p className="text-xs text-amber-700">Changes show on this page only until the server can save them.</p>}
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => { void state.save(); }}
            disabled={!state.dirty || state.saving || !state.available}
            className="inline-flex items-center gap-1.5 rounded-md bg-slate-800 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-700 disabled:opacity-50"
          >
            {state.saving && <Loader2 size={14} className="animate-spin" />}
            Save for everyone
          </button>
          {state.dirty && (
            <button type="button" onClick={state.discard} className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100">
              Discard changes
            </button>
          )}
          <button
            type="button"
            onClick={state.resetToDefaults}
            disabled={Object.keys(state.current).length === 0}
            className="ml-auto inline-flex items-center gap-1 rounded-md px-2 py-1.5 text-xs text-slate-600 hover:bg-slate-100 disabled:opacity-50"
            title="Back to finance's default for every account (Save to apply it for everyone)"
          >
            <RotateCcw size={13} />
            Defaults
          </button>
        </div>
        <p className="text-[11px] text-slate-500">{state.loading ? 'Loading the saved classification…' : saved}{state.dirty ? ' · unsaved changes' : ''}</p>
      </div>
    </aside>
  );
}
