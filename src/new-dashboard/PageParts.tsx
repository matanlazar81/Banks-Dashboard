// Page furniture shared by the New Bank Dashboard and the P&L Projection: toggles, loading, computing,
// error and warning cards.
import { useState } from 'react';
import { AlertTriangle, Loader2 } from 'lucide-react';

interface Option<T extends string> { value: T; label: string; title?: string }

export function Segmented<T extends string>({ label, value, options, onChange }: { label: string; value: T; options: Option<T>[]; onChange: (v: T) => void }) {
  return (
    <div role="group" aria-label={label} className="inline-flex rounded-md border border-slate-300 bg-white p-0.5">
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          title={o.title}
          aria-pressed={o.value === value}
          onClick={() => onChange(o.value)}
          className={`rounded px-2.5 py-1 text-xs font-medium transition-colors ${o.value === value ? 'bg-slate-800 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

export function Skeleton() {
  return (
    <div className="space-y-4" aria-busy="true" aria-label="Loading">
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        {[0, 1, 2, 3].map((i) => <div key={i} className="h-20 animate-pulse rounded-lg bg-slate-200/70" />)}
      </div>
      <div className="h-96 animate-pulse rounded-lg bg-slate-200/70" />
    </div>
  );
}

export function ComputingCard({ elapsedSec }: { elapsedSec: number | null }) {
  const s = Math.max(0, elapsedSec ?? 0);
  return (
    <div className="flex items-start gap-3 rounded-lg border border-slate-200 bg-white p-5">
      <Loader2 className="mt-0.5 shrink-0 animate-spin text-sky-600" size={20} />
      <div>
        <div className="font-medium text-slate-900">Building the projection from NetSuite and Snowflake…</div>
        <p className="mt-1 text-sm text-slate-600">
          The first load after a deploy takes a few minutes. After that the page opens instantly and updates in the background.
        </p>
        <p className="mt-1 text-xs tabular-nums text-slate-500">Running for {Math.floor(s / 60)}:{String(s % 60).padStart(2, '0')}</p>
      </div>
    </div>
  );
}

export function ErrorCard({ message, onRetry }: { message: string | null; onRetry: () => void }) {
  return (
    <div className="flex items-start gap-3 rounded-lg border border-rose-200 bg-rose-50 p-5">
      <AlertTriangle className="mt-0.5 shrink-0 text-rose-600" size={20} />
      <div>
        <div className="font-medium text-rose-900">The projection could not be loaded</div>
        <p className="mt-1 text-sm text-rose-800">{message || 'Unknown error.'}</p>
        <button type="button" onClick={onRetry} className="mt-3 rounded-md bg-rose-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-rose-700">
          Try again
        </button>
      </div>
    </div>
  );
}

/** The data warnings of a payload: the first one, the rest on demand. */
export function Warnings({ warnings }: { warnings: string[] }) {
  const [showAll, setShowAll] = useState(false);
  if (!warnings.length) return null;
  return (
    <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
      <div className="flex items-start gap-2">
        <AlertTriangle size={16} className="mt-0.5 shrink-0 text-amber-600" />
        <div className="space-y-1">
          {(showAll ? warnings : warnings.slice(0, 1)).map((w) => <p key={w}>{w}</p>)}
          {warnings.length > 1 && (
            <button type="button" onClick={() => setShowAll((s) => !s)} className="text-xs font-medium underline">
              {showAll ? 'Show less' : `Show ${warnings.length - 1} more`}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
