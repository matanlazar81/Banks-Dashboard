// The projection-year targets shared by both projection pages: GET/PUT /api/projection-targets
// (server/projection-targets.cjs), a hook holding the saved targets and the unsaved draft, and the
// Targets view (the Plan with the targets applied, src/forecast/targets.mjs).
import { useCallback, useEffect, useMemo, useState } from 'react';
import { emptyTargets, isEmptyTargets, validateTargets, type Targets, type TargetsBase } from '../forecast/targets.mjs';

export interface TargetsStore {
  ok: true;
  years: Record<string, Targets>;
  updatedAt: string | null;
  updatedBy: string | null;
  reference: { serverRatioYtd: number | null };
}

const ENDPOINT = '/api/projection-targets';

async function readJson(res: Response): Promise<unknown> {
  if (res.status === 401 || res.status === 403) {
    let msg = 'Your session has expired. Reload the page to sign in again.';
    try { const b = await res.json() as { error?: string }; if (res.status === 403 && b && b.error) msg = b.error; } catch { /* not JSON */ }
    throw new Error(msg);
  }
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  if (!body || typeof body !== 'object') {
    throw new Error(res.status === 404 ? 'Targets are not installed on this server yet.' : `The server returned an unexpected response (HTTP ${res.status}).`);
  }
  const b = body as { ok?: boolean; error?: string; details?: string[] };
  if (b.ok === false) throw new Error([b.error || 'The request failed.', ...(b.details || []).slice(0, 3)].join(' '));
  return body;
}

// Only a well-formed answer counts: anything else (a route not mounted yet answers with the host app's own
// "not found") means targets are unavailable, never a broken page.
function asStore(body: unknown): TargetsStore {
  const b = body as Partial<TargetsStore> | null;
  if (!b || b.ok !== true || !b.years || typeof b.years !== 'object' || Array.isArray(b.years)) {
    throw new Error('Targets are not installed on this server yet.');
  }
  const ratio = b.reference && typeof b.reference.serverRatioYtd === 'number' ? b.reference.serverRatioYtd : null;
  return { ok: true, years: b.years, updatedAt: b.updatedAt ?? null, updatedBy: b.updatedBy ?? null, reference: { serverRatioYtd: ratio } };
}

export async function fetchTargets(): Promise<TargetsStore> {
  return asStore(await readJson(await fetch(ENDPOINT, { credentials: 'include', headers: { Accept: 'application/json' } })));
}

export async function saveTargets(year: number, targets: Targets): Promise<TargetsStore> {
  // finance-it enforces double-submit CSRF on writes; the standalone server has no token route.
  let csrf = '';
  try {
    const r = await fetch('/api/csrf-token', { credentials: 'include' });
    if (r.ok) csrf = ((await r.json()) as { csrfToken?: string }).csrfToken || '';
  } catch { /* no token route */ }
  const res = await fetch(ENDPOINT, {
    method: 'PUT',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json', ...(csrf ? { 'x-csrf-token': csrf } : {}) },
    body: JSON.stringify({ year, targets }),
  });
  return asStore(await readJson(res));
}

export interface TargetsState {
  /** Saved targets of the base's year (empty targets when none were saved). */
  saved: Targets;
  /** What the Targets view shows: the saved targets with any unsaved edits. */
  draft: Targets;
  setDraft: (t: Targets) => void;
  dirty: boolean;
  /** Something is set (saved or not): the Targets view differs from the Plan. */
  active: boolean;
  loading: boolean;
  saving: boolean;
  error: string | null;
  updatedAt: string | null;
  updatedBy: string | null;
  serverRatioYtd: number | null;
  save: () => Promise<void>;
  reset: () => void;
  clear: () => void;
}

/** The targets of base.year: loads them once, keeps the draft, saves it. */
export function useTargets(base: TargetsBase | null | undefined): TargetsState {
  const year = base ? base.year : null;
  const [store, setStore] = useState<TargetsStore | null>(null);
  const [draft, setDraftState] = useState<Targets | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    fetchTargets()
      .then((s) => { if (alive) { setStore(s); setError(null); } })
      .catch((e: unknown) => { if (alive) setError(e instanceof Error ? e.message : String(e)); })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, []);

  const savedRaw = store && store.years && year ? store.years[String(year)] : undefined;
  // The same object until the store changes, so the Targets view is recomputed only then.
  const saved = useMemo(() => (savedRaw ? (validateTargets(savedRaw).targets || emptyTargets()) : emptyTargets()), [savedRaw]);
  const shown = draft || saved;
  const setDraft = useCallback((t: Targets) => setDraftState(t), []);

  const save = useCallback(async () => {
    if (!year || !draft) return;
    const v = validateTargets(draft);
    if (!v.ok || !v.targets) { setError(`Check the targets: ${v.errors.slice(0, 3).join('; ')}`); return; }
    setSaving(true);
    try {
      setStore(await saveTargets(year, v.targets));
      setDraftState(null);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [year, draft]);

  return {
    saved,
    draft: shown,
    setDraft,
    dirty: !!draft && JSON.stringify(draft) !== JSON.stringify(saved),
    active: !isEmptyTargets(shown),
    loading,
    saving,
    error,
    updatedAt: savedRaw && store ? store.updatedAt : null,
    updatedBy: savedRaw && store ? store.updatedBy : null,
    serverRatioYtd: (base && base.serverRatioYtd != null) ? base.serverRatioYtd : (store && store.reference ? store.reference.serverRatioYtd : null),
    save,
    reset: () => setDraftState(null),
    clear: () => setDraftState(emptyTargets()),
  };
}

/** The change a targets view makes to one cell (line × 'YYYY-MM' or 'FY-YYYY'), for its breakdown. */
export function cellDelta<F>(
  plan: { years: { year: number; rows: { mKey: string; eur: F; ils: F }[] }[] },
  withTargets: { years: { year: number; rows: { mKey: string; eur: F; ils: F }[] }[] },
  line: string, period: string, ccy: 'eur' | 'ils',
): number {
  const fy = /^FY-(\d{4})$/.exec(period);
  let d = 0;
  plan.years.forEach((y, yi) => {
    if (fy && y.year !== Number(fy[1])) return;
    y.rows.forEach((r, ri) => {
      if (!fy && r.mKey !== period) return;
      const t = withTargets.years[yi].rows[ri];
      const v = (f: F) => Number((f as unknown as Record<string, number>)[line]) || 0;
      d += v(t[ccy]) - v(r[ccy]);
    });
  });
  return Math.round(d * 100) / 100;
}
