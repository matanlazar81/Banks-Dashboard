// Which payroll accounts are permanent or temporary, saved for everyone: GET/PUT
// /api/salary-projection/classification (server/salary-projection.cjs). Only the accounts that differ from
// finance's default are saved. A hook holds the saved choices and the unsaved draft; the page re-projects
// with the draft at once.
import { useCallback, useEffect, useState } from 'react';
import type { SalaryAccount, SalaryMethod } from './types.ts';

/** Account → method, for the accounts that differ from finance's default. */
export type Methods = Record<string, SalaryMethod>;

interface SavedClassification { ok: true; value: { methods: Methods }; updatedAt: string | null; updatedBy: string | null }

const ENDPOINT = '/api/salary-projection/classification';
const SESSION = 'Your session has expired. Reload the page to sign in again.';
const NOT_INSTALLED = 'Saving the classification is not installed on this server yet.';

function asSaved(body: unknown): SavedClassification {
  const b = body as Partial<SavedClassification> | null;
  const methods = b && b.value && b.value.methods;
  if (!b || b.ok !== true || !methods || typeof methods !== 'object' || Array.isArray(methods)) throw new Error(NOT_INSTALLED);
  return { ok: true, value: { methods }, updatedAt: b.updatedAt ?? null, updatedBy: b.updatedBy ?? null };
}

async function fetchClassification(): Promise<SavedClassification> {
  const res = await fetch(ENDPOINT, { credentials: 'include', headers: { Accept: 'application/json' } });
  if (res.status === 401) throw new Error(SESSION);
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON: the route is not mounted */ }
  return asSaved(body);
}

// finance-it enforces double-submit CSRF on writes; the standalone server has no token route.
async function csrfToken(): Promise<string> {
  try {
    const r = await fetch('/api/csrf-token', { credentials: 'include' });
    if (r.ok) return ((await r.json()) as { csrfToken?: string }).csrfToken || '';
  } catch { /* no token route */ }
  return '';
}

async function saveClassification(methods: Methods): Promise<SavedClassification> {
  const csrf = await csrfToken();
  const res = await fetch(ENDPOINT, {
    method: 'PUT',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json', ...(csrf ? { 'x-csrf-token': csrf } : {}) },
    body: JSON.stringify({ value: { methods } }),
  });
  if (res.status === 401) throw new Error(SESSION);
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  const b = body as { ok?: boolean; error?: string; details?: string[] } | null;
  if (!b || b.ok !== true) {
    throw new Error(b && b.error ? [b.error, ...(b.details || []).slice(0, 3)].join(' ') : res.status === 404 ? NOT_INSTALLED : `Saving failed (HTTP ${res.status}).`);
  }
  return asSaved(body);
}

const NONE: Methods = {};

const sameMethods = (a: Methods, b: Methods) => {
  const ka = Object.keys(a);
  return ka.length === Object.keys(b).length && ka.every((k) => a[k] === b[k]);
};

export interface ClassificationState {
  /** The saved choices with any unsaved edits: what the page shows. */
  current: Methods;
  dirty: boolean;
  /** The server has the classification route (saving works). */
  available: boolean;
  loading: boolean;
  saving: boolean;
  error: string | null;
  updatedAt: string | null;
  updatedBy: string | null;
  /** Pick a method for an account; picking finance's default drops the choice. */
  choose: (account: SalaryAccount, method: SalaryMethod) => void;
  save: () => Promise<void>;
  /** Drop the unsaved edits. */
  discard: () => void;
  /** Back to finance's default for every account (unsaved until Save). */
  resetToDefaults: () => void;
}

export function useClassification(): ClassificationState {
  const [saved, setSaved] = useState<SavedClassification | null>(null);
  const [draft, setDraft] = useState<Methods | null>(null);
  const [available, setAvailable] = useState(true);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    fetchClassification()
      .then((s) => { if (alive) { setSaved(s); setAvailable(true); setError(null); } })
      .catch((e: unknown) => {
        if (!alive) return;
        const msg = e instanceof Error ? e.message : String(e);
        setAvailable(msg !== NOT_INSTALLED);
        setError(msg);
      })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, []);

  const savedMethods: Methods = saved ? saved.value.methods : NONE;
  const current = draft || savedMethods;

  const choose = useCallback((account: SalaryAccount, method: SalaryMethod) => {
    setDraft((d) => {
      const next = { ...(d || (saved ? saved.value.methods : {})) };
      if (account.defaultMethod === method) delete next[account.acct];
      else next[account.acct] = method;
      return next;
    });
  }, [saved]);

  const save = useCallback(async () => {
    if (!draft) return;
    setSaving(true);
    try {
      setSaved(await saveClassification(draft));
      setDraft(null);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [draft]);

  return {
    current,
    dirty: !!draft && !sameMethods(draft, savedMethods),
    available,
    loading,
    saving,
    error,
    updatedAt: saved ? saved.updatedAt : null,
    updatedBy: saved ? saved.updatedBy : null,
    choose,
    save,
    discard: () => { setDraft(null); setError(null); },
    resetToDefaults: () => setDraft({}),
  };
}
