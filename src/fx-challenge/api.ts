// GET /api/fx-challenge and the table the page saves (PUT /api/fx-challenge/table, server/fx-challenge.cjs).
// Relative paths: the page is served next to the dashboard API.
import type { FxTable } from '../forecast/fx-challenge.mjs';
import type { FxResponse, SavedTable } from './types.ts';

const SESSION = 'Your session has expired. Reload the page to sign in again.';
const NOT_INSTALLED = 'This server does not have the FX Challenge yet: run Pull & Build and restart it.';

export async function fetchFx(refresh: boolean): Promise<FxResponse> {
  const res = await fetch(`/api/fx-challenge${refresh ? '?refresh=true' : ''}`, { credentials: 'include', headers: { Accept: 'application/json' } });
  if (res.status === 401 || res.status === 403) throw new Error(SESSION);
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  if (!body || typeof body !== 'object' || !('status' in body)) {
    throw new Error(res.status === 404 || res.ok ? NOT_INSTALLED : `The server returned an unexpected response (HTTP ${res.status}).`);
  }
  return body as FxResponse;
}

// finance-it enforces double-submit CSRF on writes; the standalone server has no token route.
async function csrfToken(): Promise<string> {
  try {
    const r = await fetch('/api/csrf-token', { credentials: 'include' });
    if (r.ok) return ((await r.json()) as { csrfToken?: string }).csrfToken || '';
  } catch { /* no token route */ }
  return '';
}

/** Saves the table for everyone; null puts the default back. */
export async function saveTable(table: FxTable | null): Promise<SavedTable> {
  const csrf = await csrfToken();
  const res = await fetch('/api/fx-challenge/table', {
    method: 'PUT',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json', ...(csrf ? { 'x-csrf-token': csrf } : {}) },
    body: JSON.stringify({ value: table }),
  });
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  const b = body as { ok?: boolean; error?: string; details?: string[] } | null;
  if (res.status === 401) throw new Error(SESSION);
  if (!b || b.ok !== true) {
    throw new Error(b && b.error ? [b.error, ...(b.details || []).slice(0, 3)].join(' ') : `Saving failed (HTTP ${res.status}).`);
  }
  return body as SavedTable;
}
