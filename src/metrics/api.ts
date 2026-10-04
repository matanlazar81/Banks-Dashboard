// GET /api/metrics, the breakdown of one figure (?detail=), and the settings the page saves
// (server/metrics.cjs). Relative paths: the page is served next to the dashboard API.
import type { Explain, MetricsResponse, MetricsSettings, SavedDoc } from './types.ts';

const SESSION = 'Your session has expired. Reload the page to sign in again.';

export async function fetchMetrics(refresh: boolean): Promise<MetricsResponse> {
  const res = await fetch(`/api/metrics${refresh ? '?refresh=true' : ''}`, { credentials: 'include', headers: { Accept: 'application/json' } });
  if (res.status === 401 || res.status === 403) throw new Error(SESSION);
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  if (!body || typeof body !== 'object' || !('status' in body)) {
    throw new Error(res.status === 404 ? 'The Metrics service is not installed on this server yet.' : `The server returned an unexpected response (HTTP ${res.status}).`);
  }
  return body as MetricsResponse;
}

// finance-it enforces double-submit CSRF on writes; the standalone server has no token route.
async function csrfToken(): Promise<string> {
  try {
    const r = await fetch('/api/csrf-token', { credentials: 'include' });
    if (r.ok) return ((await r.json()) as { csrfToken?: string }).csrfToken || '';
  } catch { /* no token route */ }
  return '';
}

async function put<T>(url: string, value: T): Promise<SavedDoc<T>> {
  const csrf = await csrfToken();
  const res = await fetch(url, {
    method: 'PUT',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json', ...(csrf ? { 'x-csrf-token': csrf } : {}) },
    body: JSON.stringify({ value }),
  });
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  const b = body as { ok?: boolean; error?: string; details?: string[] } | null;
  if (res.status === 401) throw new Error(SESSION);
  if (!b || b.ok !== true) {
    throw new Error(b && b.error ? [b.error, ...(b.details || []).slice(0, 3)].join(' ') : `Saving failed (HTTP ${res.status}).`);
  }
  return body as SavedDoc<T>;
}

export const saveSettings = (s: MetricsSettings) => put('/api/metrics/settings', s);

/** What one pack figure is made of (GET /api/metrics?detail=…). */
export async function fetchDetail(item: string, signal?: AbortSignal): Promise<Explain> {
  const res = await fetch(`/api/metrics?detail=${encodeURIComponent(item)}`, { credentials: 'include', headers: { Accept: 'application/json' }, signal });
  if (res.status === 401 || res.status === 403) throw new Error(SESSION);
  const b = (await res.json().catch(() => null)) as { ok?: boolean; status?: string; error?: string; detail?: Explain } | null;
  if (b && b.status === 'computing') throw new Error('The projections are being rebuilt. Try again in a minute.');
  if (!b || b.ok !== true || !b.detail) {
    throw new Error(b && b.error ? b.error : res.status === 404 ? 'This server does not have the breakdowns yet: Pull & Build and restart it.' : `The server returned an unexpected response (HTTP ${res.status}).`);
  }
  return b.detail;
}
