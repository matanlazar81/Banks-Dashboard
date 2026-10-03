import type { ProjectionResponse } from './types.ts';

/** GET /api/cash-projection. Relative path: the page is served next to the dashboard API. */
export async function fetchProjection(refresh: boolean): Promise<ProjectionResponse> {
  const res = await fetch(`/api/cash-projection${refresh ? '?refresh=true' : ''}`, {
    credentials: 'include',
    headers: { Accept: 'application/json' },
  });
  if (res.status === 401 || res.status === 403) {
    throw new Error('Your session has expired. Reload the page to sign in again.');
  }
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  if (!body || typeof body !== 'object' || !('status' in body)) {
    throw new Error(res.status === 404
      ? 'The projection service is not installed on this server yet.'
      : `The server returned an unexpected response (HTTP ${res.status}).`);
  }
  return body as ProjectionResponse;
}
