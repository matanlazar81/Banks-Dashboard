// GET /api/fx-challenge, and what the page saves: the table (PUT /api/fx-challenge/table) and who gets the daily mail
// (GET/PUT /api/fx-challenge/mail), server/fx-challenge.cjs. Relative paths: the page is served next to the dashboard API.
import type { FxTable } from '../forecast/fx-challenge.mjs';
import type { FxResponse, MailSettings, SavedTable } from './types.ts';

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

/** PUT { value } as JSON, with the CSRF token; the saved document, or an error saying why not. */
async function putValue<T>(url: string, value: unknown): Promise<T> {
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
  return body as T;
}

/** Saves the table for everyone; null puts the default back. */
export const saveTable = (table: FxTable | null) => putValue<SavedTable>('/api/fx-challenge/table', table);

/** Who gets the daily mail; null when this server has no such route yet (finance-it's route file is older). */
export async function fetchMail(): Promise<MailSettings | null> {
  const res = await fetch('/api/fx-challenge/mail', { credentials: 'include', headers: { Accept: 'application/json' } });
  if (res.status === 404) return null;
  if (res.status === 401 || res.status === 403) throw new Error(SESSION);
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  if (!body || typeof body !== 'object' || (body as { ok?: boolean }).ok !== true) throw new Error(`The daily mail's settings could not be read (HTTP ${res.status}).`);
  return body as MailSettings;
}

/** Saves who gets the daily mail, for everyone; null goes back to the server's FX_MAIL_TO. */
export const saveMail = (to: string[] | null) => putValue<unknown>('/api/fx-challenge/mail', to === null ? null : { to });

/** Sends the daily mail now, to the saved list: how many it went to and its subject, or an error saying why not. */
export async function sendMailNow(): Promise<{ recipients: number; subject: string; at: string }> {
  const csrf = await csrfToken();
  const res = await fetch('/api/fx-challenge/mail/send', {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json', ...(csrf ? { 'x-csrf-token': csrf } : {}) },
    body: '{}',
  });
  let body: unknown = null;
  try { body = await res.json(); } catch { /* not JSON */ }
  const b = body as { ok?: boolean; error?: string; recipients?: number; subject?: string; at?: string } | null;
  if (res.status === 401) throw new Error(SESSION);
  if (res.status === 404) throw new Error(NOT_INSTALLED);
  if (!b || b.ok !== true) throw new Error(b && b.error ? b.error : `Sending failed (HTTP ${res.status}).`);
  return { recipients: b.recipients ?? 0, subject: b.subject ?? '', at: b.at ?? '' };
}
