// View choices live in the URL (?plan=base&ccy=ils&years=next) so a link reproduces the view.
// No localStorage: the pages run inside the finance-it iframe, where storage may be blocked.
export function readParam(name: string): string | null {
  try { return new URLSearchParams(window.location.search).get(name); } catch { return null; }
}

export function writeParam(name: string, value: string | null) {
  try {
    const url = new URL(window.location.href);
    if (value == null) url.searchParams.delete(name); else url.searchParams.set(name, value);
    window.history.replaceState(window.history.state, '', url);
  } catch { /* history unavailable in this frame — the view still switches */ }
}
