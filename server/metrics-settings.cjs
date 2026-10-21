// ─────────────────────────────────────────────────────────────────────────────
// What the Metrics page saves for everyone, and what may be saved (server/json-store.cjs stores it):
//   settings  the cloud cap (% of projected revenue, which category is cloud) and the FX Challenge's conversion
//             table (fxChallenge; none saved = the default in src/forecast/fx-challenge.mjs)
//   deposits  the deposit tracker (no longer on the page; its route stays mounted for finance-it's route file)
// ─────────────────────────────────────────────────────────────────────────────

const DEFAULT_SETTINGS = Object.freeze({
  cloudCapPct: 8,
  cloudCategory: '',
});
// Removed from the page (the innovation envelope, the USD/EUR planning rate): settings saved with them
// still validate, without them.
const LEGACY_KEYS = ['innovation', 'usdEurPlanningRate'];
const emptySettings = () => JSON.parse(JSON.stringify(DEFAULT_SETTINGS));

const CURRENCIES = ['EUR', 'USD', 'ILS', 'GBP', 'PLN', 'CHF'];
const RESERVED = new Set(['__proto__', 'constructor', 'prototype']);

function keysOnly(obj, allowed, where, errors) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) { errors.push(`${where}: expected an object`); return false; }
  for (const k of Object.keys(obj)) if (!allowed.includes(k)) errors.push(`${where}: unknown field "${k}"`);
  return true;
}
function numIn(v, lo, hi, where, errors) {
  if (typeof v !== 'number' || !Number.isFinite(v) || v < lo || v > hi) { errors.push(`${where}: a number from ${lo} to ${hi}`); return null; }
  return v;
}
function textIn(v, max, where, errors, { optional = false } = {}) {
  if (optional && (v === undefined || v === null || v === '')) return '';
  if (typeof v !== 'string' || v.length > max || /[\u0000-\u001f]/.test(v) || RESERVED.has(v.trim()) || (!optional && !v.trim())) {
    errors.push(`${where}: text of ${optional ? 0 : 1}–${max} characters`);
    return '';
  }
  return v.trim();
}
function dateIn(v, where, errors, { optional = false } = {}) {
  if (optional && (v === undefined || v === null || v === '')) return null;
  if (typeof v !== 'string' || !/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/.test(v) || Number.isNaN(Date.parse(`${v}T00:00:00Z`))) {
    errors.push(`${where}: a date (YYYY-MM-DD)`);
    return null;
  }
  return v;
}

// The FX Challenge table: the same rules as tableProblems in src/forecast/fx-challenge.mjs (the page checks
// a pasted table with it before saving).
const FX_LIMITS = { rows: 60, bounds: 30, eurK: 100000, rate: 20 };
function fxTableIn(t, errors) {
  const where = 'fxChallenge';
  if (!keysOnly(t, ['pair', 'fixingDay', 'bounds', 'rows'], where, errors)) return null;
  const pair = textIn(t.pair, 20, `${where}.pair`, errors);
  let fixingDay = t.fixingDay;
  if (!(Number.isInteger(fixingDay) && fixingDay >= 1 && fixingDay <= 28)) { errors.push(`${where}.fixingDay: a day from 1 to 28`); fixingDay = 7; }
  if (!Array.isArray(t.bounds) || !t.bounds.length || t.bounds.length > FX_LIMITS.bounds) { errors.push(`${where}.bounds: 1–${FX_LIMITS.bounds} rates`); return null; }
  const bounds = t.bounds.map((b, i) => {
    const v = numIn(b, 0.0001, FX_LIMITS.rate, `${where}.bounds[${i + 1}]`, errors);
    if (v !== null && i && !(v > t.bounds[i - 1])) errors.push(`${where}.bounds[${i + 1}]: above the bound before`);
    return v;
  });
  if (!Array.isArray(t.rows) || !t.rows.length || t.rows.length > FX_LIMITS.rows) { errors.push(`${where}.rows: 1–${FX_LIMITS.rows} months`); return null; }
  const months = new Set();
  const rows = t.rows.map((r, n) => {
    const at = `${where}.rows[${n + 1}]`;
    if (!keysOnly(r, ['month', 'cells'], at, errors)) return null;
    const month = typeof r.month === 'string' && /^\d{4}-(0[1-9]|1[0-2])$/.test(r.month) ? r.month : (errors.push(`${at}.month: YYYY-MM`), '');
    if (month && months.has(month)) errors.push(`${at}.month: ${month} twice`);
    months.add(month);
    if (!Array.isArray(r.cells) || r.cells.length !== bounds.length + 1) { errors.push(`${at}.cells: ${bounds.length + 1} (one per band)`); return null; }
    const cells = r.cells.map((c, i) => (keysOnly(c, ['eurK', 'rate'], `${at}.cells[${i + 1}]`, errors) ? {
      eurK: numIn(c.eurK, -FX_LIMITS.eurK, FX_LIMITS.eurK, `${at}.cells[${i + 1}].eurK`, errors),
      rate: numIn(c.rate, 0.0001, FX_LIMITS.rate, `${at}.cells[${i + 1}].rate`, errors),
    } : null));
    return { month, cells };
  });
  return { pair, fixingDay, bounds, rows: rows.filter(Boolean).sort((a, b) => a.month.localeCompare(b.month)) };
}

/** PUT body { value: settings } → { ok, value, errors }. fxChallenge: null puts the default table back. */
function validateSettings(body) {
  const errors = [];
  const out = emptySettings();
  if (!keysOnly(body, ['value'], 'body', errors) || !keysOnly(body.value, ['cloudCapPct', 'cloudCategory', 'fxChallenge', ...LEGACY_KEYS], 'settings', errors)) {
    return { ok: false, value: null, errors };
  }
  const v = body.value;
  if (v.cloudCapPct !== undefined) out.cloudCapPct = numIn(v.cloudCapPct, 0, 100, 'cloudCapPct', errors);
  if (v.cloudCategory !== undefined) out.cloudCategory = textIn(v.cloudCategory, 120, 'cloudCategory', errors, { optional: true });
  if (v.fxChallenge !== undefined && v.fxChallenge !== null) out.fxChallenge = fxTableIn(v.fxChallenge, errors);
  return errors.length ? { ok: false, value: null, errors } : { ok: true, value: out, errors: [] };
}

/** PUT body { value: deposits[] } → { ok, value, errors }. */
function validateDeposits(body) {
  const errors = [];
  if (!keysOnly(body, ['value'], 'body', errors)) return { ok: false, value: null, errors };
  if (!Array.isArray(body.value) || body.value.length > 300) return { ok: false, value: null, errors: ['deposits: a list of up to 300'] };
  const ids = new Set();
  const out = body.value.map((d, n) => {
    const where = `deposits[${n + 1}]`;
    if (!keysOnly(d, ['id', 'bank', 'amount', 'currency', 'placedOn', 'maturity', 'confirmed', 'confirmedOn', 'note'], where, errors)) return null;
    const id = typeof d.id === 'string' && /^[A-Za-z0-9_-]{1,40}$/.test(d.id) ? d.id : (errors.push(`${where}.id: 1–40 letters, digits, - or _`), '');
    if (id && ids.has(id)) errors.push(`${where}.id: used twice`);
    ids.add(id);
    if (!CURRENCIES.includes(d.currency)) errors.push(`${where}.currency: one of ${CURRENCIES.join(', ')}`);
    if (typeof d.confirmed !== 'boolean') errors.push(`${where}.confirmed: true or false`);
    return {
      id,
      bank: textIn(d.bank, 80, `${where}.bank`, errors),
      amount: numIn(d.amount, 0, 1_000_000_000, `${where}.amount`, errors),
      currency: d.currency,
      placedOn: dateIn(d.placedOn, `${where}.placedOn`, errors),
      maturity: dateIn(d.maturity, `${where}.maturity`, errors, { optional: true }),
      confirmed: d.confirmed === true,
      confirmedOn: dateIn(d.confirmedOn, `${where}.confirmedOn`, errors, { optional: true }),
      note: textIn(d.note, 200, `${where}.note`, errors, { optional: true }),
    };
  });
  return errors.length ? { ok: false, value: null, errors } : { ok: true, value: out, errors: [] };
}

module.exports = { DEFAULT_SETTINGS, emptySettings, validateSettings, validateDeposits, CURRENCIES };
