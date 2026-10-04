// ─────────────────────────────────────────────────────────────────────────────
// What the Metrics page saves for everyone, and what may be saved (server/json-store.cjs stores it):
//   settings  the USD/EUR planning rate and the cloud cap (% of projected revenue, which category is cloud)
//   deposits  the deposit tracker: each deposit placed and whether the bank's confirmation came back
// ─────────────────────────────────────────────────────────────────────────────

const DEFAULT_SETTINGS = Object.freeze({
  usdEurPlanningRate: null,
  cloudCapPct: 8,
  cloudCategory: '',
});
// The innovation envelope was removed from the page; settings saved with it still validate, without it.
const LEGACY_KEYS = ['innovation'];
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

/** PUT body { value: settings } → { ok, value, errors }. */
function validateSettings(body) {
  const errors = [];
  const out = emptySettings();
  if (!keysOnly(body, ['value'], 'body', errors) || !keysOnly(body.value, ['usdEurPlanningRate', 'cloudCapPct', 'cloudCategory', ...LEGACY_KEYS], 'settings', errors)) {
    return { ok: false, value: null, errors };
  }
  const v = body.value;
  if (v.usdEurPlanningRate !== undefined && v.usdEurPlanningRate !== null) out.usdEurPlanningRate = numIn(v.usdEurPlanningRate, 0.5, 2, 'usdEurPlanningRate (USD per €)', errors);
  if (v.cloudCapPct !== undefined) out.cloudCapPct = numIn(v.cloudCapPct, 0, 100, 'cloudCapPct', errors);
  if (v.cloudCategory !== undefined) out.cloudCategory = textIn(v.cloudCategory, 120, 'cloudCategory', errors, { optional: true });
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
