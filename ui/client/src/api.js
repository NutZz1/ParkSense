export async function api(path) {
  const r = await fetch(`/api${path}`);
  if (!r.ok) throw new Error((await r.json().catch(() => ({}))).error || `${path}: HTTP ${r.status}`);
  return r.json();
}
export const FULL_RATIO = 0.1; // nearly full = <= 10% of car lots free
export const sgt = (t) => (t ? new Date(t.endsWith?.('Z') || /[+-]\d\d:\d\d$/.test(t) ? t : `${t}+08:00`)
  .toLocaleString('en-SG', { timeZone: 'Asia/Singapore', hour12: false }) : '–');
export const pct = (v, d = 0) => (v == null ? '–' : `${(100 * v).toFixed(d)}%`);
export const num = (v, d = 3) => (v == null ? '–' : Number(v).toFixed(d));
// colour by share of free lots: green, amber, red (nearly full)
export function stateOf(cp) {
  if (cp.total == null || cp.available == null) return { label: 'no data', color: '#9aa4ae' };
  const free = cp.available / cp.total;
  if (free <= FULL_RATIO) return { label: 'nearly full', color: '#d64545' };
  if (free <= 0.3) return { label: 'filling', color: '#e8a33d' };
  return { label: 'space', color: '#2e9e4f' };
}
