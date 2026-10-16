// Maps every real carpark to one UI template and builds its parking slots.
// Multi-storey / basement: 100 slots per level (levels = ceil(capacity / 100)); surface lot: one level;
// roadside: small carparks (< 60 lots) drawn as bays along a street. The number of red (occupied) and green (free)
// slots equals the live counts; WHICH slots are red is illustrative (the feed reports totals only) — occupied slots
// fill from the lowest level up.
export const SLOTS_PER_LEVEL = 100;
export const ROADSIDE_MAX = 60;

export function templateOf(cp) {
  const t = (cp.type || '').toUpperCase();
  if (t.includes('BASEMENT')) return 'basement';
  if (t.includes('MULTI-STOREY') || t === 'MECHANISED CAR PARK') return 'multistorey';
  if ((cp.total ?? 0) < ROADSIDE_MAX) return 'roadside';
  return 'surface';
}

export const TEMPLATE_NAMES = { multistorey: 'Multi-storey', basement: 'Basement', surface: 'Surface lot', roadside: 'Roadside' };

export function buildLevels(cp) {
  const total = Math.max(0, cp.total ?? 0);
  const available = Math.min(total, Math.max(0, cp.available ?? 0));
  let occupied = total - available;
  const tpl = templateOf(cp);
  const sizes = [];
  if (tpl === 'multistorey' || tpl === 'basement') {
    for (let left = total; left > 0; left -= SLOTS_PER_LEVEL) sizes.push(Math.min(SLOTS_PER_LEVEL, left));
  } else sizes.push(total);
  if (!sizes.length) sizes.push(0);
  return sizes.map((n, i) => {
    const occ = Math.min(occupied, n);
    occupied -= occ;
    const name = tpl === 'basement' ? `B${i + 1}` : tpl === 'multistorey' ? `L${i + 1}` : tpl === 'roadside' ? 'Street' : 'Ground';
    return { name, size: n, occupied: occ, free: n - occ, slots: Array.from({ length: n }, (_, k) => k < occ) };
  });
}
