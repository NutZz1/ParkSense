import { Router } from 'express';
import { getRow, scanPrefix, scanRange, revTs, getMetric } from '../hbaseRest.js';
const r = Router();
const num = (v) => (v === undefined || v === '' ? null : Number(v));

export function shape(row) {
  const c = row.cols;
  return {
    id: row.key, address: c['m:address'] || null, lat: num(c['m:lat']), lon: num(c['m:lon']),
    type: c['m:type'] || 'UNKNOWN', system: c['m:system'] || null, decks: num(c['m:decks']), gantry: num(c['m:gantry']),
    basement: c['m:basement'] || null, free_parking: c['m:free_parking'] || null, night_parking: c['m:night_parking'] || null,
    short_term: c['m:short_term'] || null,
    total: num(c['s:total']), available: num(c['s:available']), occupancy: num(c['s:occupancy']),
    update_ts: c['s:update_ts'] || null, snapshot_ts: c['s:snapshot_ts'] || null,
    p_full_30: num(c['p:p_full_30']), pred_full_30: c['p:pred_full_30'] === '1', made_at: c['p:made_at'] || null,
    target_at: c['p:target_at'] || null, model_ver: c['p:model_ver'] || null,
  };
}

let cache = { at: 0, rows: [] };
export async function allCarparks() {
  if (Date.now() - cache.at < 30000) return cache.rows;
  const rows = (await scanPrefix('carpark_live')).map(shape).filter((x) => x.total !== null);
  cache = { at: Date.now(), rows };
  return rows;
}

r.get('/carparks', async (_req, res, next) => {
  try { const rows = await allCarparks(); res.json({ count: rows.length, updated: new Date().toISOString(), carparks: rows }); } catch (e) { next(e); }
});

r.get('/carparks/:id', async (req, res, next) => {
  try {
    const row = await getRow('carpark_live', req.params.id);
    if (!row) return res.status(404).json({ error: 'unknown carpark' });
    res.json(shape(row));
  } catch (e) { next(e); }
});

// newest-first keys: start at <id>#rev(now) and stop at <id>#rev(now - hours)
r.get('/carparks/:id/history', async (req, res, next) => {
  try {
    const hours = Math.min(Number(req.query.hours || 24), 24 * 14);
    const now = Date.now();
    const rows = await scanRange('carpark_history', `${req.params.id}#${revTs(now)}`, `${req.params.id}#${revTs(now - hours * 3600000)}`, 3000);
    const LONG_MAX = 9223372036854775807n;
    const pts = rows.map((x) => ({
      ts: Number(LONG_MAX - BigInt(x.key.split('#')[1])), available: num(x.cols['s:available']),
      total: num(x.cols['s:total']), occupancy: num(x.cols['s:occupancy']),
    })).sort((a, b) => a.ts - b.ts);
    res.json({ id: req.params.id, hours, points: pts });
  } catch (e) { next(e); }
});

r.get('/carparks/:id/profile', async (req, res, next) => {
  try { const m = await getMetric(`profile:${req.params.id}`); res.json(m ? m.value : { weekday: {}, weekend: {} }); } catch (e) { next(e); }
});

const hav = (a, b, c, d) => {
  const R = 6371000, t = (x) => (x * Math.PI) / 180;
  const h = Math.sin(t(c - a) / 2) ** 2 + Math.cos(t(a)) * Math.cos(t(c)) * Math.sin(t(d - b) / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
};
// nearby carparks ranked by predicted space on arrival: not-predicted-full first, then lower P(full), then more free lots
r.get('/nearby', async (req, res, next) => {
  try {
    const lat = Number(req.query.lat), lon = Number(req.query.lon), radius = Number(req.query.radius || 800);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return res.status(400).json({ error: 'lat and lon required' });
    const rows = (await allCarparks()).filter((x) => x.lat !== null)
      .map((x) => ({ ...x, distance_m: Math.round(hav(lat, lon, x.lat, x.lon)) }))
      .filter((x) => x.distance_m <= radius)
      .sort((a, b) => (a.pred_full_30 - b.pred_full_30) || ((a.p_full_30 ?? 1) - (b.p_full_30 ?? 1)) || (b.available - a.available));
    res.json({ lat, lon, radius, count: rows.length, carparks: rows.slice(0, 30) });
  } catch (e) { next(e); }
});
export default r;
