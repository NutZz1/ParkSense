import { Router } from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getMetric } from '../hbaseRest.js';

const STREAM_CSV = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../outputs/metrics/stream_batches.csv');
const r = Router();
r.get('/insights', async (_req, res, next) => {
  try {
    const names = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8'];
    const q = await Promise.all(names.map((n) => getMetric(`analytics:${n}`).catch(() => null)));
    const [top20, typeHour, daily] = await Promise.all(['insights:top20', 'insights:type_hour', 'insights:daily'].map((n) => getMetric(n).catch(() => null)));
    res.json({ analytics: Object.fromEntries(names.map((n, i) => [n, q[i]?.value || null])), top20: top20?.value || [],
      type_hour: typeHour?.value || [], daily: daily?.value || [] });
  } catch (e) { next(e); }
});
// catalogue of every analysis that was run: Spark SQL Q1-Q8 (question, SQL, runtime, result) + Scala RDD analytics
r.get('/analytics', async (_req, res, next) => {
  try {
    const names = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8'];
    const sql = (await Promise.all(names.map((n) => getMetric(`analytics:${n}`).catch(() => null))))
      .map((m, i) => (m ? { id: names[i].toUpperCase(), ...m.value, updated_at: m.updated_at } : null)).filter(Boolean);
    const rdd = (await getMetric('analytics:rdd').catch(() => null))?.value || [];
    res.json({ sql, rdd });
  } catch (e) { next(e); }
});
// citywide occupancy over the last 24 h, from the StreamingJob micro-batch log (one row per processed poll file batch)
r.get('/analytics/live-series', (_req, res, next) => {
  try {
    if (!fs.existsSync(STREAM_CSV)) return res.json({ points: [] });
    const [head, ...lines] = fs.readFileSync(STREAM_CSV, 'utf8').trim().split('\n');
    const cols = head.split(',');
    const idx = (c) => cols.indexOf(c);
    const since = Date.now() - 24 * 3600000;
    const seen = new Map();
    for (const ln of lines) {
      const f = ln.split(',');
      const t = Date.parse(`${f[idx('snapshot_sgt')]}+08:00`);
      if (!Number.isFinite(t) || t < since) continue;
      const slot = Math.floor(t / 300000) * 300000; // 5-minute buckets
      seen.set(slot, { ts: slot, occupancy: Number(f[idx('citywide_occupancy')]), nearly_full: Number(f[idx('nearly_full_now')]),
        carparks: Number(f[idx('carparks')]) });
    }
    res.json({ points: [...seen.values()].sort((a, b) => a.ts - b.ts) });
  } catch (e) { next(e); }
});
export default r;
