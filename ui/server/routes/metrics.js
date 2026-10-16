import { Router } from 'express';
import { getMetric, scanPrefix } from '../hbaseRest.js';
const r = Router();
const pick = async (names) => Object.fromEntries(await Promise.all(names.map(async (n) => [n.split(':')[1], (await getMetric(n).catch(() => null))?.value ?? null])));
r.get('/metrics/model', async (_req, res, next) => {
  try { res.json(await pick(['model:best', 'model:comparison', 'model:confusion', 'model:feature_importance', 'model:transitions', 'model:horizons', 'model:slice_type', 'model:slice_hour'])); } catch (e) { next(e); }
});
r.get('/metrics/live', async (_req, res, next) => {
  try { const m = await getMetric('live:eval'); res.json(m ? { ...m.value, metric_updated_at: m.updated_at } : null); } catch (e) { next(e); }
});
r.get('/metrics/spark', async (_req, res, next) => {
  try { res.json(await pick(['spark:rdd_summary', 'spark:cache', 'spark:partitions', 'spark:shuffle', 'spark:perf'])); } catch (e) { next(e); }
});
r.get('/metrics/stream', async (_req, res, next) => {
  try {
    const last = await getMetric('stream:last_batch');
    const types = (await scanPrefix('metrics', 'stream:type:')).map((x) => JSON.parse(x.cols['v:json']));
    res.json({ last_batch: last?.value || null, by_type: types });
  } catch (e) { next(e); }
});
export default r;
