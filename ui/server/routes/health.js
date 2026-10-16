import { Router } from 'express';
import { ping, getMetric } from '../hbaseRest.js';
const r = Router();
r.get('/health', async (_req, res) => {
  try {
    const version = await ping();
    const batch = await getMetric('stream:last_batch').catch(() => null);
    res.json({ ok: true, hbase: version, last_stream_batch: batch?.value?.processed_at || null, last_snapshot_sgt: batch?.value?.snapshot_sgt || null });
  } catch (e) { res.status(503).json({ ok: false, error: e.message }); }
});
export default r;
