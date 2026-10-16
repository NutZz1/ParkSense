import { Router } from 'express';
import { getMetric } from '../hbaseRest.js';
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
export default r;
