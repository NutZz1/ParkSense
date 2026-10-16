import { Router, json } from 'express';
import { runAgent } from '../agent/index.js';
import { llmStatus } from '../agent/llm.js';
import { EXAMPLES } from '../agent/fallback.js';
const r = Router();
r.post('/chat', json({ limit: '32kb' }), async (req, res, next) => {
  try { res.json(await runAgent({ message: req.body?.message, history: req.body?.history, client: req.ip })); } catch (e) { next(e); }
});
r.get('/chat/status', (_req, res) => res.json({ llm: llmStatus(), examples: EXAMPLES }));
export default r;
