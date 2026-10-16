// ParkSense API: Express on :5000, every read goes through the HBase REST gateway (hbaseRest.js).
// Also serves the built React client (../client/dist).
import express from 'express';
import cors from 'cors';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import carparks from './routes/carparks.js';
import insights from './routes/insights.js';
import metrics from './routes/metrics.js';
import health from './routes/health.js';
import chat from './routes/chat.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const app = express();
app.use(cors());
app.use('/api', health);
app.use('/api', carparks);
app.use('/api', insights);
app.use('/api', metrics);
app.use('/api', chat);
app.use('/api', (_req, res) => res.status(404).json({ error: 'not found' }));
app.use((err, _req, res, _next) => { console.error(err.message); res.status(503).json({ error: err.message }); });
const dist = path.resolve(here, '../client/dist');
if (fs.existsSync(dist)) {
  app.use(express.static(dist));
  app.get('*', (_req, res) => res.sendFile(path.join(dist, 'index.html')));
}
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`ParkSense API on http://localhost:${PORT} (HBase REST ${process.env.HBASE_REST || 'http://localhost:8085'})`));
