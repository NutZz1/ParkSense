import React, { useEffect, useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts';
import { api, num, pct, sgt } from '../api.js';
import MetricTable from '../components/MetricTable.jsx';
import ConfusionMatrix from '../components/ConfusionMatrix.jsx';

export default function Model() {
  const [m, setM] = useState(null);
  const [live, setLive] = useState(null);
  const [err, setErr] = useState(null);
  useEffect(() => {
    api('/metrics/model').then(setM).catch((e) => setErr(e.message));
    const load = () => api('/metrics/live').then(setLive).catch(() => {});
    load(); const t = setInterval(load, 60000); return () => clearInterval(t);
  }, []);
  if (err) return <p className="error">{err}</p>;
  if (!m) return <p>Loading model metrics…</p>;
  const best = m.best;
  const conf = m.confusion?.[(best?.algorithm || '').toLowerCase().replace(/ /g, '_').replace(/-/g, '_')];
  const imp = (m.feature_importance || []).filter((r) => r.model === best?.algorithm).slice(0, 12);
  return (
    <div>
      {best && <p>Deployed: <b>{best.algorithm}</b> {JSON.stringify(best.params)} · threshold {best.threshold} · {best.label} · version {best.model_version}</p>}
      <div className="panel"><h3>Test period — baselines vs models</h3>
        <MetricTable rows={m.comparison} cols={['model', 'threshold', 'accuracy', 'precision', 'recall', 'f1', 'pr_auc']} />
        <p className="muted">Persistence = "full in 30 min if full now". Only ~1.4% of rows change state within 30 minutes, so it is a strong baseline.</p>
      </div>
      <div className="two">
        <div className="panel"><ConfusionMatrix m={conf} title={`Confusion matrix — ${best?.algorithm}`} /></div>
        <div className="panel"><h3>Live evaluation (unseen data)</h3>
          {live ? (<table className="tbl"><tbody>
            <tr><th>verified predictions</th><td>{live.verified?.toLocaleString()}</td></tr>
            <tr><th>accuracy</th><td>{num(live.model?.accuracy)}</td></tr>
            <tr><th>precision / recall</th><td>{num(live.model?.precision)} / {num(live.model?.recall)}</td></tr>
            <tr><th>F1 (persistence F1)</th><td>{num(live.model?.f1)} ({num(live.persistence?.f1)})</td></tr>
            <tr><th>state changes seen</th><td>{live.transitions?.n} · model right on {pct(live.transitions?.model_correct_share)}</td></tr>
            <tr><th>last target</th><td>{sgt(live.last_target)} SGT</td></tr></tbody></table>) : <p className="muted">No live evaluation yet.</p>}
        </div>
      </div>
      <div className="two">
        <div className="panel"><h3>Where prediction adds value — state changes within 30 min</h3><MetricTable rows={m.transitions} /></div>
        <div className="panel"><h3>Horizon sensitivity</h3><MetricTable rows={m.horizons} cols={['horizon_min', 'state_change_rate', 'f1', 'persistence_f1']} /></div>
      </div>
      <div className="panel"><h3>Top features — {best?.algorithm}</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={imp} layout="vertical" margin={{ left: 90 }}><CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" /><YAxis type="category" dataKey="feature" width={110} /><Tooltip /><Legend />
            <Bar dataKey="importance" fill="#1f6fb2" /></BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
