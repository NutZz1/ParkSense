import React, { useEffect, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts';
import { api, pct } from '../api.js';
import MetricTable from '../components/MetricTable.jsx';

export default function Insights({ onOpen }) {
  const [d, setD] = useState(null);
  const [err, setErr] = useState(null);
  useEffect(() => { api('/insights').then(setD).catch((e) => setErr(e.message)); }, []);
  if (err) return <p className="error">{err}</p>;
  if (!d) return <p>Loading insights…</p>;
  const q1 = d.analytics.q1?.rows || [];
  const hours = Array.from({ length: 24 }, (_, h) => ({
    hour: h,
    weekday: q1.find((r) => r.hour === h && r.day_type === 'weekday')?.mean_occupancy,
    weekend: q1.find((r) => r.hour === h && r.day_type === 'weekend')?.mean_occupancy,
  }));
  return (
    <div>
      <div className="panel">
        <h3>Citywide occupancy by hour (Spark SQL Q1)</h3>
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={hours}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="hour" /><YAxis tickFormatter={(v) => pct(v)} />
            <Tooltip formatter={(v) => pct(v, 1)} /><Legend />
            <Line isAnimationActive={false} dataKey="weekday" stroke="#1f6fb2" dot={false} /><Line isAnimationActive={false} dataKey="weekend" stroke="#e8a33d" dot={false} /></LineChart>
        </ResponsiveContainer>
      </div>
      <div className="two">
        <div className="panel"><h3>Most often nearly full (Top 20)</h3>
          <table className="tbl"><thead><tr><th>#</th><th>carpark</th><th>address</th><th>nearly full</th></tr></thead>
            <tbody>{d.top20.map((r) => (
              <tr key={r.carpark_no} className="click" onClick={() => onOpen(r.carpark_no)}>
                <td>{r.rank}</td><td>{r.carpark_no}</td><td>{r.address}</td><td>{pct(r.full_rate, 1)}</td></tr>))}</tbody></table>
        </div>
        <div>
          <div className="panel"><h3>By carpark type and system (Q3)</h3><MetricTable rows={d.analytics.q3?.rows} /></div>
          <div className="panel"><h3>Free / night parking policy, evenings (Q4)</h3><MetricTable rows={d.analytics.q4?.rows} /></div>
        </div>
      </div>
      <div className="panel"><h3>Largest evening vs morning swing (Q7)</h3><MetricTable rows={d.analytics.q7?.rows?.slice(0, 12)} /></div>
    </div>
  );
}
