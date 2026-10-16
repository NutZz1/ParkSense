import React, { useEffect, useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts';
import { api, sgt, pct } from '../api.js';
import ParkingLayout from '../components/ParkingLayout.jsx';
import OccupancyBadge, { PredBadge } from '../components/OccupancyBadge.jsx';
import HistoryChart from '../components/HistoryChart.jsx';

export default function CarparkDetail({ id, setId }) {
  const [list, setList] = useState([]);
  const [cp, setCp] = useState(null);
  const [hist, setHist] = useState(null);
  const [prof, setProf] = useState(null);
  const [q, setQ] = useState('');
  const [err, setErr] = useState(null);
  useEffect(() => { api('/carparks').then((d) => setList(d.carparks)).catch(() => {}); }, []);
  useEffect(() => {
    setCp(null); setHist(null); setProf(null);
    const load = () => api(`/carparks/${id}`).then((d) => { setCp(d); setErr(null); }).catch((e) => setErr(e.message));
    load();
    api(`/carparks/${id}/history?hours=24`).then(setHist).catch(() => setHist({ points: [] }));
    api(`/carparks/${id}/profile`).then(setProf).catch(() => {});
    const t = setInterval(load, 60000);
    return () => clearInterval(t);
  }, [id]);
  const matches = q.length >= 2 ? list.filter((c) => `${c.id} ${c.address}`.toLowerCase().includes(q.toLowerCase())).slice(0, 8) : [];
  const profile = prof ? Array.from({ length: 24 }, (_, h) => ({ hour: h, weekday: prof.weekday?.[h], weekend: prof.weekend?.[h] })) : [];
  const best = profile.filter((p) => p.weekday != null).sort((a, b) => a.weekday - b.weekday).slice(0, 3).map((p) => `${String(p.hour).padStart(2, '0')}:00`);
  return (
    <div>
      <div className="toolbar search">
        <input placeholder="Search carpark number or address (e.g. HLM, Tampines)" value={q} onChange={(e) => setQ(e.target.value)} />
        {matches.length > 0 && <div className="dropdown">{matches.map((c) => (
          <div key={c.id} onClick={() => { setId(c.id); setQ(''); }}><b>{c.id}</b> {c.address}</div>))}</div>}
      </div>
      {err && <p className="error">{err}</p>}
      {!cp && !err && <p>Loading {id}…</p>}
      {cp && (
        <>
          <div className="detail-head">
            <div>
              <h2>{cp.id} <small>{cp.address}</small></h2>
              <div className="badges"><OccupancyBadge carpark={cp} /> <PredBadge carpark={cp} /></div>
              <div className="muted">Updated {sgt(cp.update_ts)} SGT · prediction for {sgt(cp.target_at)} SGT</div>
            </div>
            <table className="facts"><tbody>
              <tr><th>Type</th><td>{cp.type}</td></tr>
              <tr><th>Real decks</th><td>{cp.decks ?? '–'}</td></tr>
              <tr><th>Gantry height</th><td>{cp.gantry ? `${cp.gantry} m` : '–'}</td></tr>
              <tr><th>Parking system</th><td>{cp.system}</td></tr>
              <tr><th>Free parking</th><td>{cp.free_parking}</td></tr>
              <tr><th>Night parking</th><td>{cp.night_parking}</td></tr>
              <tr><th>Short-term</th><td>{cp.short_term}</td></tr>
            </tbody></table>
          </div>
          <ParkingLayout carpark={cp} />
          <div className="two">
            <div className="panel"><h3>Last 24 h — available car lots</h3><HistoryChart points={hist?.points} total={cp.total} /></div>
            <div className="panel">
              <h3>Best time to arrive {best.length ? `(weekday: ${best.join(', ')})` : ''}</h3>
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={profile}>
                  <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="hour" /><YAxis tickFormatter={(v) => pct(v)} domain={[0, 1]} />
                  <Tooltip formatter={(v) => pct(v, 1)} /><Legend />
                  <Bar dataKey="weekday" name="weekday occupancy" fill="#1f6fb2" /><Bar dataKey="weekend" name="weekend occupancy" fill="#9bbbd8" />
                </BarChart>
              </ResponsiveContainer>
              <p className="muted">Mean occupancy by hour over 30 days (Scala RDD aggregate). Lower = easier to park.</p>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
