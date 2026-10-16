import React, { useEffect, useMemo, useState } from 'react';
import { MapContainer, TileLayer, CircleMarker, Tooltip } from 'react-leaflet';
import { api, sgt, stateOf, pct } from '../api.js';
import { templateOf, TEMPLATE_NAMES } from '../layout.js';

export default function LiveMap({ onOpen }) {
  const [data, setData] = useState(null);
  const [err, setErr] = useState(null);
  const [tpl, setTpl] = useState('all');
  useEffect(() => {
    const load = () => api('/carparks').then((d) => { setData(d); setErr(null); }).catch((e) => setErr(e.message));
    load();
    const t = setInterval(load, 60000);
    return () => clearInterval(t);
  }, []);
  const cps = useMemo(() => (data?.carparks || []).filter((c) => c.lat && (tpl === 'all' || templateOf(c) === tpl)), [data, tpl]);
  if (err) return <p className="error">API error: {err}</p>;
  if (!data) return <p>Loading carparks…</p>;
  const all = data.carparks;
  const latest = all.map((c) => c.snapshot_ts).filter(Boolean).sort().pop();
  return (
    <div>
      <div className="kpis">
        <div><b>{all.length.toLocaleString()}</b><span>carparks live</span></div>
        <div><b>{all.filter((c) => stateOf(c).label === 'nearly full').length}</b><span>nearly full now</span></div>
        <div><b>{all.filter((c) => c.pred_full_30).length}</b><span>predicted full in 30 min</span></div>
        <div><b>{all.reduce((s, c) => s + (c.available || 0), 0).toLocaleString()}</b><span>free car lots</span></div>
        <div><b>{sgt(latest)}</b><span>last snapshot (SGT)</span></div>
      </div>
      <div className="toolbar">
        Show:
        {['all', 'multistorey', 'basement', 'surface', 'roadside'].map((k) => (
          <button key={k} className={tpl === k ? 'on' : ''} onClick={() => setTpl(k)}>{k === 'all' ? 'All' : TEMPLATE_NAMES[k]}</button>
        ))}
        <span className="legend"><i style={{ background: '#2e9e4f' }} /> space <i style={{ background: '#e8a33d' }} /> filling
          <i style={{ background: '#d64545' }} /> nearly full <i className="ring" /> predicted full in 30 min</span>
      </div>
      <MapContainer center={[1.355, 103.82]} zoom={12} className="map" preferCanvas>
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" attribution="© OpenStreetMap" />
        {cps.map((c) => (
          <CircleMarker key={c.id} center={[c.lat, c.lon]} radius={5}
            pathOptions={{ color: c.pred_full_30 ? '#111' : '#fff', weight: c.pred_full_30 ? 2.5 : 0.8, fillColor: stateOf(c).color, fillOpacity: 0.95 }}
            eventHandlers={{ click: () => onOpen(c.id) }}>
            <Tooltip>{c.id} · {c.address}<br />{c.available}/{c.total} free · in 30 min: {pct(c.p_full_30)} full risk</Tooltip>
          </CircleMarker>
        ))}
      </MapContainer>
      <p className="muted">Click a carpark to open its zoomed-in view. Updates every 60 s.</p>
    </div>
  );
}
