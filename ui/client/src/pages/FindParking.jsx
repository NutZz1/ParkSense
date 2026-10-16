import React, { useState } from 'react';
import { MapContainer, TileLayer, CircleMarker, Circle, useMapEvents } from 'react-leaflet';
import { api, stateOf } from '../api.js';
import OccupancyBadge, { PredBadge } from '../components/OccupancyBadge.jsx';

function ClickToPick({ onPick }) {
  useMapEvents({ click: (e) => onPick([e.latlng.lat, e.latlng.lng]) });
  return null;
}

export default function FindParking({ onOpen }) {
  const [pos, setPos] = useState(null);
  const [radius, setRadius] = useState(800);
  const [res, setRes] = useState(null);
  const [err, setErr] = useState(null);
  const pick = (p, r = radius) => {
    setPos(p);
    api(`/nearby?lat=${p[0]}&lon=${p[1]}&radius=${r}`).then((d) => { setRes(d); setErr(null); }).catch((e) => setErr(e.message));
  };
  return (
    <div className="two">
      <div>
        <div className="toolbar">Click your destination on the map · radius
          <select value={radius} onChange={(e) => { const r = Number(e.target.value); setRadius(r); if (pos) pick(pos, r); }}>
            {[400, 800, 1200, 2000].map((r) => <option key={r} value={r}>{r} m</option>)}
          </select>
        </div>
        <MapContainer center={[1.355, 103.82]} zoom={12} className="map small">
          <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" attribution="© OpenStreetMap" />
          <ClickToPick onPick={pick} />
          {pos && <Circle center={pos} radius={radius} pathOptions={{ color: '#1f6fb2', fillOpacity: 0.05 }} />}
          {res?.carparks.map((c) => <CircleMarker key={c.id} center={[c.lat, c.lon]} radius={6}
            pathOptions={{ color: c.pred_full_30 ? '#111' : '#fff', weight: 2, fillColor: stateOf(c).color, fillOpacity: 0.95 }}
            eventHandlers={{ click: () => onOpen(c.id) }} />)}
        </MapContainer>
      </div>
      <div>
        <h3>Nearby carparks, ranked by predicted space on arrival</h3>
        {err && <p className="error">{err}</p>}
        {!res && <p className="muted">Pick a point on the map.</p>}
        {res && !res.count && <p className="muted">No carparks within {radius} m.</p>}
        {res?.carparks.map((c, i) => (
          <div key={c.id} className="card" onClick={() => onOpen(c.id)}>
            <div className="card-top"><b>{i + 1}. {c.id}</b><span className="muted">{c.distance_m} m</span></div>
            <div className="muted">{c.address}</div>
            <div className="badges"><span>now</span> <OccupancyBadge carpark={c} /> <PredBadge carpark={c} /></div>
          </div>
        ))}
      </div>
    </div>
  );
}
