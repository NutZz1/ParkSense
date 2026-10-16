import React from 'react';
import { stateOf, pct } from '../api.js';
export default function OccupancyBadge({ carpark, small }) {
  const s = stateOf(carpark);
  return (
    <span className={`badge ${small ? 'small' : ''}`} style={{ background: s.color }}>
      {carpark.available ?? '–'}/{carpark.total ?? '–'} {small ? '' : `free · ${s.label}`}
    </span>
  );
}
export function PredBadge({ carpark }) {
  if (carpark.p_full_30 == null) {
    const why = (carpark.total ?? 0) < 10 ? 'fewer than 10 lots' : 'sensor excluded (always-zero / frozen) or no recent reading';
    return <span className="pred none" title={why}>not predicted · {why}</span>;
  }
  return (
    <span className={`pred ${carpark.pred_full_30 ? 'full' : 'ok'}`} title={`P(nearly full in 30 min) = ${pct(carpark.p_full_30, 1)}`}>
      in 30 min: {carpark.pred_full_30 ? 'likely FULL' : 'space likely'} · {pct(carpark.p_full_30)}
    </span>
  );
}
