import React from 'react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, ReferenceLine, CartesianGrid } from 'recharts';
import { FULL_RATIO } from '../api.js';
const hm = (t) => new Date(t).toLocaleTimeString('en-SG', { timeZone: 'Asia/Singapore', hour: '2-digit', minute: '2-digit', hour12: false });
// insert a null point wherever readings are > 10 min apart, so the line breaks at data gaps instead of bridging them
function withGaps(points) {
  const out = [];
  points.forEach((p, i) => {
    if (i && p.ts - points[i - 1].ts > 10 * 60000) out.push({ ts: points[i - 1].ts + 60000, available: null });
    out.push(p);
  });
  return out;
}

export default function HistoryChart({ points, total }) {
  if (!points?.length) return <p className="muted">No history in HBase yet for this carpark.</p>;
  points = withGaps(points);
  return (
    <ResponsiveContainer width="100%" height={240}>
      <LineChart data={points}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="ts" type="number" domain={['dataMin', 'dataMax']} scale="time" tickFormatter={hm} />
        <YAxis domain={[0, total || 'auto']} />
        <Tooltip labelFormatter={(t) => new Date(t).toLocaleString('en-SG', { timeZone: 'Asia/Singapore', hour12: false })} />
        <ReferenceLine y={Math.round((total || 0) * FULL_RATIO)} stroke="#d64545" strokeDasharray="5 5" label="10% free" />
        <Line dataKey="available" name="available lots" stroke="#1f6fb2" strokeWidth={2} dot={false} isAnimationActive={false} connectNulls={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}
