import React from 'react';
const fmt = (v) => (typeof v === 'number' ? (Number.isInteger(v) ? v.toLocaleString() : v.toFixed(4)) : v ?? '–');
export default function MetricTable({ rows, cols }) {
  if (!rows?.length) return <p className="muted">No data yet.</p>;
  const keys = cols || Object.keys(rows[0]);
  return (
    <table className="tbl"><thead><tr>{keys.map((k) => <th key={k}>{k.replace(/_/g, ' ')}</th>)}</tr></thead>
      <tbody>{rows.map((r, i) => <tr key={i}>{keys.map((k) => <td key={k}>{fmt(r[k])}</td>)}</tr>)}</tbody></table>
  );
}
