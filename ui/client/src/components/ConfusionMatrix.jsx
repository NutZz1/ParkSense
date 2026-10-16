import React from 'react';
export default function ConfusionMatrix({ m, title }) {
  if (!m) return null;
  const c = (a, p) => m[p]?.[a] ?? '–';
  return (
    <div><h4>{title}</h4>
      <table className="cm"><thead><tr><th /><th>pred not full</th><th>pred full</th></tr></thead>
        <tbody>
          <tr><th>actual not full</th><td>{c('actual_not_full', 'pred_not_full').toLocaleString?.()}</td><td className="bad">{c('actual_not_full', 'pred_full').toLocaleString?.()}</td></tr>
          <tr><th>actual full</th><td className="bad">{c('actual_full', 'pred_not_full').toLocaleString?.()}</td><td>{c('actual_full', 'pred_full').toLocaleString?.()}</td></tr>
        </tbody></table></div>
  );
}
