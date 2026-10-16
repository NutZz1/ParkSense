import React, { useEffect, useState } from 'react';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts';
import { api, pct } from '../api.js';
import MetricTable from '../components/MetricTable.jsx';

// Charts for the queries whose result reads better as a picture; every query also shows its table.
function QueryChart({ q }) {
  const rows = q.rows || [];
  if (q.id === 'Q1') {
    const data = Array.from({ length: 24 }, (_, h) => ({
      hour: h,
      weekday: rows.find((r) => r.hour === h && r.day_type === 'weekday')?.mean_occupancy,
      weekend: rows.find((r) => r.hour === h && r.day_type === 'weekend')?.mean_occupancy,
    }));
    return (
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={data}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="hour" /><YAxis tickFormatter={(v) => pct(v)} />
          <Tooltip formatter={(v) => pct(v, 1)} /><Legend />
          <Line isAnimationActive={false} dataKey="weekday" stroke="#1f6fb2" dot={false} />
          <Line isAnimationActive={false} dataKey="weekend" stroke="#e8a33d" dot={false} /></LineChart>
      </ResponsiveContainer>
    );
  }
  if (q.id === 'Q6') {
    return (
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={rows}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="dt" fontSize={10} /><YAxis tickFormatter={(v) => pct(v)} />
          <Tooltip formatter={(v) => pct(v, 2)} /><Legend />
          <Line isAnimationActive={false} dataKey="nearly_full_rate" name="nearly-full rate" stroke="#d64545" dot={false} />
          <Line isAnimationActive={false} dataKey="mean_occupancy" name="mean occupancy" stroke="#1f6fb2" dot={false} /></LineChart>
      </ResponsiveContainer>
    );
  }
  if (q.id === 'Q8') {
    return (
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={rows}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="dt" fontSize={10} /><YAxis /><Tooltip /><Legend />
          <Bar isAnimationActive={false} dataKey="raw_lines" name="raw lines" fill="#9bbbd8" />
          <Bar isAnimationActive={false} dataKey="clean_car_rows" name="clean car-lot rows" fill="#1f6fb2" /></BarChart>
      </ResponsiveContainer>
    );
  }
  return null;
}

function SqlCard({ q }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="panel analysis">
      <div className="analysis-head">
        <span className="qid">{q.id}</span>
        <h3>{q.question}</h3>
        <span className="muted">{q.row_count} rows · {q.seconds} s</span>
      </div>
      <button className="sql-toggle" onClick={() => setOpen(!open)}>{open ? 'Hide SQL' : 'Show SQL'}</button>
      {open && <pre className="sql">{q.sql}</pre>}
      <QueryChart q={q} />
      <div className="result"><MetricTable rows={q.id === 'Q1' || q.id === 'Q6' || q.id === 'Q8' ? q.rows.slice(0, 10) : q.rows} /></div>
    </div>
  );
}

function RddCard({ a }) {
  return (
    <div className="panel analysis">
      <div className="analysis-head"><span className="qid rdd">RDD</span><h3>{a.question}</h3>
        <span className="muted">{a.row_count} rows · {a.source}</span></div>
      <div className="ops">{a.rdd_ops}</div>
      <div className="result"><MetricTable rows={a.rows?.slice(0, 15)} /></div>
    </div>
  );
}

export default function Analytics() {
  const [d, setD] = useState(null);
  const [err, setErr] = useState(null);
  const [view, setView] = useState('sql');
  useEffect(() => { api('/analytics').then(setD).catch((e) => setErr(e.message)); }, []);
  if (err) return <p className="error">{err}</p>;
  if (!d) return <p>Loading analytics…</p>;
  return (
    <div>
      <p className="muted">
        Every analysis run on the 30-day dataset (7.85M clean records). Spark SQL queries run in PySpark over temp views on
        the Parquet written by the Scala pipeline; the RDD analytics run in Scala with key-value transformations.
      </p>
      <div className="toolbar">
        <button className={view === 'sql' ? 'on' : ''} onClick={() => setView('sql')}>Spark SQL queries ({d.sql.length})</button>
        <button className={view === 'rdd' ? 'on' : ''} onClick={() => setView('rdd')}>Scala RDD analytics ({d.rdd.length})</button>
      </div>
      {view === 'sql' && d.sql.map((q) => <SqlCard key={q.id} q={q} />)}
      {view === 'rdd' && d.rdd.map((a) => <RddCard key={a.name} a={a} />)}
    </div>
  );
}
