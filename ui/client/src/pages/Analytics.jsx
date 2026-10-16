import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { SqlCharts, RddCharts } from '../components/AnalysisCharts.jsx';
import MetricTable from '../components/MetricTable.jsx';

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
      <SqlCharts q={q} />
      <details className="result-details"><summary>Result table ({q.row_count} rows)</summary>
        <div className="result"><MetricTable rows={q.rows.map((r) => (typeof r.dt === 'number' ? { ...r, dt: new Date(r.dt).toISOString().slice(0, 10) } : r))} /></div></details>
    </div>
  );
}

function RddCard({ a }) {
  return (
    <div className="panel analysis">
      <div className="analysis-head"><span className="qid rdd">RDD</span><h3>{a.question}</h3>
        <span className="muted">{a.row_count} rows · {a.source}</span></div>
      <div className="ops">{a.rdd_ops}</div>
      <RddCharts a={a} />
      <details className="result-details"><summary>Result table ({a.row_count} rows{a.row_count > (a.rows?.length || 0) ? `, top ${a.rows?.length} shown` : ''})</summary>
        <div className="result"><MetricTable rows={a.rows} /></div></details>
    </div>
  );
}

export default function Analytics() {
  const [d, setD] = useState(null);
  const [err, setErr] = useState(null);
  const [view, setView] = useState(window.location.hash.includes('rdd') ? 'rdd' : 'sql');
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
