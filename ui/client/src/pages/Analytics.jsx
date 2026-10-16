import React, { useEffect, useMemo, useState } from 'react';
import { ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, Tooltip, Legend, CartesianGrid } from 'recharts';
import { api, pct, stateOf } from '../api.js';
import { SqlCharts, RddCharts } from '../components/AnalysisCharts.jsx';
import MetricTable from '../components/MetricTable.jsx';
import { META } from '../analyticsMeta.js';

const NO = { isAnimationActive: false };
const hm = (t) => new Date(t).toLocaleTimeString('en-SG', { timeZone: 'Asia/Singapore', hour: '2-digit', minute: '2-digit', hour12: false });
const sgHour = (t) => Number(new Date(t).toLocaleString('en-SG', { timeZone: 'Asia/Singapore', hour: '2-digit', hour12: false })) % 24;
const sgWeekend = (t) => ['Sat', 'Sun'].includes(new Date(t).toLocaleString('en-SG', { timeZone: 'Asia/Singapore', weekday: 'short' }));
const fixDt = (rows) => rows.map((r) => (typeof r.dt === 'number' ? { ...r, dt: new Date(r.dt).toISOString().slice(0, 10) } : r));

function Kpis({ items }) {
  return <div className="kpis">{items.filter((k) => k.value != null && k.value !== false).map((k) => <div key={k.label}><b>{k.value}</b><span>{k.label}</span></div>)}</div>;
}

// ------------------------------------------------------------------------------- Overview (home)
function Overview({ sql, go }) {
  const [cps, setCps] = useState(null);
  const [live, setLive] = useState(null);
  const [series, setSeries] = useState([]);
  useEffect(() => {
    const load = () => {
      api('/carparks').then((d) => setCps(d.carparks)).catch(() => {});
      api('/metrics/live').then(setLive).catch(() => {});
      api('/analytics/live-series').then((d) => setSeries(d.points)).catch(() => {});
    };
    load(); const t = setInterval(load, 60000); return () => clearInterval(t);
  }, []);
  const q1 = sql.find((q) => q.id === 'Q1')?.rows || [];
  const typical = (t) => q1.find((r) => r.hour === sgHour(t) && r.day_type === (sgWeekend(t) ? 'weekend' : 'weekday'))?.mean_occupancy;
  const line = series.map((p) => ({ ...p, typical: typical(p.ts) }));
  const pie = useMemo(() => {
    if (!cps) return [];
    const c = { space: 0, filling: 0, 'nearly full': 0 };
    cps.forEach((x) => { const s = stateOf(x).label; if (s in c) c[s] += 1; });
    return [{ name: 'space (> 30% free)', value: c.space, color: '#2e9e4f' }, { name: 'filling (10–30% free)', value: c.filling, color: '#e8a33d' },
      { name: 'nearly full (≤ 10% free)', value: c['nearly full'], color: '#d64545' }];
  }, [cps]);
  const total = cps?.reduce((s, c) => s + (c.total || 0), 0) || 0;
  const free = cps?.reduce((s, c) => s + (c.available || 0), 0) || 0;
  const lastLive = line[line.length - 1];
  const lastTyp = lastLive?.typical;
  const now = cps && total ? 1 - free / total : null;
  return (
    <div>
      <h2 className="section-title">Analytics overview — Singapore HDB parking right now</h2>
      <Kpis items={[
        { label: 'carparks reporting live', value: cps?.length.toLocaleString() },
        { label: 'free car lots now', value: cps && free.toLocaleString() },
        { label: 'citywide occupancy now', value: now != null && pct(now, 1) },
        { label: 'nearly full now (≤ 10% free)', value: cps?.filter((c) => stateOf(c).label === 'nearly full').length },
        { label: 'predicted full in 30 min', value: cps?.filter((c) => c.pred_full_30).length },
        { label: 'live predictions verified', value: live?.verified?.toLocaleString() },
        { label: 'live model accuracy', value: live?.model && pct(live.model.accuracy, 1) },
      ]} />
      <div className="charts two">
        <div className="panel">
          <h3>Line — citywide occupancy, last 24 h vs the typical 30-day pattern</h3>
          {line.length ? (
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={line}><CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="ts" type="number" scale="time" domain={['dataMin', 'dataMax']} tickFormatter={hm} />
                <YAxis tickFormatter={(v) => pct(v)} domain={[0.2, 0.7]} />
                <Tooltip labelFormatter={(t) => `${hm(t)} SGT`} formatter={(v) => pct(v, 1)} /><Legend />
                <Line {...NO} dataKey="occupancy" name="live (StreamingJob)" stroke="#1f6fb2" strokeWidth={2} dot={false} />
                <Line {...NO} dataKey="typical" name="typical for this hour (Q1)" stroke="#e8a33d" strokeDasharray="6 4" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          ) : <p className="muted">Waiting for live stream batches…</p>}
          {lastLive && lastTyp != null && (
            <p className="muted">Now {pct(lastLive.occupancy, 1)} occupied vs {pct(lastTyp, 1)} typical for this hour —{' '}
              {Math.abs(lastLive.occupancy - lastTyp) < 0.02 ? 'a normal day' : lastLive.occupancy > lastTyp ? 'busier than usual' : 'quieter than usual'}.</p>)}
        </div>
        <div className="panel">
          <h3>Pie — carparks by current state</h3>
          {cps ? (
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie {...NO} data={pie} dataKey="value" nameKey="name" innerRadius="45%" outerRadius="78%"
                  label={({ value, percent }) => `${value} (${(percent * 100).toFixed(0)}%)`}>
                  {pie.map((d) => <Cell key={d.name} fill={d.color} />)}
                </Pie>
                <Tooltip /><Legend />
              </PieChart>
            </ResponsiveContainer>
          ) : <p>Loading…</p>}
        </div>
      </div>
      <div className="panel">
        <h3>What the analytics reveal</h3>
        <ol className="reveals">
          {sql.map((q) => {
            const m = META[q.id]; const text = m?.reveal(fixDt(q.rows || []));
            return text ? <li key={q.id}><button className="link" onClick={() => go(q.id)}>Analytics {q.id.slice(1)} · {m.title}</button> — {text}</li> : null;
          })}
        </ol>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------------------- one analysis
function Detail({ q }) {
  const [open, setOpen] = useState(false);
  const m = META[q.id] || {};
  const rows = fixDt(q.rows || []);
  return (
    <div>
      <h2 className="section-title"><span className="qid">Analytics {q.id.slice(1)}</span> {m.title}</h2>
      <p className="question">{q.question}</p>
      <Kpis items={[...(m.kpis ? m.kpis(rows) : []), { label: 'query runtime', value: `${q.seconds} s` }, { label: 'result rows', value: q.row_count }]} />
      {m.reveal && <div className="reveal-box"><b>What it reveals:</b> {m.reveal(rows)}</div>}
      <div className="panel"><SqlCharts q={q} /></div>
      <div className="panel">
        <button className="sql-toggle" onClick={() => setOpen(!open)}>{open ? 'Hide SQL' : 'Show the Spark SQL that produced this'}</button>
        {open && <pre className="sql">{q.sql}</pre>}
        <h3>Result ({q.row_count} rows)</h3>
        <div className="result"><MetricTable rows={rows} /></div>
      </div>
    </div>
  );
}

function RddList({ rdd }) {
  return (
    <div>
      <h2 className="section-title"><span className="qid rdd">RDD</span> Scala RDD analytics (RddPipeline.scala)</h2>
      {rdd.map((a) => (
        <div key={a.name} className="panel analysis">
          <div className="analysis-head"><h3>{a.question}</h3><span className="muted">{a.row_count} rows · {a.source}</span></div>
          <div className="ops">{a.rdd_ops}</div>
          <RddCharts a={a} />
          <details className="result-details"><summary>Result table</summary><div className="result"><MetricTable rows={a.rows} /></div></details>
        </div>
      ))}
    </div>
  );
}

// ------------------------------------------------------------------------------- layout + navigation
const subFromHash = () => {
  const s = window.location.hash.replace(/^#\/?analytics\/?/, '');
  return s === 'rdd' ? 'rdd' : /^[1-8]$/.test(s) ? `Q${s}` : 'home';
};

export default function Analytics() {
  const [d, setD] = useState(null);
  const [err, setErr] = useState(null);
  const [sub, setSub] = useState(subFromHash());
  useEffect(() => { api('/analytics').then(setD).catch((e) => setErr(e.message)); }, []);
  const go = (s) => {
    setSub(s);
    window.location.hash = s === 'home' ? 'analytics' : s === 'rdd' ? 'analytics/rdd' : `analytics/${s.slice(1)}`;
    window.scrollTo(0, 0);
  };
  if (err) return <p className="error">{err}</p>;
  if (!d) return <p>Loading analytics…</p>;
  const q = d.sql.find((x) => x.id === sub);
  return (
    <div className="analytics-layout">
      <aside className="side">
        <button className={sub === 'home' ? 'on' : ''} onClick={() => go('home')}>Overview</button>
        <div className="side-label">Spark SQL analytics</div>
        {d.sql.map((x) => (
          <button key={x.id} className={sub === x.id ? 'on' : ''} onClick={() => go(x.id)}>
            <span className="num">{x.id.slice(1)}</span>{META[x.id]?.title || x.question}
          </button>
        ))}
        <div className="side-label">Scala RDD</div>
        <button className={sub === 'rdd' ? 'on' : ''} onClick={() => go('rdd')}>RDD analytics ({d.rdd.length})</button>
      </aside>
      <section className="side-main">
        {sub === 'home' && <Overview sql={d.sql} go={go} />}
        {q && <Detail q={q} />}
        {sub === 'rdd' && <RddList rdd={d.rdd} />}
      </section>
    </div>
  );
}
