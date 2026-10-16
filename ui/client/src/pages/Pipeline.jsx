import React, { useEffect, useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, LineChart, Line, Legend } from 'recharts';
import { api, sgt } from '../api.js';
import MetricTable from '../components/MetricTable.jsx';

export default function Pipeline() {
  const [s, setS] = useState(null);
  const [st, setSt] = useState(null);
  useEffect(() => {
    api('/metrics/spark').then(setS).catch(() => setS({}));
    const load = () => api('/metrics/stream').then(setSt).catch(() => {});
    load(); const t = setInterval(load, 60000); return () => clearInterval(t);
  }, []);
  if (!s) return <p>Loading…</p>;
  const sum = Object.fromEntries((s.rdd_summary || []).map((r) => [r.metric, r.value]));
  const perf = s.perf || [];
  const lb = st?.last_batch;
  return (
    <div>
      <img src="/fig01_architecture.png" alt="architecture" className="arch" />
      <div className="kpis">
        {['input_lines', 'lot_records_exploded', 'clean_records', 'stale_dropped', 'input_partitions_textFile', 'partitions_after_coalesce', 'runtime_ms'].map((k) => (
          <div key={k}><b>{Number(sum[k] ?? 0).toLocaleString()}</b><span>{k.replace(/_/g, ' ')}</span></div>))}
      </div>
      <div className="two">
        <div className="panel"><h3>RDD aggregation time vs partitions</h3>
          <ResponsiveContainer width="100%" height={220}><LineChart data={s.partitions || []}><CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="partitions" /><YAxis unit=" ms" /><Tooltip /><Line isAnimationActive={false} dataKey="ms" stroke="#1f6fb2" /></LineChart></ResponsiveContainer></div>
        <div className="panel"><h3>Persistence benchmark (ms)</h3>
          <ResponsiveContainer width="100%" height={220}><BarChart data={s.cache || []}><CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="mode" fontSize={10} /><YAxis /><Tooltip /><Legend />
            <Bar isAnimationActive={false} dataKey="action1_ms" name="1st" fill="#9bbbd8" /><Bar isAnimationActive={false} dataKey="action2_ms" name="2nd" fill="#1f6fb2" /></BarChart></ResponsiveContainer></div>
      </div>
      <div className="two">
        <div className="panel"><h3>Shuffle: reduceByKey vs groupByKey, joins</h3><MetricTable rows={s.shuffle} cols={['method', 'ms', 'shuffle_write_bytes', 'shuffle_write_records']} /></div>
        <div className="panel"><h3>System performance</h3><MetricTable rows={perf.filter((r) => ['parallelism', 'shuffle_partitions', 'caching', 'scalability'].includes(r.experiment))} cols={['experiment', 'setting', 'rows', 'total_s']} /></div>
      </div>
      <div className="panel"><h3>Speed layer (StreamingJob, RDD per micro-batch)</h3>
        {lb ? <p>Batch {lb.batch_id} · snapshot {sgt(lb.snapshot_sgt)} SGT · {lb.carparks} carparks · citywide occupancy {(100 * lb.citywide_occupancy).toFixed(1)}% · {lb.nearly_full_now} nearly full now · file → processed {lb.file_to_processed_s} s · RDD batch {lb.batch_ms} ms</p> : <p className="muted">No stream batch yet.</p>}
        <MetricTable rows={(st?.by_type || []).sort((a, b) => b.occupancy - a.occupancy)} cols={['type', 'occupancy', 'carparks']} />
        <MetricTable rows={perf.filter((r) => ['hbase', 'api', 'streaming', 'live'].includes(r.experiment))} cols={['setting', 'total_s', 'notes']} />
      </div>
    </div>
  );
}
