import React from 'react';
import {
  ResponsiveContainer, LineChart, Line, BarChart, Bar, PieChart, Pie, Cell, ComposedChart, AreaChart, Area,
  XAxis, YAxis, Tooltip, Legend, CartesianGrid, ReferenceLine,
} from 'recharts';
import { pct } from '../api.js';

const C = ['#1f6fb2', '#e8a33d', '#2e9e4f', '#d64545', '#7a3e9d', '#17a2b8', '#8c6d46', '#5b6772', '#c2185b', '#9bbbd8'];
const DAYS = ['', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const NO = { isAnimationActive: false };
const short = (s, n = 22) => (s && s.length > n ? `${s.slice(0, n - 1)}…` : s);
const title = (s) => <h4 className="chart-title">{s}</h4>;

function Frame({ h = 260, children }) {
  return <ResponsiveContainer width="100%" height={h}>{children}</ResponsiveContainer>;
}

function PieBlock({ data, name, value = 'value', h = 260, format = (v) => v.toLocaleString() }) {
  const total = data.reduce((s, d) => s + (d[value] || 0), 0);
  return (
    <Frame h={h}>
      <PieChart>
        <Pie {...NO} data={data} dataKey={value} nameKey={name} outerRadius="75%" innerRadius="40%"
          label={({ percent }) => (percent > 0.03 ? `${(percent * 100).toFixed(0)}%` : '')}>
          {data.map((_, i) => <Cell key={i} fill={C[i % C.length]} />)}
        </Pie>
        <Tooltip formatter={(v) => `${format(v)} (${((100 * v) / total).toFixed(1)}%)`} />
        <Legend layout="vertical" align="right" verticalAlign="middle" wrapperStyle={{ fontSize: 12 }} />
      </PieChart>
    </Frame>
  );
}

function HBar({ data, x, y, h, color = C[0], format = (v) => pct(v, 1), domain, labelWidth = 170, maxChars = 28 }) {
  return (
    <Frame h={h || Math.max(220, data.length * 24)}>
      <BarChart data={data} layout="vertical" margin={{ left: 10, right: 30 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis type="number" tickFormatter={format} domain={domain} />
        <YAxis type="category" dataKey={y} width={labelWidth} fontSize={11} interval={0} tickFormatter={(v) => short(String(v), maxChars)} />
        <Tooltip formatter={format} />
        <Bar {...NO} dataKey={x} fill={color} />
      </BarChart>
    </Frame>
  );
}

// ---------------------------------------------------------------------------------- Spark SQL
// date columns from Spark arrive as epoch ms when serialised by pandas; show them as YYYY-MM-DD
const asDay = (v) => (typeof v === 'number' ? new Date(v).toISOString().slice(0, 10) : v);

export function SqlCharts({ q }) {
  const rows = (q.rows || []).map((r) => ('dt' in r ? { ...r, dt: asDay(r.dt) } : r));
  switch (q.id) {
    case 'Q1': {
      const data = Array.from({ length: 24 }, (_, h) => {
        const wd = rows.find((r) => r.hour === h && r.day_type === 'weekday');
        const we = rows.find((r) => r.hour === h && r.day_type === 'weekend');
        return { hour: h, weekday: wd?.mean_occupancy, weekend: we?.mean_occupancy, wd_full: wd?.nearly_full_rate, we_full: we?.nearly_full_rate };
      });
      return (
        <div className="charts two">
          <div>{title('Line — mean occupancy by hour')}
            <Frame><LineChart data={data}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="hour" /><YAxis tickFormatter={(v) => pct(v)} />
              <Tooltip formatter={(v) => pct(v, 1)} /><Legend />
              <Line {...NO} dataKey="weekday" stroke={C[0]} strokeWidth={2} dot={false} /><Line {...NO} dataKey="weekend" stroke={C[1]} strokeWidth={2} dot={false} /></LineChart></Frame></div>
          <div>{title('Bar — nearly-full rate by hour')}
            <Frame><BarChart data={data}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="hour" /><YAxis tickFormatter={(v) => pct(v)} />
              <Tooltip formatter={(v) => pct(v, 2)} /><Legend />
              <Bar {...NO} dataKey="wd_full" name="weekday" fill={C[0]} /><Bar {...NO} dataKey="we_full" name="weekend" fill={C[1]} /></BarChart></Frame></div>
        </div>
      );
    }
    case 'Q2':
      return <div>{title('Horizontal bar — share of readings nearly full')}
        <HBar data={rows.map((r) => ({ ...r, label: `${r.carpark_no} · ${r.address || ''}` }))} x="nearly_full_rate" y="label" color={C[3]} domain={[0, 1]} /></div>;
    case 'Q3': {
      const byType = Object.values(rows.reduce((a, r) => {
        a[r.car_park_type] = a[r.car_park_type] || { name: r.car_park_type, value: 0 };
        a[r.car_park_type].value += r.carparks; return a;
      }, {}));
      return (
        <div className="charts two">
          <div>{title('Bar — nearly-full rate by type and system')}
            <HBar data={rows.map((r) => ({ ...r, label: `${r.car_park_type.replace(' CAR PARK', '')} · ${r.parking_system.startsWith('ELEC') ? 'electronic' : r.parking_system.startsWith('COUP') ? 'coupon' : '?'}` }))}
              x="nearly_full_rate" y="label" color={C[3]} labelWidth={210} maxChars={34} /></div>
          <div>{title('Pie — carparks by type')}<PieBlock data={byType} name="name" /></div>
        </div>
      );
    }
    case 'Q4': {
      const data = rows.map((r) => ({ ...r, label: `free: ${r.free_parking === 'NO' ? 'no' : r.free_parking.replace('SUN & PH FR ', 'Sun/PH ')} · night: ${r.night_parking.toLowerCase()}` }));
      return (
        <div className="charts two">
          <div>{title('Grouped bar — evening occupancy vs nearly-full rate by policy')}
            <Frame h={300}><BarChart data={data} margin={{ bottom: 60 }}><CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="label" angle={-20} textAnchor="end" interval={0} fontSize={10} /><YAxis tickFormatter={(v) => pct(v)} />
              <Tooltip formatter={(v) => pct(v, 1)} /><Legend verticalAlign="top" />
              <Bar {...NO} dataKey="evening_mean_occupancy" name="evening occupancy" fill={C[0]} />
              <Bar {...NO} dataKey="evening_nearly_full_rate" name="evening nearly-full rate" fill={C[3]} /></BarChart></Frame></div>
          <div>{title('Pie — carparks by policy')}<PieBlock data={data.map((r) => ({ name: r.label, value: r.carparks }))} name="name" /></div>
        </div>
      );
    }
    case 'Q5':
      return <div>{title('Bar — busiest day of the week per carpark type (mean occupancy)')}
        <HBar data={rows.map((r) => ({ ...r, label: `${r.car_park_type.replace(' CAR PARK', '')} (${DAYS[r.busiest_dow]})` }))} x="mean_occupancy" y="label" color={C[4]} domain={[0, 1]} labelWidth={230} maxChars={36} /></div>;
    case 'Q6':
      return <div>{title('Area — daily citywide nearly-full rate and mean occupancy')}
        <Frame><ComposedChart data={rows}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="dt" fontSize={10} />
          <YAxis yAxisId="l" tickFormatter={(v) => pct(v, 1)} /><YAxis yAxisId="r" orientation="right" tickFormatter={(v) => pct(v)} />
          <Tooltip formatter={(v) => pct(v, 2)} /><Legend />
          <Area {...NO} yAxisId="l" dataKey="nearly_full_rate" name="nearly-full rate" stroke={C[3]} fill="#f6c9c9" />
          <Line {...NO} yAxisId="r" dataKey="mean_occupancy" name="mean occupancy" stroke={C[0]} strokeWidth={2} dot={false} /></ComposedChart></Frame></div>;
    case 'Q7': {
      const data = rows.slice(0, 20).map((r) => ({ ...r, label: `${r.carpark_no} · ${r.address || ''}` }));
      return <div>{title('Diverging bar — evening minus morning occupancy (+ residential, − commercial)')}
        <Frame h={Math.max(260, data.length * 22)}><BarChart data={data} layout="vertical" margin={{ left: 10, right: 30 }}>
          <CartesianGrid strokeDasharray="3 3" /><XAxis type="number" tickFormatter={(v) => pct(v)} domain={[-1, 1]} />
          <YAxis type="category" dataKey="label" width={170} fontSize={11} tickFormatter={(v) => short(v, 26)} />
          <Tooltip formatter={(v) => pct(v, 1)} /><ReferenceLine x={0} stroke="#333" />
          <Bar {...NO} dataKey="swing">{data.map((r, i) => <Cell key={i} fill={r.swing > 0 ? C[0] : C[1]} />)}</Bar></BarChart></Frame>
        <p className="muted">Blue = fills at night (residential) · Orange = fills by day (commercial)</p></div>;
    }
    case 'Q8':
      return <div>{title('Bar + line — raw lines vs clean rows per day, and stale-reading rate')}
        <Frame><ComposedChart data={rows}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="dt" fontSize={10} />
          <YAxis yAxisId="l" /><YAxis yAxisId="r" orientation="right" tickFormatter={(v) => pct(v, 1)} />
          <Tooltip formatter={(v, n) => (n === 'stale rate' ? pct(v, 2) : v.toLocaleString())} /><Legend />
          <Bar {...NO} yAxisId="l" dataKey="raw_lines" name="raw lines" fill={C[9]} />
          <Bar {...NO} yAxisId="l" dataKey="clean_car_rows" name="clean car-lot rows" fill={C[0]} />
          <Line {...NO} yAxisId="r" dataKey="stale_line_rate" name="stale rate" stroke={C[3]} strokeWidth={2} dot={false} /></ComposedChart></Frame></div>;
    default:
      return null;
  }
}

// ---------------------------------------------------------------------------------- Scala RDD
export function RddCharts({ a }) {
  const rows = a.rows || [];
  switch (a.name) {
    case 'rdd_summary': {
      const v = Object.fromEntries(rows.map((r) => [r.metric, Number(r.value)]));
      const funnel = [
        { step: 'flat lines (textFile)', n: v.input_lines }, { step: 'lot records (flatMap)', n: v.lot_records_exploded },
        { step: 'clean records', n: v.clean_records }, { step: 'car-lot records', n: v.car_lot_records },
      ];
      const outcome = [
        { name: 'clean', value: v.clean_records }, { name: 'stale (> 30 min)', value: v.stale_dropped },
        { name: 'total lots <= 0', value: v.non_positive_total }, { name: 'malformed / bad lot item', value: (v.malformed_lines || 0) + (v.bad_lot_items || 0) },
      ].filter((d) => d.value > 0);
      return (
        <div className="charts two">
          <div>{title('Funnel (bar) — records through the RDD pipeline')}<HBar data={funnel} x="n" y="step" format={(n) => n.toLocaleString()} h={220} /></div>
          <div>{title('Pie — what happened to every lot record')}<PieBlock data={outcome} name="name" /></div>
        </div>
      );
    }
    case 'rdd_top20_full':
      return <div>{title('Horizontal bar — share of snapshots nearly full')}
        <HBar data={rows.map((r) => ({ ...r, label: `${r.carpark_no} · ${r.address || ''}` }))} x="full_rate" y="label" color={C[3]} domain={[0, 1]} /></div>;
    case 'rdd_type_hour': {
      const types = [...new Set(rows.map((r) => r.car_park_type))];
      const data = Array.from({ length: 24 }, (_, h) => Object.fromEntries([['hour', h],
        ...types.map((t) => [t, rows.find((r) => r.car_park_type === t && r.hour === h && String(r.is_weekend) === 'false')?.full_rate])]));
      return <div>{title('Multi-line — weekday nearly-full rate by hour for each carpark type')}
        <Frame h={300}><LineChart data={data}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="hour" /><YAxis tickFormatter={(v) => pct(v)} />
          <Tooltip formatter={(v) => pct(v, 1)} /><Legend wrapperStyle={{ fontSize: 11 }} />
          {types.map((t, i) => <Line {...NO} key={t} dataKey={t} stroke={C[i % C.length]} dot={false} strokeWidth={2} />)}</LineChart></Frame></div>;
    }
    case 'rdd_daily_full_rate':
      return <div>{title('Area — daily nearly-full rate (Scala reduceByKey)')}
        <Frame><AreaChart data={rows}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="dt" fontSize={10} /><YAxis tickFormatter={(v) => pct(v, 1)} />
          <Tooltip formatter={(v) => pct(v, 2)} /><Area {...NO} dataKey="full_rate" name="nearly-full rate" stroke={C[3]} fill="#f6c9c9" /></AreaChart></Frame></div>;
    case 'rdd_carpark_stats':
    case 'rdd_stale_rate_carpark':
    case 'rdd_excluded_carparks':
      return a.pie ? (
        <div className="charts two">
          <div>{title(`Pie — ${a.pie_title}`)}<PieBlock data={a.pie} name="name" /></div>
          <div>{title(`Bar — ${a.pie_title}`)}<HBar data={a.pie} x="value" y="name" format={(n) => n.toLocaleString()} color={C[4]} h={220} /></div>
        </div>
      ) : null;
    default:
      return null;
  }
}
