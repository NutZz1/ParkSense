// Per-analysis titles, KPI tiles and a one-line "what it reveals" finding, all computed from the query result rows.
import { pct } from './api.js';

const DAYS = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
const hh = (h) => `${String(h).padStart(2, '0')}:00`;
const maxBy = (a, k) => a.reduce((m, r) => (m == null || r[k] > m[k] ? r : m), null);
const minBy = (a, k) => a.reduce((m, r) => (m == null || r[k] < m[k] ? r : m), null);
const mean = (a, k) => (a.length ? a.reduce((s, r) => s + (r[k] || 0), 0) / a.length : null);
const asDay = (v) => (typeof v === 'number' ? new Date(v).toISOString().slice(0, 10) : v);
const policy = (r) => `${r.free_parking === 'NO' ? 'there is no free parking' : `parking is free on Sundays/holidays (${r.free_parking.replace('SUN & PH FR ', '')})`} and ${r.night_parking === 'YES' ? 'night parking is allowed' : 'there is no night parking'}`;

export const META = {
  Q1: {
    title: 'Peak hours',
    kpis: (rows) => {
      const wd = rows.filter((r) => r.day_type === 'weekday'), we = rows.filter((r) => r.day_type === 'weekend');
      const p = maxBy(wd, 'mean_occupancy'), q = minBy(wd, 'mean_occupancy'), w = maxBy(we, 'mean_occupancy'), f = maxBy(rows, 'nearly_full_rate');
      return [
        { label: 'Weekday peak', value: p && `${hh(p.hour)} · ${pct(p.mean_occupancy, 1)}` },
        { label: 'Weekday quietest', value: q && `${hh(q.hour)} · ${pct(q.mean_occupancy, 1)}` },
        { label: 'Weekend peak', value: w && `${hh(w.hour)} · ${pct(w.mean_occupancy, 1)}` },
        { label: 'Highest nearly-full rate', value: f && `${hh(f.hour)} ${f.day_type} · ${pct(f.nearly_full_rate, 1)}` },
      ];
    },
    reveal: (rows) => {
      const wd = rows.filter((r) => r.day_type === 'weekday');
      const p = maxBy(wd, 'mean_occupancy'), q = minBy(wd, 'mean_occupancy');
      return p && q && `HDB carparks are fullest overnight (${hh(p.hour)}, ${pct(p.mean_occupancy, 1)} on weekdays) because residents park at home, and emptiest mid-afternoon (${hh(q.hour)}, ${pct(q.mean_occupancy, 1)}) when they are at work.`;
    },
  },
  Q2: {
    title: 'Chronically full carparks',
    kpis: (rows) => [
      { label: 'Most often full', value: rows[0] && `${rows[0].carpark_no} · ${pct(rows[0].nearly_full_rate, 1)}` },
      { label: 'Carparks full > 90% of the time', value: rows.filter((r) => r.nearly_full_rate > 0.9).length },
      { label: 'Average of the Top 20', value: pct(mean(rows, 'nearly_full_rate'), 1) },
      { label: 'Surface carparks in Top 20', value: `${rows.filter((r) => (r.car_park_type || '').startsWith('SURFACE')).length} / ${rows.length}` },
    ],
    reveal: (rows) => rows[0] && `${rows[0].carpark_no} (${rows[0].address}) has ≤ 10% of its lots free ${pct(rows[0].nearly_full_rate, 1)} of the time; most chronically full carparks are small surface carparks — prime candidates for pricing or capacity action.`,
  },
  Q3: {
    title: 'Carpark design',
    kpis: (rows) => {
      // electronic-system groups with >= 5 carparks (coupon carparks rarely report live data)
      const known = rows.filter((r) => r.car_park_type !== 'UNKNOWN' && r.carparks >= 5 && r.parking_system.startsWith('ELECTRONIC'));
      const hi = maxBy(known, 'nearly_full_rate'), lo = minBy(known, 'nearly_full_rate');
      const ms = rows.find((r) => r.car_park_type === 'MULTI-STOREY CAR PARK' && r.parking_system.startsWith('ELECTRONIC'));
      return [
        { label: 'Most at risk', value: hi && `${hi.car_park_type.replace(' CAR PARK', '')} · ${pct(hi.nearly_full_rate, 1)}` },
        { label: 'Least at risk', value: lo && `${lo.car_park_type.replace(' CAR PARK', '')} · ${pct(lo.nearly_full_rate, 1)}` },
        { label: 'Multi-storey (electronic)', value: ms && pct(ms.nearly_full_rate, 1) },
        { label: 'Type × system groups', value: rows.length },
      ];
    },
    reveal: (rows) => {
      const s = rows.find((r) => r.car_park_type === 'SURFACE CAR PARK' && r.parking_system.startsWith('ELECTRONIC'));
      const m = rows.find((r) => r.car_park_type === 'MULTI-STOREY CAR PARK' && r.parking_system.startsWith('ELECTRONIC'));
      return s && m && `Surface carparks are nearly full ${pct(s.nearly_full_rate, 1)} of the time versus ${pct(m.nearly_full_rate, 1)} for multi-storey ones: small open lots run out of space far more often than large decks.`;
    },
  },
  Q4: {
    title: 'Parking policies (evenings)',
    kpis: (rows) => {
      const hi = maxBy(rows, 'evening_nearly_full_rate'), lo = minBy(rows, 'evening_nearly_full_rate');
      return [
        { label: 'Highest evening risk', value: hi && pct(hi.evening_nearly_full_rate, 1) },
        { label: 'Lowest evening risk', value: lo && pct(lo.evening_nearly_full_rate, 1) },
        { label: 'Risk ratio (high / low)', value: hi && lo && lo.evening_nearly_full_rate ? `${(hi.evening_nearly_full_rate / lo.evening_nearly_full_rate).toFixed(1)}×` : '–' },
        { label: 'Policy groups', value: rows.length },
      ];
    },
    reveal: (rows) => {
      const hi = maxBy(rows, 'evening_nearly_full_rate'), lo = minBy(rows, 'evening_nearly_full_rate');
      return hi && lo && `Evenings are hardest where ${policy(hi)} (${pct(hi.evening_nearly_full_rate, 1)} nearly full) and easiest where ${policy(lo)} (${pct(lo.evening_nearly_full_rate, 1)}).`;
    },
  },
  Q5: {
    title: 'Busiest day of the week',
    kpis: (rows) => {
      const count = rows.reduce((a, r) => ({ ...a, [r.busiest_dow]: (a[r.busiest_dow] || 0) + 1 }), {});
      const top = Object.entries(count).sort((a, b) => b[1] - a[1])[0];
      const hi = maxBy(rows, 'mean_occupancy');
      return [
        { label: 'Most common busiest day', value: top && `${DAYS[top[0]]} (${top[1]} types)` },
        { label: 'Fullest type on its busiest day', value: hi && `${hi.car_park_type.replace(' CAR PARK', '')} · ${pct(hi.mean_occupancy, 1)}` },
        { label: 'Carpark types', value: rows.length },
      ];
    },
    reveal: (rows) => {
      const count = rows.reduce((a, r) => ({ ...a, [r.busiest_dow]: (a[r.busiest_dow] || 0) + 1 }), {});
      const top = Object.entries(count).sort((a, b) => b[1] - a[1])[0];
      return top && `${DAYS[top[0]]} is the busiest day for ${top[1]} of ${rows.length} carpark types.`;
    },
  },
  Q6: {
    title: 'Daily trend',
    kpis: (rows) => {
      const hi = maxBy(rows, 'nearly_full_rate'), lo = minBy(rows, 'nearly_full_rate');
      return [
        { label: 'Days analysed', value: rows.length },
        { label: 'Average daily nearly-full rate', value: pct(mean(rows, 'nearly_full_rate'), 2) },
        { label: 'Worst day', value: hi && `${asDay(hi.dt)} · ${pct(hi.nearly_full_rate, 2)}` },
        { label: 'Best day', value: lo && `${asDay(lo.dt)} · ${pct(lo.nearly_full_rate, 2)}` },
      ];
    },
    reveal: (rows) => {
      const hi = maxBy(rows, 'nearly_full_rate'), lo = minBy(rows, 'nearly_full_rate');
      return hi && lo && `The citywide nearly-full rate is stable day to day (${pct(lo.nearly_full_rate, 2)}–${pct(hi.nearly_full_rate, 2)}); differences between hours and carparks matter far more than differences between days.`;
    },
  },
  Q7: {
    title: 'Residential vs commercial',
    kpis: (rows) => {
      const res = rows.filter((r) => r.swing > 0).length;
      const big = maxBy(rows.map((r) => ({ ...r, a: Math.abs(r.swing) })), 'a');
      return [
        { label: 'Residential (fill at night)', value: `${res} of ${rows.length}` },
        { label: 'Commercial (fill by day)', value: `${rows.length - res} of ${rows.length}` },
        { label: 'Largest swing', value: big && `${big.carpark_no} · ${pct(big.swing, 0)}` },
      ];
    },
    reveal: (rows) => {
      const big = maxBy(rows.map((r) => ({ ...r, a: Math.abs(r.swing) })), 'a');
      return big && `${big.carpark_no} (${big.address}) swings ${pct(Math.abs(big.swing), 0)} between morning and evening — ${big.behaviour}. Carparks with large swings are where a 30-minute prediction helps most.`;
    },
  },
  Q8: {
    title: 'Data quality',
    kpis: (rows) => {
      const hi = maxBy(rows, 'stale_line_rate');
      return [
        { label: 'Raw lines analysed', value: rows.reduce((s, r) => s + (r.raw_lines || 0), 0).toLocaleString() },
        { label: 'Average stale rate', value: pct(mean(rows, 'stale_line_rate'), 2) },
        { label: 'Worst day for staleness', value: hi && `${asDay(hi.dt)} · ${pct(hi.stale_line_rate, 2)}` },
        { label: 'Days', value: rows.length },
      ];
    },
    reveal: (rows) => `On average ${pct(mean(rows, 'stale_line_rate'), 2)} of readings are older than 30 minutes and are dropped before analysis, so the results rest on fresh sensor data.`,
  },
};
