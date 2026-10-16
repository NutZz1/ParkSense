// Data tools: read-only lookups against HBase (via the REST gateway) that turn one routed intent into a compact
// "facts" object. Derived figures (percentages, best hours) are computed here so the LLM never has to do arithmetic.
import { getMetric } from '../hbaseRest.js';
import { allCarparks } from '../routes/carparks.js';

const FULL = 0.1;
const r4 = (v) => (typeof v === 'number' && Number.isFinite(v) ? Math.round(v * 10000) / 10000 : v);
const p1 = (v) => (v == null ? null : Math.round(v * 1000) / 10); // fraction -> percent, 1 dp
const round = (o) => JSON.parse(JSON.stringify(o, (_k, v) => r4(v)));
const hh = (h) => `${String(h).padStart(2, '0')}:00`;

// timestamps -> "YYYY-MM-DD HH:MM SGT" (naive values from the API are already Singapore time)
export function sgt(ts) {
  if (!ts) return null;
  const d = new Date(/Z$|[+-]\d\d:\d\d$/.test(ts) ? ts : `${ts}+08:00`);
  if (Number.isNaN(d.getTime())) return null;
  return `${new Date(d.getTime() + 8 * 3600000).toISOString().slice(0, 16).replace('T', ' ')} SGT`;
}
export function nowSgt() {
  const d = new Date(Date.now() + 8 * 3600000);
  const dow = d.getUTCDay();
  return { text: `${d.toISOString().slice(0, 16).replace('T', ' ')} SGT`, hour: d.getUTCHours(), day_type: dow === 0 || dow === 6 ? 'weekend' : 'weekday' };
}

const stateOf = (c) => (c.total ? (c.available / c.total <= FULL ? 'nearly full' : c.available / c.total <= 0.3 ? 'filling' : 'space') : 'no data');
function brief(c, extra = {}) {
  return { carpark: c.id, address: c.address, type: c.type, total_lots: c.total, free_lots: c.available,
    free_pct: c.total ? p1(c.available / c.total) : null, state_now: stateOf(c),
    predicted_nearly_full_in_30_min: c.p_full_30 == null ? 'no prediction yet' : c.pred_full_30 ? 'yes' : 'no',
    p_full_30_pct: p1(c.p_full_30), ...extra };
}
const hav = (a, b, c, d) => {
  const R = 6371000, t = (x) => (x * Math.PI) / 180;
  const h = Math.sin(t(c - a) / 2) ** 2 + Math.cos(t(a)) * Math.cos(t(c)) * Math.sin(t(d - b) / 2) ** 2;
  return Math.round(2 * R * Math.asin(Math.sqrt(h)));
};
// space now and not predicted full first, then more free lots
const rank = (a, b) => (a.pred_full_30 - b.pred_full_30) || ((a.p_full_30 ?? 1) - (b.p_full_30 ?? 1)) || (b.available - a.available);

async function metric(name) { return (await getMetric(name).catch(() => null))?.value ?? null; }

export async function carparkIndex() {
  const carparks = await allCarparks();
  return { carparks, idSet: new Set(carparks.map((c) => c.id)), byId: new Map(carparks.map((c) => [c.id, c])) };
}

function nearby(c, carparks, k = 3, radius = 800) {
  if (c.lat == null) return [];
  return carparks.filter((x) => x.id !== c.id && x.lat != null)
    .map((x) => ({ ...x, d: hav(c.lat, c.lon, x.lat, x.lon) })).filter((x) => x.d <= radius)
    .sort(rank).slice(0, k).map((x) => brief(x, { distance_m: x.d }));
}

function profileFacts(prof, now) {
  if (!prof || !Object.keys(prof.weekday || {}).length) return null;
  const out = {};
  for (const dt of ['weekday', 'weekend']) {
    const hrs = Object.entries(prof[dt] || {}).map(([h, v]) => ({ hour: Number(h), occ: v }));
    if (!hrs.length) continue;
    const day = hrs.filter((x) => x.hour >= 7 && x.hour <= 22); // waking hours, when people actually drive
    const by = [...day].sort((a, b) => a.occ - b.occ);
    out[dt] = {
      quietest_daytime_hours: by.slice(0, 3).map((x) => ({ hour: hh(x.hour), typical_occupancy_pct: p1(x.occ) })),
      busiest_daytime_hours: by.slice(-3).reverse().map((x) => ({ hour: hh(x.hour), typical_occupancy_pct: p1(x.occ) })),
      overnight_peak: (() => { const m = hrs.reduce((a, b) => (b.occ > a.occ ? b : a)); return { hour: hh(m.hour), typical_occupancy_pct: p1(m.occ) }; })(),
      typical_occupancy_pct_by_hour: Object.fromEntries(hrs.map((x) => [hh(x.hour), p1(x.occ)])),
    };
  }
  const cur = prof[now.day_type]?.[now.hour];
  out.typical_now = cur == null ? null : { hour: hh(now.hour), day_type: now.day_type, typical_occupancy_pct: p1(cur) };
  return out;
}

function carparkFull(c, carparks, prof, now) {
  return {
    ...brief(c),
    live_reading_at: sgt(c.snapshot_ts), sensor_updated_at: sgt(c.update_ts),
    prediction_made_at: sgt(c.made_at), prediction_for: sgt(c.target_at), model_version: c.model_ver,
    occupancy_now_pct: p1(c.occupancy),
    design: { decks: c.decks, gantry_height_m: c.gantry, basement: c.basement === 'Y' ? 'yes' : 'no', parking_system: c.system },
    policy: { free_parking: c.free_parking, night_parking: c.night_parking, short_term_parking: c.short_term },
    usual_pattern: profileFacts(prof, now),
    nearby_alternatives_within_800m: c.pred_full_30 || stateOf(c) !== 'space' ? nearby(c, carparks) : undefined,
  };
}

const ANALYTICS_IDS = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8'];
async function analytics(n) {
  const m = await metric(`analytics:${ANALYTICS_IDS[n - 1]}`);
  if (!m) return null;
  // fractions -> *_pct so the answer can quote them directly (mean_occupancy 0.5416 -> mean_occupancy_pct 54.2)
  const rows = (m.rows || []).slice(0, 48).map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => (
    k === 'dt' && typeof v === 'number' ? [k, new Date(v).toISOString().slice(0, 10)]
      : /occupancy|_occ$|rate$|^swing$/.test(k) && typeof v === 'number' ? [`${k}_pct`, p1(v)] : [k, v]))));
  return { analytics_number: n, question: m.question, result_rows: m.row_count, query_seconds: m.seconds, rows,
    hour_note: 'hour is the Singapore clock hour (4 = 04:00)', note: 'rates are shares of 5-minute readings over 30 days of history; nearly full = at most 10% of car lots free' };
}

async function citywide(index, now) {
  const cps = index.carparks;
  const total = cps.reduce((s, c) => s + (c.total || 0), 0), free = cps.reduce((s, c) => s + (c.available || 0), 0);
  const q1 = await metric('analytics:q1');
  const typ = q1?.rows?.find((r) => r.hour === now.hour && r.day_type === now.day_type);
  const latest = cps.map((c) => c.snapshot_ts).filter(Boolean).sort().pop();
  return {
    carparks_reporting: cps.length, total_car_lots: total, free_car_lots: free, citywide_occupancy_pct: total ? p1(1 - free / total) : null,
    nearly_full_now: cps.filter((c) => stateOf(c) === 'nearly full').length,
    filling_10_to_30_pct_free: cps.filter((c) => stateOf(c) === 'filling').length,
    with_space_over_30_pct_free: cps.filter((c) => stateOf(c) === 'space').length,
    predicted_nearly_full_in_30_min: cps.filter((c) => c.pred_full_30).length,
    typical_occupancy_this_hour_pct: typ ? p1(typ.mean_occupancy) : null,
    latest_reading_at: sgt(latest),
  };
}

const ABOUT = {
  project: 'ParkSense SG predicts whether each of about 2,000 Singapore HDB carparks will be nearly full (at most 10% of car lots free) 30 minutes ahead.',
  data_source: 'data.gov.sg carpark-availability API (polled every minute) plus HDB carpark information (type, system, policies).',
  stack: 'HDFS data lake, Scala Spark RDD cleaning pipeline and Structured Streaming job, PySpark ML (Random Forest), Spark SQL analytics, HBase serving tables, Express + React dashboard.',
  pages: 'Live Map, Find Parking, Carpark Detail, Insights, Analytics (8 Spark SQL analyses + RDD analytics), Model, Pipeline, Assistant.',
};

/** Fetch the facts for one routed intent. Throws only when HBase itself is unreachable. */
export async function fetchFacts(r, index) {
  const now = nowSgt();
  const base = { now: now.text };
  const { byId, carparks } = index;
  switch (r.intent) {
    case 'carpark_status':
    case 'carpark_profile': {
      const c = byId.get(r.ids[0]);
      const prof = await metric(`profile:${c.id}`);
      return { facts: round({ ...base, carpark: carparkFull(c, carparks, prof, now) }),
        sources: [`HBase carpark_live/${c.id}`, `HBase metrics/profile:${c.id}`] };
    }
    case 'carpark_nearby': {
      const c = byId.get(r.ids[0]);
      return { facts: round({ ...base, carpark: brief(c), nearby_within_800m_ranked_by_space_on_arrival: nearby(c, carparks, 6) }),
        sources: [`HBase carpark_live/${c.id}`, 'HBase carpark_live (all rows, distance search)'] };
    }
    case 'compare': {
      const list = r.ids.slice(0, 4).map((id) => byId.get(id));
      const profs = await Promise.all(list.map((c) => metric(`profile:${c.id}`)));
      return { facts: round({ ...base, carparks: list.map((c, i) => ({ ...brief(c), live_reading_at: sgt(c.snapshot_ts),
        typical_now: profileFacts(profs[i], now)?.typical_now ?? null })) }),
        sources: list.map((c) => `HBase carpark_live/${c.id}`) };
    }
    case 'area': {
      const cps = r.area.ids.map((id) => byId.get(id)).filter(Boolean);
      const total = cps.reduce((s, c) => s + (c.total || 0), 0), free = cps.reduce((s, c) => s + (c.available || 0), 0);
      return { facts: round({ ...base, area: r.area.phrase, carparks_matching_address: cps.length, total_car_lots: total, free_car_lots: free,
        area_free_pct: total ? p1(free / total) : null, nearly_full_now: cps.filter((c) => stateOf(c) === 'nearly full').length,
        predicted_nearly_full_in_30_min: cps.filter((c) => c.pred_full_30).length,
        best_options_now: [...cps].sort(rank).slice(0, 5).map((c) => brief(c)),
        fullest_now: [...cps].sort((a, b) => a.available / a.total - b.available / b.total).slice(0, 3).map((c) => brief(c)) }),
        sources: [`HBase carpark_live (${cps.length} carparks with "${r.area.phrase}" in the address)`] };
    }
    case 'citywide':
    case 'general':
      return { facts: round({ ...base, citywide_now: await citywide(index, now), ...(r.intent === 'general' ? { about: ABOUT } : {}) }),
        sources: ['HBase carpark_live (all rows)', 'HBase metrics/analytics:q1'] };
    case 'predicted_full': {
      const pf = carparks.filter((c) => c.pred_full_30).sort((a, b) => (b.p_full_30 ?? 0) - (a.p_full_30 ?? 0));
      return { facts: round({ ...base, predicted_nearly_full_in_30_min_count: pf.length, carparks_reporting: carparks.length,
        highest_probability: pf.slice(0, 12).map((c) => brief(c)),
        becoming_full_now_has_space: pf.filter((c) => stateOf(c) !== 'nearly full').slice(0, 6).map((c) => brief(c)) }),
        sources: ['HBase carpark_live p: (live predictions)'] };
    }
    case 'analytics': {
      const a = await analytics(r.n);
      const extra = r.n === 8 ? { rdd_cleaning_counts: await metric('spark:rdd_summary') } : {};
      return { facts: round({ ...base, ...(a || { missing: `analytics:q${r.n} has not been published yet` }), ...extra }),
        sources: [`HBase metrics/analytics:q${r.n}`, ...(r.n === 8 ? ['HBase metrics/spark:rdd_summary'] : [])] };
    }
    case 'model': {
      const [best, comp, trans, hor, fi, live] = await Promise.all(['model:best', 'model:comparison', 'model:transitions', 'model:horizons',
        'model:feature_importance', 'live:eval'].map(metric));
      const chosen = best?.algorithm;
      return { facts: round({ ...base,
        chosen_model: best && { algorithm: best.algorithm, params: best.params, threshold: best.threshold, horizon_min: best.horizon_min,
          label: best.label, model_version: best.model_version,
          split_days: best.split && { train: best.split.train_days?.length, validation: best.split.val_days?.length, test: best.split.test_days?.length } },
        test_set_comparison: (comp || []).map((m) => ({ model: m.model, type: m.type, accuracy: m.accuracy, precision: m.precision, recall: m.recall,
          f1: m.f1, pr_auc: m.pr_auc })),
        state_changes_test_set: (trans || []).filter((x) => x.correct_share != null).map((x) => ({ model: x.model, transition: x.transition, n: x.n, correct_pct: p1(x.correct_share) })),
        horizons: (hor || []).map((h) => ({ horizon_min: h.horizon_min, f1: h.f1, persistence_f1: h.persistence_f1 })),
        top_features: (fi || []).filter((f) => f.model === chosen).slice(0, 5).map((f) => ({ feature: f.feature, importance: f.importance })),
        live_verification: live && { predictions_checked: live.verified, from: sgt(live.first_target), to: sgt(live.last_target),
          model: live.model && { accuracy: live.model.accuracy, precision: live.model.precision, recall: live.model.recall, f1: live.model.f1 },
          persistence_baseline_f1: live.persistence?.f1,
          state_changes: live.transitions && { n: live.transitions.n, model_correct_pct: p1(live.transitions.model_correct_share) } },
        meaning: 'Persistence = "it will be as full as it is now". It is hard to beat because most carparks rarely change state in 30 minutes; the model is useful for the state changes, which persistence always gets wrong.' }),
        sources: ['HBase metrics/model:*', 'HBase metrics/live:eval'] };
    }
    case 'pipeline': {
      const [rdd, cache, shuffle, parts, last] = await Promise.all(['spark:rdd_summary', 'spark:cache', 'spark:shuffle', 'spark:partitions', 'stream:last_batch'].map(metric));
      const pick = Object.fromEntries((rdd || []).filter((x) => ['input_lines', 'clean_records', 'stale_dropped', 'carparks', 'snapshots',
        'always_zero_carparks', 'never_changing_carparks', 'runtime_ms', 'shuffle_join_ms', 'broadcast_join_ms'].includes(x.metric)).map((x) => [x.metric, Number(x.value)]));
      return { facts: round({ ...base, architecture: ABOUT.stack, data_source: ABOUT.data_source, scala_rdd_pipeline: pick,
        caching_experiment: cache, shuffle_experiment: shuffle, partition_experiment: parts,
        streaming_last_batch: last && { batch_id: last.batch_id, snapshot: sgt(last.snapshot_sgt), carparks: last.carparks, seconds_from_file_to_processed: last.file_to_processed_s } }),
        sources: ['HBase metrics/spark:*', 'HBase metrics/stream:last_batch'] };
    }
    case 'about':
      return { facts: { ...base, about: ABOUT }, sources: ['static project description'] };
    default:
      return { facts: base, sources: [] };
  }
}
