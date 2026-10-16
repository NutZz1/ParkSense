// Deterministic answers built straight from the facts. Used whenever the LLM is unavailable, fails, declines, or
// its answer fails the grounding check, so the assistant keeps working (labelled "offline answer" in the UI).
const pp = (v) => (v == null ? '–' : `${v}%`); // already a percentage
const lots = (c) => `${c.free_lots}/${c.total_lots} lots free (${pp(c.free_pct)})`;
const predTxt = (c) => (c.predicted_nearly_full_in_30_min === 'yes' ? `**predicted nearly full in 30 min** (P = ${pp(c.p_full_30_pct)})`
  : c.predicted_nearly_full_in_30_min === 'no' ? `not predicted to be full in 30 min (P = ${pp(c.p_full_30_pct)})` : 'no prediction yet');
const line = (c) => `- **${c.carpark}** ${c.address || ''} — ${lots(c)}, ${predTxt(c)}${c.distance_m != null ? `, ${c.distance_m} m away` : ''}`;
const DAYS = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
const maxBy = (a, k) => a.reduce((m, r) => (m == null || r[k] > m[k] ? r : m), null);
const minBy = (a, k) => a.reduce((m, r) => (m == null || r[k] < m[k] ? r : m), null);

export const EXAMPLES = [
  'Is AM20 going to be full in 30 minutes?',
  'Best time to park at AM20 on a weekday?',
  'Carparks with space in Tampines',
  'How many carparks are nearly full right now?',
  'Which carparks are predicted to be full soon?',
  'When are HDB carparks fullest?',
  'How accurate is the model?',
  'How does the Spark pipeline work?',
];

export const HELP = `I'm the ParkSense assistant. I answer questions about Singapore HDB carparks using the project's live HBase data, the model's predictions and the Spark analytics. You can ask about:
- a carpark by its number (e.g. AM20): space now, the 30-minute prediction, the best time to arrive, nearby alternatives
- an area (e.g. Tampines, Ang Mo Kio): carparks with space now
- citywide status: how many carparks are nearly full now or predicted full soon
- the analytics (peak hours, chronically full carparks, carpark types, policies, trends, data quality)
- the model and the pipeline (accuracy vs the persistence baseline, Spark RDD/SQL/Streaming, HBase)`;

export const OFF_TOPIC = "Sorry, that's outside what I can help with. I only answer questions about Singapore HDB carpark availability, ParkSense's predictions and analytics, and how the system works.";

function analyticsText(f) {
  if (!f.rows) return `That analysis has not been published yet (${f.missing}).`;
  const rows = f.rows;
  switch (f.analytics_number) {
    case 1: {
      const wd = rows.filter((r) => r.day_type === 'weekday'), we = rows.filter((r) => r.day_type === 'weekend');
      const p = maxBy(wd, 'mean_occupancy_pct'), q = minBy(wd, 'mean_occupancy_pct'), w = maxBy(we, 'mean_occupancy_pct');
      return `HDB carparks are fullest overnight and emptiest mid-afternoon.\n- Weekday peak: ${String(p.hour).padStart(2, '0')}:00 (${pp(p.mean_occupancy_pct)} occupied)\n- Weekday quietest: ${String(q.hour).padStart(2, '0')}:00 (${pp(q.mean_occupancy_pct)})\n- Weekend peak: ${String(w.hour).padStart(2, '0')}:00 (${pp(w.mean_occupancy_pct)})`;
    }
    case 2: return `The carparks most often nearly full (share of readings with ≤ 10% free):\n${rows.slice(0, 5).map((r) => `- **${r.carpark_no}** ${r.address} — ${pp(r.nearly_full_rate_pct)} of the time (${r.total_lots} lots, ${r.car_park_type.toLowerCase()})`).join('\n')}`;
    case 3: return `Nearly-full rate by carpark type and parking system:\n${rows.filter((r) => r.carparks >= 5).slice(0, 6).map((r) => `- ${r.car_park_type.toLowerCase()} · ${r.parking_system.toLowerCase()}: ${pp(r.nearly_full_rate_pct)} (${r.carparks} carparks)`).join('\n')}`;
    case 4: return `Evening (18:00–23:00) nearly-full rate by parking policy:\n${rows.map((r) => `- free parking "${r.free_parking}", night parking ${r.night_parking}: ${pp(r.evening_nearly_full_rate_pct)} (${r.carparks} carparks)`).join('\n')}`;
    case 5: return `Busiest day of the week by carpark type:\n${rows.map((r) => `- ${r.car_park_type.toLowerCase()}: ${DAYS[r.busiest_dow]} (${pp(r.mean_occupancy_pct)} occupied)`).join('\n')}`;
    case 6: {
      const hi = maxBy(rows, 'nearly_full_rate_pct'), lo = minBy(rows, 'nearly_full_rate_pct');
      return `Over ${rows.length} days the citywide nearly-full rate ranged from ${pp(lo.nearly_full_rate_pct)} (${lo.dt}) to ${pp(hi.nearly_full_rate_pct)} (${hi.dt}); it is stable from day to day.`;
    }
    case 7: return `Carparks with the largest morning-vs-evening swing:\n${rows.slice(0, 5).map((r) => `- **${r.carpark_no}** ${r.address}: ${r.behaviour} (morning ${pp(r.morning_occ_pct)}, evening ${pp(r.evening_occ_pct)})`).join('\n')}`;
    case 8: {
      const avg = rows.reduce((s, r) => s + r.stale_line_rate_pct, 0) / rows.length;
      const rs = Object.fromEntries((f.rdd_cleaning_counts || []).map((x) => [x.metric, x.value]));
      return `Data quality over ${rows.length} days: on average ${`${avg.toFixed(2)}%`} of raw readings were stale (sensor not updated for over 30 min) and were dropped.${rs.stale_dropped ? ` The Scala RDD pipeline dropped ${rs.stale_dropped} stale records, and excluded ${rs.always_zero_carparks} always-zero and ${rs.never_changing_carparks} never-changing carparks from the ML.` : ''}`;
    }
    default: return 'Analysis not found.';
  }
}

export function templateAnswer(intent, facts) {
  switch (intent) {
    case 'help': return HELP;
    case 'carpark_status': {
      const c = facts.carpark;
      let s = `**${c.carpark}** (${c.address}, ${c.type.toLowerCase()}) has ${lots(c)} as of ${c.live_reading_at}. It is ${predTxt(c)}.`;
      if (c.usual_pattern?.typical_now) s += ` Typically it is ${pp(c.usual_pattern.typical_now.typical_occupancy_pct)} occupied at this hour.`;
      if (c.nearby_alternatives_within_800m?.length) s += `\nNearby alternatives:\n${c.nearby_alternatives_within_800m.map(line).join('\n')}`;
      return s;
    }
    case 'carpark_profile': {
      const c = facts.carpark, u = c.usual_pattern;
      if (!u) return `I don't have a usage profile for ${c.carpark} yet. Right now it has ${lots(c)}.`;
      const part = (dt) => u[dt] && `- ${dt}s: quietest ${u[dt].quietest_daytime_hours.map((h) => `${h.hour} (${pp(h.typical_occupancy_pct)})`).join(', ')}; busiest ${u[dt].busiest_daytime_hours.map((h) => `${h.hour} (${pp(h.typical_occupancy_pct)})`).join(', ')}`;
      return `Typical occupancy of **${c.carpark}** (${c.address}) between 07:00 and 22:00:\n${[part('weekday'), part('weekend')].filter(Boolean).join('\n')}\nRight now it has ${lots(c)} and is ${predTxt(c)}.`;
    }
    case 'carpark_nearby': {
      const n = facts.nearby_within_800m_ranked_by_space_on_arrival;
      return n.length ? `Carparks within 800 m of **${facts.carpark.carpark}**, best chance of space on arrival first:\n${n.map(line).join('\n')}`
        : `I found no other carparks with coordinates within 800 m of ${facts.carpark.carpark}.`;
    }
    case 'compare': return `Side by side right now:\n${facts.carparks.map(line).join('\n')}`;
    case 'area': return `${facts.carparks_matching_address} carparks have "${facts.area}" in their address: ${facts.free_car_lots} of ${facts.total_car_lots} car lots free (${pp(facts.area_free_pct)}), ${facts.nearly_full_now} nearly full now, ${facts.predicted_nearly_full_in_30_min} predicted full in 30 min.\nBest options now:\n${facts.best_options_now.map(line).join('\n')}`;
    case 'citywide':
    case 'general': {
      const c = facts.citywide_now;
      return `As of ${c.latest_reading_at}, ${c.carparks_reporting} carparks are reporting with ${c.free_car_lots} of ${c.total_car_lots} car lots free (citywide occupancy ${pp(c.citywide_occupancy_pct)}${c.typical_occupancy_this_hour_pct != null ? ` vs ${pp(c.typical_occupancy_this_hour_pct)} typical for this hour` : ''}).\n- Nearly full now: **${c.nearly_full_now}**\n- Filling (10–30% free): ${c.filling_10_to_30_pct_free}\n- Predicted nearly full in 30 min: ${c.predicted_nearly_full_in_30_min}`;
    }
    case 'predicted_full':
      return `**${facts.predicted_nearly_full_in_30_min_count}** of ${facts.carparks_reporting} carparks are predicted to be nearly full in 30 minutes. Highest probability:\n${facts.highest_probability.slice(0, 6).map(line).join('\n')}`
        + (facts.becoming_full_now_has_space.length ? `\nStill have some space now but expected to fill: ${facts.becoming_full_now_has_space.map((c) => c.carpark).join(', ')}.` : '');
    case 'analytics': return analyticsText(facts);
    case 'model': {
      const b = facts.chosen_model, rf = facts.test_set_comparison.find((m) => m.model === b?.algorithm), pe = facts.test_set_comparison.find((m) => m.model === 'Persistence');
      const tr = facts.state_changes_test_set.find((x) => x.model === b?.algorithm && x.transition === 'all transitions');
      const lv = facts.live_verification;
      return `The deployed model is a **${b.algorithm}** (threshold ${b.threshold}) predicting "${b.label}".\n- Test set: F1 ${rf?.f1} vs ${pe?.f1} for the persistence baseline; PR-AUC ${rf?.pr_auc}\n`
        + (tr ? `- On state changes (where persistence is always wrong) it is right ${pp(tr.correct_pct)} of the time\n` : '')
        + (lv ? `- Live: ${lv.predictions_checked} predictions checked against what happened, F1 ${lv.model?.f1?.toFixed?.(4) ?? lv.model?.f1} vs ${lv.persistence_baseline_f1?.toFixed?.(4) ?? lv.persistence_baseline_f1} for persistence` : '');
    }
    case 'pipeline': {
      const p = facts.scala_rdd_pipeline;
      return `${facts.architecture}\n- The Scala RDD pipeline read ${p.input_lines} raw lines and produced ${p.clean_records} clean records for ${p.carparks} carparks\n- Broadcast join ${p.broadcast_join_ms} ms vs shuffle join ${p.shuffle_join_ms} ms`
        + (facts.streaming_last_batch ? `\n- Last streaming batch #${facts.streaming_last_batch.batch_id} processed the ${facts.streaming_last_batch.snapshot} snapshot for ${facts.streaming_last_batch.carparks} carparks` : '');
    }
    case 'about': return `${facts.about.project}\n- Data: ${facts.about.data_source}\n- Stack: ${facts.about.stack}\n- Pages: ${facts.about.pages}`;
    default: return OFF_TOPIC;
  }
}

export function suggestionsFor(intent, ids = []) {
  const id = ids[0];
  if (id && intent.startsWith('carpark')) return [`Best time to park at ${id}?`, `Nearby carparks to ${id} with space`, `Is ${id} predicted to be full?`].filter((s) => !s.startsWith(intent === 'carpark_profile' ? 'Best' : intent === 'carpark_nearby' ? 'Nearby' : 'Is'));
  if (intent === 'model') return ['How does the model compare to persistence?', 'How does the Spark pipeline work?'];
  if (intent === 'analytics') return ['When are HDB carparks fullest?', 'Which carparks are always full?', 'Does night parking change evening demand?'];
  return EXAMPLES.slice(0, 4);
}
