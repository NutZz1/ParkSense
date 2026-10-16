// Deterministic intent router: finds carpark numbers, place names and the kind of question with rules, so the
// data that is fetched (and therefore what the LLM may talk about) is decided by code, not by the model.

// words that never identify a place (query vocabulary + generic address words)
const STOP = new Set(`A AN THE AND OR OF IN ON AT TO FOR FROM BY WITH NEAR NEARBY AROUND IS ARE WAS BE IT ITS THIS THAT THERE
HERE WHAT WHICH WHO WHEN WHERE WHY HOW CAN COULD WOULD WILL SHOULD DO DOES DID I ME MY WE YOU YOUR ANY SOME MOST MORE LESS
BEST GOOD BAD WORST TIME TIMES NOW TODAY TONIGHT TOMORROW MORNING AFTERNOON EVENING NIGHT CURRENTLY RIGHT GO GOING GET FIND
SHOW TELL LIST GIVE PLEASE THANKS CAR CARS CARPARK CARPARKS PARK PARKS PARKING LOT LOTS SPACE SPACES SLOT SLOTS FREE FULL
EMPTY AVAILABLE AVAILABILITY BUSY BUSIEST QUIET QUIETEST PEAK HOUR HOURS DAY DAYS WEEK WEEKDAY WEEKEND HDB SINGAPORE AREA
TOWN ESTATE STREET ST ROAD RD AVENUE AVE DRIVE DR CRESCENT CRES CLOSE LANE WAY PLACE RISE WALK LINK VIEW CENTRAL NORTH
SOUTH EAST WEST UPPER LOWER NEW BLK BLOCK BLOCKS CENTRE CENTER MARKET HAWKER MULTI STOREY SURFACE BASEMENT TYPE SYSTEM
PREDICT PREDICTION PREDICTED MODEL DATA ANALYTICS LIKELY CHANCE PROBABILITY MINUTES MIN MINS THIRTY ALSO ABOUT STATUS IF SO AS UP NO YES OK HI HEY THANK`.split(/\s+/));

// alpha-only carpark numbers that are also everyday abbreviations are not treated as carparks
const ABBREV = new Set(['HDB', 'MRT', 'AM', 'PM', 'SG', 'UTC', 'SGT', 'RDD', 'SQL', 'ML', 'RF', 'API', 'UI', 'OK', 'ID', 'PR', 'AUC', 'GBT']);

const has = (re, t) => re.test(t);
const words = (t) => t.toUpperCase().replace(/[^A-Z0-9 ]/g, ' ').split(/\s+/).filter(Boolean);

export function findCarparkIds(text, idSet) {
  const found = [];
  const unknown = [];
  for (const tok of text.match(/\b[A-Za-z]{1,4}\d{0,4}[A-Za-z]{0,2}\b/g) || []) {
    const up = tok.toUpperCase();
    const digit = /\d/.test(up);
    if (digit) {
      if (idSet.has(up)) found.push(up);
      else if (/^[A-Z]{1,4}\d{1,4}[A-Z]?$/.test(up) && !/^Q[1-8]$|^F1$|^H\d+$/.test(up)) unknown.push(up);
    } else if (tok === up && up.length >= 2 && idSet.has(up) && !ABBREV.has(up)) found.push(up);
  }
  return { ids: [...new Set(found)], unknown: [...new Set(unknown)] };
}

// longest run of place words (1-3) that appears in at least one carpark address
export function findArea(text, carparks) {
  const w = words(text).filter((x) => x.length >= 2 && !/\d/.test(x) && !STOP.has(x));
  if (!w.length) return null;
  for (let n = Math.min(3, w.length); n >= 1; n -= 1) {
    let best = null;
    for (let i = 0; i + n <= w.length; i += 1) {
      const phrase = w.slice(i, i + n).join(' ');
      const re = new RegExp(`\\b${phrase}\\b`);
      const ids = carparks.filter((c) => c.address && re.test(c.address.toUpperCase())).map((c) => c.id);
      if (ids.length && (!best || ids.length > best.ids.length)) best = { phrase, ids, whole: n === w.length };
    }
    if (best) return best;
  }
  return null;
}

// rough "is this about our system" lexicon (used together with the intents below)
const TOPIC = /\b(car ?parks?|parking|parked|park|lots?|slots?|spaces?|full|empty|available|availability|vacan\w*|occupan\w*|busy|busiest|quiet\w*|crowded|peak|rush|hdb|predict\w*|forecast\w*|model|accura\w*|precision|recall|f1|analytics?|spark|rdd|hbase|hdfs|hadoop|pipeline|stream\w*|sensor|stale|data ?quality|multi-?storey|surface|basement|night parking|free parking|gantry|parksense)\b/i;

/**
 * @returns {{intent: string, ids: string[], unknown: string[], area: object|null, n: number|null, followUp: boolean}}
 */
export function route(text, { idSet, carparks, lastId }) {
  const t = text.toLowerCase();
  const { ids, unknown } = findCarparkIds(text, idSet);
  const out = { intent: 'none', ids, unknown, area: null, n: null, followUp: false };
  const set = (intent, extra = {}) => Object.assign(out, { intent }, extra);

  if (has(/^(hi|hello|hey|hai|good (morning|afternoon|evening)|help|start|menu)\b[!. ?]*$|what can you (do|answer)|how (do|can) i use (you|this)|who are you/, t)) return set('help');
  // tasks that are not questions about the data (creative writing, code, homework) are refused even if they mention parking
  if (has(/\b(poem|poetry|haiku|story|stories|joke|song|lyrics|rap|essay|limerick|recipe|translate|homework|assignment|(write|generate|give me) (some |a |the )?(code|program|script|sql|python|javascript))\b/, t)) return set('unsupported_task');
  const an = t.match(/\b(?:analytics?|analysis|question|query)\s*(?:#|no\.?|number)?\s*([1-8])\b|\bq([1-8])\b/);
  if (an) return set('analytics', { n: Number(an[1] || an[2]) });

  if (ids.length >= 2) return set('compare');
  if (ids.length === 1) {
    if (has(/\b(near|nearby|around|alternative|instead|other carparks?|close to|next to)\b/, t)) return set('carpark_nearby');
    if (has(/\b(best time|when|what time|which hour|hours?|quiet\w*|busiest|busy|peak|morning|afternoon|evening|night|tonight|tomorrow|weekend|weekday|usually|typical\w*)\b/, t)) return set('carpark_profile');
    return set('carpark_status');
  }
  if (unknown.length) return set('unknown_carpark');
  // follow-up about the carpark from an earlier turn ("when is it quietest?")
  if (lastId && has(/\b(it|its|there|that one|this one|that carpark|this carpark|same carpark)\b/, t)) {
    out.ids = [lastId]; out.followUp = true;
    return set(has(/\b(when|time|hours?|quiet\w*|busy|busiest|peak|tomorrow|tonight|morning|evening|usually)\b/, t) ? 'carpark_profile'
      : has(/\b(near|nearby|alternative|instead)\b/, t) ? 'carpark_nearby' : 'carpark_status');
  }

  if (has(/\b(stale|data quality|missing data|broken sensors?|unreliable|excluded|frozen|always[- ]zero|clean(ing|ed)?)\b/, t)) return set('analytics', { n: 8 });
  if (has(/\b(model|accura\w*|precision|recall|f1|random forest|gbt|gradient|baseline|persistence|how good|how reliable|trust|confusion|threshold|features?|horizon|machine learning|ml)\b/, t)) return set('model');
  if (has(/\b(spark|rdd|hbase|hdfs|hadoop|pipeline|architecture|streaming|stream job|cache|caching|partitions?|shuffle|reducebykey|groupbykey|broadcast|how does (it|this|parksense) work|tech stack|database)\b/, t)) return set('pipeline');

  // a place name counts only in a parking question, after a preposition, or on its own ("Tampines")
  const area = findArea(text, carparks);
  if (area && (TOPIC.test(text) || area.whole || /\b(in|at|near|around)\b/.test(t))) return set('area', { area });

  if (has(/\b(will|going to|predict\w*|forecast\w*|expected|next (30|half an hour)|in 30|soon|later)\b/, t) && has(/\bfull\b/, t)) return set('predicted_full');
  if (has(/\b(always|often|chronic\w*|most full|fullest carparks?|worst carparks?|top ?20|hardest)\b/, t)) return set('analytics', { n: 2 });
  if (has(/\b(residential|commercial|swing|day vs night|morning vs evening)\b/, t)) return set('analytics', { n: 7 });
  if (has(/\b(free parking|night parking|polic(y|ies)|public holidays?|sunday parking)\b/, t)) return set('analytics', { n: 4 });
  if (has(/\b(multi-?storey|surface|basement|mechani[sz]ed|covered|carpark types?|types? of carparks?|design|coupon|electronic)\b/, t)) return set('analytics', { n: 3 });
  if (has(/\b(day of (the )?week|monday|tuesday|wednesday|thursday|friday|saturday|sunday|which day|busiest day)\b/, t)) return set('analytics', { n: 5 });
  if (has(/\b(trend|daily|over time|per day|each day|last (month|30 days)|history)\b/, t)) return set('analytics', { n: 6 });
  if (has(/\b(now|right now|currently|current|at the moment|today|overall|citywide|city-wide|whole of singapore|all carparks|how many)\b/, t)) return set('citywide');
  if (has(/\b(peak|busiest (time|hour)s?|what time|which hours?|when (are|is|do)|rush hour|fullest)\b/, t)) return set('analytics', { n: 1 });

  if (has(/\b(parksense|this (project|system|app|website|dashboard)|what is this|about (you|this))\b/, t)) return set('about');
  if (TOPIC.test(text)) return set('general');
  return out; // intent 'none' -> off topic
}
