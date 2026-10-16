// Output rules applied to every LLM answer before it is shown: non-empty, short, no leaked configuration, and
// every number grounded in the retrieved facts (or the question). A failed check sends the template answer instead.
const NUM = /\d[\d,]*(?:\.\d+)?/g;
const SAFE_INTS = new Set([...Array(61).keys(), 100, 800]); // clock hours/minutes, "30 minutes", "100%", the 800 m radius

function allowedNumbers(...sources) {
  const vals = new Set();
  const add = (v) => { if (Number.isFinite(v)) vals.add(v); };
  for (const s of sources) {
    for (const m of JSON.stringify(s ?? '').match(NUM) || []) {
      const v = Number(m.replace(/,/g, ''));
      add(v);
      if (v > 0 && v <= 1) add(v * 100); // fraction quoted as a percentage
    }
  }
  return [...vals];
}

const decimals = (s) => (s.includes('.') ? s.split('.')[1].length : 0);
// x (as written, with d decimals) is grounded if some allowed value rounds to it at that precision
function grounded(written, allowed) {
  const x = Number(written.replace(/,/g, ''));
  if (Number.isInteger(x) && SAFE_INTS.has(x)) return true;
  const d = decimals(written);
  const f = 10 ** d;
  return allowed.some((a) => Math.round(a * f) / f === x || Math.abs(a - x) < 1e-9);
}

export function checkAnswer(text, { facts, question }) {
  if (!text) return { ok: false, reason: 'empty answer' };
  if (text.length > 1400) return { ok: false, reason: 'answer too long' };
  if (/sk-ant-|ANTHROPIC_API_KEY|system prompt|<data|<\/?question>/i.test(text)) return { ok: false, reason: 'answer leaked configuration' };
  const allowed = allowedNumbers(facts, question);
  const bad = (text.match(NUM) || []).filter((n) => !grounded(n, allowed));
  if (bad.length) return { ok: false, reason: `numbers not found in the data: ${[...new Set(bad)].slice(0, 5).join(', ')}` };
  return { ok: true };
}
