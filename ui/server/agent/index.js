// ParkSense chat agent: rules -> router -> relevance gate -> HBase data tools -> LLM -> grounding check -> answer,
// with a deterministic fallback at every step. Each step is recorded in a trace that the UI shows.
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { sanitize, checkLength, checkRate, checkSafety, cleanHistory } from './guardrails.js';
import { route } from './router.js';
import { carparkIndex, fetchFacts } from './tools.js';
import { askLLM, llmGate, MODEL } from './llm.js';
import { checkAnswer } from './validate.js';
import { templateAnswer, suggestionsFor, OFF_TOPIC, HELP, EXAMPLES } from './fallback.js';

const LOG = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../outputs/logs/chat.log');
function log(entry) {
  try { fs.mkdirSync(path.dirname(LOG), { recursive: true }); fs.appendFileSync(LOG, `${JSON.stringify(entry)}\n`); } catch { /* logging never breaks a reply */ }
}

const NO_LLM_INTENTS = new Set(['help', 'unknown_carpark']); // fully answered by code

export async function runAgent({ message, history, client }) {
  const t0 = Date.now();
  const trace = [];
  const step = (name, status, detail) => trace.push({ step: name, status, detail, ms: Date.now() - t0 });
  const reply = (o) => {
    const out = { answer: o.answer, path: o.path, intent: o.intent || null, sources: o.sources || [], model: o.model || null,
      suggestions: o.suggestions || EXAMPLES.slice(0, 4), notice: o.notice || null, trace, ms: Date.now() - t0 };
    log({ at: new Date().toISOString(), q: (o.q ?? message ?? '').slice(0, 200), intent: out.intent, path: out.path, ms: out.ms,
      fail: trace.filter((s) => s.status === 'fail').map((s) => `${s.step}: ${s.detail}`) });
    return out;
  };

  // 1. input rules
  const q = sanitize(message);
  const len = checkLength(q);
  if (!len.ok) { step('input rules', 'fail', len.reason); return reply({ answer: len.reply, path: 'blocked', q }); }
  step('input rules', 'pass', `${q.length} chars, sanitised`);
  const rate = checkRate(client);
  if (!rate.ok) { step('rate limit', 'fail', rate.reason); return reply({ answer: rate.reply, path: 'blocked', q }); }
  step('rate limit', 'pass', 'within 12 questions/min');
  const safe = checkSafety(q);
  if (!safe.ok) { step('safety screen', 'fail', `${safe.reason} (${safe.rule})`); return reply({ answer: safe.reply, path: 'blocked', q }); }
  step('safety screen', 'pass', 'no prompt injection or personal data');
  const hist = cleanHistory(history);

  // 2. route (needs the carpark list from HBase for IDs and place names)
  let index;
  try { index = await carparkIndex(); } catch (e) {
    step('database', 'fail', e.message);
    return reply({ answer: "I can't reach the ParkSense database (HBase) right now, so I can't answer with live data. Please try again in a minute.", path: 'error', q });
  }
  const lastId = [...hist].reverse().map((m) => (m.text.match(/\b[A-Z]{1,4}\d{1,4}[A-Z]?\b/g) || []).find((x) => index.idSet.has(x))).find(Boolean);
  const r = route(q, { idSet: index.idSet, carparks: index.carparks, lastId });
  step('intent router', 'pass', `${r.intent}${r.ids.length ? ` · ${r.ids.join(', ')}` : ''}${r.area ? ` · area "${r.area.phrase}"` : ''}${r.n ? ` · analytics ${r.n}` : ''}${r.followUp ? ' (from earlier turn)' : ''}`);

  // 3. relevance gate: nothing off-topic reaches the LLM
  if (r.intent === 'none') {
    step('relevance gate', 'fail', 'not about parking, ParkSense data or the system');
    return reply({ answer: OFF_TOPIC, path: 'off_topic', intent: 'none', q });
  }
  if (r.intent === 'unsupported_task') {
    step('relevance gate', 'fail', 'a task, not a question about the data');
    return reply({ answer: "I only answer questions about the ParkSense data (carpark space, predictions, analytics and how the system works), so I can't write poems, stories or code. Try one of the questions below.", path: 'off_topic', intent: r.intent, q });
  }
  step('relevance gate', 'pass', 'on topic');
  if (r.intent === 'help') return reply({ answer: HELP, path: 'rules', intent: 'help', q });
  if (r.intent === 'unknown_carpark') {
    const u = r.unknown[0];
    const prefix = u.match(/^[A-Z]+/)[0];
    const similar = index.carparks.filter((c) => c.id.startsWith(prefix)).map((c) => c.id).slice(0, 6);
    step('data tools', 'fail', `carpark ${u} not in carpark_live`);
    return reply({ answer: `I couldn't find carpark **${u}** in the live data.${similar.length ? ` Did you mean ${similar.join(', ')}?` : ''} Carpark numbers look like AM20 or TPM1; you can also ask by area, e.g. "carparks in Tampines".`,
      path: 'rules', intent: r.intent, q, suggestions: similar.slice(0, 3).map((id) => `Is ${id} full now?`) });
  }

  // 4. data tools
  let facts, sources;
  try { ({ facts, sources } = await fetchFacts(r, index)); } catch (e) {
    step('data tools', 'fail', e.message);
    return reply({ answer: "I can't read that data from HBase right now. Please try again in a minute.", path: 'error', intent: r.intent, q });
  }
  step('data tools', 'pass', sources.join('; '));
  const fallback = (why) => reply({ answer: templateAnswer(r.intent, facts), path: 'fallback', intent: r.intent, sources, q,
    suggestions: suggestionsFor(r.intent, r.ids), notice: `Offline answer: ${why}. Shown directly from the data.` });

  // 5. LLM (only if its preconditions hold)
  if (NO_LLM_INTENTS.has(r.intent)) return fallback('answered by rules');
  const gate = llmGate();
  if (!gate.ok) { step(`LLM (${MODEL})`, 'skip', `${gate.kind}: ${gate.detail}`); return fallback(gate.detail); }
  let llm;
  try { llm = await askLLM({ question: q, intent: r.intent, facts, history: hist }); } catch (e) {
    step(`LLM (${MODEL})`, 'fail', `${e.kind}: ${e.message}`);
    return fallback(e.message);
  }
  step(`LLM (${llm.model})`, 'pass', `${llm.ms} ms · ${llm.usage.input_tokens} in / ${llm.usage.output_tokens} out tokens`);
  if (llm.out_of_scope) {
    step('grounding check', 'skip', 'model judged the question out of scope');
    return reply({ answer: OFF_TOPIC, path: 'off_topic', intent: r.intent, q });
  }

  // 6. output rules
  const ok = checkAnswer(llm.text, { facts, question: q });
  if (!ok.ok) { step('grounding check', 'fail', ok.reason); return fallback(`the AI answer failed the grounding check (${ok.reason})`); }
  step('grounding check', 'pass', 'every number is in the retrieved data');
  return reply({ answer: llm.text, path: 'llm', intent: r.intent, sources, model: llm.model, q, suggestions: suggestionsFor(r.intent, r.ids) });
}
