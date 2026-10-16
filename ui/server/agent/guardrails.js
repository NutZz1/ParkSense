// Rule layer that runs BEFORE anything can reach the LLM: input hygiene, per-client rate limit, prompt-injection
// and personal-data screens. A failed rule returns a canned reply and the LLM is never called.
export const MAX_LEN = 400;
const WINDOW_MS = 60000;
const MAX_PER_WINDOW = 12;
const hits = new Map(); // client -> recent request timestamps

const INJECTION = [
  /\b(ignore|disregard|forget|override)\b.{0,30}\b(instructions?|rules|prompts?|guidelines|system)\b/i,
  /\b(system|developer|hidden)\s+(prompt|message|instructions?)\b/i,
  /\byou\s+are\s+(now|no\s+longer)\b/i,
  /\b(pretend|roleplay|role-play)\b/i,
  /\bact\s+as\b/i,
  /\bjail\s*break|\bdeveloper\s+mode\b|\bDAN\b/i,
  /\b(api|secret|access)\s*[-_]?keys?\b|\bpasswords?\b|\bcredentials?\b|sk-ant-/i,
  /<\/?\s*(system|instructions?|data|question|assistant)\s*>/i,
  /```|\{\{|\}\}/,
];
const PERSONAL = [
  { re: /\b[STFGM]\d{7}[A-Z]\b/i, what: 'an NRIC/FIN number' },
  { re: /\b[\w.+-]+@[\w-]+\.[\w.]+\b/, what: 'an email address' },
  { re: /(\+65[\s-]?)?\b[689]\d{3}[\s-]?\d{4}\b/, what: 'a phone number' },
  { re: /\b(?:\d[ -]?){13,19}\b/, what: 'a card number' },
];

// normalise unicode, drop control / zero-width characters, collapse whitespace
export function sanitize(text) {
  return String(text ?? '')
    .normalize('NFKC')
    .replace(/[\u0000-\u0008\u000B-\u001F\u007F​-‏‪-‮⁠-⁤﻿]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

export function checkLength(text) {
  if (!text) return { ok: false, reason: 'empty', reply: 'Please type a question about Singapore HDB carparks.' };
  if (text.length > MAX_LEN) return { ok: false, reason: 'too_long', reply: `Please keep questions under ${MAX_LEN} characters (yours has ${text.length}).` };
  return { ok: true };
}

export function checkRate(client) {
  const now = Date.now();
  const recent = (hits.get(client) || []).filter((t) => now - t < WINDOW_MS);
  if (recent.length >= MAX_PER_WINDOW) {
    hits.set(client, recent);
    return { ok: false, reason: 'rate_limited', reply: `You're asking a bit fast. Please wait ${Math.ceil((WINDOW_MS - (now - recent[0])) / 1000)} s and try again.` };
  }
  recent.push(now);
  hits.set(client, recent);
  if (hits.size > 5000) hits.clear(); // bounded memory
  return { ok: true };
}

export function checkSafety(text) {
  const inj = INJECTION.find((re) => re.test(text));
  if (inj) return { ok: false, reason: 'prompt_injection', rule: String(inj),
    reply: "I can only answer questions about ParkSense carpark data, so I can't follow instructions that change how I work or reveal configuration. Ask me about a carpark, an area, the predictions or the analytics." };
  const pii = PERSONAL.find((p) => p.re.test(text));
  if (pii) return { ok: false, reason: 'personal_data', rule: pii.what,
    reply: `Your message looks like it contains ${pii.what}. Please don't share personal details — ParkSense only needs a carpark number or an area name.` };
  return { ok: true };
}

// prior turns come from the browser, so they are re-screened and trimmed before being reused as context
export function cleanHistory(history) {
  if (!Array.isArray(history)) return [];
  return history.slice(-6)
    .map((m) => ({ role: m?.role === 'assistant' ? 'assistant' : 'user', text: sanitize(m?.text).slice(0, 600) }))
    .filter((m) => m.text && (m.role === 'assistant' || checkSafety(m.text).ok));
}
