import React, { useEffect, useRef, useState } from 'react';
import { api } from '../api.js';

const MAX_LEN = 400;
const STORE = 'parksense-chat';
const PATHS = {
  llm: { label: 'AI answer · grounded', cls: 'ok' },
  fallback: { label: 'Offline answer', cls: 'warn' },
  rules: { label: 'Answered by rules', cls: 'info' },
  off_topic: { label: 'Out of scope', cls: 'muted' },
  blocked: { label: 'Blocked by rules', cls: 'bad' },
  error: { label: 'Database unavailable', cls: 'bad' },
};
const STEPS = [
  ['input rules', 'Length limit, unicode clean-up, control characters removed'],
  ['rate limit', 'At most 12 questions per minute per user'],
  ['safety screen', 'Blocks prompt injection, requests for keys/config, and personal data (NRIC, phone, email)'],
  ['intent router', 'Deterministic rules find carpark numbers, place names and the kind of question'],
  ['relevance gate', 'Off-topic questions and tasks (poems, code) are answered by rules — never sent to the LLM'],
  ['data tools', 'Read-only HBase lookups build a compact fact sheet (live state, predictions, profiles, analytics, model metrics)'],
  ['LLM', 'One Claude call that may use only those facts; skipped when the key is missing, the hourly budget is used, or the circuit breaker is open'],
  ['grounding check', 'Every number in the answer must appear in the facts, otherwise the template answer is shown'],
];

// tiny markdown: "- " bullets and **bold**, rendered as React nodes (no HTML injection)
function Rich({ text, onOpen }) {
  const bold = (t, key) => (onOpen && /^[A-Z]{1,4}\d{0,4}[A-Z]{0,2}$/.test(t) && /\d/.test(t)
    ? <button key={key} className="link cp" onClick={() => onOpen(t)} title={`Open ${t}`}>{t}</button> : <b key={key}>{t}</b>);
  const inline = (s, k) => s.split(/(\*\*[^*]+\*\*)/g).map((p, i) => (p.startsWith('**') && p.endsWith('**') ? bold(p.slice(2, -2), `${k}-${i}`) : p));
  const out = [];
  let list = [];
  const flush = (k) => { if (list.length) { out.push(<ul key={`u${k}`}>{list}</ul>); list = []; } };
  text.split('\n').forEach((ln, i) => {
    if (/^\s*[-•]\s+/.test(ln)) list.push(<li key={i}>{inline(ln.replace(/^\s*[-•]\s+/, ''), i)}</li>);
    else { flush(i); if (ln.trim()) out.push(<p key={i}>{inline(ln, i)}</p>); }
  });
  flush('end');
  return <>{out}</>;
}

function load() {
  try { return JSON.parse(sessionStorage.getItem(STORE) || '[]'); } catch { return []; }
}

// deep link #assistant/ask/<question> asks that question once on open
const askFromHash = () => {
  const m = window.location.hash.match(/^#\/?assistant\/ask\/(.+)$/);
  return m ? decodeURIComponent(m[1]).slice(0, MAX_LEN) : null;
};

export default function Chat({ onOpen }) {
  const [msgs, setMsgs] = useState(load);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState(null);
  const end = useRef(null);
  useEffect(() => { api('/chat/status').then(setStatus).catch(() => setStatus({ error: true })); }, [msgs.length]);
  useEffect(() => {
    try { sessionStorage.setItem(STORE, JSON.stringify(msgs.slice(-30))); } catch { /* storage may be blocked */ }
    end.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [msgs, busy]);

  const send = async (text) => {
    const q = (text ?? input).trim();
    if (!q || busy) return;
    const history = msgs.slice(-6).map((m) => ({ role: m.role, text: m.text }));
    setMsgs((m) => [...m, { role: 'user', text: q }]);
    setInput('');
    setBusy(true);
    try {
      const r = await fetch('/api/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: q, history }) });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || `HTTP ${r.status}`);
      setMsgs((m) => [...m, { role: 'assistant', text: d.answer, meta: d }]);
    } catch (e) {
      setMsgs((m) => [...m, { role: 'assistant', text: `The assistant service did not respond (${e.message}). Please try again.`, meta: { path: 'error', trace: [] } }]);
    } finally { setBusy(false); }
  };

  useEffect(() => {
    const q = askFromHash();
    if (q) { window.location.hash = 'assistant'; send(q); }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const last = [...msgs].reverse().find((m) => m.role === 'assistant')?.meta;
  const examples = status?.examples || [];
  const llm = status?.llm;
  const stepState = (name) => last?.trace?.find((s) => s.step.startsWith(name));
  return (
    <div className="chat-layout">
      <section className="chat">
        <div className="chat-head">
          <h2 className="section-title">ParkSense assistant</h2>
          {msgs.length > 0 && <button className="sql-toggle" onClick={() => setMsgs([])}>Clear chat</button>}
        </div>
        <div className="chat-log">
          {!msgs.length && (
            <div className="chat-empty">
              <p>Ask about any HDB carpark, an area, the 30-minute predictions, the analytics or the pipeline. Answers come from the live HBase data.</p>
              <div className="chips">{examples.map((e) => <button key={e} onClick={() => send(e)}>{e}</button>)}</div>
            </div>
          )}
          {msgs.map((m, i) => (
            <div key={i} className={`bubble ${m.role}`}>
              {m.role === 'user' ? <p>{m.text}</p> : <Rich text={m.text} onOpen={onOpen} />}
              {m.meta && (
                <div className="bubble-meta">
                  <span className={`path ${PATHS[m.meta.path]?.cls || 'muted'}`}>{PATHS[m.meta.path]?.label || m.meta.path}</span>
                  {m.meta.intent && <span className="tag">{m.meta.intent.replace(/_/g, ' ')}</span>}
                  {m.meta.model && <span className="tag">{m.meta.model}</span>}
                  {m.meta.ms != null && <span className="tag">{(m.meta.ms / 1000).toFixed(1)} s</span>}
                  {m.meta.notice && <div className="notice">{m.meta.notice}</div>}
                  {m.meta.sources?.length > 0 && <div className="sources">Data: {m.meta.sources.join(' · ')}</div>}
                  {i === msgs.length - 1 && m.meta.suggestions?.length > 0 && (
                    <div className="chips">{m.meta.suggestions.map((s) => <button key={s} onClick={() => send(s)}>{s}</button>)}</div>
                  )}
                </div>
              )}
            </div>
          ))}
          {busy && <div className="bubble assistant typing"><span /><span /><span /></div>}
          <div ref={end} />
        </div>
        <form className="chat-input" onSubmit={(e) => { e.preventDefault(); send(); }}>
          <input value={input} maxLength={MAX_LEN} onChange={(e) => setInput(e.target.value)} placeholder="e.g. Is AM20 going to be full in 30 minutes?" disabled={busy} />
          <span className="count">{input.length}/{MAX_LEN}</span>
          <button type="submit" disabled={busy || !input.trim()}>Send</button>
        </form>
      </section>
      <aside className="agent-panel">
        <h3>How the assistant works</h3>
        <p className="muted">Every question passes these steps in order. Rules run before anything is sent to the LLM, and every failure has a fallback.</p>
        <ol className="agent-steps">
          {STEPS.map(([name, desc]) => {
            const s = stepState(name);
            return (
              <li key={name} className={s ? s.status : ''}>
                <b>{name === 'LLM' ? `LLM (${llm?.model || 'Claude'})` : name}</b>
                <span>{desc}</span>
                {s && <em>{s.status}: {s.detail}</em>}
              </li>
            );
          })}
        </ol>
        <h3>Fallbacks</h3>
        <ul className="agent-fallbacks">
          <li>API key missing or rejected, rate limit, timeout or server error → template answer built from the same facts</li>
          <li>3 failures in a row → circuit breaker skips the LLM for 2 minutes (10 minutes for an invalid key)</li>
          <li>Model declines or the answer fails the grounding check → template answer</li>
          <li>Unknown carpark number → similar carpark numbers suggested</li>
          <li>HBase unreachable → clear error, no guessing</li>
        </ul>
        <h3>LLM status</h3>
        {llm ? (
          <table className="facts"><tbody>
            <tr><th>configured</th><td>{llm.configured ? 'yes' : 'no (offline answers only)'}</td></tr>
            <tr><th>model</th><td>{llm.model}</td></tr>
            <tr><th>refusal fallback</th><td>server-side ({llm.server_side_fallbacks})</td></tr>
            <tr><th>circuit breaker</th><td>{llm.circuit_open ? `open until ${new Date(llm.circuit_open_until).toLocaleTimeString('en-SG', { timeZone: 'Asia/Singapore', hour12: false })}` : 'closed'}</td></tr>
            <tr><th>LLM calls this hour</th><td>{llm.calls_this_hour} / {llm.max_calls_per_hour}</td></tr>
            {llm.last_error && <tr><th>last error</th><td>{llm.last_error}</td></tr>}
          </tbody></table>
        ) : <p className="muted">{status?.error ? 'Status unavailable' : 'Loading…'}</p>}
      </aside>
    </div>
  );
}
