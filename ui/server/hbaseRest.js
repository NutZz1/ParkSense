// Minimal HBase REST client: JSON over HTTP, base64-decoded keys/columns/values, row + range scans.
const REST = process.env.HBASE_REST || 'http://localhost:8085';
const NS = 'parksense';
const TIMEOUT = 5000;
const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');
const unb64 = (s) => Buffer.from(s, 'base64').toString('utf8');

async function call(path, opts = {}) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), opts.timeout || TIMEOUT);
  try {
    const r = await fetch(`${REST}${path}`, { ...opts, signal: ctl.signal, headers: { Accept: 'application/json', ...(opts.headers || {}) } });
    if (r.status === 404) return null;
    if (!r.ok) throw new Error(`HBase REST ${r.status} for ${path}`);
    return r;
  } catch (e) {
    throw new Error(e.name === 'AbortError' ? `HBase REST timeout (${path})` : `HBase REST unavailable: ${e.message}`);
  } finally { clearTimeout(t); }
}

// {"Row":[{"key","Cell":[{"column","$"}]}]} -> [{key, cols: {"s:total": "105", ...}}]
function decodeRows(json) {
  return (json?.Row || []).map((r) => ({
    key: unb64(r.key),
    cols: Object.fromEntries((r.Cell || []).map((c) => [unb64(c.column), unb64(c.$)])),
  }));
}

export async function getRow(table, key) {
  const r = await call(`/${NS}:${table}/${encodeURIComponent(key)}`);
  if (!r) return null;
  return decodeRows(await r.json())[0] || null;
}

// all rows (or rows with a key prefix) via the REST globbing syntax  /table/prefix*
export async function scanPrefix(table, prefix = '') {
  const r = await call(`/${NS}:${table}/${encodeURIComponent(prefix)}*`, { timeout: 20000 });
  return r ? decodeRows(await r.json()) : [];
}

// bounded range scan with a stateful scanner (startRow inclusive, endRow exclusive)
export async function scanRange(table, startRow, endRow, limit = 2000) {
  const body = JSON.stringify({ batch: limit, startRow: b64(startRow), endRow: b64(endRow) });
  const r = await call(`/${NS}:${table}/scanner`, { method: 'PUT', body, headers: { 'Content-Type': 'application/json' } });
  const loc = r.headers.get('location');
  const scannerPath = new URL(loc).pathname;
  try {
    const out = await call(scannerPath);
    if (!out || out.status === 204) return [];
    return decodeRows(await out.json());
  } finally {
    await call(scannerPath, { method: 'DELETE' }).catch(() => {});
  }
}

export async function ping() {
  const r = await call('/version/cluster');
  return r ? (await r.json()).Version || 'up' : null;
}

export const LONG_MAX = 9223372036854775807n;
export const revTs = (ms) => (LONG_MAX - BigInt(ms)).toString().padStart(19, '0');
export async function getMetric(name) {
  const row = await getRow('metrics', name);
  return row ? { value: JSON.parse(row.cols['v:json']), updated_at: row.cols['v:updated_at'] } : null;
}
