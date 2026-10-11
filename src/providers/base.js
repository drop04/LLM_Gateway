import { UpstreamError } from '../errors.js';

function parseRetryAfter(h) {
  if (!h) return null;
  const secs = Number(h);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const d = Date.parse(h);
  return Number.isNaN(d) ? null : Math.max(0, d - Date.now());
}

function httpError(res, text) {
  let msg = text.slice(0, 500);
  try {
    const j = JSON.parse(text);
    msg = j.error?.message ?? j.message ?? msg;
  } catch { /* not JSON */ }
  return new UpstreamError(`upstream ${res.status}: ${msg}`, {
    status: res.status, kind: 'http', retryAfterMs: parseRetryAfter(res.headers.get('retry-after')), body: text,
  });
}

// fetch() with a deadline on response headers (or on the whole body when `wholeBody`)
// that distinguishes our own timeout from the caller aborting.
// Returns { res, ac, done() } - call done() once the body has been consumed.
export async function fetchUpstream(url, init, { timeoutMs, signal }) {
  const ac = new AbortController();
  let timedOut = false;
  const onAbort = () => ac.abort(signal.reason);
  if (signal?.aborted) ac.abort(signal.reason);
  else signal?.addEventListener('abort', onAbort, { once: true });
  const timer = setTimeout(() => { timedOut = true; ac.abort(); }, timeoutMs);
  const done = () => { clearTimeout(timer); signal?.removeEventListener('abort', onAbort); };
  try {
    const res = await fetch(url, { ...init, signal: ac.signal });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      done();
      throw httpError(res, text);
    }
    return { res, ac, done, wasTimeout: () => timedOut };
  } catch (e) {
    done();
    if (e instanceof UpstreamError) throw e;
    if (timedOut) throw new UpstreamError(`upstream timed out after ${timeoutMs}ms`, { kind: 'timeout' });
    if (signal?.aborted) throw e; // the caller (client or router) aborted - not the provider's fault
    throw new UpstreamError(`network error: ${e.cause?.code ?? e.message}`, { kind: 'network' });
  }
}

// Non-streaming JSON call with a deadline covering headers AND body.
export async function postJson(url, headers, body, { timeoutMs, signal }) {
  const { res, done, wasTimeout } = await fetchUpstream(
    url, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) },
    { timeoutMs, signal },
  );
  try {
    return await res.json();
  } catch (e) {
    if (wasTimeout()) throw new UpstreamError(`upstream timed out after ${timeoutMs}ms`, { kind: 'timeout' });
    if (signal?.aborted) throw e;
    throw new UpstreamError(`invalid JSON from upstream: ${e.message}`, { kind: 'stream' });
  } finally {
    done();
  }
}
