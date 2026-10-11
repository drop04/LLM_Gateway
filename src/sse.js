import { UpstreamError } from './errors.js';

function parseFrame(frame) {
  let event = null;
  const data = [];
  for (const line of frame.split('\n')) {
    if (!line || line.startsWith(':')) continue; // blank or comment/keep-alive
    const i = line.indexOf(':');
    const field = i === -1 ? line : line.slice(0, i);
    let value = i === -1 ? '' : line.slice(i + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'event') event = value;
    else if (field === 'data') data.push(value);
  }
  return data.length || event ? { event, data: data.join('\n') } : null;
}

function readWithIdleTimeout(reader, ms, onTimeout) {
  if (!ms) return reader.read();
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => {
      onTimeout?.();
      reject(new UpstreamError(`upstream stream stalled for ${ms}ms`, { kind: 'timeout' }));
    }, ms);
    reader.read().then(
      (v) => { clearTimeout(t); resolve(v); },
      (e) => { clearTimeout(t); reject(e); },
    );
  });
}

// Incremental SSE parser over a fetch() body. Yields { event, data } as soon as a
// full frame arrives - nothing is buffered beyond the current partial frame.
export async function* sseEvents(body, { idleMs = 0, onTimeout, signal } = {}) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  try {
    while (true) {
      let r;
      try {
        r = await readWithIdleTimeout(reader, idleMs, onTimeout);
      } catch (e) {
        if (e instanceof UpstreamError || signal?.aborted) throw e;
        throw new UpstreamError(`stream read failed: ${e.cause?.code ?? e.message}`, { kind: 'network' });
      }
      if (r.done) break;
      buf = (buf + decoder.decode(r.value, { stream: true })).replace(/\r\n/g, '\n');
      let idx;
      while ((idx = buf.indexOf('\n\n')) !== -1) {
        const ev = parseFrame(buf.slice(0, idx));
        buf = buf.slice(idx + 2);
        if (ev) yield ev;
      }
    }
    buf += decoder.decode();
    if (buf.trim()) {
      const ev = parseFrame(buf);
      if (ev) yield ev;
    }
  } finally {
    // Closing the reader tears down the upstream connection if the consumer bails early.
    reader.cancel().catch(() => {});
  }
}
