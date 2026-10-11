import { UpstreamError } from './errors.js';

// A chunk is "meaningful" once the model has actually started producing output.
// Role-only preamble chunks are held back so we can still fail over cleanly.
export function isMeaningful(chunk) {
  const c = chunk.choices?.[0];
  if (!c) return !!chunk.usage;
  if (c.finish_reason) return true;
  return Object.entries(c.delta ?? {}).some(([k, v]) => k !== 'role' && v != null && v !== '' && !(Array.isArray(v) && !v.length));
}

export class AllRoutesFailed extends Error {
  constructor(attempts, lastError, allSkipped) {
    super(allSkipped ? 'all providers are temporarily unavailable (circuit open)' : `all providers failed; last error: ${lastError?.message}`);
    this.attempts = attempts;
    this.lastError = lastError;
    this.allSkipped = allSkipped;
  }
}

export class Router {
  constructor({ config, providers, breaker }) {
    this.config = config;
    this.providers = providers;
    this.breaker = breaker;
  }

  routesFor(alias) {
    return this.config.models[alias]?.routes;
  }

  // Try routes in priority order until one succeeds. `attempt(route)` does the actual call.
  // Fallback is only possible because nothing has been sent to the client yet.
  async run(routes, attempt, { signal } = {}) {
    const attempts = [];
    let lastError = null;
    for (const route of routes) {
      if (!this.breaker.canAttempt(route.key)) {
        attempts.push({ route: route.key, skipped: 'circuit_open' });
        continue;
      }
      const t0 = Date.now();
      try {
        const result = await attempt(route);
        this.breaker.success(route.key);
        attempts.push({ route: route.key, ok: true, ms: Date.now() - t0 });
        return { result, route, attempts };
      } catch (e) {
        if (signal?.aborted) throw e;               // client left - not the provider's fault
        if (!(e instanceof UpstreamError)) throw e; // bug / GatewayError: don't mask it
        attempts.push({ route: route.key, error: e.message, status: e.status || undefined, ms: Date.now() - t0 });
        e.attempts = attempts;
        if (!e.fallbackable) throw e;               // 400/422: same request would fail everywhere
        if (e.kind === 'unsupported') { /* route can't express this request; no health signal */ }
        else if (e.status === 429) this.breaker.coolDown(route.key, e.retryAfterMs ?? 5_000);
        else this.breaker.failure(route.key);
        lastError = e;
      }
    }
    throw new AllRoutesFailed(attempts, lastError, attempts.every((a) => a.skipped));
  }

  async complete(route, body, signal) {
    return this.providers[route.provider].complete(body, route.model, { signal });
  }

  // Open a stream and read until the first *meaningful* chunk. Until that point the
  // client has received nothing, so any failure (connect error, 5xx, overloaded event,
  // first-token timeout) can transparently fail over to the next route.
  async openStream(route, body, clientSignal) {
    const ac = new AbortController();
    const onAbort = () => ac.abort(clientSignal.reason);
    if (clientSignal.aborted) ac.abort(); else clientSignal.addEventListener('abort', onAbort, { once: true });
    const dispose = () => clientSignal.removeEventListener('abort', onAbort);
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; ac.abort(); }, this.config.timeouts.firstChunkMs);
    const head = [];
    try {
      const s = await this.providers[route.provider].stream(body, route.model, { signal: ac.signal });
      while (true) {
        const { value, done } = await s.iterator.next();
        if (done) {
          if (!head.length) throw new UpstreamError('upstream returned an empty stream', { kind: 'stream' });
          break;
        }
        head.push(value);
        if (isMeaningful(value)) break;
      }
      clearTimeout(timer);
      return { iterator: s.iterator, head, partialUsage: s.partialUsage, abort: () => ac.abort(), dispose };
    } catch (e) {
      clearTimeout(timer);
      dispose();
      ac.abort();
      if (clientSignal.aborted) throw e;
      if (timedOut) throw new UpstreamError(`no first token within ${this.config.timeouts.firstChunkMs}ms`, { kind: 'timeout' });
      throw e;
    }
  }
}
