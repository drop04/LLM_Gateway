import http from 'node:http';
import crypto from 'node:crypto';
import { Store } from './db.js';
import { Router, AllRoutesFailed } from './router.js';
import { Breaker } from './breaker.js';
import { SemanticCache } from './cache.js';
import { createProviders } from './providers/index.js';
import { priceFor, costMicro, normalizeUsage } from './pricing.js';
import { GatewayError, UpstreamError, ClientAbort } from './errors.js';
import { rand, sha256, log, estimateTokens, estimateInputTokens, microToUsd, usdToMicro } from './util.js';

const SSE_HEADERS = {
  'content-type': 'text/event-stream; charset=utf-8',
  'cache-control': 'no-cache, no-transform',
  connection: 'keep-alive',
  'x-accel-buffering': 'no', // tell nginx not to buffer
};

class RateLimiter {
  constructor() { this.hits = new Map(); }
  check(key) {
    if (!key.rpm) return { ok: true };
    const now = Date.now();
    const arr = (this.hits.get(key.id) ?? []).filter((t) => now - t < 60_000);
    if (arr.length >= key.rpm) {
      this.hits.set(key.id, arr);
      return { ok: false, retryAfter: Math.max(1, Math.ceil((arr[0] + 60_000 - now) / 1000)) };
    }
    arr.push(now);
    this.hits.set(key.id, arr);
    return { ok: true };
  }
}

async function readJson(req, limit) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > limit) throw new GatewayError(413, 'body_too_large', `request body exceeds ${limit} bytes`);
    chunks.push(c);
  }
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'); }
  catch { throw new GatewayError(400, 'invalid_json', 'request body is not valid JSON'); }
}

const sendJson = (res, status, body, headers = {}) => {
  const data = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data), ...headers });
  res.end(data);
};

// Write respecting backpressure so a slow client can't make us buffer unboundedly.
async function write(res, str) {
  if (res.destroyed || res.writableEnded) throw new ClientAbort();
  if (res.write(str)) return;
  await new Promise((resolve, reject) => {
    const cleanup = () => { res.off('drain', onDrain); res.off('close', onClose); };
    const onDrain = () => { cleanup(); resolve(); };
    const onClose = () => { cleanup(); reject(new ClientAbort()); };
    res.once('drain', onDrain);
    res.once('close', onClose);
  });
}

function validateChat(b) {
  if (typeof b.model !== 'string' || !b.model) throw new GatewayError(400, 'invalid_request', '"model" is required');
  if (!Array.isArray(b.messages) || !b.messages.length) throw new GatewayError(400, 'invalid_request', '"messages" must be a non-empty array');
  for (const m of b.messages) {
    if (!m || typeof m.role !== 'string') throw new GatewayError(400, 'invalid_request', 'every message needs a "role"');
  }
}

export function createGateway(config) {
  const store = new Store(config.dbPath);
  const providers = createProviders(config);
  const breaker = new Breaker(config.breaker);
  const router = new Router({ config, providers, breaker });
  const cache = new SemanticCache({ config: config.cache, store, providers });
  const limiter = new RateLimiter();
  const counters = { requests: 0, cache_hits: 0, fallbacks: 0, upstream_errors: 0 };

  // ---------------------------------------------------------------- auth
  function authenticate(req) {
    const h = req.headers.authorization ?? '';
    const token = h.startsWith('Bearer ') ? h.slice(7).trim() : req.headers['x-api-key'];
    if (!token) throw new GatewayError(401, 'missing_api_key', 'Provide an API key as "Authorization: Bearer <key>"');
    const key = store.findKey(token);
    if (!key || key.revoked) throw new GatewayError(401, 'invalid_api_key', 'Invalid or revoked API key');
    return key;
  }
  function requireAdmin(req) {
    const given = sha256((req.headers.authorization ?? '').replace(/^Bearer /, ''));
    if (!config.adminKey || !crypto.timingSafeEqual(Buffer.from(given), Buffer.from(sha256(config.adminKey)))) {
      throw new GatewayError(401, 'admin_auth_failed', 'admin key required');
    }
  }

  // ---------------------------------------------------------------- chat
  async function chat(req, res) {
    const requestId = 'req_' + rand(9);
    const t0 = Date.now();
    counters.requests++;
    const key = authenticate(req);
    const rl = limiter.check(key);
    if (!rl.ok) throw new GatewayError(429, 'rate_limited', `rate limit of ${key.rpm} requests/min exceeded`, { headers: { 'retry-after': String(rl.retryAfter) }, type: 'rate_limit_error' });

    const body = await readJson(req, config.maxBodyBytes);
    validateChat(body);
    const alias = body.model;
    const routes = router.routesFor(alias);
    if (!routes) throw new GatewayError(404, 'model_not_found', `unknown model "${alias}"`);
    if (key.allowed_models && !key.allowed_models.includes(alias)) throw new GatewayError(403, 'model_not_allowed', `this key may not use "${alias}"`);

    const stream = body.stream === true;
    const wantsUsage = !!body.stream_options?.include_usage;
    const cacheMode = String(req.headers['x-gateway-cache'] ?? '').toLowerCase(); // bypass | refresh

    const clientAc = new AbortController();
    res.on('close', () => { if (!res.writableFinished) clientAc.abort(); });

    const base = { requestId, alias, stream };
    const baseHeaders = { 'x-gateway-request-id': requestId };

    // ---- 1. semantic cache -------------------------------------------------
    let cacheCtx = null;
    if (cache.enabled && cacheMode !== 'bypass') {
      cacheCtx = cache.prepare(body, key);
      if (cacheCtx && cacheMode !== 'refresh') {
        const hit = await cache.lookup(cacheCtx);
        if (hit) {
          counters.cache_hits++;
          const { entry } = hit;
          const route = routes.find((r) => r.key === entry.routeKey) ?? routes[0];
          const saved = costMicro(priceFor(config, route) ?? priceFor(config, { key: entry.routeKey, model: '' }), entry.usage);
          store.settle(key.id, 0, 0, { ...base, provider: route.provider, model: route.model, status: 'ok', httpStatus: 200,
            usage: entry.usage, savedMicro: saved, cacheHit: true, latencyMs: Date.now() - t0, ttfbMs: Date.now() - t0 });
          const headers = { ...baseHeaders, 'x-gateway-cache': hit.kind === 'exact' ? 'hit' : 'hit-semantic',
            'x-gateway-cache-similarity': hit.similarity.toFixed(4) };
          return replayCached(res, entry.response, { stream, wantsUsage, headers });
        }
      }
    }

    // ---- 2. budget reservation (worst case across candidate routes) ------------
    const inputEst = estimateInputTokens(body);
    const maxOut = body.max_completion_tokens ?? body.max_tokens ?? config.defaultMaxTokens;
    const reserveMicro = Math.max(...routes.map((r) => costMicro(priceFor(config, r), { input: inputEst, output: maxOut, cacheRead: 0, cacheWrite: 0 })));
    const r = store.reserve(key.id, reserveMicro);
    if (!r.ok) {
      throw new GatewayError(402, 'budget_exceeded',
        `budget exceeded: this request may cost up to $${microToUsd(reserveMicro).toFixed(6)} but only $${microToUsd(r.remainingMicro).toFixed(6)} remains` +
        ' (lower max_tokens or raise the key budget)', { type: 'insufficient_quota' });
    }

    // Everything below must settle the reservation exactly once.
    let settled = false;
    const finish = ({ route, usage, estimated = false, status, httpStatus, error, attempts, ttfbMs }) => {
      if (settled) return 0;
      settled = true;
      const cost = route && usage ? costMicro(priceFor(config, route), usage) : 0;
      store.settle(key.id, reserveMicro, cost, { ...base, provider: route?.provider, model: route?.model, status, httpStatus, usage,
        estimated, error, attempts, latencyMs: Date.now() - t0, ttfbMs });
      return cost;
    };

    try {
      if (!stream) {
        const { result, route, attempts } = await router.run(routes, (rt) => router.complete(rt, body, clientAc.signal), { signal: clientAc.signal });
        let usage = result.usage, estimated = false;
        if (!usage) { // provider didn't report usage: bill an estimate, flag it
          usage = { input: inputEst, output: estimateTokens(result.response.choices?.[0]?.message?.content ?? ''), cacheRead: 0, cacheWrite: 0 };
          estimated = true;
        }
        const cost = finish({ route, usage, estimated, status: 'ok', httpStatus: 200, attempts, ttfbMs: Date.now() - t0 });
        if (attempts.length > 1) counters.fallbacks++;
        maybeStore(cacheCtx, result.response, usage, route);
        return sendJson(res, 200, result.response, {
          ...baseHeaders, 'x-gateway-provider': route.provider, 'x-gateway-model': route.model,
          'x-gateway-attempts': String(attempts.length), 'x-gateway-cost-usd': microToUsd(cost).toFixed(6),
          'x-gateway-cache': cacheCtx ? 'miss' : 'bypass',
        });
      }

      // ---- streaming ---------------------------------------------------------
      const { result: s, route, attempts } = await router.run(routes, (rt) => router.openStream(rt, body, clientAc.signal), { signal: clientAc.signal });
      if (attempts.length > 1) counters.fallbacks++;
      res.writeHead(200, { ...SSE_HEADERS, ...baseHeaders, 'x-gateway-provider': route.provider, 'x-gateway-model': route.model,
        'x-gateway-attempts': String(attempts.length), 'x-gateway-cache': cacheCtx ? 'miss' : 'bypass' });
      res.socket?.setNoDelay(true);
      res.flushHeaders();

      let usage = null, text = '', finishReason = null, hasTool = false, completed = false, error = null, firstId = null, firstModel = null;
      const ttfbMs = Date.now() - t0;
      try {
        const emit = async (chunk) => {
          firstId ??= chunk.id; firstModel ??= chunk.model;
          if (chunk.usage) usage = normalizeUsage(chunk.usage);
          const c = chunk.choices?.[0];
          if (c) {
            text += c.delta?.content ?? '';
            if (c.delta?.tool_calls) hasTool = true;
            if (c.finish_reason) finishReason = c.finish_reason;
          }
          if (chunk.usage && !chunk.choices?.length && !wantsUsage) return; // we asked for it, client didn't
          await write(res, `data: ${JSON.stringify(chunk)}\n\n`);
        };
        for (const c of s.head) await emit(c);
        for await (const c of s.iterator) await emit(c);
        await write(res, 'data: [DONE]\n\n');
        completed = true;
      } catch (e) {
        error = e;
      } finally {
        s.dispose();
        s.abort(); // make sure the upstream socket is closed whatever happened
      }

      // Usage fallbacks: provider told us -> exact; else partial (Anthropic input) + estimate of what we streamed.
      let estimated = false;
      if (!usage) {
        const p = s.partialUsage();
        usage = { input: p?.input ?? inputEst, output: Math.max(p?.output ?? 0, estimateTokens(text)), cacheRead: p?.cacheRead ?? 0, cacheWrite: p?.cacheWrite ?? 0 };
        estimated = true;
      }
      const clientGone = error instanceof ClientAbort || clientAc.signal.aborted;
      const status = completed ? 'ok' : clientGone ? 'client_abort' : 'stream_error';
      finish({ route, usage, estimated, status, httpStatus: 200, attempts, ttfbMs, error: error && !clientGone ? error.message : undefined });
      if (completed) {
        if (finishReason === 'stop' && !hasTool) {
          maybeStore(cacheCtx, { id: firstId, object: 'chat.completion', created: Math.floor(Date.now() / 1000), model: firstModel,
            choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }],
            usage: { prompt_tokens: usage.input + usage.cacheRead + usage.cacheWrite, completion_tokens: usage.output,
              total_tokens: usage.input + usage.cacheRead + usage.cacheWrite + usage.output } }, usage, route);
        }
        return res.end();
      }
      if (!clientGone) {
        // Headers are already sent, so we can't change the status. Tell the client in-band.
        counters.upstream_errors++;
        log('warn', 'stream failed mid-flight', { requestId, route: route.key, error: error.message });
        res.write(`data: ${JSON.stringify({ error: { message: `upstream stream failed: ${error.message}`, type: 'api_error', code: 'upstream_stream_error' } })}\n\ndata: [DONE]\n\n`);
      }
      return res.end();
    } catch (e) {
      // Failure before anything was sent: settle (no charge) and let the top-level handler reply.
      const attempts = e.attempts;
      if (e instanceof AllRoutesFailed) {
        counters.upstream_errors++;
        finish({ status: 'error', httpStatus: e.allSkipped ? 503 : 502, error: e.message, attempts });
        throw new GatewayError(e.allSkipped ? 503 : 502, e.allSkipped ? 'providers_unavailable' : 'upstream_failed', e.message,
          { headers: { 'x-gateway-attempts': String(attempts.length) } });
      }
      if (e instanceof UpstreamError) { // non-fallbackable (client error): pass it through
        finish({ status: 'error', httpStatus: e.status, error: e.message, attempts });
        throw new GatewayError(e.status === 429 ? 429 : e.status || 400, 'upstream_rejected', e.message);
      }
      finish({ status: clientAc.signal.aborted ? 'client_abort' : 'error', httpStatus: e.status ?? 500, error: e.message });
      throw e;
    }
  }

  function maybeStore(ctx, response, usage, route) {
    if (!ctx || response.choices?.[0]?.finish_reason !== 'stop' || response.choices[0].message?.tool_calls) return;
    cache.store_(ctx, { response, usage, routeKey: route.key }).catch((e) => log('warn', 'cache store failed', { error: e.message }));
  }

  // Serve a cache hit as JSON or as a synthetic SSE stream.
  async function replayCached(res, cached, { stream, wantsUsage, headers }) {
    const id = 'chatcmpl-' + rand(), created = Math.floor(Date.now() / 1000);
    const content = cached.choices[0].message.content ?? '';
    if (!stream) return sendJson(res, 200, { ...cached, id, created }, headers);
    res.writeHead(200, { ...SSE_HEADERS, ...headers });
    const mk = (delta, finish = null) => ({ id, object: 'chat.completion.chunk', created, model: cached.model, choices: [{ index: 0, delta, finish_reason: finish }] });
    try {
      await write(res, `data: ${JSON.stringify(mk({ role: 'assistant', content: '' }))}\n\n`);
      for (let i = 0; i < content.length; i += 48) await write(res, `data: ${JSON.stringify(mk({ content: content.slice(i, i + 48) }))}\n\n`);
      await write(res, `data: ${JSON.stringify(mk({}, 'stop'))}\n\n`);
      if (wantsUsage) await write(res, `data: ${JSON.stringify({ id, object: 'chat.completion.chunk', created, model: cached.model, choices: [], usage: cached.usage })}\n\n`);
      await write(res, 'data: [DONE]\n\n');
    } catch (e) { if (!(e instanceof ClientAbort)) throw e; }
    res.end();
  }

  // ---------------------------------------------------------------- admin
  const keyView = (k) => ({
    id: k.id, name: k.name, key_prefix: k.key_prefix, revoked: !!k.revoked, period: k.period, rpm: k.rpm, allowed_models: k.allowed_models,
    budget_usd: k.budget_micro == null ? null : microToUsd(k.budget_micro),
    spent_usd: microToUsd(k.spent_micro), reserved_usd: microToUsd(k.reserved_micro),
    remaining_usd: k.budget_micro == null ? null : microToUsd(k.budget_micro - k.spent_micro - k.reserved_micro),
  });
  const patchOf = (b) => {
    const p = {};
    if ('name' in b) p.name = b.name;
    if ('budget_usd' in b) p.budgetMicro = b.budget_usd == null ? null : usdToMicro(b.budget_usd);
    if ('rpm' in b) p.rpm = b.rpm;
    if ('allowed_models' in b) p.allowedModels = b.allowed_models;
    if ('revoked' in b) p.revoked = b.revoked;
    if ('period' in b) {
      if (!['none', 'daily', 'monthly'].includes(b.period)) throw new GatewayError(400, 'invalid_request', 'period must be none|daily|monthly');
      p.period = b.period;
    }
    return p;
  };
  async function admin(req, res, url) {
    requireAdmin(req);
    const p = url.pathname.replace(/\/$/, '');
    if (p === '/admin/keys' && req.method === 'POST') {
      const b = await readJson(req, 1 << 20);
      if (!b.name) throw new GatewayError(400, 'invalid_request', '"name" is required');
      const { key, plaintext } = store.createKey({ name: b.name, budgetMicro: b.budget_usd == null ? null : usdToMicro(b.budget_usd),
        period: b.period ?? 'none', rpm: b.rpm ?? null, allowedModels: b.allowed_models ?? null });
      return sendJson(res, 201, { ...keyView(key), key: plaintext, note: 'store this key now; it cannot be shown again' });
    }
    if (p === '/admin/keys' && req.method === 'GET') return sendJson(res, 200, { data: store.listKeys().map(keyView) });
    const m = /^\/admin\/keys\/([\w-]+)$/.exec(p);
    if (m && ['PATCH', 'DELETE', 'GET'].includes(req.method)) {
      if (!store.getKey(m[1])) throw new GatewayError(404, 'not_found', 'no such key');
      if (req.method === 'GET') return sendJson(res, 200, keyView(store.getKey(m[1])));
      const patch = req.method === 'DELETE' ? { revoked: true } : patchOf(await readJson(req, 1 << 20));
      return sendJson(res, 200, keyView(store.updateKey(m[1], patch)));
    }
    if (p === '/admin/usage' && req.method === 'GET') {
      const since = url.searchParams.get('since');
      const s = store.usageSummary({ keyId: url.searchParams.get('key_id') ?? undefined, since: since ? (Number(since) || Date.parse(since)) : undefined });
      const usd = (r) => ({ ...r, cost_usd: microToUsd(r.cost_micro ?? 0), saved_usd: microToUsd(r.saved_micro ?? 0) });
      return sendJson(res, 200, { totals: usd(s.totals), by_model: s.by_model.map(usd), by_key: s.by_key.map(usd) });
    }
    if (p === '/admin/events' && req.method === 'GET') {
      return sendJson(res, 200, { data: store.recentEvents({ keyId: url.searchParams.get('key_id') ?? undefined, limit: Math.min(500, Number(url.searchParams.get('limit')) || 50) }) });
    }
    if (p === '/admin/status') return sendJson(res, 200, { counters, breakers: breaker.snapshot(), cache: cache.summary() });
    throw new GatewayError(404, 'not_found', 'unknown admin route');
  }

  // ---------------------------------------------------------------- routing
  async function handle(req, res) {
    const url = new URL(req.url, 'http://gateway');
    if (url.pathname === '/health') return sendJson(res, 200, { ok: true });
    if (url.pathname.startsWith('/admin/')) return admin(req, res, url);
    if (req.method === 'POST' && url.pathname === '/v1/chat/completions') return chat(req, res);
    if (req.method === 'GET' && url.pathname === '/v1/models') {
      const key = authenticate(req);
      const ids = Object.keys(config.models).filter((m) => !key.allowed_models || key.allowed_models.includes(m));
      return sendJson(res, 200, { object: 'list', data: ids.map((id) => ({ id, object: 'model', owned_by: 'gateway' })) });
    }
    if (req.method === 'GET' && url.pathname === '/v1/key') return sendJson(res, 200, keyView(authenticate(req)));
    throw new GatewayError(404, 'not_found', `no route for ${req.method} ${url.pathname}`);
  }

  const server = http.createServer((req, res) => {
    handle(req, res).catch((e) => {
      if (res.headersSent) { res.destroy(); return; }
      if (e instanceof GatewayError) return sendJson(res, e.status, e.body(), e.headers);
      log('error', 'unhandled', { error: e.stack });
      sendJson(res, 500, { error: { message: 'internal gateway error', type: 'api_error', code: 'internal_error' } });
    });
  });
  server.keepAliveTimeout = 65_000;

  return { server, store, cache, breaker, close: () => new Promise((r) => server.close(() => r())) };
}
