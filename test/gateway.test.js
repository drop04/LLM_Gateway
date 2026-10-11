import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setup, readSSE, sleep } from './helpers.js';
import { toAnthropicRequest } from '../src/providers/anthropic.js';

const msg = (content = 'hi') => [{ role: 'user', content }];

async function withGw(overrides, fn) {
  const g = await setup(overrides);
  try { await fn(g); } finally { await g.close(); }
}

test('auth: missing, invalid, revoked keys and model ACL', () => withGw({}, async (g) => {
  assert.equal((await fetch(g.url + '/v1/models')).status, 401);
  assert.equal((await g.chat('sk-gw-nope', { model: 'chat', messages: msg() })).status, 401);
  const k = await g.newKey({ allowed_models: ['solo'] });
  assert.equal((await g.chat(k.key, { model: 'chat', messages: msg() })).status, 403);
  assert.equal((await g.chat(k.key, { model: 'nope', messages: msg() })).status, 404);
  await g.admin(`/admin/keys/${k.id}`, 'DELETE');
  assert.equal((await g.chat(k.key, { model: 'solo', messages: msg() })).status, 401);
}));

test('non-streaming proxy bills exact token cost', () => withGw({}, async (g) => {
  const k = await g.newKey();
  const r = await g.chat(k.key, { model: 'chat', messages: msg() });
  assert.equal(r.status, 200);
  assert.equal((await r.json()).choices[0].message.content, 'Hello from openai');
  assert.equal(r.headers.get('x-gateway-provider'), 'oa');
  assert.equal(r.headers.get('x-gateway-cost-usd'), '0.000050'); // 10*1 + 20*2 micro-USD
  const key = await g.admin(`/admin/keys/${k.id}`);
  assert.equal(key.spent_usd, 0.00005);
  assert.equal(key.reserved_usd, 0); // reservation fully released
}));

test('streaming is true passthrough: first token arrives long before the stream ends', () => withGw({}, async (g) => {
  g.oa.st.delay = 150; // 4 pieces => ~600ms upstream total
  const k = await g.newKey();
  const res = await g.chat(k.key, { model: 'solo', messages: msg(), stream: true });
  assert.equal(res.headers.get('content-type').startsWith('text/event-stream'), true);
  const s = await readSSE(res);
  assert.equal(s.text, 'Hello from openai');
  assert.ok(s.done);
  const first = s.objs.find((o) => o.choices?.[0]?.delta?.content);
  const last = s.objs.filter((o) => o.choices?.[0]?.delta?.content).at(-1);
  assert.ok(last.t - first.t >= 300, `chunks should be spread out (${first.t} -> ${last.t})`);
  // Gateway silently asked upstream for usage, but must not leak it to a client that didn't ask.
  assert.equal(g.oa.st.bodies[0].stream_options.include_usage, true);
  assert.ok(!s.objs.some((o) => o.usage), 'usage chunk must be stripped');
  const ev = (await g.events(k.id))[0];
  assert.equal(ev.cost_micro, 50);
  assert.equal(ev.status, 'ok');
  assert.equal(ev.estimated, 0);
}));

test('streaming usage is forwarded when the client asks for it', () => withGw({}, async (g) => {
  const k = await g.newKey();
  const s = await readSSE(await g.chat(k.key, { model: 'solo', messages: msg(), stream: true, stream_options: { include_usage: true } }));
  assert.equal(s.objs.find((o) => o.usage).usage.completion_tokens, 20);
}));

test('non-streaming fallback: provider 500 -> next provider, response translated to OpenAI shape', () => withGw({}, async (g) => {
  g.oa.st.mode = '500';
  const k = await g.newKey();
  const r = await g.chat(k.key, { model: 'chat', messages: [{ role: 'system', content: 'be brief' }, ...msg('yo')], temperature: 1.7 });
  const j = await r.json();
  assert.equal(r.status, 200);
  assert.equal(j.choices[0].message.content, 'Hello from claude');
  assert.equal(j.choices[0].finish_reason, 'stop');
  assert.equal(j.usage.total_tokens, 37);
  assert.equal(r.headers.get('x-gateway-provider'), 'an');
  assert.equal(r.headers.get('x-gateway-attempts'), '2');
  assert.equal(r.headers.get('x-gateway-cost-usd'), '0.000186'); // billed at the provider that actually served it
  const sent = g.an.st.bodies[0];
  assert.equal(sent.system, 'be brief');
  assert.equal(sent.max_tokens, 100);        // Anthropic requires it; filled from defaultMaxTokens
  assert.equal(sent.temperature, 1);          // clamped
  assert.deepEqual(sent.messages, [{ role: 'user', content: [{ type: 'text', text: 'yo' }] }]);
}));

test('streaming fallback before first token: Anthropic SSE is translated and billed correctly', () => withGw({}, async (g) => {
  g.oa.st.mode = '500';
  const k = await g.newKey();
  const res = await g.chat(k.key, { model: 'chat', messages: msg(), stream: true });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('x-gateway-attempts'), '2');
  const s = await readSSE(res);
  assert.equal(s.text, 'Hello from claude');
  assert.equal(s.objs.find((o) => o.choices?.[0]?.finish_reason).choices[0].finish_reason, 'stop');
  const ev = (await g.events(k.id))[0];
  assert.equal(ev.provider, 'an');
  assert.equal(ev.cost_micro, 186);
  assert.equal(JSON.parse(ev.attempts).length, 2);
}));

test('anthropic overloaded error event before any token also triggers fallback', () => withGw({
  models: { rev: { routes: [{ provider: 'an', model: 'claude-x' }, { provider: 'oa', model: 'gpt-x' }] } },
}, async (g) => {
  g.an.st.mode = 'errorevent';
  const k = await g.newKey();
  const s = await readSSE(await g.chat(k.key, { model: 'rev', messages: msg(), stream: true }));
  assert.equal(s.text, 'Hello from openai');
  assert.equal(g.oa.st.calls, 1);
}));

test('mid-stream failure: no fallback (output already sent), client gets in-band error, partial usage billed', () => withGw({}, async (g) => {
  g.oa.st.mode = 'midfail';
  g.oa.st.delay = 40; // let the first tokens actually reach the client before the connection dies
  const k = await g.newKey();
  const s = await readSSE(await g.chat(k.key, { model: 'chat', messages: msg(), stream: true }));
  assert.equal(s.text, 'Hello ');
  assert.equal(s.error.code, 'upstream_stream_error');
  assert.ok(s.done);
  assert.equal(g.an.st.calls, 0, 'must not retry on another provider after bytes were sent');
  const ev = (await g.events(k.id))[0];
  assert.equal(ev.status, 'stream_error');
  assert.equal(ev.estimated, 1);
  assert.ok(ev.cost_micro > 0);
  assert.equal((await g.admin(`/admin/keys/${k.id}`)).reserved_usd, 0);
}));

test('client errors (400) are surfaced, not retried on other providers', () => withGw({}, async (g) => {
  g.oa.st.mode = '400';
  const k = await g.newKey();
  const r = await g.chat(k.key, { model: 'chat', messages: msg() });
  assert.equal(r.status, 400);
  assert.equal(g.an.st.calls, 0);
  assert.equal((await g.admin(`/admin/keys/${k.id}`)).spent_usd, 0);
}));

test('all providers down -> 502, nothing charged, reservation released', () => withGw({}, async (g) => {
  g.oa.st.mode = '500'; g.an.st.mode = '529';
  const k = await g.newKey();
  const r = await g.chat(k.key, { model: 'chat', messages: msg() });
  assert.equal(r.status, 502);
  const key = await g.admin(`/admin/keys/${k.id}`);
  assert.equal(key.spent_usd, 0);
  assert.equal(key.reserved_usd, 0);
}));

test('budget: concurrent requests cannot overspend (reservations are atomic)', () => withGw({}, async (g) => {
  g.oa.st.delay = 120; // keep requests in flight simultaneously
  // reservation per request ~ 5 input + 50*2 = 105 micro; 260 micro of budget fits exactly two.
  const k = await g.newKey({ budget_usd: 0.00026 });
  const rs = await Promise.all(Array.from({ length: 6 }, () => g.chat(k.key, { model: 'solo', messages: msg(), max_tokens: 50 })));
  const statuses = rs.map((r) => r.status).sort();
  assert.deepEqual(statuses, [200, 200, 402, 402, 402, 402]);
  const key = await g.admin(`/admin/keys/${k.id}`);
  assert.equal(key.spent_usd, 0.0001);
  assert.equal(key.reserved_usd, 0);
  // Real usage was lower than the reservation, so the freed budget is usable again.
  assert.equal((await g.chat(k.key, { model: 'solo', messages: msg(), max_tokens: 50 })).status, 200);
}));

test('budget: exhausted key is rejected with 402 before any provider is called', () => withGw({}, async (g) => {
  const k = await g.newKey({ budget_usd: 0.00001 });
  const r = await g.chat(k.key, { model: 'chat', messages: msg() });
  assert.equal(r.status, 402);
  assert.equal((await r.json()).error.code, 'budget_exceeded');
  assert.equal(g.oa.st.calls, 0);
}));

test('circuit breaker skips a failing provider, then probes it after the cooldown', () => withGw({}, async (g) => {
  g.oa.st.mode = '500';
  const k = await g.newKey();
  // distinct prompts so the semantic cache doesn't answer for us
  for (let i = 0; i < 3; i++) assert.equal((await g.chat(k.key, { model: 'chat', messages: msg('q' + i + ' unrelated words ' + 'x'.repeat(i * 9)) })).status, 200);
  assert.equal(g.oa.st.calls, 3);
  const r = await g.chat(k.key, { model: 'chat', messages: msg('another distinct prompt entirely') }); // breaker open: oa not even tried
  assert.equal(g.oa.st.calls, 3);
  assert.equal(r.headers.get('x-gateway-provider'), 'an');
  assert.equal((await g.admin('/admin/status')).breakers['oa/gpt-x'].open, true);
  g.oa.st.mode = 'ok';
  await sleep(300);
  const r2 = await g.chat(k.key, { model: 'chat', messages: msg('probe request after cooldown') }); // half-open probe succeeds
  assert.equal(r2.headers.get('x-gateway-provider'), 'oa');
  assert.equal((await g.admin('/admin/status')).breakers['oa/gpt-x'].open, false);
}));

test('semantic cache: exact, near-duplicate, guarded numbers, scoping, streaming replay, bypass', () => withGw({}, async (g) => {
  const k = await g.newKey(), k2 = await g.newKey();
  const ask = (key, q, extra = {}, headers) => g.chat(key, { model: 'solo', temperature: 0, messages: msg(q), ...extra }, headers);

  let r = await ask(k.key, 'What is the capital of France?');
  assert.equal(r.headers.get('x-gateway-cache'), 'miss');
  await sleep(30);
  assert.equal(g.oa.st.calls, 1);

  r = await ask(k.key, 'what is the capital of  FRANCE?'); // normalises to the same text
  assert.equal(r.headers.get('x-gateway-cache'), 'hit');
  assert.equal((await r.json()).choices[0].message.content, 'Hello from openai');

  r = await ask(k.key, 'what is the capital of france please');
  assert.equal(r.headers.get('x-gateway-cache'), 'hit-semantic');
  assert.ok(Number(r.headers.get('x-gateway-cache-similarity')) >= 0.9);
  assert.equal(g.oa.st.calls, 1, 'no upstream calls for hits');

  r = await ask(k.key, 'What is the capital of Germany?');
  assert.equal(r.headers.get('x-gateway-cache'), 'miss');
  assert.equal(g.oa.st.calls, 2);

  // number guard: lexically near-identical but different figure must NOT hit
  await ask(k.key, 'What is the refund policy if I cancel within 30 days of purchase?'); await sleep(30);
  r = await ask(k.key, 'What is the refund policy if I cancel within 60 days of purchase?');
  assert.equal(r.headers.get('x-gateway-cache'), 'miss');
  // different sampling params => different partition
  r = await ask(k.key, 'What is the capital of France?', { max_tokens: 10 });
  assert.equal(r.headers.get('x-gateway-cache'), 'miss'); await sleep(30);
  // different key => never shares answers (scope: key)
  r = await ask(k2.key, 'What is the capital of France?');
  assert.equal(r.headers.get('x-gateway-cache'), 'miss');
  // bypass header
  const before = g.oa.st.calls;
  r = await ask(k.key, 'What is the capital of France?', {}, { 'x-gateway-cache': 'bypass' });
  assert.equal(g.oa.st.calls, before + 1);

  // streaming request served from a cached non-streaming answer
  const res = await ask(k.key, 'What is the capital of France?', { stream: true });
  assert.equal(res.headers.get('x-gateway-cache'), 'hit');
  assert.equal((await readSSE(res)).text, 'Hello from openai');

  // hits are free and tracked as savings
  const evs = (await g.events(k.id)).filter((e) => e.cache_hit);
  assert.ok(evs.length >= 3);
  assert.ok(evs.every((e) => e.cost_micro === 0 && e.saved_micro === 50));
}));

test('streamed answers populate the cache too', () => withGw({}, async (g) => {
  const k = await g.newKey();
  await readSSE(await g.chat(k.key, { model: 'solo', messages: msg('tell me a joke'), stream: true }));
  await sleep(30);
  const r = await g.chat(k.key, { model: 'solo', messages: msg('tell me a joke') });
  assert.equal(r.headers.get('x-gateway-cache'), 'hit');
  assert.equal((await r.json()).choices[0].message.content, 'Hello from openai');
}));

test('client disconnect mid-stream aborts upstream, bills an estimate, releases the reservation', () => withGw({}, async (g) => {
  g.oa.st.delay = 150;
  const k = await g.newKey();
  const ac = new AbortController();
  const res = await fetch(g.url + '/v1/chat/completions', { method: 'POST', signal: ac.signal,
    headers: { authorization: `Bearer ${k.key}`, 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'solo', messages: msg(), stream: true }) });
  const reader = res.body.getReader();
  await reader.read(); await reader.read();
  ac.abort();
  await sleep(500);
  assert.equal(g.oa.st.aborted, 1, 'upstream request must be cancelled');
  assert.equal(g.oa.st.sentAll, 0);
  const ev = (await g.events(k.id))[0];
  assert.equal(ev.status, 'client_abort');
  assert.ok(ev.cost_micro > 0);
  assert.equal((await g.admin(`/admin/keys/${k.id}`)).reserved_usd, 0);
}));

test('rate limit: rpm per key', () => withGw({}, async (g) => {
  const k = await g.newKey({ rpm: 2 });
  assert.equal((await g.chat(k.key, { model: 'solo', messages: msg('a') })).status, 200);
  assert.equal((await g.chat(k.key, { model: 'solo', messages: msg('b') })).status, 200);
  const r = await g.chat(k.key, { model: 'solo', messages: msg('c') });
  assert.equal(r.status, 429);
  assert.ok(Number(r.headers.get('retry-after')) >= 1);
}));

test('admin: usage report aggregates cost by model and key', () => withGw({}, async (g) => {
  const k = await g.newKey();
  await g.chat(k.key, { model: 'solo', messages: msg('a') });
  g.oa.st.mode = '500';
  await g.chat(k.key, { model: 'chat', messages: msg('b') });
  const u = await g.admin(`/admin/usage?key_id=${k.id}`);
  assert.equal(u.totals.requests, 2);
  assert.equal(u.totals.cost_micro, 50 + 186);
  assert.deepEqual(u.by_model.map((m) => m.name).sort(), ['an/claude-x', 'oa/gpt-x']);
}));

test('OpenAI -> Anthropic request translation: tools, tool results, images, turn merging', () => {
  const body = toAnthropicRequest({
    messages: [
      { role: 'system', content: 'sys' },
      { role: 'user', content: [{ type: 'text', text: 'weather?' }, { type: 'image_url', image_url: { url: 'data:image/png;base64,AAAA' } }] },
      { role: 'assistant', content: null, tool_calls: [{ id: 't1', type: 'function', function: { name: 'get_weather', arguments: '{"city":"Paris"}' } }] },
      { role: 'tool', tool_call_id: 't1', content: '18C' },
      { role: 'user', content: 'thanks' },
    ],
    tools: [{ type: 'function', function: { name: 'get_weather', description: 'd', parameters: { type: 'object', properties: { city: { type: 'string' } } } } }],
    tool_choice: 'required', stop: 'END', max_completion_tokens: 77,
  }, 'claude-x', false, 4096);
  assert.equal(body.system, 'sys');
  assert.equal(body.max_tokens, 77);
  assert.deepEqual(body.stop_sequences, ['END']);
  assert.deepEqual(body.tool_choice, { type: 'any' });
  assert.equal(body.tools[0].input_schema.properties.city.type, 'string');
  assert.deepEqual(body.messages[0].content[1], { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AAAA' } });
  assert.deepEqual(body.messages[1].content[0], { type: 'tool_use', id: 't1', name: 'get_weather', input: { city: 'Paris' } });
  // tool result + following user text merge into a single user turn (strict alternation)
  assert.equal(body.messages.length, 3);
  assert.equal(body.messages[2].role, 'user');
  assert.equal(body.messages[2].content[0].type, 'tool_result');
  assert.equal(body.messages[2].content[1].text, 'thanks');
});

test('cache and ledger persist across a gateway restart', async () => {
  const { mkdtempSync } = await import('node:fs');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  const dbPath = join(mkdtempSync(join(tmpdir(), 'gw-')), 'gw.db');
  let keyPlain, keyId;
  await withGw({ dbPath }, async (g) => {
    const k = await g.newKey(); keyPlain = k.key; keyId = k.id;
    await g.chat(k.key, { model: 'solo', messages: msg('persist me please') });
    await sleep(30);
  });
  await withGw({ dbPath }, async (g) => {
    const r = await g.chat(keyPlain, { model: 'solo', messages: msg('persist me please') });
    assert.equal(r.headers.get('x-gateway-cache'), 'hit');
    assert.equal(g.oa.st.calls, 0);
    assert.equal((await g.admin(`/admin/keys/${keyId}`)).spent_usd, 0.00005);
  });
});
