import http from 'node:http';
import { finalizeConfig } from '../src/config.js';
import { createGateway } from '../src/server.js';

process.env.GATEWAY_LOG = 'silent';
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const readBody = async (req) => { let s = ''; for await (const c of req) s += c; return s ? JSON.parse(s) : {}; };
const listen = (server) => new Promise((r) => server.listen(0, '127.0.0.1', () => r(server.address().port)));

// Fake OpenAI-compatible upstream. state.mode: ok | 500 | 400 | midfail
export async function mockOpenAI() {
  const st = { mode: 'ok', calls: 0, bodies: [], delay: 0, pieces: ['Hel', 'lo ', 'from ', 'openai'], aborted: 0, sentAll: 0 };
  const server = http.createServer(async (req, res) => {
    const body = await readBody(req);
    st.calls++; st.bodies.push(body);
    if (st.mode === '500') { res.writeHead(500, { 'content-type': 'application/json' }); return res.end('{"error":{"message":"boom"}}'); }
    if (st.mode === '400') { res.writeHead(400, { 'content-type': 'application/json' }); return res.end('{"error":{"message":"bad param"}}'); }
    if (!body.stream) {
      if (st.delay) await sleep(st.delay);
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ id: 'cmpl-1', object: 'chat.completion', model: body.model,
        choices: [{ index: 0, message: { role: 'assistant', content: st.pieces.join('') }, finish_reason: 'stop' }],
        usage: { prompt_tokens: 10, completion_tokens: 20 } }));
    }
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.on('close', () => { if (!res.writableFinished) st.aborted++; });
    const send = (o) => res.write(`data: ${JSON.stringify(o)}\n\n`);
    const mk = (delta, fr = null) => ({ id: 'cmpl-1', object: 'chat.completion.chunk', model: body.model, choices: [{ index: 0, delta, finish_reason: fr }] });
    send(mk({ role: 'assistant', content: '' }));
    for (let i = 0; i < st.pieces.length; i++) {
      if (st.delay) await sleep(st.delay);
      if (st.mode === 'midfail' && i === 2) return res.destroy();
      if (res.destroyed) return;
      send(mk({ content: st.pieces[i] }));
    }
    send(mk({}, 'stop'));
    if (body.stream_options?.include_usage) send({ id: 'cmpl-1', object: 'chat.completion.chunk', model: body.model, choices: [], usage: { prompt_tokens: 10, completion_tokens: 20 } });
    res.write('data: [DONE]\n\n');
    st.sentAll++;
    res.end();
  });
  const port = await listen(server);
  return { st, port, close: () => new Promise((r) => { server.closeAllConnections(); server.close(r); }) };
}

// Fake Anthropic upstream. state.mode: ok | 529 | errorevent
export async function mockAnthropic() {
  const st = { mode: 'ok', calls: 0, bodies: [], text: ['Hello ', 'from ', 'claude'] };
  const server = http.createServer(async (req, res) => {
    const body = await readBody(req);
    st.calls++; st.bodies.push(body);
    if (st.mode === '529') { res.writeHead(529, { 'content-type': 'application/json' }); return res.end('{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}'); }
    if (!body.stream) {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ id: 'msg_1', type: 'message', role: 'assistant', model: body.model, stop_reason: 'end_turn',
        content: [{ type: 'text', text: st.text.join('') }], usage: { input_tokens: 12, output_tokens: 25 } }));
    }
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    const ev = (type, o) => res.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...o })}\n\n`);
    ev('message_start', { message: { id: 'msg_1', model: body.model, usage: { input_tokens: 12, output_tokens: 1 } } });
    if (st.mode === 'errorevent') { ev('error', { error: { type: 'overloaded_error', message: 'Overloaded' } }); return res.end(); }
    res.write('event: ping\ndata: {"type":"ping"}\n\n');
    ev('content_block_start', { index: 0, content_block: { type: 'text', text: '' } });
    for (const t of st.text) ev('content_block_delta', { index: 0, delta: { type: 'text_delta', text: t } });
    ev('content_block_stop', { index: 0 });
    ev('message_delta', { delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 25 } });
    ev('message_stop', {});
    res.end();
  });
  const port = await listen(server);
  return { st, port, close: () => new Promise((r) => { server.closeAllConnections(); server.close(r); }) };
}

// Boots mocks + a gateway on an ephemeral port.
// Pricing (USD/1M): oa input 1 output 2 -> a 10-in/20-out call costs 50 micro-USD.
//                   an input 3 output 6 -> a 12-in/25-out call costs 186 micro-USD.
export async function setup(overrides = {}) {
  const oa = await mockOpenAI();
  const an = await mockAnthropic();
  const config = finalizeConfig({
    dbPath: ':memory:', adminKey: 'admin-secret', defaultMaxTokens: 100,
    providers: {
      oa: { type: 'openai', baseUrl: `http://127.0.0.1:${oa.port}/v1`, apiKey: 'k1' },
      an: { type: 'anthropic', baseUrl: `http://127.0.0.1:${an.port}`, apiKey: 'k2' },
    },
    models: {
      chat: { routes: [{ provider: 'oa', model: 'gpt-x' }, { provider: 'an', model: 'claude-x' }] },
      solo: { routes: [{ provider: 'oa', model: 'gpt-x' }] },
    },
    pricing: { 'oa/gpt-x': { input: 1, output: 2 }, 'an/claude-x': { input: 3, output: 6 } },
    breaker: { threshold: 3, cooldownMs: 250 },
    timeouts: { headersMs: 1500, requestMs: 3000, firstChunkMs: 1500, idleMs: 1500 },
    cache: { enabled: true, threshold: 0.9 },
    ...overrides,
  });
  const gw = createGateway(config);
  await new Promise((r) => gw.server.listen(0, '127.0.0.1', r));
  const url = `http://127.0.0.1:${gw.server.address().port}`;

  const admin = async (path, method = 'GET', body) => {
    const r = await fetch(url + path, { method, headers: { authorization: 'Bearer admin-secret', 'content-type': 'application/json' }, body: body && JSON.stringify(body) });
    return r.json();
  };
  const newKey = async (o = {}) => (await admin('/admin/keys', 'POST', { name: 't', budget_usd: 1, ...o }));
  const chat = (key, body, headers = {}) => fetch(url + '/v1/chat/completions', {
    method: 'POST', headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json', ...headers }, body: JSON.stringify(body),
  });
  const events = async (keyId) => (await admin(`/admin/events?key_id=${keyId}&limit=100`)).data.reverse();
  return { url, oa, an, gw, admin, newKey, chat, events, close: async () => { await gw.close(); gw.server.closeAllConnections?.(); await oa.close(); await an.close(); } };
}

// Reads an SSE response, recording when each data frame arrived.
export async function readSSE(res) {
  const frames = [];
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  const t0 = Date.now();
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let i;
    while ((i = buf.indexOf('\n\n')) !== -1) {
      const f = buf.slice(0, i); buf = buf.slice(i + 2);
      if (f.startsWith('data: ')) frames.push({ t: Date.now() - t0, data: f.slice(6) });
    }
  }
  const objs = frames.filter((f) => f.data !== '[DONE]').map((f) => ({ t: f.t, ...JSON.parse(f.data) }));
  return { frames, objs, done: frames.at(-1)?.data === '[DONE]',
    text: objs.map((o) => o.choices?.[0]?.delta?.content ?? '').join(''), error: objs.find((o) => o.error)?.error };
}
