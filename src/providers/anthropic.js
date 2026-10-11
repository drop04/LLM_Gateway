import { UpstreamError } from '../errors.js';
import { sseEvents } from '../sse.js';
import { rand, contentText } from '../util.js';
import { fetchUpstream, postJson } from './base.js';

const STOP = { end_turn: 'stop', stop_sequence: 'stop', max_tokens: 'length', tool_use: 'tool_calls', refusal: 'content_filter' };
const unsupported = (msg) => new UpstreamError(msg, { kind: 'unsupported' });

function safeJson(s) {
  try { return s ? JSON.parse(s) : {}; } catch { return {}; }
}

function toBlocks(content) {
  if (content == null) return [];
  if (typeof content === 'string') return content ? [{ type: 'text', text: content }] : [];
  return content.flatMap((p) => {
    if (p.type === 'text') return p.text ? [{ type: 'text', text: p.text }] : [];
    if (p.type === 'image_url') {
      const url = typeof p.image_url === 'string' ? p.image_url : p.image_url.url;
      const m = /^data:([^;]+);base64,(.*)$/s.exec(url);
      return [{ type: 'image', source: m ? { type: 'base64', media_type: m[1], data: m[2] } : { type: 'url', url } }];
    }
    throw unsupported(`content part "${p.type}" is not supported on Anthropic routes`);
  });
}

// OpenAI chat request -> Anthropic Messages request
export function toAnthropicRequest(req, model, stream, defaultMaxTokens) {
  if (req.n && req.n > 1) throw unsupported('n > 1 is not supported on Anthropic routes');
  const system = [];
  const out = [];
  const push = (role, blocks) => {
    if (!blocks.length) return;
    const last = out.at(-1);
    if (last?.role === role) last.content.push(...blocks); // Anthropic wants strictly alternating turns
    else out.push({ role, content: blocks });
  };
  for (const m of req.messages) {
    if (m.role === 'system' || m.role === 'developer') system.push(contentText(m.content));
    else if (m.role === 'user') push('user', toBlocks(m.content));
    else if (m.role === 'assistant') {
      const blocks = toBlocks(m.content);
      for (const tc of m.tool_calls ?? []) {
        blocks.push({ type: 'tool_use', id: tc.id, name: tc.function.name, input: safeJson(tc.function.arguments) });
      }
      push('assistant', blocks);
    } else if (m.role === 'tool') {
      push('user', [{ type: 'tool_result', tool_use_id: m.tool_call_id, content: contentText(m.content) }]);
    }
  }
  const body = {
    model,
    max_tokens: req.max_completion_tokens ?? req.max_tokens ?? defaultMaxTokens,
    messages: out,
    stream,
  };
  if (system.length) body.system = system.join('\n\n');
  if (req.temperature != null) body.temperature = Math.min(1, req.temperature); // Anthropic caps at 1
  else if (req.top_p != null) body.top_p = req.top_p; // newer Claude models reject temperature+top_p together
  if (req.stop) body.stop_sequences = [].concat(req.stop);
  if (req.user) body.metadata = { user_id: String(req.user) };
  if (req.tools?.length) {
    body.tools = req.tools.map((t) => ({
      name: t.function.name, description: t.function.description, input_schema: t.function.parameters ?? { type: 'object', properties: {} },
    }));
    const tc = req.tool_choice;
    if (tc === 'required') body.tool_choice = { type: 'any' };
    else if (tc === 'none') body.tool_choice = { type: 'none' };
    else if (tc?.function) body.tool_choice = { type: 'tool', name: tc.function.name };
    else if (tc === 'auto') body.tool_choice = { type: 'auto' };
  }
  return body;
}

// Anthropic usage -> OpenAI-shaped usage (cache details preserved for billing)
export function toOpenAIUsage(u = {}) {
  const read = u.cache_read_input_tokens ?? 0;
  const write = u.cache_creation_input_tokens ?? 0;
  const prompt = (u.input_tokens ?? 0) + read + write;
  const out = u.output_tokens ?? 0;
  return {
    prompt_tokens: prompt, completion_tokens: out, total_tokens: prompt + out,
    prompt_tokens_details: { cached_tokens: read }, cache_creation_input_tokens: write,
  };
}

export function fromAnthropicResponse(r) {
  const text = r.content.filter((b) => b.type === 'text').map((b) => b.text).join('');
  const tool_calls = r.content.filter((b) => b.type === 'tool_use').map((b) => ({
    id: b.id, type: 'function', function: { name: b.name, arguments: JSON.stringify(b.input ?? {}) },
  }));
  const message = { role: 'assistant', content: text || (tool_calls.length ? null : '') };
  if (tool_calls.length) message.tool_calls = tool_calls;
  return {
    id: r.id ? `chatcmpl-${r.id}` : `chatcmpl-${rand()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: r.model,
    choices: [{ index: 0, message, finish_reason: STOP[r.stop_reason] ?? 'stop' }],
    usage: toOpenAIUsage(r.usage),
  };
}

export class AnthropicProvider {
  constructor(name, cfg, timeouts, defaultMaxTokens) {
    this.name = name;
    this.cfg = cfg;
    this.timeouts = timeouts;
    this.defaultMaxTokens = defaultMaxTokens;
    this.base = cfg.baseUrl.replace(/\/$/, '');
  }
  headers() {
    return { 'x-api-key': this.cfg.apiKey, 'anthropic-version': this.cfg.version ?? '2023-06-01', ...(this.cfg.headers ?? {}) };
  }

  async complete(req, model, { signal }) {
    const body = toAnthropicRequest(req, model, false, this.defaultMaxTokens);
    const r = await postJson(`${this.base}/v1/messages`, this.headers(), body, { timeoutMs: this.timeouts.requestMs, signal });
    const response = fromAnthropicResponse(r);
    return { response, usage: { input: r.usage?.input_tokens ?? 0, output: r.usage?.output_tokens ?? 0,
      cacheRead: r.usage?.cache_read_input_tokens ?? 0, cacheWrite: r.usage?.cache_creation_input_tokens ?? 0 } };
  }

  async stream(req, model, { signal }) {
    const body = toAnthropicRequest(req, model, true, this.defaultMaxTokens);
    const { res, ac, done } = await fetchUpstream(
      `${this.base}/v1/messages`,
      { method: 'POST', headers: { 'content-type': 'application/json', accept: 'text/event-stream', ...this.headers() }, body: JSON.stringify(body) },
      { timeoutMs: this.timeouts.headersMs, signal },
    );
    const idleMs = this.timeouts.idleMs;
    const usage = {}; // accumulates across message_start / message_delta
    const iterator = (async function* () {
      const id = `chatcmpl-${rand()}`;
      const created = Math.floor(Date.now() / 1000);
      let outModel = model;
      const toolIdx = new Map(); // anthropic content-block index -> openai tool_calls index
      let nTools = 0, sawStop = false;
      const mk = (delta, finish = null) => ({ id, object: 'chat.completion.chunk', created, model: outModel, choices: [{ index: 0, delta, finish_reason: finish }] });
      try {
        for await (const ev of sseEvents(res.body, { idleMs, onTimeout: () => ac.abort(), signal })) {
          let d;
          try { d = JSON.parse(ev.data); } catch { continue; }
          switch (d.type ?? ev.event) {
            case 'message_start':
              outModel = d.message?.model ?? outModel;
              Object.assign(usage, d.message?.usage);
              yield mk({ role: 'assistant', content: '' });
              break;
            case 'content_block_start':
              if (d.content_block?.type === 'tool_use') {
                const i = nTools++;
                toolIdx.set(d.index, i);
                yield mk({ tool_calls: [{ index: i, id: d.content_block.id, type: 'function', function: { name: d.content_block.name, arguments: '' } }] });
              }
              break;
            case 'content_block_delta':
              if (d.delta?.type === 'text_delta' && d.delta.text) yield mk({ content: d.delta.text });
              else if (d.delta?.type === 'input_json_delta' && toolIdx.has(d.index)) {
                yield mk({ tool_calls: [{ index: toolIdx.get(d.index), function: { arguments: d.delta.partial_json } }] });
              }
              break;
            case 'message_delta':
              for (const [k, v] of Object.entries(d.usage ?? {})) if (v != null) usage[k] = v; // cumulative
              yield mk({}, STOP[d.delta?.stop_reason] ?? 'stop');
              break;
            case 'message_stop':
              sawStop = true;
              yield { id, object: 'chat.completion.chunk', created, model: outModel, choices: [], usage: toOpenAIUsage(usage) };
              return;
            case 'error':
              throw new UpstreamError(`anthropic stream error: ${d.error?.type ?? 'unknown'}: ${d.error?.message ?? ''}`, { kind: 'stream' });
          }
        }
      } finally { done(); }
      if (!sawStop) throw new UpstreamError('anthropic closed the stream early', { kind: 'stream' });
    })();
    // Input tokens are known as soon as message_start arrives; lets us bill sensibly on aborts.
    const partialUsage = () => (usage.input_tokens == null ? null : {
      input: usage.input_tokens, output: usage.output_tokens ?? 0,
      cacheRead: usage.cache_read_input_tokens ?? 0, cacheWrite: usage.cache_creation_input_tokens ?? 0,
    });
    return { iterator, partialUsage };
  }
}
