import { UpstreamError } from '../errors.js';
import { sseEvents } from '../sse.js';
import { normalizeUsage } from '../pricing.js';
import { fetchUpstream, postJson } from './base.js';

// Works with OpenAI and anything that speaks the same dialect (Azure-style gateways,
// Together, Groq, vLLM, Ollama, OpenRouter ...).
export class OpenAIProvider {
  constructor(name, cfg, timeouts) {
    this.name = name;
    this.cfg = cfg;
    this.timeouts = timeouts;
    this.base = cfg.baseUrl.replace(/\/$/, '');
  }
  headers() {
    return { ...(this.cfg.apiKey ? { authorization: `Bearer ${this.cfg.apiKey}` } : {}), ...(this.cfg.headers ?? {}) };
  }

  async complete(req, model, { signal }) {
    const body = { ...req, model };
    delete body.stream; delete body.stream_options;
    const response = await postJson(`${this.base}/chat/completions`, this.headers(), body,
      { timeoutMs: this.timeouts.requestMs, signal });
    return { response, usage: normalizeUsage(response.usage) };
  }

  async stream(req, model, { signal }) {
    // Force include_usage: without it OpenAI never reports token counts on streams,
    // and we couldn't bill accurately. The router strips the usage chunk again if the
    // client didn't ask for it.
    const body = { ...req, model, stream: true, stream_options: { ...req.stream_options, include_usage: true } };
    const { res, ac, done } = await fetchUpstream(
      `${this.base}/chat/completions`,
      { method: 'POST', headers: { 'content-type': 'application/json', accept: 'text/event-stream', ...this.headers() }, body: JSON.stringify(body) },
      { timeoutMs: this.timeouts.headersMs, signal },
    );
    const idleMs = this.timeouts.idleMs;
    const iterator = (async function* () {
      let sawDone = false, sawFinish = false;
      try {
        for await (const ev of sseEvents(res.body, { idleMs, onTimeout: () => ac.abort(), signal })) {
          if (ev.data === '[DONE]') { sawDone = true; return; }
          let obj;
          try { obj = JSON.parse(ev.data); } catch { continue; }
          if (obj.error) {
            throw new UpstreamError(`upstream stream error: ${obj.error.message ?? JSON.stringify(obj.error)}`, { kind: 'stream' });
          }
          if (obj.choices?.some((c) => c.finish_reason)) sawFinish = true;
          yield obj;
        }
      } finally { done(); }
      if (!sawDone && !sawFinish) throw new UpstreamError('upstream closed the stream early', { kind: 'stream' });
    })();
    return { iterator, partialUsage: () => null };
  }

  async embed(text, model, { signal } = {}) {
    const j = await postJson(`${this.base}/embeddings`, this.headers(), { model, input: text },
      { timeoutMs: this.timeouts.requestMs, signal });
    return j.data[0].embedding;
  }
}
