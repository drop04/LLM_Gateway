import { sha256, rand, contentText } from './util.js';

// Embedders
const fnv = (s) => {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return h >>> 0;
};
const normalize = (v) => {
  let n = 0;
  for (const x of v) n += x * x;
  n = Math.sqrt(n) || 1;
  for (let i = 0; i < v.length; i++) v[i] /= n;
  return v;
};

// Zero-dependency, offline "embedding": signed feature hashing of word unigrams, bigrams
// and character trigrams. It captures lexical similarity (case, punctuation, small wording
// changes, typos) - not true semantics. For real paraphrase matching use a provider embedder.
export function hashEmbedder({ dim = 1024 } = {}) {
  return async (text) => {
    const v = new Float32Array(dim);
    const words = text.toLowerCase().replace(/[^\p{L}\p{N}\s]/gu, ' ').split(/\s+/).filter(Boolean);
    const add = (f, w) => { const h = fnv(f); v[h % dim] += (h & 0x80000000 ? -1 : 1) * w; };
    words.forEach((w, i) => {
      add('u:' + w, 1);
      if (i > 0) add('b:' + words[i - 1] + ' ' + w, 1);
      const p = `^${w}$`;
      for (let j = 0; j + 3 <= p.length; j++) add('c:' + p.slice(j, j + 3), 0.4);
    });
    return normalize(v);
  };
}

export function providerEmbedder(provider, model) {
  return async (text) => normalize(Float32Array.from(await provider.embed(text, model)));
}

// Guards: cheap exact-match checks that stop the classic false positives
// ("within 30 days" vs "within 60 days", "can I" vs "can't I").
const NEG = new Set(['not', 'no', 'never', 'without', 'cannot', 'dont', 'cant', 'wont', 'isnt', 'arent', 'doesnt', 'didnt', 'wasnt', 'shouldnt', 'nothing', 'none']);
export function guardOf(text) {
  const t = text.toLowerCase().replace(/['’]/g, '');
  const nums = t.match(/\d+(?:[.,]\d+)*/g) ?? [];
  const negs = (t.match(/[a-z]+/g) ?? []).filter((w) => NEG.has(w));
  return JSON.stringify([nums, [...new Set(negs)].sort()]);
}
const normText = (s) => s.toLowerCase().replace(/\s+/g, ' ').trim();

const dot = (a, b) => { let s = 0; for (let i = 0; i < a.length; i++) s += a[i] * b[i]; return s; };

// Sampling params that change the answer and so must match exactly for a hit.
const PARAMS = ['temperature', 'top_p', 'max_tokens', 'max_completion_tokens', 'stop', 'response_format', 'seed',
  'presence_penalty', 'frequency_penalty', 'logit_bias', 'reasoning_effort'];

export class SemanticCache {
  constructor({ config, store, providers }) {
    this.cfg = config;
    this.enabled = config.enabled;
    this.store = store;
    this.partitions = new Map(); // partition -> Entry[]
    this.lru = new Map();        // entry id -> entry (insertion order = recency)
    this.stats = { hits_exact: 0, hits_semantic: 0, misses: 0, stores: 0, embed_errors: 0 };
    if (!this.enabled) return;
    const e = config.embedder;
    this.embed = e.type === 'provider' ? providerEmbedder(providers[e.provider], e.model) : hashEmbedder(e);
    for (const entry of store.loadCacheEntries(Date.now())) this.#insert(entry);
  }

  #insert(e) {
    if (!this.partitions.has(e.partition)) this.partitions.set(e.partition, []);
    this.partitions.get(e.partition).push(e);
    this.lru.set(e.id, e);
  }
  #remove(e) {
    const list = this.partitions.get(e.partition);
    if (list) {
      const i = list.indexOf(e);
      if (i >= 0) list.splice(i, 1);
      if (!list.length) this.partitions.delete(e.partition);
    }
    this.lru.delete(e.id);
    this.store.deleteCacheEntry(e.id);
  }

  // Decide whether this request may use the cache and compute its lookup context.
  prepare(body, apiKey) {
    const msgs = body.messages;
    const last = msgs.at(-1);
    if (last?.role !== 'user') return null;                 // e.g. ends with a tool result
    if (body.tools?.length || (body.n ?? 1) > 1) return null; // tool flows & multi-choice aren't cacheable
    if (Array.isArray(last.content) && last.content.some((p) => p.type !== 'text')) return null;
    const max = this.cfg.maxTemperature;
    if (max != null && (body.temperature ?? 1) > max) return null;
    const text = contentText(last.content);
    if (!text.trim() || text.length > 8000) return null;
    const scope = this.cfg.scope === 'global' ? 'global' : apiKey.id;
    // Everything except the final user message must match EXACTLY; only the last
    // user turn is compared semantically.
    const params = Object.fromEntries(PARAMS.filter((p) => body[p] !== undefined).map((p) => [p, body[p]]));
    const partition = sha256(JSON.stringify([scope, body.model, msgs.slice(0, -1), params]));
    return { partition, text, textHash: sha256(normText(text)), guard: guardOf(text), vec: null };
  }

  async lookup(ctx) {
    const now = Date.now();
    const list = this.partitions.get(ctx.partition);
    if (!list) { this.stats.misses++; return null; }
    for (const e of [...list]) if (e.expiresAt <= now) this.#remove(e);

    const exact = list.find((e) => e.textHash === ctx.textHash);
    if (exact) return this.#hit(exact, 1, 'exact');

    try { ctx.vec ??= await this.embed(ctx.text); }
    catch { this.stats.embed_errors++; this.stats.misses++; return null; } // embedder down => just a miss

    let best = null, bestSim = -1;
    for (const e of list) {
      if (e.guard !== ctx.guard) continue;
      const s = dot(ctx.vec, e.vec);
      if (s > bestSim) { bestSim = s; best = e; }
    }
    if (best && bestSim >= this.cfg.threshold) return this.#hit(best, bestSim, 'semantic');
    this.stats.misses++;
    return null;
  }
  #hit(e, similarity, kind) {
    e.hits++;
    this.lru.delete(e.id); this.lru.set(e.id, e); // touch
    this.stats[kind === 'exact' ? 'hits_exact' : 'hits_semantic']++;
    return { entry: e, similarity, kind };
  }

  async store_(ctx, { response, usage, routeKey }) {
    try { ctx.vec ??= await this.embed(ctx.text); } catch { this.stats.embed_errors++; return; }
    const now = Date.now();
    const entry = {
      id: 'c_' + rand(8), partition: ctx.partition, textHash: ctx.textHash, guard: ctx.guard, vec: ctx.vec,
      response, usage, routeKey, createdAt: now, expiresAt: now + this.cfg.ttlSeconds * 1000, hits: 0,
    };
    // Replace an identical-question entry rather than accumulating duplicates (matters for x-gateway-cache: refresh).
    const dupe = this.partitions.get(ctx.partition)?.find((e) => e.textHash === ctx.textHash);
    if (dupe) this.#remove(dupe);
    this.#insert(entry);
    this.store.saveCacheEntry(entry);
    this.stats.stores++;
    while (this.lru.size > this.cfg.maxEntries) this.#remove(this.lru.values().next().value); // evict LRU
  }

  summary() { return { enabled: this.enabled, entries: this.lru.size, ...this.stats }; }
}
