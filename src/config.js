import fs from 'node:fs';
import { deepMerge } from './util.js';

const DEFAULTS = {
  port: 8080,
  host: '0.0.0.0',
  dbPath: './gateway.db',
  adminKey: null,
  defaultMaxTokens: 4096,       // used for budget reservation + Anthropic's required max_tokens
  maxBodyBytes: 10 * 1024 * 1024,
  timeouts: {
    headersMs: 30_000,          // connect + upstream response headers
    requestMs: 120_000,         // whole non-streaming request
    firstChunkMs: 30_000,       // streaming: until the first token (this is the fallback window)
    idleMs: 60_000,             // streaming: max silence between events once committed
  },
  breaker: { threshold: 5, cooldownMs: 30_000 },
  cache: {
    enabled: false,
    scope: 'key',               // 'key' = never share answers across API keys; 'global' = share
    threshold: 0.92,            // cosine similarity needed for a semantic hit
    ttlSeconds: 3600,
    maxEntries: 5000,
    maxTemperature: null,       // e.g. 0.3 => only cache low-temperature requests
    embedder: { type: 'hash', dim: 1024 }, // or { type:'provider', provider:'openai', model:'text-embedding-3-small' }
  },
  providers: {},
  models: {},
  pricing: {},
};

export function loadConfig(path) {
  return finalizeConfig(JSON.parse(fs.readFileSync(path, 'utf8')));
}

export function finalizeConfig(raw) {
  const cfg = resolveEnv(deepMerge(DEFAULTS, raw));
  for (const [name, p] of Object.entries(cfg.providers)) {
    if (!['openai', 'anthropic'].includes(p.type)) throw new Error(`provider ${name}: unknown type "${p.type}"`);
    if (!p.baseUrl) throw new Error(`provider ${name}: baseUrl required`);
  }
  for (const [alias, m] of Object.entries(cfg.models)) {
    if (!m.routes?.length) throw new Error(`model ${alias}: needs at least one route`);
    m.routes = m.routes.map((r) => {
      if (!cfg.providers[r.provider]) throw new Error(`model ${alias}: unknown provider "${r.provider}"`);
      const key = `${r.provider}/${r.model}`;
      // Refuse to boot with an unpriced route: it would be silently unbilled.
      if (!cfg.pricing[key] && !cfg.pricing[r.model]) {
        throw new Error(`no pricing for route ${key} (add it to "pricing", use {"input":0,"output":0} if free)`);
      }
      return { provider: r.provider, model: r.model, key };
    });
  }
  return cfg;
}

function resolveEnv(v) {
  if (typeof v === 'string' && v.startsWith('env:')) {
    const val = process.env[v.slice(4)];
    if (val === undefined) throw new Error(`missing environment variable ${v.slice(4)}`);
    return val;
  }
  if (Array.isArray(v)) return v.map(resolveEnv);
  if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, resolveEnv(x)]));
  return v;
}
