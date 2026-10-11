import { DatabaseSync } from 'node:sqlite';
import { sha256, rand } from './util.js';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS api_keys (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  key_hash TEXT NOT NULL UNIQUE,
  key_prefix TEXT NOT NULL,
  budget_micro INTEGER,                 -- NULL = unlimited
  spent_micro INTEGER NOT NULL DEFAULT 0,
  reserved_micro INTEGER NOT NULL DEFAULT 0,  -- in-flight reservations
  period TEXT NOT NULL DEFAULT 'none',  -- none | daily | monthly
  period_id TEXT NOT NULL DEFAULT '',
  rpm INTEGER,
  allowed_models TEXT,                  -- JSON array or NULL
  revoked INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS usage_events (
  id TEXT PRIMARY KEY,
  ts INTEGER NOT NULL,
  key_id TEXT NOT NULL,
  alias TEXT,
  provider TEXT,
  model TEXT,
  stream INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,                 -- ok | error | stream_error | client_abort
  http_status INTEGER,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens INTEGER NOT NULL DEFAULT 0,
  cache_write_tokens INTEGER NOT NULL DEFAULT 0,
  cost_micro INTEGER NOT NULL DEFAULT 0,
  saved_micro INTEGER NOT NULL DEFAULT 0,   -- what a gateway cache hit saved
  cache_hit INTEGER NOT NULL DEFAULT 0,
  estimated INTEGER NOT NULL DEFAULT 0,     -- usage was estimated, not reported by the provider
  latency_ms INTEGER,
  ttfb_ms INTEGER,
  attempts TEXT,
  error TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_key_ts ON usage_events(key_id, ts);
CREATE TABLE IF NOT EXISTS cache_entries (
  id TEXT PRIMARY KEY,
  partition TEXT NOT NULL,
  text_hash TEXT NOT NULL,
  guard TEXT NOT NULL,
  vec BLOB NOT NULL,
  response TEXT NOT NULL,
  usage TEXT NOT NULL,
  route_key TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);
`;

const periodId = (period, d = new Date()) =>
  period === 'monthly' ? d.toISOString().slice(0, 7) : period === 'daily' ? d.toISOString().slice(0, 10) : '';

export class Store {
  constructor(path) {
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;');
    this.db.exec(SCHEMA);
    // Single-instance assumption: anything "in flight" before a restart is gone.
    this.db.exec('UPDATE api_keys SET reserved_micro = 0');
  }

  #tx(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try { const r = fn(); this.db.exec('COMMIT'); return r; }
    catch (e) { this.db.exec('ROLLBACK'); throw e; }
  }

  // ---- keys -------------------------------------------------------------
  createKey({ name, budgetMicro = null, period = 'none', rpm = null, allowedModels = null }) {
    const id = 'key_' + rand(8);
    const plaintext = 'sk-gw-' + rand(32);
    this.db.prepare(
      `INSERT INTO api_keys (id,name,key_hash,key_prefix,budget_micro,period,period_id,rpm,allowed_models,created_at)
       VALUES (?,?,?,?,?,?,?,?,?,?)`,
    ).run(id, name, sha256(plaintext), plaintext.slice(0, 12), budgetMicro, period, periodId(period), rpm,
      allowedModels ? JSON.stringify(allowedModels) : null, Date.now());
    return { key: this.getKey(id), plaintext };
  }

  #hydrate(row) {
    if (!row) return undefined;
    const k = { ...row, allowed_models: row.allowed_models ? JSON.parse(row.allowed_models) : null };
    // Budget windows roll over lazily on first use after the boundary.
    const pid = periodId(k.period);
    if (k.period !== 'none' && pid !== k.period_id) {
      this.db.prepare('UPDATE api_keys SET spent_micro = 0, period_id = ? WHERE id = ?').run(pid, k.id);
      k.spent_micro = 0;
      k.period_id = pid;
    }
    return k;
  }
  findKey(plaintext) {
    return this.#hydrate(this.db.prepare('SELECT * FROM api_keys WHERE key_hash = ?').get(sha256(plaintext)));
  }
  getKey(id) {
    return this.#hydrate(this.db.prepare('SELECT * FROM api_keys WHERE id = ?').get(id));
  }
  listKeys() {
    return this.db.prepare('SELECT id FROM api_keys ORDER BY created_at').all().map((r) => this.getKey(r.id));
  }
  updateKey(id, p) {
    const sets = [], vals = [];
    const add = (c, v) => { sets.push(`${c} = ?`); vals.push(v); };
    if ('name' in p) add('name', p.name);
    if ('budgetMicro' in p) add('budget_micro', p.budgetMicro);
    if ('rpm' in p) add('rpm', p.rpm);
    if ('allowedModels' in p) add('allowed_models', p.allowedModels ? JSON.stringify(p.allowedModels) : null);
    if ('revoked' in p) add('revoked', p.revoked ? 1 : 0);
    if ('period' in p) { add('period', p.period); add('period_id', periodId(p.period)); }
    if (sets.length) this.db.prepare(`UPDATE api_keys SET ${sets.join(', ')} WHERE id = ?`).run(...vals, id);
    return this.getKey(id);
  }

  // ---- budget: reserve -> settle -------------------------------------------
  // Reserve the worst-case cost *before* calling the provider so concurrent requests
  // can't collectively overspend. The check and the increment happen in one
  // transaction (and JS is single-threaded between DB calls).
  reserve(keyId, amountMicro) {
    return this.#tx(() => {
      const k = this.db.prepare('SELECT budget_micro, spent_micro, reserved_micro FROM api_keys WHERE id = ?').get(keyId);
      if (k.budget_micro != null && k.spent_micro + k.reserved_micro + amountMicro > k.budget_micro) {
        return { ok: false, remainingMicro: Math.max(0, k.budget_micro - k.spent_micro - k.reserved_micro) };
      }
      this.db.prepare('UPDATE api_keys SET reserved_micro = reserved_micro + ? WHERE id = ?').run(amountMicro, keyId);
      return { ok: true };
    });
  }

  // Release the reservation, charge the real cost and write the ledger row atomically.
  settle(keyId, reservedMicro, costMicro, ev) {
    this.#tx(() => {
      this.db.prepare(
        'UPDATE api_keys SET reserved_micro = MAX(0, reserved_micro - ?), spent_micro = spent_micro + ? WHERE id = ?',
      ).run(reservedMicro, costMicro, keyId);
      this.db.prepare(
        `INSERT INTO usage_events (id,ts,key_id,alias,provider,model,stream,status,http_status,input_tokens,output_tokens,
           cache_read_tokens,cache_write_tokens,cost_micro,saved_micro,cache_hit,estimated,latency_ms,ttfb_ms,attempts,error)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      ).run(
        ev.requestId, Date.now(), keyId, ev.alias ?? null, ev.provider ?? null, ev.model ?? null, ev.stream ? 1 : 0,
        ev.status, ev.httpStatus ?? null,
        ev.usage?.input ?? 0, ev.usage?.output ?? 0, ev.usage?.cacheRead ?? 0, ev.usage?.cacheWrite ?? 0,
        costMicro, ev.savedMicro ?? 0, ev.cacheHit ? 1 : 0, ev.estimated ? 1 : 0,
        ev.latencyMs ?? null, ev.ttfbMs ?? null, ev.attempts ? JSON.stringify(ev.attempts) : null, ev.error ?? null,
      );
    });
  }

  // ---- reporting ------------------------------------------------------------
  usageSummary({ keyId, since } = {}) {
    const where = ['1=1'], args = [];
    if (keyId) { where.push('key_id = ?'); args.push(keyId); }
    if (since) { where.push('ts >= ?'); args.push(since); }
    const agg = (group) => this.db.prepare(
      `SELECT ${group} AS name, COUNT(*) AS requests, SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens,
              SUM(cache_read_tokens) AS cache_read_tokens, SUM(cost_micro) AS cost_micro, SUM(saved_micro) AS saved_micro,
              SUM(cache_hit) AS cache_hits, SUM(status != 'ok') AS errors
       FROM usage_events WHERE ${where.join(' AND ')} GROUP BY ${group} ORDER BY cost_micro DESC`,
    ).all(...args);
    return {
      totals: this.db.prepare(
        `SELECT COUNT(*) AS requests, COALESCE(SUM(input_tokens),0) AS input_tokens, COALESCE(SUM(output_tokens),0) AS output_tokens,
                COALESCE(SUM(cost_micro),0) AS cost_micro, COALESCE(SUM(saved_micro),0) AS saved_micro, COALESCE(SUM(cache_hit),0) AS cache_hits
         FROM usage_events WHERE ${where.join(' AND ')}`,
      ).get(...args),
      by_model: agg("COALESCE(provider,'-') || '/' || COALESCE(model,'-')"),
      by_key: agg('key_id'),
    };
  }
  recentEvents({ keyId, limit = 50 } = {}) {
    return keyId
      ? this.db.prepare('SELECT * FROM usage_events WHERE key_id = ? ORDER BY ts DESC LIMIT ?').all(keyId, limit)
      : this.db.prepare('SELECT * FROM usage_events ORDER BY ts DESC LIMIT ?').all(limit);
  }

  // ---- semantic cache persistence ---------------------------------------------
  saveCacheEntry(e) {
    this.db.prepare(
      `INSERT OR REPLACE INTO cache_entries (id,partition,text_hash,guard,vec,response,usage,route_key,created_at,expires_at)
       VALUES (?,?,?,?,?,?,?,?,?,?)`,
    ).run(e.id, e.partition, e.textHash, e.guard, new Uint8Array(e.vec.buffer, e.vec.byteOffset, e.vec.byteLength),
      JSON.stringify(e.response), JSON.stringify(e.usage), e.routeKey, e.createdAt, e.expiresAt);
  }
  deleteCacheEntry(id) { this.db.prepare('DELETE FROM cache_entries WHERE id = ?').run(id); }
  loadCacheEntries(nowMs) {
    this.db.prepare('DELETE FROM cache_entries WHERE expires_at <= ?').run(nowMs);
    return this.db.prepare('SELECT * FROM cache_entries ORDER BY created_at').all().map((r) => ({
      id: r.id, partition: r.partition, textHash: r.text_hash, guard: r.guard,
      vec: new Float32Array(r.vec.buffer.slice(r.vec.byteOffset, r.vec.byteOffset + r.vec.byteLength)),
      response: JSON.parse(r.response), usage: JSON.parse(r.usage), routeKey: r.route_key,
      createdAt: r.created_at, expiresAt: r.expires_at, hits: 0,
    }));
  }
}
