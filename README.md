# llm-gateway

An OpenAI-compatible proxy in front of multiple LLM providers. Zero npm dependencies (Node ≥ 22.13: built-in `fetch`, `node:http`, `node:sqlite`).

| Feature | Where | How it works |
|---|---|---|
| Per-key budgets | `db.js`, `server.js` | **Reserve → settle.** Worst-case cost is reserved atomically before the upstream call, then replaced by the real cost. Concurrent requests can't overspend. |
| Fallback routing | `router.js`, `breaker.js` | Ordered routes per model alias; fail over on network errors, timeouts, 429/5xx/401/403/404. 400/422 are *not* retried (the request is bad everywhere). Per-route circuit breaker + `Retry-After` cool-down. |
| Streaming passthrough | `sse.js`, `providers/*` | Incremental SSE parse → re-emit per event with backpressure. Anthropic's event stream is translated to OpenAI chunks on the fly. |
| Semantic cache | `cache.js` | Embedding similarity on the last user turn; everything else (system prompt, history, sampling params, key) must match exactly. Persistent in SQLite. |
| Token-level billing | `pricing.js`, `db.js` | Integer micro-USD ledger. Input / output / cache-read / cache-write priced separately. One `usage_events` row per request. |

## Run

```bash
cp config.example.json config.json      # edit models + pricing
export GATEWAY_ADMIN_KEY=... OPENAI_API_KEY=... ANTHROPIC_API_KEY=...
npm start

# mint a key: $5/month, 60 req/min
curl -s localhost:8080/admin/keys -H "authorization: Bearer $GATEWAY_ADMIN_KEY" \
  -H 'content-type: application/json' \
  -d '{"name":"team-a","budget_usd":5,"period":"monthly","rpm":60}'

# use it with any OpenAI SDK (base_url = http://localhost:8080/v1)
curl -N localhost:8080/v1/chat/completions -H "authorization: Bearer sk-gw-..." \
  -H 'content-type: application/json' \
  -d '{"model":"fast","stream":true,"messages":[{"role":"user","content":"hi"}]}'

npm test
```

`model` is a gateway **alias** (`fast`, `smart`) mapped to an ordered list of `provider/model` routes.

### Endpoints
- `POST /v1/chat/completions` – OpenAI-compatible, streaming or not
- `GET /v1/models`, `GET /v1/key` (own budget/spend)
- `POST|GET /admin/keys`, `GET|PATCH|DELETE /admin/keys/:id`
- `GET /admin/usage?key_id=&since=` – totals, by model, by key (incl. cache savings)
- `GET /admin/events?key_id=&limit=` – raw ledger rows
- `GET /admin/status` – circuit-breaker state, cache stats, counters

Response headers: `x-gateway-request-id`, `-provider`, `-model`, `-attempts`, `-cache` (`hit` / `hit-semantic` / `miss` / `bypass`), `-cost-usd` (non-streaming).
Request header `x-gateway-cache: bypass | refresh`.

## Design decisions worth knowing

**Streaming and fallback.** The client gets nothing until the upstream produces its first *meaningful* chunk (role-only preambles are held back). Any failure before that point – connect error, 5xx, Anthropic `overloaded_error` event, first-token timeout – silently fails over. After the first token is sent, HTTP status and bytes are committed, so a mid-stream failure ends with an in-band `error` event + `[DONE]` and is **not** retried on another provider (the output would be inconsistent). The partial usage is still billed.

**Billing correctness.**
- OpenAI streams only report usage if asked; the gateway always sets `stream_options.include_usage` and strips the extra chunk if the client didn't request it.
- If a provider doesn't report usage (client abort, crash, compat server), the gateway bills an *estimate* (partial input tokens + ~chars/3 of streamed output) and flags the row `estimated = 1`.
- Disconnects abort the upstream request, so you stop paying for tokens nobody reads.
- Boot refuses to start if any route has no price (otherwise it would be silently free).
- Anthropic cache-read/write tokens are priced separately (`cacheRead`, `cacheWrite` in `pricing`).

**Budget reservation** uses `input_estimate × price + max_tokens × price` (max over the candidate routes). If `max_tokens` is omitted, `defaultMaxTokens` is assumed, so tiny budgets + huge default → 402. The error message says how much the request could cost.

**Semantic cache safety.**
- Scoped per API key by default (`cache.scope`); `global` shares answers across keys.
- Not used for tool calls, `n>1`, non-text user content, or requests above `cache.maxTemperature` (if set).
- Only completed, `finish_reason: stop` answers are stored.
- A *guard* requires numbers and negations to match exactly, blocking "within 30 days" ↔ "within 60 days" and "can" ↔ "can't".
- The default embedder is an **offline lexical hasher** (case/punctuation/typo/small-wording variation). For true paraphrase matching, configure a provider embedder: `"embedder": {"type":"provider","provider":"openai","model":"text-embedding-3-small"}` and tune `threshold`.
- Hits are free (`cost_micro = 0`) and recorded with `saved_micro`.

## Known limits / next steps
- **Single process.** Rate limiting and the circuit breaker are in-memory; SQLite is the only shared state. For multiple replicas move budgets to Postgres/Redis (reserve/settle maps directly to `UPDATE … WHERE spent+reserved+x <= budget`) and share breaker state.
- Cache lookup is a linear scan per partition (fine to a few thousand entries per partition); swap in an ANN index/pgvector beyond that.
- Only `/v1/chat/completions`. Embeddings, Responses API, and an inbound Anthropic-native endpoint are natural additions.
- Anthropic translation covers text, images, tools, stop, temperature; not `response_format`, `logprobs`, or thinking blocks.
- Prices in `config.example.json` are examples – verify against provider pricing pages.
- No TLS / admin UI: put it behind a reverse proxy.
