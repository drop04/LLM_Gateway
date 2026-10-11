// Errors the *client* sees (turned into OpenAI-style JSON error bodies).
export class GatewayError extends Error {
  constructor(status, code, message, { headers = {}, type } = {}) {
    super(message);
    this.status = status;
    this.code = code;
    this.headers = headers;
    this.type = type ?? (status >= 500 ? 'api_error' : 'invalid_request_error');
  }
  body() {
    return { error: { message: this.message, type: this.type, code: this.code } };
  }
}

// Errors from an upstream provider. `kind` says where it went wrong:
//   http        - provider answered with a non-2xx status
//   network     - connection refused/reset, DNS, TLS ...
//   timeout     - headers / first token / stream stalled
//   stream      - provider broke or truncated an SSE stream
//   unsupported - this route can't express the request (e.g. n>1 on Anthropic)
export class UpstreamError extends Error {
  constructor(message, { status = 0, kind = 'http', retryAfterMs = null, body = null } = {}) {
    super(message);
    this.status = status;
    this.kind = kind;
    this.retryAfterMs = retryAfterMs;
    this.upstreamBody = body;
  }
  // Should we try the next route? A 400/422 means *the request* is bad, and the
  // next provider would reject it too, so we surface it instead of burning fallbacks.
  get fallbackable() {
    if (this.kind !== 'http') return true;
    const s = this.status;
    return s === 401 || s === 403 || s === 404 || s === 408 || s === 409 || s === 429 || s >= 500;
  }
}

// Thrown inside the streaming loop when the downstream client went away.
export class ClientAbort extends Error {
  constructor() { super('client disconnected'); this.name = 'ClientAbort'; }
}
