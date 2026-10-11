// Per-route circuit breaker (route key = "provider/model").
//   closed    -> normal
//   open      -> skip the route until `openUntil`
//   half-open -> after the cooldown, let ONE request through as a probe; the window is
//                re-armed immediately so concurrent requests keep skipping the route.
export class Breaker {
  constructor({ threshold = 5, cooldownMs = 30_000 } = {}) {
    this.threshold = threshold;
    this.cooldownMs = cooldownMs;
    this.state = new Map(); // key -> { fails, openUntil }
  }
  #get(key) {
    let s = this.state.get(key);
    if (!s) this.state.set(key, (s = { fails: 0, openUntil: 0 }));
    return s;
  }
  canAttempt(key, now = Date.now()) {
    const s = this.#get(key);
    if (s.openUntil === 0) return true;
    if (now < s.openUntil) return false;
    s.openUntil = now + this.cooldownMs; // grant a single probe
    return true;
  }
  success(key) {
    const s = this.#get(key);
    s.fails = 0;
    s.openUntil = 0;
  }
  failure(key, now = Date.now()) {
    const s = this.#get(key);
    s.fails += 1;
    if (s.fails >= this.threshold) s.openUntil = now + this.cooldownMs;
  }
  // Provider said "429, retry in N ms": pause the route without counting a hard failure.
  coolDown(key, ms, now = Date.now()) {
    const s = this.#get(key);
    s.openUntil = Math.max(s.openUntil, now + ms);
  }
  snapshot(now = Date.now()) {
    return Object.fromEntries(
      [...this.state].map(([k, s]) => [k, { consecutive_failures: s.fails, open: s.openUntil > now, retry_in_ms: Math.max(0, s.openUntil - now) }]),
    );
  }
}
