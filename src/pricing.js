// All prices are USD per 1,000,000 tokens. Because 1 USD = 1,000,000 micro-USD,
//   cost_in_micro_usd = tokens * price_per_million
// so the ledger can stay in exact integers.

export const priceFor = (config, route) => config.pricing[route.key] ?? config.pricing[route.model];

// Normalised usage: { input, output, cacheRead, cacheWrite }
//   input      = prompt tokens that were NOT served from / written to the provider's prompt cache
//   cacheRead  = prompt tokens read from the provider's prompt cache (usually cheaper)
//   cacheWrite = prompt tokens written to the provider's prompt cache (Anthropic bills extra)
export function normalizeUsage(u) {
  if (!u) return null;
  const prompt = u.prompt_tokens ?? 0;
  const cacheRead = u.prompt_tokens_details?.cached_tokens ?? 0;
  const cacheWrite = u.cache_creation_input_tokens ?? 0;
  return {
    input: Math.max(0, prompt - cacheRead - cacheWrite),
    output: u.completion_tokens ?? 0,
    cacheRead,
    cacheWrite,
  };
}

export function costMicro(price, u) {
  if (!price || !u) return 0;
  const c =
    u.input * price.input +
    u.output * price.output +
    u.cacheRead * (price.cacheRead ?? price.input) +
    u.cacheWrite * (price.cacheWrite ?? price.input);
  return Math.ceil(Number(c.toFixed(6))); // round *up* to a whole micro-dollar, ignoring float fuzz
}

export const totalPromptTokens = (u) => u.input + u.cacheRead + u.cacheWrite;
