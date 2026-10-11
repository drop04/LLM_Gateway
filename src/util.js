import crypto from 'node:crypto';

export const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');
export const rand = (n = 12) => crypto.randomBytes(n).toString('base64url');

export function log(level, msg, fields = {}) {
  if (process.env.GATEWAY_LOG === 'silent') return;
  process.stdout.write(JSON.stringify({ ts: new Date().toISOString(), level, msg, ...fields }) + '\n');
}

// Deliberately conservative (over-estimates) - it's used for budget *reservations*
// and as a fallback when a provider doesn't report usage.
export const estimateTokens = (s) => Math.ceil((s?.length ?? 0) / 3);

export function contentText(content) {
  if (content == null) return '';
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) return content.map((p) => (p?.type === 'text' ? p.text ?? '' : '')).join('');
  return '';
}

export function estimateInputTokens(body) {
  let t = 0;
  for (const m of body.messages ?? []) {
    t += 4 + estimateTokens(contentText(m.content));
    if (Array.isArray(m.content)) for (const p of m.content) if (p?.type === 'image_url') t += 1200;
    if (m.tool_calls) t += estimateTokens(JSON.stringify(m.tool_calls));
  }
  if (body.tools) t += estimateTokens(JSON.stringify(body.tools));
  return t;
}

export const microToUsd = (m) => m / 1e6;
export const usdToMicro = (u) => Math.round(u * 1e6);

export function deepMerge(base, over) {
  if (over === undefined) return base;
  if (!isPlain(base) || !isPlain(over)) return over;
  const out = { ...base };
  for (const [k, v] of Object.entries(over)) out[k] = deepMerge(base[k], v);
  return out;
}
const isPlain = (v) => v && typeof v === 'object' && !Array.isArray(v);
