import { OpenAIProvider } from './openai.js';
import { AnthropicProvider } from './anthropic.js';

export function createProviders(config) {
  return Object.fromEntries(Object.entries(config.providers).map(([name, p]) => [
    name,
    p.type === 'anthropic'
      ? new AnthropicProvider(name, p, config.timeouts, config.defaultMaxTokens)
      : new OpenAIProvider(name, p, config.timeouts),
  ]));
}
