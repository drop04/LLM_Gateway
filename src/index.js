import { loadConfig } from './config.js';
import { createGateway } from './server.js';
import { log } from './util.js';

const configPath = process.argv[2] ?? process.env.GATEWAY_CONFIG ?? './config.json';
const config = loadConfig(configPath);
const gw = createGateway(config);
gw.server.listen(config.port, config.host, () => log('info', 'gateway listening', { port: config.port, models: Object.keys(config.models) }));

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => { log('info', 'shutting down'); await gw.close(); process.exit(0); });
}
