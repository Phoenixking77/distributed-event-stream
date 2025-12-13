'use strict';

import { Broker } from '../broker/src';

async function main() {
  console.log('Starting broker in test mode...');

  const broker = new Broker({
    config: {
      broker: {
        port: 9092,
        host: '0.0.0.0',
        dataDir: './test-data'
      },
      log: {
        maxSegmentSize: 1024 * 1024,
        syncInterval: 500
      }
    }
  });

  await broker.start();

  setInterval(() => {
    const stats = broker.getStats();
    console.log('Broker stats:', JSON.stringify(stats, null, 2));
  }, 5000);
}

main().catch((err) => {
  console.error('Failed to start broker:', err);
  process.exit(1);
});
