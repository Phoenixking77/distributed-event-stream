'use strict';

const { Client } = require('../client-sdk/src');

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 9092
  });

  try {
    await client.connect();
    console.log('Connected to broker');

    for (let i = 0; i < 10; i += 1) {
      const result = await client.publish('orders', {
        orderId: i + 1,
        product: `Product ${i + 1}`,
        quantity: Math.floor(Math.random() * 10) + 1,
        timestamp: Date.now()
      });

      console.log(`Published message ${i + 1}:`, {
        offset: result.offset,
        timestamp: result.timestamp
      });

      await sleep(500);
    }

    console.log('All messages published');
  } catch (err) {
    console.error('Error:', err);
  } finally {
    client.disconnect();
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
