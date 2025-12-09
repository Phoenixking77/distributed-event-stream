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

    await client.subscribe('orders', (msg) => {
      console.log('Received message:', {
        topic: msg.topic,
        offset: msg.offset,
        timestamp: new Date(msg.timestamp).toISOString(),
        message: msg.message
      });
    }, { offset: 0 });

    console.log('Subscribed to topic: orders');
    console.log('Waiting for messages... (Ctrl+C to exit)');

    await new Promise(() => {});
  } catch (err) {
    console.error('Error:', err);
    client.disconnect();
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
