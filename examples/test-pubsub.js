#!/usr/bin/env node
'use strict';

import { Client } from '../client-sdk/src/client.js';

const client = new Client({
  host: process.env.BROKER_HOST || 'localhost',
  port: parseInt(process.env.BROKER_PORT || '9092', 10)
});

async function main() {
  try {
    console.log('Connecting to broker...');
    await client.connect();
    console.log('Connected!\n');

    const topic = 'test-topic';
    let receivedCount = 0;
    let publishedCount = 0;

    console.log(`Subscribing to topic "${topic}"...`);
    await client.subscribe(topic, (message) => {
      receivedCount++;
      console.log(`[RECEIVED #${receivedCount}] Offset: ${message.offset} -`, message.message);
    }, { offset: 0 });

    console.log('Subscribed! Starting to publish messages...\n');

    for (let i = 0; i < 5; i++) {
      const message = {
        id: i + 1,
        text: `Test message ${i + 1}`,
        timestamp: Date.now()
      };

      const result = await client.publish(topic, message);
      publishedCount++;
      console.log(`[PUBLISHED #${publishedCount}] Offset: ${result.offset} -`, message);

      await new Promise(resolve => setTimeout(resolve, 500));
    }

    await new Promise(resolve => setTimeout(resolve, 1000));

    const metrics = client.getMetrics();
    console.log(`\n=== Summary ===`);
    console.log(`Published: ${publishedCount} messages`);
    console.log(`Received: ${receivedCount} messages`);
    console.log(`Total published: ${metrics.messagesPublished}`);
    console.log(`Total received: ${metrics.messagesReceived}`);

    await client.disconnect();
    console.log('\nDisconnected');
    process.exit(0);
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
}

main();
