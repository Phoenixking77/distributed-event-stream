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

    const topic = process.argv[2] || 'test-topic';
    const messageCount = parseInt(process.argv[3] || '10', 10);
    const delay = parseInt(process.argv[4] || '1000', 10);

    console.log(`Publishing ${messageCount} messages to topic "${topic}"`);
    console.log(`Delay between messages: ${delay}ms\n`);

    for (let i = 0; i < messageCount; i++) {
      const message = {
        id: i + 1,
        timestamp: Date.now(),
        data: `Message ${i + 1} at ${new Date().toISOString()}`
      };

      const result = await client.publish(topic, message);
      console.log(`[${i + 1}/${messageCount}] Published - Offset: ${result.offset}, Topic: ${result.topic}`);

      if (i < messageCount - 1) {
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }

    const metrics = client.getMetrics();
    console.log(`\nPublished ${metrics.messagesPublished} messages`);
    console.log(`Total bytes: ${metrics.messagesBytesPublished}`);

    await client.disconnect();
    console.log('\nDisconnected');
    process.exit(0);
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
}

main();
