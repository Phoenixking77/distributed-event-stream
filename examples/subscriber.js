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
    const offset = parseInt(process.argv[3] || '0', 10);
    const groupId = process.argv[4] || null;

    console.log(`Subscribing to topic "${topic}"`);
    if (offset > 0) {
      console.log(`Starting from offset: ${offset}`);
    }
    if (groupId) {
      console.log(`Consumer group: ${groupId}`);
    }
    console.log('\nWaiting for messages... (Press Ctrl+C to stop)\n');

    let messageCount = 0;

    await client.subscribe(topic, (message) => {
      messageCount++;
      console.log(`[${messageCount}] Received message:`);
      console.log(`  Offset: ${message.offset}`);
      console.log(`  Timestamp: ${new Date(message.timestamp).toISOString()}`);
      console.log(`  Data:`, message.message);
      console.log('');
    }, {
      offset,
      groupId
    });

    process.on('SIGINT', async () => {
      console.log('\n\nStopping subscriber...');
      const metrics = client.getMetrics();
      console.log(`Received ${metrics.messagesReceived} messages`);
      console.log(`Total bytes: ${metrics.messagesBytesReceived}`);
      await client.disconnect();
      console.log('Disconnected');
      process.exit(0);
    });

  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
}

main();
