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

    const topic = process.argv[2] || 'orders';
    const groupId = process.argv[3] || 'order-processors';

    console.log(`Subscribing to topic "${topic}" with consumer group "${groupId}"`);
    console.log('Messages will be auto-committed as they are consumed\n');

    let messageCount = 0;

    await client.subscribe(topic, (message) => {
      messageCount++;
      console.log(`[${messageCount}] Consumer Group: ${groupId}`);
      console.log(`  Offset: ${message.offset}`);
      console.log(`  Message:`, message.message);
      
      const groupState = client.getConsumerGroupState(groupId);
      if (groupState) {
        console.log(`  Committed Offset: ${groupState.committedOffset}`);
      }
      console.log('');
    }, {
      groupId
    });

    console.log('Waiting for messages... (Press Ctrl+C to stop)\n');

    process.on('SIGINT', async () => {
      console.log('\n\nStopping consumer...');
      const groupState = client.getConsumerGroupState(groupId);
      if (groupState) {
        console.log(`Final committed offset: ${groupState.committedOffset}`);
      }
      const metrics = client.getMetrics();
      console.log(`Received ${metrics.messagesReceived} messages`);
      console.log(`Offset commits: ${metrics.offsetCommits}`);
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
