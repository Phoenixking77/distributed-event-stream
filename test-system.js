#!/usr/bin/env node
'use strict';

import { Client } from './client-sdk/src/client.js';
import http from 'http';

const BROKER_HOST = process.env.BROKER_HOST || 'localhost';
const BROKER_PORT = parseInt(process.env.BROKER_PORT || '9092', 10);
const CONTROLLER_PORT = parseInt(process.env.CONTROLLER_PORT || '8080', 10);

async function checkController() {
  return new Promise((resolve, reject) => {
    const req = http.get(`http://${BROKER_HOST}:${CONTROLLER_PORT}/health`, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          const result = JSON.parse(data);
          resolve(result);
        } catch (e) {
          reject(e);
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(2000, () => {
      req.destroy();
      reject(new Error('Timeout'));
    });
  });
}

async function testPubSub() {
  console.log('Testing Pub/Sub functionality...\n');

  const client = new Client({
    host: BROKER_HOST,
    port: BROKER_PORT
  });

  try {
    console.log('1. Connecting to broker...');
    await client.connect();
    console.log('   ✓ Connected\n');

    const topic = 'test-topic';
    const testMessages = [
      { id: 1, text: 'Hello' },
      { id: 2, text: 'World' },
      { id: 3, text: 'Test' }
    ];

    let receivedCount = 0;
    const receivedMessages = [];

    console.log('2. Subscribing to topic...');
    await client.subscribe(topic, (message) => {
      receivedCount++;
      receivedMessages.push(message);
      console.log(`   ✓ Received message ${receivedCount}:`, message.message);
    }, { offset: 0 });
    console.log('   ✓ Subscribed\n');

    await new Promise(resolve => setTimeout(resolve, 500));

    console.log('3. Publishing messages...');
    for (const msg of testMessages) {
      const result = await client.publish(topic, msg);
      console.log(`   ✓ Published: offset=${result.offset}, topic=${result.topic}`);
    }
    console.log('');

    await new Promise(resolve => setTimeout(resolve, 1000));

    console.log('4. Verifying received messages...');
    if (receivedCount === testMessages.length) {
      console.log(`   ✓ Received all ${receivedCount} messages\n`);
    } else {
      console.log(`   ✗ Expected ${testMessages.length} messages, got ${receivedCount}\n`);
    }

    const metrics = client.getMetrics();
    console.log('5. Metrics:');
    console.log(`   Published: ${metrics.messagesPublished}`);
    console.log(`   Received: ${metrics.messagesReceived}`);
    console.log(`   Errors: ${metrics.errors}\n`);

    await client.disconnect();
    console.log('6. Disconnected\n');

    if (receivedCount === testMessages.length && metrics.errors === 0) {
      console.log('✓ All tests passed!');
      return true;
    } else {
      console.log('✗ Some tests failed');
      return false;
    }
  } catch (err) {
    console.error('✗ Test failed:', err.message);
    console.error(err.stack);
    return false;
  }
}

async function main() {
  console.log('='.repeat(60));
  console.log('System Integration Test');
  console.log('='.repeat(60));
  console.log(`Broker: ${BROKER_HOST}:${BROKER_PORT}`);
  console.log(`Controller: ${BROKER_HOST}:${CONTROLLER_PORT}\n`);

  try {
    console.log('Checking controller health...');
    const health = await checkController();
    console.log('✓ Controller is healthy:', health);
    console.log('');
  } catch (err) {
    console.log('⚠ Controller not responding (may still be starting)');
    console.log('');
  }

  const success = await testPubSub();

  process.exit(success ? 0 : 1);
}

main();
