'use strict';

const { Broker } = require('../broker/src');
const { Client } = require('../client-sdk/src');

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log('\n=== Distributed Event Stream - Quick Test ===\n');

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

  console.log('1. Starting broker...');
  await broker.start();
  await sleep(1000);

  const publisher = new Client({ host: 'localhost', port: 9092 });
  const subscriber = new Client({ host: 'localhost', port: 9092 });

  console.log('2. Connecting clients...');
  await publisher.connect();
  await subscriber.connect();

  console.log('3. Setting up subscriber...');
  let messagesReceived = 0;

  await subscriber.subscribe('test-topic', (msg) => {
    messagesReceived += 1;
    console.log(`   Received message ${messagesReceived}:`, msg.message);
  }, { offset: 0 });

  await sleep(500);

  console.log('4. Publishing messages...');
  for (let i = 0; i < 5; i += 1) {
    const result = await publisher.publish('test-topic', {
      id: i + 1,
      message: `Test message ${i + 1}`,
      timestamp: Date.now()
    });
    console.log(`   Published message ${i + 1} at offset ${result.offset}`);
    await sleep(200);
  }

  await sleep(2000);

  console.log(`\n5. Verification: Published 5 messages, received ${messagesReceived} messages`);

  if (messagesReceived === 5) {
    console.log('✅ TEST PASSED: All messages delivered successfully!');
  } else {
    console.log('❌ TEST FAILED: Message count mismatch');
  }

  console.log('\n6. Testing crash recovery...');
  console.log('   Closing clients...');
  publisher.disconnect();
  subscriber.disconnect();

  await sleep(500);

  console.log('   Stopping broker...');
  await broker.stop();

  await sleep(1000);

  console.log('   Restarting broker...');
  await broker.start();

  await sleep(1000);

  const subscriber2 = new Client({ host: 'localhost', port: 9092 });
  await subscriber2.connect();

  console.log('   Subscribing to replay from offset 0...');
  let replayCount = 0;

  await subscriber2.subscribe('test-topic', (msg) => {
    replayCount += 1;
    console.log(`   Replayed message ${replayCount}:`, msg.message);
  }, { offset: 0 });

  await sleep(2000);

  console.log(`\n7. Replay verification: Received ${replayCount} messages after restart`);

  if (replayCount === 5) {
    console.log('✅ CRASH RECOVERY TEST PASSED: All messages preserved!');
  } else {
    console.log('❌ CRASH RECOVERY TEST FAILED');
  }

  console.log('\n8. Cleanup...');
  subscriber2.disconnect();
  await broker.stop();

  console.log('\n=== Test Complete ===\n');
  process.exit(0);
}

main().catch((err) => {
  console.error('Test failed:', err);
  process.exit(1);
});
