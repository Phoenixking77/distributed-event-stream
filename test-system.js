import { Client } from './client-sdk/src/index.js';

async function test() {
  console.log('\n🧪 TESTING PUB/SUB SYSTEM\n');

  const publisher = new Client({ host: 'localhost', port: 9092 });
  const subscriber = new Client({ host: 'localhost', port: 9092 });

  try {
    // Connect
    console.log('📤 Connecting publisher...');
    await publisher.connect();
    console.log('✅ Publisher connected\n');

    console.log('📥 Connecting subscriber...');
    await subscriber.connect();
    console.log('✅ Subscriber connected\n');

    // Subscribe
    let messagesReceived = [];
    console.log('👂 Setting up subscriber on topic "test-topic"...');
    await subscriber.subscribe('test-topic', (msg) => {
      messagesReceived.push(msg);
      console.log(`   ✓ Received: "${msg.message}"`);
    }, { offset: 0 });
    console.log('✅ Subscriber ready\n');

    // Publish
    console.log('📨 Publishing 3 messages...\n');
    for (let i = 1; i <= 3; i++) {
      await publisher.publish('test-topic', `Message ${i}`);
      console.log(`   ✓ Published: "Message ${i}"`);
      await new Promise(r => setTimeout(r, 500));
    }

    // Wait for messages
    console.log('\n⏳ Waiting for subscriber to receive messages...\n');
    await new Promise(r => setTimeout(r, 3000));

    // Results
    console.log('\n📊 TEST RESULTS:');
    console.log(`   Messages published: 3`);
    console.log(`   Messages received:  ${messagesReceived.length}`);
    console.log(`   Status: ${messagesReceived.length === 3 ? '✅ SUCCESS' : '❌ FAILED'}\n`);

    await publisher.disconnect();
    await subscriber.disconnect();
  } catch (err) {
    console.error('❌ Test failed:', err.message);
    process.exit(1);
  }
}

test().then(() => process.exit(0));
