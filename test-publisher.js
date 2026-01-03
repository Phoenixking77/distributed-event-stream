import { Client } from './client-sdk/src/index.js';

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 9092
  });

  try {
    console.log('Connecting...');
    await client.connect();
    console.log('Connected!');

    console.log('Publishing message...');
    const result = await client.publish('test', { data: 'hello' });
    console.log('Published:', result);

    await new Promise(r => setTimeout(r, 1000));
  } catch (err) {
    console.error('Error:', err.message, err.stack);
  } finally {
    client.disconnect();
  }
}

main().catch(err => console.error('Fatal:', err));
