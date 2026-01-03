import { createConnection } from 'net';

const socket = createConnection(9092, 'localhost');

socket.on('connect', () => {
  console.log('Connected');
  // Send some bytes
  socket.write(Buffer.from([0xda, 0x7a, 0x01, 0x01]));
  setTimeout(() => socket.destroy(), 2000);
});

socket.on('data', (data) => {
  console.log('Received data:', data.toString('hex'));
});

socket.on('close', () => {
  console.log('Closed');
});

socket.on('error', (err) => {
  console.error('Error:', err);
});
