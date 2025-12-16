# Distributed Event Stream - Client SDK Documentation

A production-grade Node.js client SDK for the Distributed Event Stream platform with features inspired by Apache Kafka.

## Features

- **High-Performance Publishing**: Async/await API with backpressure handling
- **Consumer Groups**: Auto-commit with offset management
- **Batch Publishing**: Efficient bulk publishing with configurable timeouts
- **Connection Pooling**: Multi-broker support with intelligent load balancing
- **Automatic Reconnection**: Exponential backoff with circuit breaker pattern
- **Compression Support**: gzip, deflate compression options
- **Message Encoding/Decoding**: Robust protocol handling with validation
- **Comprehensive Metrics**: Built-in monitoring and observability
- **Error Handling & Retry Logic**: Intelligent retry policies for transient failures
- **TypeScript Support**: Full type definitions included

## Installation

```bash
npm install distributed-event-stream
```

## Quick Start

### Basic Publisher

```javascript
import { createClient } from 'distributed-event-stream';

const client = createClient({
  host: 'localhost',
  port: 9092
});

await client.connect();

// Publish a message
const result = await client.publish('orders', {
  orderId: '12345',
  amount: 99.99,
  timestamp: Date.now()
});

console.log(`Published to offset: ${result.offset}`);

await client.disconnect();
```

### Basic Subscriber

```javascript
import { createClient } from 'distributed-event-stream';

const client = createClient({
  host: 'localhost',
  port: 9092
});

await client.connect();

// Subscribe to topic
await client.subscribe('orders', (message) => {
  console.log('Received message:', message);
  // message.offset, message.timestamp, message.message
});

// Keep running to receive messages
await new Promise(() => {}); // Never resolves
```

## Advanced Usage

### Batch Publishing for High Throughput

```javascript
import { createClient, createBatchPublisher } from 'distributed-event-stream';

const client = createClient({ host: 'localhost', port: 9092 });
await client.connect();

const publisher = createBatchPublisher(client, {
  batchSize: 1000,          // Flush every 1000 messages
  batchTimeoutMs: 5000,     // Or every 5 seconds
  compression: 'gzip'       // Compress batches
});

// Add messages - they're buffered and sent in batches
for (let i = 0; i < 10000; i++) {
  await publisher.add('events', {
    id: i,
    timestamp: Date.now(),
    priority: i % 10 // Processed by priority
  }, { priority: i % 10 });
}

// Wait for all pending messages
await publisher.drain();

// Get publishing metrics
console.log(publisher.getMetrics());
// { batchesPublished: 10, messagesPublished: 10000, ... }
```

### Consumer Groups with Auto-Commit

```javascript
import { createClient, createConsumerGroup } from 'distributed-event-stream';

const client = createClient({ host: 'localhost', port: 9092 });
await client.connect();

const consumerGroup = createConsumerGroup(client, {
  groupId: 'order-processor',
  enableAutoCommit: true,
  autoCommitIntervalMs: 5000,
  autoOffsetReset: 'latest'
});

// Register topics and subscribe
consumerGroup.registerTopic('orders', 0);

await client.subscribe('orders', (message) => {
  console.log('Processing order:', message.message);
  
  // Offset is auto-committed every 5 seconds
  // Consumer group tracks position in topic
  
  consumerGroup.updateOffset('orders', message.offset);
}, { groupId: 'order-processor' });

// Get consumer state
const state = consumerGroup.getState();
console.log('Current lag:', state['orders'].lag);
```

### Multi-Broker Connection Pooling

```javascript
import { createClientPool } from 'distributed-event-stream';

const pool = createClientPool([
  { host: 'broker1.example.com', port: 9092 },
  { host: 'broker2.example.com', port: 9092 },
  { host: 'broker3.example.com', port: 9092 }
], {
  loadBalancingStrategy: 'least-connections', // or 'round-robin', 'random'
  clientOptions: {
    maxRetries: 5,
    connectTimeout: 5000
  }
});

await pool.connect();

// Publishes automatically distribute across brokers
for (let i = 0; i < 1000; i++) {
  await pool.publish('events', { id: i });
}

// Health check
const health = pool.getHealthStatus();
console.log(health);
// {
//   'broker1.example.com:9092': { connected: true, inflightRequests: 2, ... },
//   'broker2.example.com:9092': { connected: true, inflightRequests: 1, ... },
//   ...
// }

await pool.disconnect();
```

### Custom Error Handling & Retry

```javascript
import { createClient, ErrorHandler, RetryPolicy } from 'distributed-event-stream';

const errorHandler = new ErrorHandler({
  maxAttempts: 3,
  initialDelayMs: 100,
  maxDelayMs: 10000,
  backoffMultiplier: 2
});

const client = createClient({ host: 'localhost', port: 9092 });
await client.connect();

try {
  await errorHandler.handleWithRetry(async () => {
    return await client.publish('critical-topic', { data: '...' });
  }, { operation: 'publish', topic: 'critical-topic' });
} catch (err) {
  const formatted = errorHandler.formatError(err);
  console.error('Final error:', formatted);
  // { category: 'NETWORK', message: '...', timestamp: '...', ... }
}

const metrics = errorHandler.getMetrics();
console.log(`Retry success rate: ${metrics.retrySuccessRate * 100}%`);
```

### Message Compression

```javascript
import { MessageEncoder, MessageDecoder } from 'distributed-event-stream';

// Encode with compression
const message = { 
  largeData: 'x'.repeat(100000) 
};

const encoded = await MessageEncoder.encode(message, {
  compression: 'gzip',
  key: 'msg-key-1'
});

console.log(`Original: ${JSON.stringify(message).length} bytes`);
console.log(`Compressed: ${encoded.length} bytes`);

// Decode with automatic decompression
const decoded = await MessageDecoder.decode(encoded);
console.log(decoded);
// { message: {...}, key: 'msg-key-1', compression: 'gzip', decompressed: true }
```

### Batch Encoding/Decoding

```javascript
import { MessageEncoder, MessageDecoder } from 'distributed-event-stream';

const messages = [
  { id: 1, data: 'message 1' },
  { id: 2, data: 'message 2' },
  { id: 3, data: 'message 3' }
];

// Batch encode
const batch = await MessageEncoder.encodeBatch(messages, {
  compression: 'gzip'
});

// Batch decode
const decoded = await MessageDecoder.decodeBatch(batch);
console.log(decoded.length); // 3
```

## Configuration Options

### Client Options

```javascript
const client = createClient({
  // Connection
  host: 'localhost',                    // Broker host
  port: 9092,                           // Broker port
  
  // Reconnection strategy
  maxRetries: 10,                       // Max reconnection attempts
  initialRetryDelay: 100,               // Initial retry delay (ms)
  maxRetryDelay: 30000,                 // Max retry delay (ms)
  backoffMultiplier: 2,                 // Exponential backoff multiplier
  
  // Timeouts
  connectTimeout: 5000,                 // Connection timeout (ms)
  requestTimeout: 10000,                // Request timeout (ms)
  
  // Backpressure
  maxInflightRequests: 100,             // Max concurrent requests
  
  // Consumer groups
  offsetCommitInterval: 5000,           // Auto-commit interval (ms)
  
  // Logging
  logger: customLogger                  // Custom logger instance
});
```

### Batch Publisher Options

```javascript
const publisher = createBatchPublisher(client, {
  batchSize: 100,                      // Trigger flush at N messages
  batchTimeoutMs: 1000,                // Trigger flush after N ms
  compression: 'gzip'                  // Compression algorithm
});
```

### Consumer Group Options

```javascript
const group = createConsumerGroup(client, {
  groupId: 'my-group',                 // Consumer group ID
  enableAutoCommit: true,              // Auto-commit offsets
  autoCommitIntervalMs: 5000,          // Commit interval (ms)
  autoOffsetReset: 'latest'            // 'earliest', 'latest', or 'none'
});
```

### Client Pool Options

```javascript
const pool = createClientPool(brokers, {
  loadBalancingStrategy: 'round-robin', // 'round-robin', 'least-connections', 'random'
  clientOptions: {                      // Passed to each client
    maxRetries: 5,
    connectTimeout: 5000
  }
});
```

## Monitoring & Metrics

### Client Metrics

```javascript
const metrics = client.getMetrics();
console.log(metrics);
// {
//   messagesPublished: 1000,
//   messagesBytesPublished: 50000,
//   messagesReceived: 500,
//   messagesBytesReceived: 25000,
//   reconnections: 0,
//   errors: 1,
//   offsetCommits: 50,
//   offsetCommitErrors: 0,
//   subscriberCount: 2,
//   inflightRequests: 5,
//   subscriptionCount: 2,
//   consumerGroupCount: 1,
//   uptime: 3600000
// }
```

### Batch Publisher Metrics

```javascript
const metrics = publisher.getMetrics();
console.log(metrics);
// {
//   batchesPublished: 10,
//   messagesPublished: 1000,
//   bytesPublished: 50000,
//   errors: 0,
//   pendingMessages: 25
// }
```

### Consumer Group Metrics

```javascript
const metrics = group.getMetrics();
console.log(metrics);
// {
//   offsetsCommitted: 100,
//   offsetCommitErrors: 0,
//   messagesProcessed: 5000,
//   rebalances: 0,
//   topicsCount: 2,
//   pendingCommits: 10,
//   rebalancing: false
// }
```

### Connection Pool Health

```javascript
const health = pool.getHealthStatus();
console.log(health);
// {
//   'broker1:9092': {
//     connected: true,
//     inflightRequests: 5,
//     metrics: { messagesPublished: 1000, ... }
//   },
//   'broker2:9092': {
//     connected: true,
//     inflightRequests: 3,
//     metrics: { messagesPublished: 950, ... }
//   }
// }
```

## Error Handling

The SDK provides intelligent error handling with automatic retry for transient failures:

### Transient Errors (Retried)
- `ECONNREFUSED` - Connection refused
- `ECONNRESET` - Connection reset
- `ETIMEDOUT` - Timeout
- `EHOSTUNREACH` - Host unreachable
- Network timeouts

### Permanent Errors (Not Retried)
- `VALIDATION` - Invalid parameters
- `CIRCUIT_BREAK` - Circuit breaker open
- Missing required fields

## Best Practices

1. **Always use consumer groups** for reliable message processing
2. **Use batch publishing** for high-throughput scenarios
3. **Enable auto-commit** to simplify offset management
4. **Monitor metrics** regularly for health checks
5. **Use connection pools** for multi-broker setups
6. **Set appropriate timeouts** based on network conditions
7. **Handle errors gracefully** with custom error handlers
8. **Compress large messages** to reduce network bandwidth

## Events

### Client Events

```javascript
client.on('connected', () => {
  console.log('Connected to broker');
});

client.on('disconnected', () => {
  console.log('Disconnected from broker');
});

client.on('error', (err) => {
  console.error('Client error:', err);
});
```

## License

MIT
