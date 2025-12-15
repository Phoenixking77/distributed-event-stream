# API Reference

## Client SDK API

The client SDK provides a production-grade API for publishing and subscribing to topics.

### Installation

```javascript
import { Client } from './client-sdk/src/client.js';
```

### Client Constructor

```javascript
const client = new Client({
  host: 'localhost',              // Broker host
  port: 9092,                     // Broker port
  logger: logger,                 // Logger instance (optional)
  maxRetries: 10,                 // Max reconnection attempts
  initialRetryDelay: 100,         // Initial retry delay in ms
  maxRetryDelay: 30000,           // Max retry delay in ms
  backoffMultiplier: 2,           // Exponential backoff multiplier
  connectTimeout: 5000,           // Connection timeout in ms
  requestTimeout: 10000,          // Request timeout in ms
  maxInflightRequests: 100,       // Max concurrent requests
  offsetCommitInterval: 5000      // Offset commit interval in ms
});
```

### Connection Management

#### `await client.connect()`

Establish connection to broker with automatic reconnection on failure.

```javascript
await client.connect();
// Emits: 'connected'
```

**Parameters**: None

**Returns**: `Promise<void>`

**Throws**: `Error` if connection fails after max retries

---

#### `await client.disconnect()`

Gracefully shutdown client and flush pending offsets.

```javascript
await client.disconnect();
// Emits: 'disconnected'
```

**Parameters**: None

**Returns**: `Promise<void>`

---

### Publishing

#### `await client.publish(topic, message, options)`

Publish a message to a topic.

```javascript
const result = await client.publish('orders', {
  orderId: 12345,
  amount: 99.99,
  status: 'pending'
}, {
  key: '12345',           // Partition key (optional)
  partition: 0,           // Target partition (optional)
  compression: 'gzip'     // Compression type (optional)
});

// result: { topic, offset, timestamp, partition }
```

**Parameters**:

| Name | Type | Description |
|------|------|-------------|
| `topic` | `string` | Topic name |
| `message` | `*` | Message payload (will be JSON-serialized) |
| `options` | `Object` | Publishing options |
| `options.key` | `string` | Optional partition key |
| `options.partition` | `number` | Target partition number |
| `options.compression` | `string` | Compression algorithm (none, gzip, snappy) |

**Returns**: `Promise<PublishResult>`

```typescript
interface PublishResult {
  topic: string;
  offset: number;
  timestamp: number;
  partition: string;
}
```

**Throws**: 
- `Error` if not connected
- `Error` if broker rejects publish

**Examples**:

```javascript
// Simple publish
await client.publish('events', { type: 'click', x: 100, y: 200 });

// With partition key (messages with same key go to same partition)
await client.publish('orders', order, { key: order.customerId });

// With compression
await client.publish('logs', largePayload, { compression: 'gzip' });
```

---

### Subscribing

#### `await client.subscribe(topic, handler, options)`

Subscribe to a topic and receive messages.

```javascript
const subscriberId = await client.subscribe('orders', (message) => {
  console.log('Received:', message);
}, {
  offset: 0,                      // Start from offset
  groupId: 'order-processor'      // Consumer group (optional)
});
```

**Parameters**:

| Name | Type | Description |
|------|------|-------------|
| `topic` | `string` | Topic name |
| `handler` | `Function` | Callback for each message |
| `options` | `Object` | Subscribe options |
| `options.offset` | `number` | Starting offset (default: 0) |
| `options.groupId` | `string` | Consumer group ID |

**Handler Signature**:

```javascript
(message) => {
  // message.topic: string
  // message.offset: number
  // message.timestamp: number
  // message.message: *
  // message.partition: string
}
```

**Returns**: `Promise<string>` - subscriber ID

**Throws**:
- `Error` if not connected
- `Error` if broker rejects subscribe

**Examples**:

```javascript
// Basic subscription
await client.subscribe('orders', (msg) => {
  console.log(`Order offset ${msg.offset}:`, msg.message);
});

// With consumer group (auto-commits offsets)
await client.subscribe('events', (msg) => {
  processEvent(msg.message);
}, {
  groupId: 'event-processor'
});

// From specific offset
await client.subscribe('logs', (msg) => {
  console.log(msg.message);
}, {
  offset: 1000
});
```

---

### Consumer Groups

#### `await client.commitOffset(groupId, topic, offset)`

Commit an offset for a consumer group.

```javascript
await client.commitOffset('order-processor', 'orders', 42);
```

**Parameters**:

| Name | Type | Description |
|------|------|-------------|
| `groupId` | `string` | Consumer group ID |
| `topic` | `string` | Topic name |
| `offset` | `number` | Offset to commit |

**Returns**: `Promise<void>`

**Throws**: `Error` if commit fails

**Note**: Offsets auto-commit when using consumer group; manual commits are optional.

---

#### `client.getConsumerGroupState(groupId)`

Get current state of a consumer group.

```javascript
const state = client.getConsumerGroupState('order-processor');
// { topic: 'orders', committedOffset: 42, lastCommit: 1700000000 }
```

**Parameters**:

| Name | Type | Description |
|------|------|-------------|
| `groupId` | `string` | Consumer group ID |

**Returns**: `Object | null` - Consumer group state or null if not found

---

### Flow Control

#### `client.pauseConsume(topic)`

Pause consuming messages from a topic.

```javascript
client.pauseConsume('orders');
```

---

#### `client.resumeConsume(topic)`

Resume consuming messages from a topic.

```javascript
client.resumeConsume('orders');
```

---

### Metrics

#### `client.getMetrics()`

Get client performance metrics.

```javascript
const metrics = client.getMetrics();
// {
//   messagesPublished: 1000,
//   messagesBytesPublished: 1024000,
//   messagesReceived: 500,
//   messagesBytesReceived: 512000,
//   reconnections: 0,
//   errors: 0,
//   offsetCommits: 50,
//   offsetCommitErrors: 0,
//   subscriberCount: 1,
//   inflightRequests: 5,
//   subscriptionCount: 1,
//   consumerGroupCount: 1,
//   uptime: 1700000000
// }
```

**Returns**: `Object` - Metrics snapshot

---

### Events

The client extends `EventEmitter` and emits:

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

---

## Dashboard API

The dashboard exposes REST endpoints for monitoring.

### Base URL

```
http://localhost:8090
```

---

### GET /health

Get overall cluster health status.

**Response** (200 OK):

```json
{
  "status": "healthy",
  "cluster": {
    "alive": 3,
    "total": 3,
    "quorumSatisfied": true
  },
  "uptime": 86400000
}
```

---

### GET /cluster/state

Get detailed cluster state.

**Response** (200 OK):

```json
{
  "cluster": {
    "leader": "broker-0",
    "followers": ["broker-1", "broker-2"],
    "term": 5,
    "alive": 3
  },
  "stats": {
    "uptime": 86400000,
    "messagesPublished": 1000000,
    "bytesPublished": 1073741824
  },
  "metrics": {
    "cpuUsage": 0.15,
    "memoryUsage": 512000000,
    "diskUsage": 5368709120
  }
}
```

---

### GET /metrics/node

Get broker node metrics.

**Query Parameters**:

| Name | Type | Description |
|------|------|-------------|
| `nodeId` | `string` | Broker ID (optional) |

**Response** (200 OK):

```json
{
  "nodeId": "broker-0",
  "role": "leader",
  "cpu": 0.15,
  "memory": 512000000,
  "disk": 5368709120,
  "topicCount": 10,
  "messageCount": 1000000,
  "bytesStored": 1073741824,
  "replicationLag": 0,
  "lastHeartbeat": 1700000000
}
```

---

### GET /metrics/topic/:topic

Get topic metrics.

**Path Parameters**:

| Name | Type | Description |
|------|------|-------------|
| `topic` | `string` | Topic name |

**Response** (200 OK):

```json
{
  "topic": "orders",
  "leader": "broker-0",
  "followers": ["broker-1", "broker-2"],
  "isr": ["broker-0", "broker-1", "broker-2"],
  "stats": {
    "messageCount": 100000,
    "bytesStored": 107374182,
    "latestOffset": 99999,
    "replicationLag": {
      "broker-1": { "offset": 99950, "lagMs": 50 },
      "broker-2": { "offset": 99960, "lagMs": 40 }
    }
  }
}
```

---

### GET /topic/:topic/lag

Get replication lag for topic.

**Path Parameters**:

| Name | Type | Description |
|------|------|-------------|
| `topic` | `string` | Topic name |

**Response** (200 OK):

```json
{
  "topic": "orders",
  "leader": "broker-0",
  "followers": [
    {
      "nodeId": "broker-1",
      "state": "in-sync",
      "offset": 99950,
      "leaderOffset": 100000,
      "lagMs": 50
    },
    {
      "nodeId": "broker-2",
      "state": "in-sync",
      "offset": 99960,
      "leaderOffset": 100000,
      "lagMs": 40
    }
  ]
}
```

---

## Controller API

The controller exposes HTTP API on port 8080.

### GET /health

Get controller health status.

---

### GET /cluster

Get cluster metadata.

---

### GET /cluster/stats

Get cluster statistics.

---

### GET /topics

List all topics with metadata.

---

### GET /topics/:topic

Get specific topic metadata.

---

## Error Handling

All APIs follow consistent error response format:

**Error Response** (4xx, 5xx):

```json
{
  "error": "Topic not found",
  "code": "TOPIC_NOT_FOUND",
  "timestamp": 1700000000,
  "requestId": "abc-123"
}
```

---

## Rate Limiting

The client implements backpressure:

- **Max inflight requests**: 100 (configurable)
- **Client waits** if limit reached
- Prevents overwhelming broker

---

## Timeouts

| Operation | Default | Configurable |
|-----------|---------|--------------|
| Connect | 5s | `connectTimeout` |
| Request | 10s | `requestTimeout` |
| Retry | 30s max | `maxRetryDelay` |
| Offset commit | 5s | `offsetCommitInterval` |

---

## Best Practices

### 1. Connection Management

```javascript
// Reuse single client across application
const client = new Client({ host: 'broker.example.com' });
await client.connect();
// Use across entire app
// Disconnect gracefully on shutdown
process.on('SIGTERM', () => client.disconnect());
```

### 2. Error Handling

```javascript
client.on('error', (err) => {
  logger.error('Client error:', err);
  // Implement alerting
});

try {
  await client.publish('orders', message);
} catch (err) {
  logger.error('Publish failed:', err);
  // Implement retry logic
}
```

### 3. Consumer Groups

```javascript
// Multiple consumers in same group split messages
const groupId = 'order-processor';
await client.subscribe('orders', processOrder, { groupId });
// Offset auto-commits after processing
```

### 4. Backpressure

```javascript
// Client automatically throttles if too many inflight requests
// Configure based on broker capacity
const client = new Client({
  maxInflightRequests: 50  // Lower for memory-constrained envs
});
```

### 5. Monitoring

```javascript
setInterval(() => {
  const metrics = client.getMetrics();
  prometheus.gauges['client_published_total'].set(metrics.messagesPublished);
  prometheus.gauges['client_lag_sum'].set(metrics.inflightRequests);
}, 60000);
```

---

## Changelog

### v2.0.0

- ✅ Consumer groups with offset commits
- ✅ Exponential backoff reconnection
- ✅ Backpressure handling
- ✅ Comprehensive metrics
- ✅ Event emitters
- ✅ Graceful shutdown
