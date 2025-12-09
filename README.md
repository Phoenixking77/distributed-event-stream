# Distributed Event Streaming System

A production-grade, Kafka-like event streaming platform built from scratch with raw Node.js.

## Phase 1: Single-Node Core (Complete)

Phase 1 implements a fully functional single-broker system with:
- ✅ Durable commit log with segment rotation
- ✅ Binary protocol over TCP
- ✅ CRC32 checksum verification
- ✅ Real-time pub/sub messaging
- ✅ Offset-based message replay
- ✅ Crash recovery
- ✅ Client SDK

## Architecture

```
┌─────────────────┐         ┌──────────────────┐
│   Client SDK    │ ◄─────► │   Broker Node    │
│                 │   TCP   │                  │
│  - Publisher    │         │  - TCP Server    │
│  - Subscriber   │         │  - Commit Log    │
│                 │         │  - Segment Mgr   │
└─────────────────┘         └──────────────────┘
                                     │
                                     ▼
                            ┌────────────────┐
                            │  Disk Storage  │
                            │  topics/       │
                            │    orders/     │
                            │      seg-0.log │
                            │      seg-1.log │
                            └────────────────┘
```

## Quick Start

### 1. Start the Broker

```bash
node broker/src/index.js
```

The broker will start on `localhost:9092` and store data in `./data/topics/`.

### 2. Run Publisher (in another terminal)

```bash
node examples/publisher.js
```

This will publish 10 test messages to the `orders` topic.

### 3. Run Subscriber (in another terminal)

```bash
node examples/subscriber.js
```

This will subscribe to the `orders` topic and receive messages in real-time.

## Client SDK Usage

### Publishing Messages

```javascript
const { Client } = require('./client-sdk/src');

const client = new Client({ host: 'localhost', port: 9092 });
await client.connect();

const result = await client.publish('orders', {
  orderId: 123,
  amount: 99.99
});

console.log('Published at offset:', result.offset);
client.disconnect();
```

### Subscribing to Topics

```javascript
const { Client } = require('./client-sdk/src');

const client = new Client({ host: 'localhost', port: 9092 });
await client.connect();

await client.subscribe('orders', (msg) => {
  console.log('Received:', msg.message);
}, { offset: 0 });
```

## Configuration

Edit `broker/src/config/default.json`:

```json
{
  "broker": {
    "port": 9092,
    "host": "0.0.0.0",
    "dataDir": "./data"
  },
  "log": {
    "maxSegmentSize": 67108864,
    "syncInterval": 1000
  }
}
```

Or use environment variables:

```bash
BROKER_PORT=9093 BROKER_DATADIR=/var/data node broker/src/index.js
```

## Binary Protocol

### Packet Format

```
[ MAGIC (2 bytes) ]
[ VERSION (1 byte) ]
[ OPCODE (1 byte) ]
[ TIMESTAMP (8 bytes) ]
[ PAYLOAD_LENGTH (4 bytes) ]
[ PAYLOAD (variable) ]
[ CRC32 (4 bytes) ]
```

### Opcodes

- `0x01` PUBLISH - Publish message to topic
- `0x02` SUBSCRIBE - Subscribe to topic
- `0x03` ACK - Acknowledgment
- `0x04` HEARTBEAT - Keep-alive
- `0x05` REPLICATION_PACKET - Replication (Phase 2)

## Storage Format

### Commit Log Structure

```
data/
  topics/
    orders/
      segment-0.log    # First 64MB segment
      segment-1.log    # Second segment
      segment-2.log    # Active segment
```

### Log Entry Format

```
[ MESSAGE_SIZE (4 bytes) ]
[ TIMESTAMP (8 bytes) ]
[ CRC32 (4 bytes) ]
[ MESSAGE (variable) ]
```

## Features

### Durability

- All messages written to disk with fsync
- CRC32 checksums for data integrity
- Automatic recovery on restart

### Performance

- Sequential disk writes
- Segment rotation at 64MB (configurable)
- Zero-copy reads
- Buffer pooling

### Crash Recovery

The broker automatically recovers on restart:
1. Scans existing segment files
2. Validates checksums
3. Resumes from last valid offset

### Offset Management

- Global offset across all segments
- Subscribers can replay from any offset
- Offset tracking per subscriber

## Testing

### Manual Testing

1. Start broker: `node broker/src/index.js`
2. Run publisher: `node examples/publisher.js`
3. Run subscriber: `node examples/subscriber.js`
4. Kill broker (Ctrl+C)
5. Restart broker
6. Verify subscriber receives remaining messages

### Performance Test

```bash
# Run broker
node broker/src/index.js

# In another terminal, flood with messages
node scripts/floodPublisher.js
```

## Project Structure

```
distributed-event-stream/
├── broker/                # Broker server
│   └── src/
│       ├── log/          # Commit log engine
│       ├── server/       # TCP server
│       ├── state/        # Broker state
│       └── index.js      # Entry point
│
├── client-sdk/           # Client SDK
│   └── src/
│       ├── connection/   # TCP client
│       └── client.js     # High-level API
│
├── shared/               # Shared utilities
│   ├── protocol/        # Binary protocol
│   └── utils/           # Logger, config
│
└── examples/            # Example scripts
    ├── publisher.js
    └── subscriber.js
```

## Next: Phase 2

Phase 2 will add:
- Controller node for cluster coordination
- Multi-broker replication
- Leader election (Raft-lite)
- Automatic failover
- ISR (In-Sync Replicas)

## License

MIT

## Author

Production-grade distributed systems implementation