# Pub/Sub Examples

Example scripts for testing the distributed event streaming system.

## Prerequisites

Make sure the system is running:
```bash
./start.sh
# or
node scripts/startAll.js
```

## Examples

### 1. Simple Publisher

Publishes messages to a topic:

```bash
node examples/publisher.js [topic] [count] [delay]
```

**Examples:**
```bash
# Publish 10 messages to 'test-topic' with 1 second delay
node examples/publisher.js test-topic 10 1000

# Publish 5 messages to 'orders' with 500ms delay
node examples/publisher.js orders 5 500

# Use custom broker
BROKER_HOST=localhost BROKER_PORT=9093 node examples/publisher.js
```

### 2. Simple Subscriber

Subscribes to a topic and prints received messages:

```bash
node examples/subscriber.js [topic] [offset] [groupId]
```

**Examples:**
```bash
# Subscribe from beginning (offset 0)
node examples/subscriber.js test-topic 0

# Subscribe from offset 5
node examples/subscriber.js test-topic 5

# Subscribe with consumer group
node examples/subscriber.js orders 0 my-group

# Use custom broker
BROKER_HOST=localhost BROKER_PORT=9093 node examples/subscriber.js
```

### 3. Test Pub/Sub

Publishes and subscribes in the same script to verify end-to-end:

```bash
node examples/test-pubsub.js
```

This script:
- Connects to broker
- Subscribes to 'test-topic'
- Publishes 5 messages
- Shows received messages
- Displays summary

### 4. Consumer Group Example

Demonstrates consumer groups with auto-commit:

```bash
node examples/consumer-group.js [topic] [groupId]
```

**Examples:**
```bash
# Subscribe to 'orders' with consumer group 'order-processors'
node examples/consumer-group.js orders order-processors

# Default topic and group
node examples/consumer-group.js
```

## Testing Scenarios

### Scenario 1: Basic Pub/Sub

Terminal 1 (Subscriber):
```bash
node examples/subscriber.js test-topic 0
```

Terminal 2 (Publisher):
```bash
node examples/publisher.js test-topic 10 1000
```

### Scenario 2: Multiple Subscribers

Terminal 1:
```bash
node examples/subscriber.js orders 0
```

Terminal 2:
```bash
node examples/subscriber.js orders 0
```

Terminal 3 (Publisher):
```bash
node examples/publisher.js orders 20 500
```

### Scenario 3: Consumer Groups

Terminal 1:
```bash
node examples/consumer-group.js orders group-1
```

Terminal 2:
```bash
node examples/consumer-group.js orders group-1
```

Terminal 3 (Publisher):
```bash
node examples/publisher.js orders 50 200
```

### Scenario 4: Offset Replay

Terminal 1 (Publisher):
```bash
node examples/publisher.js events 100 100
```

Terminal 2 (Subscriber - start from offset 50):
```bash
node examples/subscriber.js events 50
```

## Environment Variables

- `BROKER_HOST` - Broker hostname (default: localhost)
- `BROKER_PORT` - Broker port (default: 9092)

## Tips

1. **Start subscriber first** - Subscribers can receive messages published after they connect
2. **Use offsets** - Start from offset 0 to get all messages, or use a specific offset to replay
3. **Consumer groups** - Multiple consumers in the same group share the load
4. **Multiple brokers** - Test with different broker ports (9092, 9093, 9094) for load balancing
