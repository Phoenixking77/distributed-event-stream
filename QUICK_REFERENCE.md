# Quick Reference Guide

## 🚀 Getting Started (5 minutes)

### 1. Start Cluster (3 terminals)

**Terminal 1 - Controller & all services**:
```bash
node scripts/clusterStart.js --num-brokers 2
# Starts controller, 2 brokers, and dashboard
# Press Ctrl+C to stop everything
```

OR manually in separate terminals:

**Terminal 1 - Controller**:
```bash
npm run controller
```

**Terminal 2 - Broker**:
```bash
npm run broker
```

**Terminal 3 - Dashboard**:
```bash
npm run dashboard
```

### 2. View Dashboard
```
Open http://localhost:8090 in browser
```

### 3. Publish & Subscribe (2 more terminals)

**Terminal 4 - Subscriber**:
```bash
node examples/subscriber.js
# Waits for messages on 'test-topic'
```

**Terminal 5 - Publisher**:
```bash
node examples/publisher.js
# Publishes messages continuously
```

---

## 📊 Benchmarking

### Quick Benchmark (30 seconds)
```bash
npm run benchmark -- --mode throughput --duration 30
```

### Full Benchmark Suite (all modes)
```bash
# Throughput test
npm run benchmark -- --mode throughput --duration 30 --batch-size 1000

# Latency test
npm run benchmark -- --mode latency --samples 10000

# Sustained load test
npm run benchmark -- --mode sustained --duration 300 --connections 5

# Subscribe performance test
npm run benchmark -- --mode subscribe --duration 30
```

### Flood Publisher (stress test)
```bash
# 50,000 msg/s for 5 minutes with 10 connections
node scripts/floodPublisher.js --rate 50000 --duration 300 --connections 10
```

---

## 🔍 Debugging & Inspection

### List All Topics
```bash
node scripts/topicInspector.js

# Output:
# 📁 Topic: orders
#    Segments: 3
#    Total Size: 256 MB
# 📁 Topic: events
#    Segments: 1
#    Total Size: 64 MB
```

### View Topic Messages
```bash
# View first 20 messages in segment 0
node scripts/topicInspector.js --topic orders --segment 0 --limit 20

# Output:
# Message 1:
#   Offset:     0
#   Timestamp:  2024-01-15T10:30:45.123Z
#   Size:       256 bytes
#   Checksum:   a3f2d8c9
#   Payload:    {"orderId": 123, ...}
```

---

## 🗑️ Storage Management

### Check Storage Usage
```bash
node scripts/cleanupLogs.js --mode list

# Output:
# 📁 Topic: orders
#    Segments: 5
#    Total Size: 512 MB
# 📁 Topic: events
#    Segments: 2
#    Total Size: 128 MB
# Total: 640 MB
```

### Delete Old Segments (DRY RUN)
```bash
node scripts/cleanupLogs.js --mode delete-old --days 7

# Output: Shows what WOULD be deleted (no actual deletion)
```

### Delete Old Segments (EXECUTE)
```bash
node scripts/cleanupLogs.js --mode delete-old --days 7 --no-dry-run

# ⚠️ Confirms before deletion
```

### Delete Entire Topic
```bash
node scripts/cleanupLogs.js --mode delete-topic --topic old-events --no-dry-run

# Permanently deletes the topic
```

---

## 💻 Using the Client SDK

### Basic Example
```javascript
import { Client } from './client-sdk/src/client.js';

const client = new Client({
  host: 'localhost',
  port: 9092
});

// Connect
await client.connect();

// Publish
const result = await client.publish('orders', {
  orderId: 123,
  amount: 99.99
});
console.log(`Published at offset ${result.offset}`);

// Subscribe
await client.subscribe('orders', (message) => {
  console.log('Received:', message.message);
});

// Disconnect gracefully
await client.disconnect();
```

### Consumer Groups
```javascript
// Multiple subscribers in same group share messages
const groupId = 'order-processor';

await client.subscribe('orders', processOrder, {
  groupId: groupId
});

// Offset auto-commits after processing
// If app crashes, resumes from last committed offset
```

### Advanced Features
```javascript
// Publish with partition key
await client.publish('orders', order, {
  key: order.customerId  // Same customer → same partition
});

// Manual offset commit
await client.commitOffset('order-processor', 'orders', 1000);

// Pause/resume consuming
client.pauseConsume('orders');
client.resumeConsume('orders');

// Get metrics
const metrics = client.getMetrics();
console.log(`Messages published: ${metrics.messagesPublished}`);
console.log(`Messages received: ${metrics.messagesReceived}`);
console.log(`Reconnections: ${metrics.reconnections}`);
```

---

## 📡 Dashboard API

### Health Check
```bash
curl http://localhost:8090/health

# Response:
# { "status": "healthy", "cluster": { "alive": 3, "total": 3 } }
```

### Cluster State
```bash
curl http://localhost:8090/cluster/state

# Response:
# { "cluster": { "leader": "broker-0" }, ... }
```

### Topic Metrics
```bash
curl http://localhost:8090/metrics/topic/orders

# Response:
# { "topic": "orders", "messageCount": 1000000, ... }
```

### Topic Replication Lag
```bash
curl http://localhost:8090/topic/orders/lag

# Response:
# { "topic": "orders", "followers": [ ... ], "isr": [...] }
```

---

## 🔧 Common Operations

### Restart Cluster
```bash
# Kill all processes (Ctrl+C on clusterStart)
# Then start again
node scripts/clusterStart.js --num-brokers 3
```

### Add New Broker
```bash
# Update --num-brokers
node scripts/clusterStart.js --num-brokers 4
```

### Monitor Real-Time
```bash
# Terminal 1: Watch dashboard
# http://localhost:8090

# Terminal 2: Watch logs
tail -f logs/broker.log | grep -E "(published|replicated|error)"

# Terminal 3: Monitor CPU/Memory
watch -n 1 'ps aux | grep node'
```

### Test Failover
```bash
# 1. Start 3-broker cluster
node scripts/clusterStart.js --num-brokers 3

# 2. In another terminal, start publisher
node examples/floodPublisher.js --rate 1000

# 3. Kill leader broker (Ctrl+C)
# Watch dashboard - new leader should be elected in <5 seconds

# 4. Publisher continues working (fault tolerance!)
```

---

## 📈 Performance Tuning

### Increase Throughput
```javascript
// Larger batch size
npm run benchmark -- --batch-size 5000 --duration 30

// More connections
npm run benchmark -- --connections 20

// Larger messages
npm run benchmark -- --message-size 8192
```

### Reduce Latency
```javascript
// Smaller batches (more latency, less throughput)
npm run benchmark -- --mode latency --batch-size 1

// Dedicated connection
const client = new Client({
  maxInflightRequests: 1  // Lower = lower latency, lower throughput
});
```

### Resource Usage
```bash
# Monitor during benchmark
watch -n 1 'ps aux | grep node | grep -v grep'

# Check disk space
df -h data/

# Monitor network
nethogs -d 5
```

---

## 🐛 Troubleshooting Quick Reference

| Problem | Solution |
|---------|----------|
| Port already in use | `lsof -i :9092` then `kill -9 <PID>` |
| Broker won't start | Check `logs/broker.log`, verify ports 9092, 8080, 9091 |
| No replication | Check controller is running, brokers can reach it |
| Slow throughput | Reduce `--batch-size`, check network, increase `--connections` |
| High latency | Check CPU usage, disk I/O, network latency |
| Out of disk | Run `node scripts/cleanupLogs.js --mode delete-old --days 7 --no-dry-run` |
| Messages lost | Enable replication: `--num-brokers 3` for fault tolerance |

---

## 📚 Documentation Links

- **Full API Reference**: [docs/api-reference.md](docs/api-reference.md)
- **Deployment Guide**: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)
- **Architecture Deep Dive**: [docs/architecture.md](docs/architecture.md)
- **Protocol Specification**: [docs/protocol-spec.md](docs/protocol-spec.md)
- **Replication Design**: [docs/replication-design.md](docs/replication-design.md)

---

## 🎯 Quick Demo Script

Create `demo.sh`:

```bash
#!/bin/bash

echo "🚀 Starting Distributed Event Stream Demo"
echo ""

# Terminal 1: Cluster
echo "Terminal 1: Starting cluster..."
node scripts/clusterStart.js --num-brokers 2 &
CLUSTER_PID=$!
sleep 5

# Terminal 2: Benchmark
echo ""
echo "Terminal 2: Running benchmark..."
npm run benchmark -- --mode throughput --duration 30

# Cleanup
kill $CLUSTER_PID

echo ""
echo "✅ Demo complete!"
```

Run:
```bash
bash demo.sh
```

---

## 💡 Tips & Tricks

### Monitor Throughput Live
```bash
watch -n 1 'curl -s http://localhost:8090/metrics/topic/test-topic | jq .stats.messageCount'
```

### Follow Broker Logs with Colors
```bash
tail -f logs/broker.log | grep -E --color=auto "(published|ERROR|warning)"
```

### Count Messages in Topic
```bash
node scripts/topicInspector.js --topic orders | grep "Messages:"
```

### Estimate Disk Usage
```bash
node scripts/cleanupLogs.js --mode list | grep "Total Size:"
```

### Health Check Loop
```bash
while true; do
  clear
  echo "Cluster Health:"
  curl -s http://localhost:8090/health | jq .
  sleep 5
done
```

---

**Happy streaming! 🎉**
