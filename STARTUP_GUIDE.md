# Distributed Event Stream - Startup Guide

## ✅ Project Fixed

All ES module issues have been resolved. The project is now using ES modules consistently (`import`/`export` instead of `require`/`module.exports`).

### What Was Fixed
- ✅ Added root `package.json` with `"type": "module"`
- ✅ Converted all `require()` to `import` statements
- ✅ Converted all `module.exports` to `export` statements
- ✅ Converted all `export default` to named exports
- ✅ Added `.js` extensions to all import paths
- ✅ Converted `require.main === module` to `import.meta.url` check

## Running the System

### Option 1: Using npm Scripts

```bash
# Terminal 1 - Start Broker
npm run broker

# Terminal 2 - Start Controller
npm run controller

# Terminal 3 - Start Dashboard
npm run dashboard

# Terminal 4 - Run Publisher
npm run publisher

# Terminal 5 - Run Subscriber  
npm run subscriber
```

### Option 2: Direct Node Commands

```bash
# Terminal 1 - Start Broker
node broker/src/index.js

# Terminal 2 - Start Controller
node controller/src/index.js

# Terminal 3 - Start Dashboard
node dashboard/src/index.js

# Terminal 4 - Run Publisher
node examples/publisher.js

# Terminal 5 - Run Subscriber
node examples/subscriber.js
```

## Quick Integration Test

Run a comprehensive automated test:

```bash
npm run test
# or
node examples/quick-test.js
```

This verifies:
- ✅ Broker startup
- ✅ Client connections
- ✅ Message publishing
- ✅ Real-time subscription
- ✅ Message delivery
- ✅ Broker restart
- ✅ Crash recovery

## Expected Output

### Broker Startup
```
{"timestamp":"2025-12-15T...","level":"info","logger":"broker","message":"Starting broker","brokerId":"broker-...",...}
{"timestamp":"2025-12-15T...","level":"info","logger":"tcpServer","message":"TCP server listening",{"host":"0.0.0.0","port":9092}}
```

### Controller Startup
```
{"timestamp":"2025-12-15T...","level":"info","logger":"controller","message":"Controller started"}
{"timestamp":"2025-12-15T...","level":"info","logger":"udpHeartbeatListener","message":"UDP heartbeat listener started"}
```

### Publisher Output
```
{"timestamp":"2025-12-15T...","level":"info","logger":"client","message":"Published message"}
```

### Subscriber Output
```
{"timestamp":"2025-12-15T...","level":"info","logger":"client","message":"Received message"}
```

## Troubleshooting

### Port Already in Use
If you get "Address already in use" errors:

```bash
# Kill existing processes
pkill -f "node broker/src/index.js"
pkill -f "node controller/src/index.js"
pkill -f "node dashboard/src/index.js"

# Then restart
```

### Check System is Working
```bash
# Test broker syntax
node -c broker/src/index.js

# Test controller syntax
node -c controller/src/index.js

# Run quick test
npm run test
```

## Architecture Ports

- **Broker TCP**: 9092 (client connections)
- **Controller Heartbeat (UDP)**: 9091  
- **Controller Election (UDP)**: 9093
- **Controller HTTP API**: 8080
- **Dashboard Web UI**: 8090

## Next Steps

1. Start broker in Terminal 1
2. Start controller in Terminal 2  
3. Start dashboard in Terminal 3
4. Visit `http://localhost:8090` to see the dashboard
5. Run publisher in Terminal 4
6. Run subscriber in Terminal 5
7. Observe real-time pub/sub working!

## File Structure

```
broker/           - Event streaming broker
controller/       - Cluster coordinator
client-sdk/       - Client library
dashboard/        - Web monitoring UI
shared/           - Protocol & utilities
examples/         - Test scripts
scripts/          - Utilities
docs/             - Documentation
```

---

**Status**: ✅ Ready to run!
