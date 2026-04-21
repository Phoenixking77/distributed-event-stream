# Startup Scripts

This directory contains scripts to start the distributed event streaming system.

## Quick Start

### Start Everything at Once (Recommended)

```bash
node scripts/startAll.js
```

This will start:
- 3 controllers (ports 8080-8082 for HTTP, 9010-9012 for election, 9091-9093 for heartbeat)
- 3 brokers (ports 9092-9094)
- 1 dashboard (port 8090)

### Custom Configuration

```bash
# Start with custom number of controllers and brokers
node scripts/startAll.js --num-controllers 3 --num-brokers 3

# Start without dashboard
node scripts/startAll.js --no-dashboard

# Custom ports
node scripts/startAll.js \
  --base-http-port 8080 \
  --base-election-port 9010 \
  --base-data-port 9092 \
  --dashboard-port 8090
```

## Individual Component Scripts

### Start Controllers Only

```bash
node scripts/startControllers.js --num-controllers 3
```

Options:
- `--num-controllers N` - Number of controllers to start (default: 3)
- `--base-http-port PORT` - Starting HTTP port (default: 8080)
- `--base-election-port PORT` - Starting election port (default: 9010)
- `--base-heartbeat-port PORT` - Starting heartbeat port (default: 9091)
- `--host HOST` - Host address (default: localhost)

### Start Brokers Only

```bash
node scripts/startBrokers.js --num-brokers 3
```

Options:
- `--num-brokers N` - Number of brokers to start (default: 3)
- `--base-data-port PORT` - Starting data port (default: 9092)
- `--controller-host HOST` - Controller host (default: localhost)
- `--controller-port PORT` - Controller HTTP port (default: 8080)
- `--controller-heartbeat-port PORT` - Controller heartbeat port (default: 9091)

## Stopping the System

Press `Ctrl+C` in the terminal where the script is running. All processes will be gracefully shut down.

## Verification

After starting, verify the system is working:

```bash
# Check controller health
curl http://localhost:8080/health

# Check cluster leader
curl http://localhost:8080/cluster/leader

# Check all nodes
curl http://localhost:8080/cluster/nodes

# Access dashboard
open http://localhost:8090
```

## Troubleshooting

### Port Already in Use

If you get port conflicts, either:
1. Stop existing processes: `pkill -f "node.*(controller|broker|dashboard)"`
2. Use different ports with the command-line options

### Controllers Not Electing Leader

- Ensure at least 3 controllers for quorum (or use odd number)
- Check that controllers can reach each other on election ports
- Verify no firewall blocking UDP ports

### Brokers Not Registering

- Ensure controllers are running first
- Check that `CONTROLLER_HOST` and `CONTROLLER_PORT` are correct
- Verify broker can reach controller HTTP API
