#!/usr/bin/env node
'use strict';

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

// Parse command line arguments
const args = process.argv.slice(2);
let numControllers = 3;
let numBrokers = 3;
let startDashboard = true;
let baseHttpPort = 8080;
let baseElectionPort = 9010;
let baseHeartbeatPort = 9091;
let baseDataPort = 9092;
let dashboardPort = 8090;
let host = 'localhost';

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--num-controllers' && args[i + 1]) {
    numControllers = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--num-brokers' && args[i + 1]) {
    numBrokers = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--no-dashboard') {
    startDashboard = false;
  } else if (args[i] === '--base-http-port' && args[i + 1]) {
    baseHttpPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--base-election-port' && args[i + 1]) {
    baseElectionPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--base-heartbeat-port' && args[i + 1]) {
    baseHeartbeatPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--base-data-port' && args[i + 1]) {
    baseDataPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--dashboard-port' && args[i + 1]) {
    dashboardPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--host' && args[i + 1]) {
    host = args[i + 1];
    i++;
  }
}

const processes = [];
const controllerScript = join(projectRoot, 'controller', 'src', 'index.js');
const brokerScript = join(projectRoot, 'broker', 'src', 'index.js');
const dashboardScript = join(projectRoot, 'dashboard', 'src', 'index.js');

async function startSystem() {
  console.log('='.repeat(60));
  console.log('Starting Distributed Event Streaming System');
  console.log('='.repeat(60));
  console.log(`Controllers: ${numControllers}`);
  console.log(`Brokers: ${numBrokers}`);
  console.log(`Dashboard: ${startDashboard ? 'Yes' : 'No'}`);
  console.log('='.repeat(60));
  console.log();

  console.log('Step 1: Starting controllers...\n');

  for (let i = 0; i < numControllers; i++) {
    const httpPort = baseHttpPort + i;
    const electionPort = baseElectionPort + i;
    const heartbeatPort = baseHeartbeatPort + i;
    
    const peers = [];
    for (let j = 0; j < numControllers; j++) {
      if (j !== i) {
        peers.push(`${host}:${baseElectionPort + j}`);
      }
    }

    const env = {
      ...process.env,
      HTTP_PORT: httpPort.toString(),
      ELECTION_PORT: electionPort.toString(),
      HEARTBEAT_PORT: heartbeatPort.toString(),
      HOST: host,
      CONTROLLER_PEERS: peers.join(',')
    };

    console.log(`  Controller ${i}: HTTP @ ${host}:${httpPort}, Election @ ${host}:${electionPort}`);

    const proc = spawn('node', [controllerScript], {
      env,
      stdio: 'inherit',
      cwd: projectRoot
    });

    proc.on('error', (err) => {
      console.error(`  ✗ Controller ${i} failed to start:`, err);
    });

    proc.on('exit', (code) => {
      if (code !== 0 && code !== null) {
        console.error(`  ✗ Controller ${i} exited with code ${code}`);
      }
    });

    processes.push({ type: 'controller', index: i, process: proc });
  }

  console.log('\nWaiting for leader election (5 seconds)...');
  await new Promise(resolve => setTimeout(resolve, 5000));

  try {
    const http = await import('http');
    const leaderUrl = `http://${host}:${baseHttpPort}/cluster/leader`;
    
    const checkLeader = () => {
      return new Promise((resolve, reject) => {
        const req = http.default.get(leaderUrl, (res) => {
          let data = '';
          res.on('data', chunk => { data += chunk; });
          res.on('end', () => {
            try {
              resolve(JSON.parse(data));
            } catch (e) {
              reject(e);
            }
          });
        });
        req.on('error', reject);
        req.setTimeout(2000, () => {
          req.destroy();
          reject(new Error('Timeout'));
        });
      });
    };

    let attempts = 0;
    while (attempts < 10) {
      try {
        const result = await checkLeader();
        if (result.leader && result.leader.nodeId) {
          console.log(`  ✓ Leader elected: ${result.leader.nodeId} (term ${result.leader.term || 'N/A'})\n`);
          break;
        }
      } catch (err) {
      }
      attempts++;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } catch (err) {
    console.log('  ⚠ Could not verify leader election (controllers may still be starting)\n');
  }

  console.log('Step 2: Starting brokers...\n');

  for (let i = 0; i < numBrokers; i++) {
    const dataPort = baseDataPort + i;
    const brokerId = `broker-${i}`;

    const env = {
      ...process.env,
      BROKER_PORT: dataPort.toString(),
      BROKER_ID: brokerId,
      CONTROLLER_HOST: host,
      CONTROLLER_HTTP_PORT: baseHttpPort.toString(),
      CONTROLLER_HEARTBEAT_PORT: baseHeartbeatPort.toString()
    };

    console.log(`  Broker ${i} (${brokerId}): Data @ ${host}:${dataPort}`);

    const proc = spawn('node', [brokerScript], {
      env,
      stdio: 'inherit',
      cwd: projectRoot
    });

    proc.on('error', (err) => {
      console.error(`  ✗ Broker ${i} failed to start:`, err);
    });

    proc.on('exit', (code) => {
      if (code !== 0 && code !== null) {
        console.error(`  ✗ Broker ${i} exited with code ${code}`);
      }
    });

    processes.push({ type: 'broker', index: i, process: proc });
  }

  console.log('\nWaiting for brokers to register (3 seconds)...');
  await new Promise(resolve => setTimeout(resolve, 3000));

  if (startDashboard) {
    console.log('\nStep 3: Starting dashboard...\n');

    const env = {
      ...process.env,
      DASHBOARD_PORT: dashboardPort.toString(),
      CONTROLLER_HOST: host,
      CONTROLLER_HTTP_PORT: baseHttpPort.toString()
    };

    console.log(`  Dashboard: http://${host}:${dashboardPort}`);

    const proc = spawn('node', [dashboardScript], {
      env,
      stdio: 'inherit',
      cwd: projectRoot
    });

    proc.on('error', (err) => {
      console.error(`  ✗ Dashboard failed to start:`, err);
    });

    proc.on('exit', (code) => {
      if (code !== 0 && code !== null) {
        console.error(`  ✗ Dashboard exited with code ${code}`);
      }
    });

    processes.push({ type: 'dashboard', index: 0, process: proc });
  }

  console.log('\n' + '='.repeat(60));
  console.log('System Started Successfully!');
  console.log('='.repeat(60));
  console.log(`Controller API: http://${host}:${baseHttpPort}`);
  console.log(`Dashboard: ${startDashboard ? `http://${host}:${dashboardPort}` : 'Not started'}`);
  console.log(`Brokers: ${numBrokers} brokers on ports ${baseDataPort}-${baseDataPort + numBrokers - 1}`);
  console.log('\nPress Ctrl+C to stop all services');
  console.log('='.repeat(60));
}

startSystem().catch(err => {
  console.error('Failed to start system:', err);
  process.exit(1);
});

const shutdown = async (signal) => {
  console.log(`\n\nReceived ${signal}, shutting down all services...\n`);
  
  const shutdownOrder = ['dashboard', 'broker', 'controller'];
  
  for (const type of shutdownOrder) {
    const procs = processes.filter(p => p.type === type);
    if (procs.length > 0) {
      console.log(`Shutting down ${type}s...`);
      for (const { process: proc } of procs) {
        proc.kill('SIGTERM');
      }
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
  
  await Promise.all(processes.map(({ process: proc }) => {
    return new Promise(resolve => {
      proc.on('exit', resolve);
      if (!proc.killed) {
        setTimeout(() => {
          if (!proc.killed) {
            proc.kill('SIGKILL');
          }
          resolve();
        }, 5000);
      } else {
        resolve();
      }
    });
  }));
  
  console.log('\nAll services stopped.');
  process.exit(0);
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
