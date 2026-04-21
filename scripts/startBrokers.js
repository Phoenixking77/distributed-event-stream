#!/usr/bin/env node
'use strict';

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

const args = process.argv.slice(2);
let numBrokers = 3;
let baseDataPort = 9092;
let controllerHost = 'localhost';
let controllerPort = 8080;
let controllerHeartbeatPort = 9091;

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--num-brokers' && args[i + 1]) {
    numBrokers = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--base-data-port' && args[i + 1]) {
    baseDataPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--controller-host' && args[i + 1]) {
    controllerHost = args[i + 1];
    i++;
  } else if (args[i] === '--controller-port' && args[i + 1]) {
    controllerPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--controller-heartbeat-port' && args[i + 1]) {
    controllerHeartbeatPort = parseInt(args[i + 1], 10);
    i++;
  }
}

const processes = [];
const brokerScript = join(projectRoot, 'broker', 'src', 'index.js');

console.log(`Starting ${numBrokers} brokers...\n`);

for (let i = 0; i < numBrokers; i++) {
  const dataPort = baseDataPort + i;
  const brokerId = `broker-${i}`;

  const env = {
    ...process.env,
    BROKER_PORT: dataPort.toString(),
    BROKER_ID: brokerId,
    CONTROLLER_HOST: controllerHost,
    CONTROLLER_HTTP_PORT: controllerPort.toString(),
    CONTROLLER_HEARTBEAT_PORT: controllerHeartbeatPort.toString()
  };

  console.log(`Broker ${i} (${brokerId}): Data @ localhost:${dataPort}`);

  const proc = spawn('node', [brokerScript], {
    env,
    stdio: 'inherit',
    cwd: projectRoot
  });

  proc.on('error', (err) => {
    console.error(`Broker ${i} failed to start:`, err);
  });

  proc.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      console.error(`Broker ${i} exited with code ${code}`);
    }
  });

  processes.push(proc);
}

console.log('\nBrokers started. They will register with the controller...\n');

const shutdown = async (signal) => {
  console.log(`\nReceived ${signal}, shutting down all brokers...`);
  for (const proc of processes) {
    proc.kill('SIGTERM');
  }
  
  await Promise.all(processes.map(proc => {
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
  
  process.exit(0);
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
