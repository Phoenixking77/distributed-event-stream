#!/usr/bin/env node
'use strict';

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

const args = process.argv.slice(2);
let numControllers = 3;
let baseHttpPort = 8080;
let baseElectionPort = 9010;
let baseHeartbeatPort = 9091;
let host = 'localhost';

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--num-controllers' && args[i + 1]) {
    numControllers = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--base-http-port' && args[i + 1]) {
    baseHttpPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--base-election-port' && args[i + 1]) {
    baseElectionPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--base-heartbeat-port' && args[i + 1]) {
    baseHeartbeatPort = parseInt(args[i + 1], 10);
    i++;
  } else if (args[i] === '--host' && args[i + 1]) {
    host = args[i + 1];
    i++;
  }
}

const processes = [];
const controllerScript = join(projectRoot, 'controller', 'src', 'index.js');

console.log(`Starting ${numControllers} controllers...\n`);

const peers = [];
for (let i = 0; i < numControllers; i++) {
  const electionPort = baseElectionPort + i;
  if (i > 0) {
    peers.push(`${host}:${electionPort}`);
  }
}

for (let i = 0; i < numControllers; i++) {
  const httpPort = baseHttpPort + i;
  const electionPort = baseElectionPort + i;
  const heartbeatPort = baseHeartbeatPort + i;
  
  const controllerPeers = [];
  for (let j = 0; j < numControllers; j++) {
    if (j !== i) {
      controllerPeers.push(`${host}:${baseElectionPort + j}`);
    }
  }

  const env = {
    ...process.env,
    HTTP_PORT: httpPort.toString(),
    ELECTION_PORT: electionPort.toString(),
    HEARTBEAT_PORT: heartbeatPort.toString(),
    HOST: host,
    CONTROLLER_PEERS: controllerPeers.join(',')
  };

  console.log(`Controller ${i}: HTTP @ ${host}:${httpPort}, Election @ ${host}:${electionPort}, Heartbeat @ ${host}:${heartbeatPort}`);

  const proc = spawn('node', [controllerScript], {
    env,
    stdio: 'inherit',
    cwd: projectRoot
  });

  proc.on('error', (err) => {
    console.error(`Controller ${i} failed to start:`, err);
  });

  proc.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      console.error(`Controller ${i} exited with code ${code}`);
    }
  });

  processes.push(proc);
}

console.log('\nWaiting for leader election...\n');

setTimeout(async () => {
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
              const result = JSON.parse(data);
              resolve(result);
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
          console.log(`✓ Controller elected as LEADER: ${result.leader.nodeId} (term ${result.leader.term || 'N/A'})`);
          break;
        }
      } catch (err) {
      }
      attempts++;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } catch (err) {
    console.log('Note: Could not verify leader election (controllers may still be starting)');
  }
}, 2000);

const shutdown = async (signal) => {
  console.log(`\nReceived ${signal}, shutting down all controllers...`);
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
