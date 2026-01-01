'use strict';

import { spawn } from 'child_process';
import { Logger } from '../shared/utils/logger.js';

/**
 * Start multiple controllers with peer configuration for leader election testing
 * 
 * Usage:
 *   node scripts/startControllers.js [--num-controllers 3] [--base-http-port 8080] [--base-election-port 9093]
 */

const logger = new Logger({ name: 'controller-orchestrator' });

class ControllerOrchestrator {
  constructor(options) {
    const opts = options || {};
    this.numControllers = opts.numControllers || 3;
    this.baseHttpPort = opts.baseHttpPort || 8080;
    this.baseHeartbeatPort = opts.baseHeartbeatPort || 9001;
    this.baseElectionPort = opts.baseElectionPort || 9010;
    this.host = opts.host || 'localhost';
    
    this.processes = new Map();
    this.running = false;
  }

  /**
   * Generate peer configuration for controllers
   */
  _generatePeers() {
    const peers = [];
    for (let i = 0; i < this.numControllers; i++) {
      peers.push({
        host: this.host,
        port: this.baseElectionPort + i
      });
    }
    return peers;
  }

  /**
   * Format peers for CONTROLLER_PEERS env var
   */
  _formatPeersEnv(allPeers, excludeIndex) {
    return allPeers
      .filter((_, i) => i !== excludeIndex)
      .map(p => `${p.host}:${p.port}`)
      .join(',');
  }

  /**
   * Start controllers
   */
  async start() {
    logger.info('Starting multiple controllers with peer configuration', {
      numControllers: this.numControllers,
      baseHttpPort: this.baseHttpPort,
      baseElectionPort: this.baseElectionPort,
      host: this.host
    });

    this.running = true;
    const allPeers = this._generatePeers();

    try {
      for (let i = 0; i < this.numControllers; i++) {
        await this._startController(i, allPeers);
        await this._wait(1500); // Stagger startup
      }

      console.log('\n' + '='.repeat(80));
      console.log('✅ CONTROLLERS STARTED');
      console.log('='.repeat(80));
      console.log(`\nStarted ${this.numControllers} controllers with peer configuration:\n`);
      
      const sharedHeartbeatPort = this.baseHeartbeatPort; // All controllers share this port
      console.log(`🔔 SHARED HEARTBEAT PORT: ${sharedHeartbeatPort}`);
      console.log(`   (All brokers send heartbeats to this port)\n`);
      
      for (let i = 0; i < this.numControllers; i++) {
        const httpPort = this.baseHttpPort + i;
        const electionPort = this.baseElectionPort + i;
        console.log(`Controller ${i + 1}:`);
        console.log(`  HTTP API:        http://${this.host}:${httpPort}`);
        console.log(`  Heartbeat Port:  ${sharedHeartbeatPort}`);
        console.log(`  Election Port:   ${electionPort}`);
        console.log();
      }

      console.log('Peers Configuration:');
      for (let i = 0; i < this.numControllers; i++) {
        const peersForController = this._formatPeersEnv(allPeers, i);
        console.log(`  Controller ${i + 1} knows peers: ${peersForController}`);
      }

      console.log('\n' + '='.repeat(80));
      console.log('MONITORING ELECTION PROCESS...');
      console.log('='.repeat(80));
      console.log('\nWatch for:');
      console.log('  1. Controllers entering FOLLOWER state');
      console.log('  2. Election timeout triggering');
      console.log('  3. Candidates requesting votes from peers');
      console.log('  4. Votes being counted');
      console.log('  5. One controller being elected LEADER');
      console.log('\nTo test failover:');
      console.log('  Kill a controller with Ctrl+C and watch others detect failure');
      console.log('='.repeat(80) + '\n');

      // Keep running
      await new Promise(() => {});
    } catch (err) {
      logger.error('Failed to start controllers', err);
      await this.stop();
      process.exit(1);
    }
  }

  /**
   * Start a single controller
   */
  async _startController(index, allPeers) {
    const httpPort = this.baseHttpPort + index;
    const heartbeatPort = this.baseHeartbeatPort; // FIXED: All controllers listen on same heartbeat port
    const electionPort = this.baseElectionPort + index;
    const peersString = this._formatPeersEnv(allPeers, index);

    logger.info(`Starting controller ${index}...`, {
      httpPort,
      heartbeatPort,
      electionPort,
      peers: peersString
    });

    const proc = spawn('node', ['controller/src/index.js'], {
      stdio: 'inherit',
      env: {
        ...process.env,
        NODE_ENV: 'dev',
        HTTP_PORT: httpPort,
        HEARTBEAT_PORT: heartbeatPort,
        ELECTION_PORT: electionPort,
        CONTROLLER_PEERS: peersString,
        // Force the first controller to be the static leader
        FORCE_LEADER: (index === 0) ? 'true' : 'false',
        DATA_DIR: './data'
      }
    });

    this.processes.set(`controller-${index}`, proc);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        resolve(); // Assume it started
      }, 3000);

      proc.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  /**
   * Stop all controllers
   */
  async stop() {
    logger.info('Stopping all controllers...');

    const promises = [];

    for (const [name, proc] of this.processes) {
      promises.push(
        new Promise((resolve) => {
          logger.info(`Stopping ${name}...`);

          const timeout = setTimeout(() => {
            proc.kill('SIGKILL');
            resolve();
          }, 5000);

          proc.on('exit', () => {
            clearTimeout(timeout);
            logger.info(`${name} stopped`);
            resolve();
          });

          proc.kill('SIGTERM');
        })
      );
    }

    await Promise.all(promises);
    this.processes.clear();
    this.running = false;

    logger.info('All controllers stopped');
  }

  /**
   * Utility: wait
   */
  _wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

/**
 * Parse command-line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    numControllers: 3,
    baseHttpPort: 8080,
    baseHeartbeatPort: 9091,
    baseElectionPort: 9093,
    host: 'localhost'
  };

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace('--', '').replace(/-/g, '');
    const value = args[i + 1];

    if (key === 'numcontrollers') options.numControllers = parseInt(value, 10);
    if (key === 'basehttpport') options.baseHttpPort = parseInt(value, 10);
    if (key === 'baseheartbeatport') options.baseHeartbeatPort = parseInt(value, 10);
    if (key === 'baseelectionport') options.baseElectionPort = parseInt(value, 10);
    if (key === 'host') options.host = value;
  }

  return options;
}

/**
 * Main
 */
async function main() {
  const options = parseArgs();

  const orchestrator = new ControllerOrchestrator(options);

  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\n\nShutdown signal received...');
    await orchestrator.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\n\nTermination signal received...');
    await orchestrator.stop();
    process.exit(0);
  });

  await orchestrator.start();
}

main().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});
