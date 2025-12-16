'use strict';

import { spawn } from 'child_process';
import { Logger } from '../shared/utils/logger.js';

/**
 * Start complete cluster with multiple controllers and brokers
 * to demonstrate leader election and failover
 * 
 * Usage:
 *   node scripts/electionDemo.js [--num-controllers 3] [--num-brokers 2]
 */

const logger = new Logger({ name: 'election-demo' });

class ElectionDemo {
  constructor(options) {
    const opts = options || {};
    this.numControllers = opts.numControllers || 3;
    this.numBrokers = opts.numBrokers || 2;
    this.baseHttpPort = opts.baseHttpPort || 8080;
    this.baseHeartbeatPort = opts.baseHeartbeatPort || 9091;
    this.baseElectionPort = opts.baseElectionPort || 9093;
    this.baseBrokerPort = opts.baseBrokerPort || 9092;
    this.host = opts.host || 'localhost';
    
    this.processes = new Map();
    this.running = false;
  }

  /**
   * Generate peer configuration for controllers
   */
  _generateControllerPeers() {
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
   * Start complete system
   */
  async start() {
    logger.info('Starting election demonstration cluster', {
      numControllers: this.numControllers,
      numBrokers: this.numBrokers,
      host: this.host
    });

    this.running = true;
    const controllerPeers = this._generateControllerPeers();

    try {
      // Start all controllers
      for (let i = 0; i < this.numControllers; i++) {
        await this._startController(i, controllerPeers);
        await this._wait(1500);
      }

      await this._wait(2000);

      // Start all brokers
      for (let i = 0; i < this.numBrokers; i++) {
        await this._startBroker(i);
        await this._wait(1000);
      }

      console.log('\n' + '='.repeat(90));
      console.log('✅ CLUSTER STARTED - ELECTION DEMONSTRATION');
      console.log('='.repeat(90));
      console.log(`\nCONTROLLERS (${this.numControllers} nodes):\n`);
      
      for (let i = 0; i < this.numControllers; i++) {
        const httpPort = this.baseHttpPort + i;
        const heartbeatPort = this.baseHeartbeatPort + i;
        const electionPort = this.baseElectionPort + i;
        console.log(`  Controller ${i + 1}:`);
        console.log(`    HTTP API:        http://${this.host}:${httpPort}/api/status`);
        console.log(`    Heartbeat Port:  ${heartbeatPort}`);
        console.log(`    Election Port:   ${electionPort}`);
      }

      console.log(`\nBROKERS (${this.numBrokers} nodes):\n`);
      for (let i = 0; i < this.numBrokers; i++) {
        const brokerPort = this.baseBrokerPort + i;
        console.log(`  Broker ${i + 1}: localhost:${brokerPort}`);
      }

      console.log('\n' + '='.repeat(90));
      console.log('ELECTION PROCESS IN ACTION');
      console.log('='.repeat(90));
      console.log('\nWATCH FOR:');
      console.log('  1. Controllers starting as FOLLOWERs');
      console.log('  2. Election timeout triggering (~8s)');
      console.log('  3. First CANDIDATE requesting votes from peers');
      console.log('  4. Other controllers granting/denying votes');
      console.log('  5. Winning candidate becoming LEADER');
      console.log('  6. Leader announcing leadership to peers');
      console.log('\n' + '='.repeat(90));
      console.log('TO TEST FAILOVER:');
      console.log('='.repeat(90));
      console.log('\nOnce a leader is elected, stop it by pressing Ctrl+C.');
      console.log('Then watch the other controllers:');
      console.log('  1. Detect the leader has failed (no heartbeat)');
      console.log('  2. Start a new election');
      console.log('  3. Elect a new leader');
      console.log('\n' + '='.repeat(90) + '\n');

      // Keep running
      await new Promise(() => {});
    } catch (err) {
      logger.error('Failed to start cluster', err);
      await this.stop();
      process.exit(1);
    }
  }

  /**
   * Start a single controller
   */
  async _startController(index, allPeers) {
    const httpPort = this.baseHttpPort + index;
    const heartbeatPort = this.baseHeartbeatPort + index;
    const electionPort = this.baseElectionPort + index;
    const peersString = this._formatPeersEnv(allPeers, index);

    logger.info(`Starting controller ${index}...`, {
      httpPort,
      heartbeatPort,
      electionPort,
      peers: peersString || '(none - will be leader)'
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
        DATA_DIR: './data'
      }
    });

    this.processes.set(`controller-${index}`, proc);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        resolve();
      }, 3000);

      proc.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  /**
   * Start a single broker
   */
  async _startBroker(index) {
    const brokerPort = this.baseBrokerPort + index;
    const controllerHost = this.host;
    const controllerPort = this.baseHttpPort; // Primary controller

    logger.info(`Starting broker ${index}...`, {
      brokerPort,
      controllerPort
    });

    const proc = spawn('node', ['broker/src/index.js'], {
      stdio: 'inherit',
      env: {
        ...process.env,
        NODE_ENV: 'dev',
        BROKER_PORT: brokerPort,
        BROKER_ID: index,
        CONTROLLER_HOST: controllerHost,
        CONTROLLER_PORT: controllerPort,
        DATA_DIR: './data'
      }
    });

    this.processes.set(`broker-${index}`, proc);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        resolve();
      }, 3000);

      proc.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  /**
   * Stop all processes
   */
  async stop() {
    logger.info('Stopping all processes...');

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

    logger.info('All processes stopped');
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
    numBrokers: 2,
    baseHttpPort: 8080,
    baseHeartbeatPort: 9091,
    baseElectionPort: 9093,
    baseBrokerPort: 9092,
    host: 'localhost'
  };

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace('--', '').replace(/-/g, '');
    const value = args[i + 1];

    if (key === 'numcontrollers') options.numControllers = parseInt(value, 10);
    if (key === 'numbrokers') options.numBrokers = parseInt(value, 10);
    if (key === 'basehttpport') options.baseHttpPort = parseInt(value, 10);
    if (key === 'baseheartbeatport') options.baseHeartbeatPort = parseInt(value, 10);
    if (key === 'baseelectionport') options.baseElectionPort = parseInt(value, 10);
    if (key === 'basebrokerport') options.baseBrokerPort = parseInt(value, 10);
    if (key === 'host') options.host = value;
  }

  return options;
}

/**
 * Main
 */
async function main() {
  const options = parseArgs();
  const demo = new ElectionDemo(options);

  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\n\n' + '='.repeat(90));
    console.log('SHUTDOWN INITIATED');
    console.log('='.repeat(90));
    await demo.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\n\n' + '='.repeat(90));
    console.log('TERMINATION SIGNAL');
    console.log('='.repeat(90));
    await demo.stop();
    process.exit(0);
  });

  await demo.start();
}

main().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});
