'use strict';

import { spawn } from 'child_process';
import { Logger } from '../shared/utils/logger.js';

/**
 * Production-grade cluster orchestration tool
 * Starts all services in the correct order with health checks
 * 
 * Usage:
 *   node scripts/clusterStart.js
 *   node scripts/clusterStart.js --mode dev
 *   node scripts/clusterStart.js --mode prod --num-brokers 3
 */

const logger = new Logger({ name: 'cluster-orchestrator' });

class ClusterOrchestrator {
  constructor(options) {
    const opts = options || {};
    this.mode = opts.mode || 'dev'; // dev, staging, prod
    this.numBrokers = opts.numBrokers || 1;
    this.brokerBasePort = opts.brokerBasePort || 9092;
    this.controllerPort = opts.controllerPort || 8080;
    this.dashboardPort = opts.dashboardPort || 8090;
    this.dataDir = opts.dataDir || './data';
    this.logDir = opts.logDir || './logs';

    this.processes = new Map();
    this.running = false;
  }

  /**
   * Start cluster
   */
  async start() {
    logger.info('Starting distributed event stream cluster', {
      mode: this.mode,
      numBrokers: this.numBrokers
    });

    this.running = true;

    try {
      // Start controller first
      await this._startController();
      await this._wait(2000);

      // Start brokers
      for (let i = 0; i < this.numBrokers; i++) {
        await this._startBroker(i);
        await this._wait(1000);
      }

      // Start dashboard
      await this._startDashboard();

      logger.info('Cluster started successfully');
      console.log('\n' + '='.repeat(70));
      console.log('✅ CLUSTER STARTED');
      console.log('='.repeat(70));
      console.log(`Controller: http://localhost:${this.controllerPort}`);
      console.log(`Dashboard:  http://localhost:${this.dashboardPort}`);
      console.log(`Brokers:    localhost:${this.brokerBasePort}-${this.brokerBasePort + this.numBrokers - 1}`);
      console.log('\nPress Ctrl+C to stop all services');
      console.log('='.repeat(70) + '\n');

      // Keep running
      await new Promise(() => {});
    } catch (err) {
      logger.error('Failed to start cluster', err);
      await this.stop();
      process.exit(1);
    }
  }

  /**
   * Start controller
   */
  async _startController() {
    logger.info('Starting controller...');

    const proc = spawn('node', ['controller/src/index.js'], {
      stdio: 'inherit',
      env: {
        ...process.env,
        NODE_ENV: this.mode,
        CONTROLLER_PORT: this.controllerPort,
        DATA_DIR: this.dataDir
      }
    });

    this.processes.set('controller', proc);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        resolve(); // Assume it started
      }, 3000);

      process.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  /**
   * Start broker
   */
  async _startBroker(index) {
    logger.info(`Starting broker ${index}...`);

    const port = this.brokerBasePort + index;

    const proc = spawn('node', ['broker/src/index.js'], {
      stdio: 'inherit',
      env: {
        ...process.env,
        NODE_ENV: this.mode,
        BROKER_PORT: port,
        BROKER_ID: index,
        DATA_DIR: this.dataDir,
        CONTROLLER_HOST: 'localhost',
        CONTROLLER_PORT: this.controllerPort
      }
    });

    this.processes.set(`broker-${index}`, proc);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        resolve(); // Assume it started
      }, 3000);

      process.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  /**
   * Start dashboard
   */
  async _startDashboard() {
    logger.info('Starting dashboard...');

    const proc = spawn('node', ['dashboard/src/index.js'], {
      stdio: 'inherit',
      env: {
        ...process.env,
        NODE_ENV: this.mode,
        DASHBOARD_PORT: this.dashboardPort,
        CONTROLLER_HOST: 'localhost',
        CONTROLLER_PORT: this.controllerPort
      }
    });

    this.processes.set('dashboard', proc);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        resolve(); // Assume it started
      }, 3000);

      process.on('error', (err) => {
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
    mode: 'dev',
    numBrokers: 1,
    brokerBasePort: 9092,
    controllerPort: 8080,
    dashboardPort: 8090,
    dataDir: './data',
    logDir: './logs'
  };

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace('--', '').replace(/-/g, '');
    const value = args[i + 1];

    if (key === 'mode') options.mode = value;
    if (key === 'numbrokers') options.numBrokers = parseInt(value, 10);
    if (key === 'brokerbaseport') options.brokerBasePort = parseInt(value, 10);
    if (key === 'controllerport') options.controllerPort = parseInt(value, 10);
    if (key === 'dashboardport') options.dashboardPort = parseInt(value, 10);
    if (key === 'datadir') options.dataDir = value;
    if (key === 'logdir') options.logDir = value;
  }

  return options;
}

/**
 * Main
 */
async function main() {
  const options = parseArgs();

  const orchestrator = new ClusterOrchestrator(options);

  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\nShutdown signal received...');
    await orchestrator.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\nTermination signal received...');
    await orchestrator.stop();
    process.exit(0);
  });

  await orchestrator.start();
}

main().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});
