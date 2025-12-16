'use strict';

import { spawn } from 'child_process';
import { Logger } from '../shared/utils/logger.js';

const logger = new Logger({ name: 'broker-orchestrator' });

class BrokerOrchestrator {
  constructor(options) {
    const opts = options || {};
    this.numBrokers = opts.numBrokers || 3;
    this.basePort = opts.basePort || 9092;
    this.host = opts.host || '0.0.0.0';
    this.controllerPort = opts.controllerPort || 8080;
    
    this.processes = new Map();
    this.running = false;
  }

  async start() {
    logger.info('Starting brokers', {
      numBrokers: this.numBrokers,
      basePort: this.basePort,
      controllerPort: this.controllerPort
    });

    this.running = true;

    try {
      for (let i = 0; i < this.numBrokers; i++) {
        await this._startBroker(i);
        await this._wait(2000);
      }

      console.log('\n' + '='.repeat(80));
      console.log('✅ BROKERS STARTED');
      console.log('='.repeat(80));
      for (let i = 0; i < this.numBrokers; i++) {
        console.log(`Broker ${i + 1}: localhost:${this.basePort + i}`);
      }
      console.log('='.repeat(80) + '\n');

      // Keep running
      await new Promise(() => {});
    } catch (err) {
      logger.error('Failed to start brokers', err);
      await this.stop();
      process.exit(1);
    }
  }

  async _startBroker(index) {
    const port = this.basePort + index;
    logger.info(`Starting broker ${index + 1}...`, { port });

    const proc = spawn('node', ['broker/src/index.js'], {
      stdio: 'inherit',
      env: {
        ...process.env,
        BROKER_PORT: port.toString()
      }
    });

    this.processes.set(`broker-${index}`, proc);

    return new Promise((resolve) => {
      setTimeout(resolve, 2000);
    });
  }

  async _wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async stop() {
    logger.info('Stopping brokers');
    for (const [name, proc] of this.processes) {
      try {
        proc.kill('SIGTERM');
      } catch (err) {
        logger.error('Error killing process', { name, error: err.message });
      }
    }
  }
}

const orchestrator = new BrokerOrchestrator({
  numBrokers: 3,
  basePort: 9092
});

orchestrator.start().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});

const shutdown = async (signal) => {
  console.log(`\nReceived ${signal}, shutting down gracefully...`);
  await orchestrator.stop();
  process.exit(0);
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
