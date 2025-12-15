'use strict';

import { Client } from '../client-sdk/src/client.js';
import { Logger } from '../shared/utils/logger.js';

/**
 * Production-grade flood publisher for stress testing
 * Continuously publishes messages to stress the system
 * 
 * Usage:
 *   node scripts/floodPublisher.js --rate 10000 --topic orders --duration 300
 *   node scripts/floodPublisher.js --rate 50000 --connections 10 --message-size 4096
 */

const logger = new Logger({ name: 'flood-publisher' });

class FloodPublisher {
  constructor(options) {
    const opts = options || {};

    this.rate = opts.rate || 10000; // messages/sec
    this.topic = opts.topic || 'flood-test';
    this.duration = opts.duration || 60; // seconds
    this.connections = opts.connections || 1;
    this.messageSize = opts.messageSize || 1024;
    this.brokerHost = opts.brokerHost || 'localhost';
    this.brokerPort = opts.brokerPort || 9092;
    this.logInterval = opts.logInterval || 5000; // ms

    this.clients = [];
    this.running = false;
    this.stats = {
      published: 0,
      failed: 0,
      startTime: null,
      endTime: null
    };
  }

  /**
   * Start flooding
   */
  async start() {
    logger.info('Starting flood publisher', {
      rate: this.rate,
      topic: this.topic,
      duration: this.duration,
      connections: this.connections,
      messageSize: this.messageSize
    });

    // Create clients
    for (let i = 0; i < this.connections; i++) {
      try {
        const client = new Client({
          host: this.brokerHost,
          port: this.brokerPort,
          logger: logger.child({ component: `client-${i}` })
        });

        await client.connect();
        this.clients.push(client);
        logger.info(`Client ${i} connected`);
      } catch (err) {
        logger.error(`Failed to connect client ${i}`, err);
      }
    }

    if (this.clients.length === 0) {
      throw new Error('Failed to connect any clients');
    }

    this.running = true;
    this.stats.startTime = Date.now();

    // Start logging
    const logTimer = setInterval(() => {
      this._logStats();
    }, this.logInterval);

    // Start publishing
    const endTime = Date.now() + (this.duration * 1000);
    const msPerMessage = 1000 / this.rate;

    let messageCount = 0;

    try {
      while (Date.now() < endTime && this.running) {
        const clientIdx = messageCount % this.clients.length;
        const client = this.clients[clientIdx];

        const message = this._generateMessage(messageCount);

        client.publish(this.topic, message)
          .then(() => {
            this.stats.published++;
          })
          .catch((err) => {
            this.stats.failed++;
            logger.error('Publish failed', err);
          });

        messageCount++;

        // Rate limiting
        const elapsed = (Date.now() - this.stats.startTime);
        const expectedMessages = (elapsed / 1000) * this.rate;

        if (messageCount > expectedMessages) {
          const delay = msPerMessage - (Date.now() - this.stats.startTime - (elapsed));
          if (delay > 0) {
            await new Promise(resolve => setTimeout(resolve, delay));
          }
        }
      }
    } finally {
      clearInterval(logTimer);
      this.stats.endTime = Date.now();
      this.running = false;
      await this._shutdown();
      this._logFinalStats();
    }
  }

  /**
   * Stop flooding
   */
  stop() {
    this.running = false;
    logger.info('Flood publisher stopped');
  }

  /**
   * Shutdown clients
   */
  async _shutdown() {
    logger.info('Shutting down clients');

    await Promise.all(
      this.clients.map((client, idx) =>
        client.disconnect()
          .then(() => logger.info(`Client ${idx} disconnected`))
          .catch(err => logger.error(`Failed to disconnect client ${idx}`, err))
      )
    );

    this.clients = [];
  }

  /**
   * Generate test message
   */
  _generateMessage(seq) {
    const payload = {
      seq,
      timestamp: Date.now(),
      random: Math.random(),
      data: Buffer.alloc(Math.max(0, this.messageSize - 100)).toString('base64')
    };

    return payload;
  }

  /**
   * Log current statistics
   */
  _logStats() {
    const elapsed = (Date.now() - this.stats.startTime) / 1000;
    const throughput = (this.stats.published / elapsed).toFixed(0);
    const successRate = this.stats.published / (this.stats.published + this.stats.failed);

    process.stdout.write(
      `\rPublished: ${this.stats.published} | Failed: ${this.stats.failed} | ` +
      `Throughput: ${throughput} msg/s | Success: ${(successRate * 100).toFixed(2)}%`
    );
  }

  /**
   * Log final statistics
   */
  _logFinalStats() {
    const durationMs = this.stats.endTime - this.stats.startTime;
    const durationSec = durationMs / 1000;
    const throughput = (this.stats.published / durationSec).toFixed(2);
    const successRate = this.stats.published / (this.stats.published + this.stats.failed);

    console.log('\n\n' + '='.repeat(70));
    console.log('FLOOD PUBLISHER FINAL STATISTICS');
    console.log('='.repeat(70));
    console.log(`Duration:           ${durationMs}ms (${durationSec.toFixed(2)}s)`);
    console.log(`Messages Published: ${this.stats.published}`);
    console.log(`Messages Failed:    ${this.stats.failed}`);
    console.log(`Throughput:         ${throughput} msg/s`);
    console.log(`Success Rate:       ${(successRate * 100).toFixed(2)}%`);
    console.log(`Message Size:       ${this.messageSize} bytes`);
    console.log(`Total Data:         ${((this.stats.published * this.messageSize) / 1024 / 1024).toFixed(2)} MB`);
    console.log('='.repeat(70) + '\n');
  }
}

/**
 * Parse command-line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    rate: 10000,
    topic: 'flood-test',
    duration: 60,
    connections: 1,
    messageSize: 1024,
    brokerHost: 'localhost',
    brokerPort: 9092,
    logInterval: 5000
  };

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace('--', '').replace(/-/g, '');
    const value = args[i + 1];

    if (key === 'rate') options.rate = parseInt(value, 10);
    if (key === 'topic') options.topic = value;
    if (key === 'duration') options.duration = parseInt(value, 10);
    if (key === 'connections') options.connections = parseInt(value, 10);
    if (key === 'messagesize') options.messageSize = parseInt(value, 10);
    if (key === 'brokerhost') options.brokerHost = value;
    if (key === 'brokerport') options.brokerPort = parseInt(value, 10);
    if (key === 'loginterval') options.logInterval = parseInt(value, 10);
  }

  return options;
}

/**
 * Main
 */
async function main() {
  const options = parseArgs();

  logger.info('Flood Publisher Configuration', options);

  const publisher = new FloodPublisher(options);

  // Handle graceful shutdown
  process.on('SIGINT', () => {
    logger.info('SIGINT received, stopping publisher');
    publisher.stop();
  });

  process.on('SIGTERM', () => {
    logger.info('SIGTERM received, stopping publisher');
    publisher.stop();
  });

  await publisher.start();

  process.exit(0);
}

main().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});
