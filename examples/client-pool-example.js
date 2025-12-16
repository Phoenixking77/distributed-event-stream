#!/usr/bin/env node
'use strict';

/**
 * Example: Multi-broker client pool
 * Demonstrates connection pooling and load balancing across multiple brokers
 */

import { createClientPool } from '../client-sdk/src/index.js';
import { Logger } from '../shared/utils/logger.js';

const logger = new Logger({ name: 'client-pool-example' });

async function main() {
  // Define broker cluster
  const brokers = [
    { host: 'localhost', port: 9092 },
    { host: 'localhost', port: 9093 },
    { host: 'localhost', port: 9094 }
  ];

  // Create connection pool with load balancing
  const pool = createClientPool(brokers, {
    loadBalancingStrategy: 'least-connections', // Distributes by active connections
    clientOptions: {
      maxRetries: 5,
      connectTimeout: 5000
    }
  });

  try {
    logger.info('Connecting to broker cluster...', { brokerCount: brokers.length });
    await pool.connect();
    logger.info('Connected to all brokers');

    // Publishing distributes across brokers
    logger.info('Publishing 100 messages across broker pool...');
    for (let i = 0; i < 100; i++) {
      try {
        await pool.publish('events', {
          id: i,
          timestamp: Date.now(),
          data: `Event ${i}`
        });

        if ((i + 1) % 20 === 0) {
          logger.info(`Published ${i + 1} messages`);
        }
      } catch (err) {
        logger.warn(`Failed to publish message ${i}`, { error: err.message });
        // Pool automatically fails over to another broker
      }
    }

    // Check broker health
    logger.info('Checking broker health...');
    const health = pool.getHealthStatus();
    for (const [brokerId, status] of Object.entries(health)) {
      logger.info('Broker health', {
        broker: brokerId,
        connected: status.connected,
        inflightRequests: status.inflightRequests,
        messagesPublished: status.metrics.messagesPublished
      });
    }

    // Get pool metrics
    const metrics = pool.getMetrics();
    logger.info('Pool metrics', {
      routedRequests: metrics.routedRequests,
      failovers: metrics.failovers,
      totalBrokers: metrics.brokerCount
    });
  } finally {
    logger.info('Disconnecting from broker cluster...');
    await pool.disconnect();
    logger.info('Disconnected');
  }
}

main().catch(err => {
  logger.error('Error in client pool example', err);
  process.exit(1);
});
