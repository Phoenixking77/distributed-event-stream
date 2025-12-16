#!/usr/bin/env node
'use strict';

/**
 * Example: High-throughput batch publishing
 * Demonstrates batching for efficient publishing at scale
 */

import { createClient, createBatchPublisher } from '../client-sdk/src/index.js';
import { Logger } from '../shared/utils/logger.js';

const logger = new Logger({ name: 'batch-publisher-example' });

async function main() {
  const client = createClient({
    host: 'localhost',
    port: 9092,
    logger
  });

  await client.connect();
  logger.info('Connected to broker');

  // Create batch publisher
  const publisher = createBatchPublisher(client, {
    batchSize: 1000,      // Flush every 1000 messages
    batchTimeoutMs: 5000, // Or every 5 seconds
    compression: 'none'   // No compression for speed
  });

  try {
    // Generate and publish test data
    logger.info('Publishing 10,000 test messages...');
    const startTime = Date.now();
    let published = 0;

    for (let i = 0; i < 10000; i++) {
      const priority = i % 5; // Distribute priorities
      await publisher.add('benchmark', {
        messageId: i,
        timestamp: Date.now(),
        data: `Message ${i}`,
        priority
      }, { priority });

      if ((i + 1) % 1000 === 0) {
        logger.info(`Published ${i + 1} messages...`);
      }
    }

    // Flush remaining messages
    await publisher.drain();

    const elapsedMs = Date.now() - startTime;
    const metrics = publisher.getMetrics();

    logger.info('Publishing complete', {
      messagesPublished: metrics.messagesPublished,
      batchesPublished: metrics.batchesPublished,
      elapsedMs,
      throughput: Math.round((metrics.messagesPublished / elapsedMs) * 1000) + ' msg/sec'
    });

    // Show client metrics
    const clientMetrics = client.getMetrics();
    logger.info('Client metrics', {
      messagesPublished: clientMetrics.messagesPublished,
      bytesPublished: clientMetrics.messagesBytesPublished,
      errors: clientMetrics.errors
    });
  } finally {
    await publisher.close();
    await client.disconnect();
    logger.info('Disconnected');
  }
}

main().catch(err => {
  logger.error('Error in batch publisher example', err);
  process.exit(1);
});
