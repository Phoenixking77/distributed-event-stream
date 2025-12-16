#!/usr/bin/env node
'use strict';

/**
 * Example: Consumer groups with auto-commit
 * Demonstrates reliable message processing with offset management
 */

import { createClient, createConsumerGroup } from '../client-sdk/src/index.js';
import { Logger } from '../shared/utils/logger.js';

const logger = new Logger({ name: 'consumer-group-example' });

async function main() {
  const client = createClient({
    host: 'localhost',
    port: 9092,
    logger
  });

  await client.connect();
  logger.info('Connected to broker');

  // Create consumer group
  const consumerGroup = createConsumerGroup(client, {
    groupId: 'order-processors',
    enableAutoCommit: true,
    autoCommitIntervalMs: 5000,
    autoOffsetReset: 'earliest'
  });

  let processedCount = 0;
  let lastLogTime = Date.now();

  try {
    // Register topic
    consumerGroup.registerTopic('orders', 0);

    logger.info('Subscribed to topic: orders');

    // Subscribe and process messages
    await client.subscribe('orders', (message) => {
      try {
        const order = message.message;

        // Simulate processing
        if (order.amount > 1000) {
          logger.info('Processing high-value order', {
            orderId: order.orderId,
            amount: order.amount,
            offset: message.offset
          });
        }

        // Update offset in consumer group
        consumerGroup.updateOffset('orders', message.offset);
        processedCount++;

        // Log progress every 5 seconds
        const now = Date.now();
        if (now - lastLogTime > 5000) {
          const state = consumerGroup.getState();
          logger.info('Consumer group state', {
            processedCount,
            offset: state['orders'].offset,
            lag: state['orders'].lag,
            committed: state['orders'].committed
          });
          lastLogTime = now;
        }
      } catch (err) {
        logger.error('Error processing message', { error: err.message, offset: message.offset });
      }
    }, {
      groupId: 'order-processors'
    });

    // Keep running
    logger.info('Listening for messages... (press Ctrl+C to exit)');
    await new Promise(() => {}); // Never resolves
  } catch (err) {
    logger.error('Error in consumer group', err);
  } finally {
    // Commit pending offsets and cleanup
    await consumerGroup.close();
    await client.disconnect();
    logger.info('Disconnected', { totalProcessed: processedCount });
  }
}

main().catch(err => {
  logger.error('Fatal error', err);
  process.exit(1);
});
