#!/usr/bin/env node
'use strict';

/**
 * Example: Error handling and retry logic
 * Demonstrates resilient publishing with custom error handling
 */

import { createClient, ErrorHandler } from '../client-sdk/src/index.js';
import { Logger } from '../shared/utils/logger.js';

const logger = new Logger({ name: 'error-handling-example' });

async function main() {
  const client = createClient({
    host: 'localhost',
    port: 9092,
    maxRetries: 3,
    logger
  });

  // Create error handler with retry policy
  const errorHandler = new ErrorHandler({
    maxAttempts: 3,
    initialDelayMs: 100,
    maxDelayMs: 5000,
    backoffMultiplier: 2,
    logger: logger.child({ component: 'errorHandler' })
  });

  try {
    logger.info('Connecting to broker...');
    await client.connect();
    logger.info('Connected');

    // Example 1: Successful publish with automatic retry capability
    logger.info('Publishing with error handling...');
    const result1 = await errorHandler.handleWithRetry(
      () => client.publish('orders', {
        orderId: '12345',
        amount: 99.99,
        timestamp: Date.now()
      }),
      { operation: 'publish', topic: 'orders' }
    );
    logger.info('Published successfully', { offset: result1.offset });

    // Example 2: Publishing multiple messages with error handling
    logger.info('Publishing multiple messages...');
    const messages = [
      { orderId: '001', amount: 50 },
      { orderId: '002', amount: 75 },
      { orderId: '003', amount: 100 },
      { orderId: '004', amount: 125 },
      { orderId: '005', amount: 150 }
    ];

    for (const message of messages) {
      try {
        await errorHandler.handleWithRetry(
          () => client.publish('orders', message),
          { operation: 'publish', messageId: message.orderId }
        );
        logger.info('Published message', { orderId: message.orderId });
      } catch (err) {
        const formatted = errorHandler.formatError(err, {
          operation: 'publish',
          orderId: message.orderId
        });
        logger.error('Failed to publish after retries', formatted);
      }
    }

    // Show error handling metrics
    logger.info('Error handler metrics', errorHandler.getMetrics());

    // Example 3: Custom error categorization
    logger.info('Error categories:');
    try {
      // Simulate error
      throw new Error('Connection timeout');
    } catch (err) {
      const category = errorHandler.categorizeError(err);
      logger.info('Error categorized', { error: err.message, category });
    }

  } catch (err) {
    logger.error('Fatal error', err);
  } finally {
    await client.disconnect();
    logger.info('Disconnected');

    // Print final metrics
    const finalMetrics = errorHandler.getMetrics();
    logger.info('Final error handling metrics', {
      totalErrors: finalMetrics.totalErrors,
      retriedErrors: finalMetrics.retriedErrors,
      retriedSuccessfully: finalMetrics.retriedSuccessfully,
      retrySuccessRate: `${(finalMetrics.retrySuccessRate * 100).toFixed(2)}%`
    });
  }
}

main().catch(err => {
  logger.error('Unhandled error', err);
  process.exit(1);
});
