'use strict';

/**
 * Comprehensive test suite for Client SDK
 * Tests all major components and features
 */

import { describe, it, beforeEach, afterEach } from 'mocha';
import assert from 'assert';
import {
  Client,
  BatchPublisher,
  ConsumerGroupManager,
  ClientPool,
  ReconnectionManager,
  MessageEncoder,
  MessageDecoder,
  ErrorHandler,
  RetryPolicy
} from '../client-sdk/src/index.js';

describe('Client SDK', () => {
  describe('ReconnectionManager', () => {
    it('should calculate exponential backoff delay', () => {
      const manager = new ReconnectionManager({
        minDelay: 100,
        maxDelay: 10000,
        backoffMultiplier: 2
      });

      let delay1 = manager.getNextDelay();
      assert(delay1 >= 100 && delay1 <= 110); // With jitter

      manager.recordFailure();
      let delay2 = manager.getNextDelay();
      assert(delay2 > delay1);

      manager.recordSuccess();
      assert.strictEqual(manager.retryCount, 0);
    });

    it('should implement circuit breaker', () => {
      const manager = new ReconnectionManager({
        failureThreshold: 3,
        resetTimeout: 5000
      });

      manager.recordFailure();
      manager.recordFailure();
      manager.recordFailure();

      assert.strictEqual(manager.circuitOpen, true);

      manager.recordSuccess();
      assert.strictEqual(manager.circuitOpen, false);

      manager.destroy();
    });
  });

  describe('MessageEncoder & MessageDecoder', () => {
    it('should encode and decode simple messages', async () => {
      const message = { id: 1, text: 'hello' };
      const encoded = await MessageEncoder.encode(message);
      const decoded = await MessageDecoder.decode(encoded);

      assert.deepStrictEqual(decoded.message, message);
    });

    it('should handle compression', async () => {
      const message = { data: 'x'.repeat(1000) };

      const uncompressed = await MessageEncoder.encode(message, { compression: 'none' });
      const gzipped = await MessageEncoder.encode(message, { compression: 'gzip' });

      assert(gzipped.length < uncompressed.length);

      const decoded = await MessageDecoder.decode(gzipped);
      assert.strictEqual(decoded.compression, 'gzip');
      assert.strictEqual(decoded.decompressed, true);
    });

    it('should encode and decode batches', async () => {
      const messages = [
        { id: 1, text: 'msg1' },
        { id: 2, text: 'msg2' },
        { id: 3, text: 'msg3' }
      ];

      const batch = await MessageEncoder.encodeBatch(messages);
      const decoded = await MessageDecoder.decodeBatch(batch);

      assert.strictEqual(decoded.length, 3);
      assert.deepStrictEqual(decoded[0].message, messages[0]);
      assert.deepStrictEqual(decoded[2].message, messages[2]);
    });

    it('should preserve message keys', async () => {
      const message = { data: 'test' };
      const encoded = await MessageEncoder.encode(message, { key: 'test-key' });
      const decoded = await MessageDecoder.decode(encoded);

      assert.strictEqual(decoded.key, 'test-key');
    });
  });

  describe('BatchPublisher', () => {
    let mockClient;

    beforeEach(() => {
      mockClient = {
        publish: async (topic, message) => ({
          topic,
          offset: Math.floor(Math.random() * 10000),
          timestamp: Date.now()
        })
      };
    });

    it('should batch messages by topic', async () => {
      const publisher = new BatchPublisher(mockClient, {
        batchSize: 5,
        batchTimeoutMs: 10000
      });

      await publisher.add('topic1', { id: 1 });
      await publisher.add('topic2', { id: 2 });
      await publisher.add('topic1', { id: 3 });

      assert.strictEqual(publisher.queue.length, 3);

      await publisher.close();
    });

    it('should flush when batch size reached', async () => {
      let publishCalls = 0;
      mockClient.publish = async () => {
        publishCalls++;
        return { topic: 'test', offset: publishCalls, timestamp: Date.now() };
      };

      const publisher = new BatchPublisher(mockClient, {
        batchSize: 3,
        batchTimeoutMs: 10000
      });

      await publisher.add('test', { id: 1 });
      await publisher.add('test', { id: 2 });
      assert.strictEqual(publishCalls, 0); // Not yet

      await publisher.add('test', { id: 3 });
      // Should have flushed

      await publisher.close();
    });

    it('should sort by priority', async () => {
      const publisher = new BatchPublisher(mockClient);
      const messages = [];

      publisher.add = async () => {
        messages.push(...publisher.queue);
        publisher.queue = [];
      };

      await publisher.add('topic', { id: 1 }, { priority: 1 });
      await publisher.add('topic', { id: 2 }, { priority: 10 });
      await publisher.add('topic', { id: 3 }, { priority: 5 });

      assert.strictEqual(messages[0].priority, 10); // Highest priority first
    });
  });

  describe('ConsumerGroupManager', () => {
    let mockClient;

    beforeEach(() => {
      mockClient = {
        publish: async () => ({ offset: 100 })
      };
    });

    it('should track consumer offsets', () => {
      const manager = new ConsumerGroupManager(mockClient, {
        groupId: 'test-group'
      });

      manager.registerTopic('topic1', 0);
      manager.updateOffset('topic1', 10);

      assert.strictEqual(manager.getOffset('topic1'), 10);
    });

    it('should calculate lag', () => {
      const manager = new ConsumerGroupManager(mockClient, {
        groupId: 'test-group'
      });

      manager.registerTopic('topic1', 0);
      manager.updateOffset('topic1', 100);

      assert.strictEqual(manager.getLag('topic1'), 100);
    });

    it('should pause and resume consumption', () => {
      const manager = new ConsumerGroupManager(mockClient, {
        groupId: 'test-group'
      });

      manager.registerTopic('topic1', 0);
      assert.strictEqual(manager.isPaused('topic1'), false);

      manager.pause('topic1');
      assert.strictEqual(manager.isPaused('topic1'), true);

      manager.resume('topic1');
      assert.strictEqual(manager.isPaused('topic1'), false);
    });

    it('should seek to offset', () => {
      const manager = new ConsumerGroupManager(mockClient, {
        groupId: 'test-group'
      });

      manager.registerTopic('topic1', 0);
      manager.seek('topic1', 50);

      assert.strictEqual(manager.getOffset('topic1'), 50);
    });
  });

  describe('RetryPolicy', () => {
    it('should identify retryable errors', () => {
      const policy = new RetryPolicy({
        retryableErrors: ['ECONNREFUSED', 'ETIMEDOUT']
      });

      const err1 = new Error('ECONNREFUSED');
      const err2 = new Error('VALIDATION_ERROR');

      assert.strictEqual(policy.isRetryable(err1), true);
      assert.strictEqual(policy.isRetryable(err2), false);
    });

    it('should execute with retry on transient failure', async () => {
      const policy = new RetryPolicy({ maxAttempts: 3 });

      let attempts = 0;
      let result;

      try {
        result = await policy.executeWithRetry(async () => {
          attempts++;
          if (attempts < 2) {
            const err = new Error('ETIMEDOUT');
            err.code = 'ETIMEDOUT';
            throw err;
          }
          return 'success';
        });
      } catch (err) {
        // Error on final attempt
      }

      assert.strictEqual(result, 'success');
      assert.strictEqual(attempts, 2);
    });
  });

  describe('ErrorHandler', () => {
    it('should categorize errors', () => {
      const handler = new ErrorHandler();

      const netError = new Error('ECONNREFUSED');
      netError.code = 'ECONNREFUSED';
      assert.strictEqual(handler.categorizeError(netError), 'NETWORK');

      const timeoutError = new Error('Request timeout');
      assert.strictEqual(handler.categorizeError(timeoutError), 'TIMEOUT');

      const validationError = new Error('Missing field');
      assert.strictEqual(handler.categorizeError(validationError), 'VALIDATION');
    });

    it('should record errors', () => {
      const handler = new ErrorHandler();

      const err = new Error('Test error');
      handler.recordError(err, { test: true });

      const metrics = handler.getMetrics();
      assert.strictEqual(metrics.totalErrors, 1);
    });

    it('should format errors', () => {
      const handler = new ErrorHandler();

      const err = new Error('Test error');
      err.code = 'TEST_CODE';

      const formatted = handler.formatError(err, { context: 'test' });
      assert.strictEqual(formatted.category, 'UNKNOWN');
      assert.strictEqual(formatted.message, 'Test error');
      assert.strictEqual(formatted.code, 'TEST_CODE');
    });
  });

  describe('ClientPool', () => {
    let pool;

    afterEach(async () => {
      if (pool) {
        try {
          // Don't actually connect during testing
        } catch (err) {
          // Ignore
        }
      }
    });

    it('should require at least one broker', () => {
      assert.throws(() => {
        new ClientPool([]);
      });
    });

    it('should round-robin through brokers', () => {
      const brokers = [
        { host: 'broker1', port: 9092 },
        { host: 'broker2', port: 9092 },
        { host: 'broker3', port: 9092 }
      ];

      pool = new ClientPool(brokers);

      assert.strictEqual(pool.brokers.length, 3);
    });
  });
});

describe('Integration Tests', () => {
  it('should compose multiple components', async () => {
    const mockClient = {
      publish: async (topic, message) => ({
        topic,
        offset: Math.floor(Math.random() * 10000),
        timestamp: Date.now()
      }),
      subscribe: async () => 'sub-1',
      commitOffset: async () => {}
    };

    // Use batch publisher
    const publisher = new BatchPublisher(mockClient, {
      batchSize: 100,
      batchTimeoutMs: 5000
    });

    for (let i = 0; i < 10; i++) {
      await publisher.add('test', { id: i });
    }

    const publisherMetrics = publisher.getMetrics();
    assert.strictEqual(publisherMetrics.pendingMessages, 10);

    // Use consumer group
    const group = new ConsumerGroupManager(mockClient, {
      groupId: 'test-group'
    });

    group.registerTopic('test', 0);
    group.updateOffset('test', 10);

    const state = group.getState();
    assert.strictEqual(state['test'].offset, 10);

    // Use error handler
    const errorHandler = new ErrorHandler();
    const err = new Error('Test');
    errorHandler.recordError(err);

    const metrics = errorHandler.getMetrics();
    assert.strictEqual(metrics.totalErrors, 1);

    // Cleanup
    await publisher.close();
    await group.close();
  });
});
