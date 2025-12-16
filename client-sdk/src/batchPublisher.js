'use strict';

import { Logger } from '../../shared/utils/logger.js';

/**
 * Batch publisher for high-throughput scenarios
 * Accumulates messages and sends in batches for efficiency
 */
class BatchPublisher {
  constructor(client, options) {
    if (!client) {
      throw new Error('Client instance required');
    }

    const opts = options || {};

    this.client = client;
    this.logger = opts.logger || new Logger({ name: 'batchPublisher' });

    // Batch settings
    this.batchSize = opts.batchSize || 100;
    this.batchTimeoutMs = opts.batchTimeoutMs || 1000;
    this.compression = opts.compression || 'none';

    // Queue
    this.queue = [];
    this.batchTimer = null;
    this.drainPromises = [];

    // Metrics
    this.metrics = {
      batchesPublished: 0,
      messagesPublished: 0,
      bytesPublished: 0,
      errors: 0
    };
  }

  /**
   * Add message to batch
   * @param {string} topic
   * @param {*} message
   * @param {Object} options - { key?, priority? }
   * @returns {Promise<void>}
   */
  async add(topic, message, options) {
    if (!topic) {
      throw new Error('Topic required');
    }

    const opts = options || {};

    this.queue.push({
      topic,
      message,
      key: opts.key,
      timestamp: Date.now(),
      priority: opts.priority || 0
    });

    // Trigger publish if batch is full
    if (this.queue.length >= this.batchSize) {
      return this._flush();
    }

    // Start timer if first message
    if (this.queue.length === 1) {
      this._startBatchTimer();
    }

    return Promise.resolve();
  }

  /**
   * Flush pending messages immediately
   */
  async _flush() {
    if (this.queue.length === 0) {
      return;
    }

    this._stopBatchTimer();

    // Group by topic for efficiency
    const byTopic = new Map();
    for (const item of this.queue) {
      if (!byTopic.has(item.topic)) {
        byTopic.set(item.topic, []);
      }
      byTopic.get(item.topic).push(item);
    }

    this.queue = [];

    const promises = Array.from(byTopic.entries()).map(([topic, items]) =>
      this._publishBatch(topic, items)
    );

    await Promise.all(promises);
  }

  /**
   * Publish a batch of messages for a topic
   */
  async _publishBatch(topic, items) {
    try {
      // Sort by priority (descending) then by timestamp
      items.sort((a, b) => {
        if (a.priority !== b.priority) {
          return b.priority - a.priority;
        }
        return a.timestamp - b.timestamp;
      });

      // Publish in parallel with backpressure
      const concurrency = 10;
      for (let i = 0; i < items.length; i += concurrency) {
        const batch = items.slice(i, i + concurrency);
        await Promise.all(
          batch.map(item =>
            this.client.publish(topic, item.message, {
              key: item.key,
              compression: this.compression
            })
          )
        );
      }

      this.metrics.batchesPublished++;
      this.metrics.messagesPublished += items.length;

      this.logger.debug('Batch published', {
        topic,
        count: items.length
      });
    } catch (err) {
      this.metrics.errors++;
      this.logger.error('Batch publish failed', {
        topic,
        count: items.length,
        error: err.message
      });
      throw err;
    }
  }

  /**
   * Start batch timeout timer
   */
  _startBatchTimer() {
    if (this.batchTimer) {
      return;
    }

    this.batchTimer = setTimeout(() => {
      this.batchTimer = null;
      this._flush().catch(err => {
        this.logger.error('Scheduled flush failed', err);
      });
    }, this.batchTimeoutMs);
  }

  /**
   * Stop batch timer
   */
  _stopBatchTimer() {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
  }

  /**
   * Wait for all pending messages to be published
   */
  async drain() {
    if (this.queue.length === 0 && !this.batchTimer) {
      return;
    }

    return new Promise((resolve) => {
      this._flush().then(resolve).catch((err) => {
        this.logger.error('Drain failed', err);
        resolve(); // Resolve anyway
      });
    });
  }

  /**
   * Get metrics
   */
  getMetrics() {
    return {
      ...this.metrics,
      pendingMessages: this.queue.length
    };
  }

  /**
   * Cleanup
   */
  async close() {
    await this.drain();
    this._stopBatchTimer();
  }
}

export { BatchPublisher };
