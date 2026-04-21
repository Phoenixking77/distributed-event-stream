'use strict';

import { Logger } from '../../shared/utils/logger.js';


class BatchPublisher {
  constructor(client, options) {
    if (!client) {
      throw new Error('Client instance required');
    }

    const opts = options || {};

    this.client = client;
    this.logger = opts.logger || new Logger({ name: 'batchPublisher' });

    
    this.batchSize = opts.batchSize || 100;
    this.batchTimeoutMs = opts.batchTimeoutMs || 1000;
    this.compression = opts.compression || 'none';

    
    this.queue = [];
    this.batchTimer = null;
    this.drainPromises = [];

    
    this.metrics = {
      batchesPublished: 0,
      messagesPublished: 0,
      bytesPublished: 0,
      errors: 0
    };
  }

  
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

    
    if (this.queue.length >= this.batchSize) {
      return this._flush();
    }

    
    if (this.queue.length === 1) {
      this._startBatchTimer();
    }

    return Promise.resolve();
  }

  
  async _flush() {
    if (this.queue.length === 0) {
      return;
    }

    this._stopBatchTimer();

    
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

  
  async _publishBatch(topic, items) {
    try {
      
      items.sort((a, b) => {
        if (a.priority !== b.priority) {
          return b.priority - a.priority;
        }
        return a.timestamp - b.timestamp;
      });

      
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

  
  _stopBatchTimer() {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
  }

  
  async drain() {
    if (this.queue.length === 0 && !this.batchTimer) {
      return;
    }

    return new Promise((resolve) => {
      this._flush().then(resolve).catch((err) => {
        this.logger.error('Drain failed', err);
        resolve(); 
      });
    });
  }

  
  getMetrics() {
    return {
      ...this.metrics,
      pendingMessages: this.queue.length
    };
  }

  
  async close() {
    await this.drain();
    this._stopBatchTimer();
  }
}

export { BatchPublisher };
