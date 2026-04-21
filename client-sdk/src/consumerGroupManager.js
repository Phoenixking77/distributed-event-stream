'use strict';

import { Logger } from '../../shared/utils/logger.js';


class ConsumerGroupManager {
  constructor(client, options) {
    if (!client) {
      throw new Error('Client instance required');
    }

    const opts = options || {};

    this.client = client;
    this.logger = opts.logger || new Logger({ name: 'consumerGroupManager' });

    
    this.groupId = opts.groupId;
    this.enableAutoCommit = opts.enableAutoCommit !== false;
    this.autoCommitIntervalMs = opts.autoCommitIntervalMs || 5000;
    this.autoOffsetReset = opts.autoOffsetReset || 'latest'; 
    this.maxPartitionBehind = opts.maxPartitionBehind || 1000;

    
    this.topics = new Map(); 
    this.commitTimer = null;
    this.pendingOffsets = new Map(); 
    this.rebalancing = false;

    
    this.metrics = {
      offsetsCommitted: 0,
      offsetCommitErrors: 0,
      messagesProcessed: 0,
      rebalances: 0
    };
  }

  
  registerTopic(topic, startOffset) {
    if (this.topics.has(topic)) {
      return;
    }

    const offset = typeof startOffset === 'number'
      ? startOffset
      : (this.autoOffsetReset === 'earliest' ? 0 : -1);

    this.topics.set(topic, {
      offset,
      lastCommitOffset: offset,
      committed: false,
      paused: false
    });

    if (this.enableAutoCommit && !this.commitTimer) {
      this._startCommitTimer();
    }
  }

  
  updateOffset(topic, offset) {
    if (!this.topics.has(topic)) {
      this.registerTopic(topic, offset);
    }

    const state = this.topics.get(topic);
    state.offset = offset;
    this.pendingOffsets.set(topic, offset);
    this.metrics.messagesProcessed++;
  }

  
  async commitOffset(topic, offset) {
    if (!this.groupId) {
      this.logger.warn('No group ID set, skipping offset commit');
      return;
    }

    try {
      
      const state = this.topics.get(topic);
      if (state) {
        state.lastCommitOffset = offset;
        state.committed = true;
      }

      this.pendingOffsets.delete(topic);
      this.metrics.offsetsCommitted++;

      this.logger.debug('Offset committed', {
        groupId: this.groupId,
        topic,
        offset
      });
    } catch (err) {
      this.metrics.offsetCommitErrors++;
      this.logger.error('Failed to commit offset', {
        groupId: this.groupId,
        topic,
        offset,
        error: err.message
      });
      throw err;
    }
  }

  
  async commitAll() {
    const commits = Array.from(this.pendingOffsets.entries()).map(
      ([topic, offset]) => this.commitOffset(topic, offset)
    );

    return Promise.all(commits);
  }

  
  seek(topic, offset) {
    if (!this.topics.has(topic)) {
      this.registerTopic(topic, offset);
      return;
    }

    const state = this.topics.get(topic);
    state.offset = offset;
    state.committed = false;

    this.logger.info('Seeked to offset', { topic, offset });
  }

  
  pause(topic) {
    const state = this.topics.get(topic);
    if (state) {
      state.paused = true;
      this.logger.info('Consumer paused', { topic });
    }
  }

  
  resume(topic) {
    const state = this.topics.get(topic);
    if (state) {
      state.paused = false;
      this.logger.info('Consumer resumed', { topic });
    }
  }

  
  isPaused(topic) {
    const state = this.topics.get(topic);
    return state ? state.paused : false;
  }

  
  getOffset(topic) {
    const state = this.topics.get(topic);
    return state ? state.offset : null;
  }

  
  getLag(topic) {
    const state = this.topics.get(topic);
    if (!state) return null;

    const lag = state.offset - state.lastCommitOffset;
    return lag > 0 ? lag : 0;
  }

  
  getState() {
    const state = {};
    for (const [topic, data] of this.topics) {
      state[topic] = {
        offset: data.offset,
        lastCommitOffset: data.lastCommitOffset,
        lag: data.offset - data.lastCommitOffset,
        committed: data.committed,
        paused: data.paused
      };
    }
    return state;
  }

  
  _startCommitTimer() {
    if (this.commitTimer) {
      return;
    }

    this.commitTimer = setInterval(() => {
      this.commitAll().catch(err => {
        this.logger.error('Auto-commit failed', err);
      });
    }, this.autoCommitIntervalMs);
  }

  
  _stopCommitTimer() {
    if (this.commitTimer) {
      clearInterval(this.commitTimer);
      this.commitTimer = null;
    }
  }

  
  getMetrics() {
    return {
      ...this.metrics,
      topicsCount: this.topics.size,
      pendingCommits: this.pendingOffsets.size,
      rebalancing: this.rebalancing
    };
  }

  
  async close() {
    this._stopCommitTimer();
    await this.commitAll();
  }
}

export { ConsumerGroupManager };
