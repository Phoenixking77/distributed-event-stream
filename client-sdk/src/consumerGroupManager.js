'use strict';

import { Logger } from '../../shared/utils/logger.js';

/**
 * Consumer group manager
 * Tracks offset state and handles auto-commit logic
 */
class ConsumerGroupManager {
  constructor(client, options) {
    if (!client) {
      throw new Error('Client instance required');
    }

    const opts = options || {};

    this.client = client;
    this.logger = opts.logger || new Logger({ name: 'consumerGroupManager' });

    // Group settings
    this.groupId = opts.groupId;
    this.enableAutoCommit = opts.enableAutoCommit !== false;
    this.autoCommitIntervalMs = opts.autoCommitIntervalMs || 5000;
    this.autoOffsetReset = opts.autoOffsetReset || 'latest'; // 'earliest', 'latest', 'none'
    this.maxPartitionBehind = opts.maxPartitionBehind || 1000;

    // State
    this.topics = new Map(); // topic -> { offset, lastCommitOffset, committed, paused }
    this.commitTimer = null;
    this.pendingOffsets = new Map(); // topic -> offset
    this.rebalancing = false;

    // Metrics
    this.metrics = {
      offsetsCommitted: 0,
      offsetCommitErrors: 0,
      messagesProcessed: 0,
      rebalances: 0
    };
  }

  /**
   * Register topic for consumer group
   */
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

  /**
   * Update offset when message is processed
   */
  updateOffset(topic, offset) {
    if (!this.topics.has(topic)) {
      this.registerTopic(topic, offset);
    }

    const state = this.topics.get(topic);
    state.offset = offset;
    this.pendingOffsets.set(topic, offset);
    this.metrics.messagesProcessed++;
  }

  /**
   * Commit offset for topic
   */
  async commitOffset(topic, offset) {
    if (!this.groupId) {
      this.logger.warn('No group ID set, skipping offset commit');
      return;
    }

    try {
      // In a real scenario, this would persist to broker
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

  /**
   * Commit all pending offsets
   */
  async commitAll() {
    const commits = Array.from(this.pendingOffsets.entries()).map(
      ([topic, offset]) => this.commitOffset(topic, offset)
    );

    return Promise.all(commits);
  }

  /**
   * Seek to specific offset
   */
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

  /**
   * Pause consuming from topic
   */
  pause(topic) {
    const state = this.topics.get(topic);
    if (state) {
      state.paused = true;
      this.logger.info('Consumer paused', { topic });
    }
  }

  /**
   * Resume consuming from topic
   */
  resume(topic) {
    const state = this.topics.get(topic);
    if (state) {
      state.paused = false;
      this.logger.info('Consumer resumed', { topic });
    }
  }

  /**
   * Check if paused
   */
  isPaused(topic) {
    const state = this.topics.get(topic);
    return state ? state.paused : false;
  }

  /**
   * Get current offset for topic
   */
  getOffset(topic) {
    const state = this.topics.get(topic);
    return state ? state.offset : null;
  }

  /**
   * Get lag (difference between current and committed)
   */
  getLag(topic) {
    const state = this.topics.get(topic);
    if (!state) return null;

    const lag = state.offset - state.lastCommitOffset;
    return lag > 0 ? lag : 0;
  }

  /**
   * Get consumer group state
   */
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

  /**
   * Start auto-commit timer
   */
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

  /**
   * Stop auto-commit timer
   */
  _stopCommitTimer() {
    if (this.commitTimer) {
      clearInterval(this.commitTimer);
      this.commitTimer = null;
    }
  }

  /**
   * Get metrics
   */
  getMetrics() {
    return {
      ...this.metrics,
      topicsCount: this.topics.size,
      pendingCommits: this.pendingOffsets.size,
      rebalancing: this.rebalancing
    };
  }

  /**
   * Cleanup
   */
  async close() {
    this._stopCommitTimer();
    await this.commitAll();
  }
}

export { ConsumerGroupManager };
