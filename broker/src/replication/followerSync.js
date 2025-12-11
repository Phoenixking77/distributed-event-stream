'use strict';

const { Logger } = require('../../../shared/utils/logger');
const { REPLICATION_TYPES, createReplicationMessage } = require('../../../shared/protocol/clusterTypes');

/**
 * FollowerSync handles receiving and applying replicated log entries from the leader.
 * Implements:
 * - Log entry validation
 * - Offset ordering verification
 * - Acknowledgment sending
 * - Lag tracking
 */
class FollowerSync {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'followerSync' });
    
    this.brokerId = opts.brokerId;
    this.brokerState = opts.brokerState;
    
    if (!this.brokerId) {
      throw new Error('brokerId required');
    }
    if (!this.brokerState) {
      throw new Error('brokerState required');
    }

    // Current leader connection
    this.leaderSocket = null;
    this.leaderId = null;

    // Replication state per topic: topicName -> { lastOffset, lastApplied }
    this.topicState = new Map();

    this.stats = {
      totalEntriesReceived: 0,
      totalBytesReceived: 0,
      totalEntriesApplied: 0,
      outOfOrderEntries: 0,
      replicationErrors: 0
    };

    this.closed = false;
  }

  /**
   * Handle replication message from leader
   */
  handleReplicationMessage(message, socket, leaderId) {
    if (this.closed) {
      return;
    }

    if (!this.leaderSocket) {
      this.leaderSocket = socket;
      this.leaderId = leaderId;
      this.logger.info('Follower sync established with leader', { leaderId });
    }

    const { type, data } = message;

    switch (type) {
      case REPLICATION_TYPES.APPEND_ENTRIES:
        this._handleAppendEntries(data, socket);
        break;
      case REPLICATION_TYPES.HEARTBEAT:
        this._handleHeartbeat(data, socket);
        break;
      default:
        this.logger.warn('Unknown replication message type', { type });
    }
  }

  /**
   * Handle append entries request
   */
  async _handleAppendEntries(data, socket) {
    const { leaderId, topic, entries } = data;

    if (!topic || !Array.isArray(entries)) {
      this.logger.warn('Invalid append entries request', { topic, entriesType: typeof entries });
      return;
    }

    this.logger.debug('Received append entries', {
      leaderId,
      topic,
      entryCount: entries.length
    });

    this.stats.totalEntriesReceived += entries.length;

    try {
      // Get or create topic
      const commitLog = await this.brokerState.getOrCreateTopic(topic);
      const topicState = this._getOrCreateTopicState(topic);

      for (const entry of entries) {
        const { offset, message, timestamp } = entry;

        // Validate offset ordering
        if (topicState.lastOffset >= 0 && offset <= topicState.lastOffset) {
          this.logger.warn('Out of order entry received', {
            topic,
            receivedOffset: offset,
            expectedOffset: topicState.lastOffset + 1
          });
          this.stats.outOfOrderEntries += 1;
          continue;
        }

        // Decode message from base64
        const messageBuffer = Buffer.from(message, 'base64');

        // Apply to commit log
        try {
          const result = commitLog.append(messageBuffer);
          
          topicState.lastOffset = offset;
          topicState.lastApplied = Date.now();
          this.stats.totalEntriesApplied += 1;

          this.logger.trace('Entry applied', {
            topic,
            offset,
            commitOffset: result.offset
          });

        } catch (err) {
          this.logger.error('Failed to apply entry', {
            topic,
            offset,
            error: err.message
          });
          this.stats.replicationErrors += 1;
        }
      }

      // Send acknowledgment
      if (entries.length > 0) {
        const lastEntry = entries[entries.length - 1];
        this._sendAck(socket, topic, lastEntry.offset);
      }

    } catch (err) {
      this.logger.error('Failed to handle append entries', {
        topic,
        error: err.message
      });
      this.stats.replicationErrors += 1;
    }
  }

  /**
   * Handle heartbeat from leader
   */
  _handleHeartbeat(data, socket) {
    this.logger.trace('Replication heartbeat received', data);
    // Heartbeat acknowledged implicitly by TCP connection
  }

  /**
   * Send acknowledgment to leader
   */
  _sendAck(socket, topic, offset) {
    const ack = createReplicationMessage(REPLICATION_TYPES.APPEND_ACK, {
      followerId: this.brokerId,
      topic,
      offset,
      timestamp: Date.now()
    });

    const payload = Buffer.from(JSON.stringify(ack), 'utf8');

    try {
      socket.write(payload, (err) => {
        if (err) {
          this.logger.error('Failed to send ACK', {
            topic,
            offset,
            error: err.message
          });
        } else {
          this.logger.trace('ACK sent', { topic, offset });
        }
      });
    } catch (err) {
      this.logger.error('Exception sending ACK', {
        topic,
        offset,
        error: err.message
      });
    }
  }

  /**
   * Get or create topic state
   */
  _getOrCreateTopicState(topic) {
    if (!this.topicState.has(topic)) {
      this.topicState.set(topic, {
        lastOffset: -1,
        lastApplied: Date.now()
      });
    }
    return this.topicState.get(topic);
  }

  /**
   * Get replication lag for a topic
   */
  getReplicationLag(topic) {
    const state = this.topicState.get(topic);
    if (!state) {
      return -1;
    }

    return Date.now() - state.lastApplied;
  }

  /**
   * Get follower statistics
   */
  getStats() {
    const topics = [];

    for (const [topicName, state] of this.topicState.entries()) {
      topics.push({
        topic: topicName,
        lastOffset: state.lastOffset,
        lag: Date.now() - state.lastApplied
      });
    }

    return {
      ...this.stats,
      leaderId: this.leaderId,
      topicCount: this.topicState.size,
      topics
    };
  }

  /**
   * Reset leader connection
   */
  resetLeader() {
    this.logger.info('Resetting leader connection', { oldLeader: this.leaderId });
    this.leaderSocket = null;
    this.leaderId = null;
  }

  /**
   * Close follower sync
   */
  close() {
    if (this.closed) {
      return;
    }

    this.logger.info('Closing follower sync');
    this.closed = true;

    this.leaderSocket = null;
    this.leaderId = null;
    this.topicState.clear();

    this.logger.info('Follower sync closed');
  }
}

module.exports = {
  FollowerSync
};