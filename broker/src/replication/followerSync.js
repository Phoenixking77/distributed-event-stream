'use strict';

import { Logger } from '../../../shared/utils/logger.js';
import { encodePacket } from '../../../shared/protocol/codec.js';
import { OPCODES } from '../../../shared/protocol/packetTypes.js';
import { REPLICATION_TYPES, createReplicationMessage } from '../../../shared/protocol/clusterTypes.js';


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

    
    this.leaderSocket = null;
    this.leaderId = null;

    
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
      
      const commitLog = await this.brokerState.getOrCreateTopic(topic);
      const topicState = this._getOrCreateTopicState(topic);

      for (const entry of entries) {
        const { offset, message, timestamp } = entry;

        
        if (topicState.lastOffset >= 0 && offset <= topicState.lastOffset) {
          this.logger.warn('Out of order entry received', {
            topic,
            receivedOffset: offset,
            expectedOffset: topicState.lastOffset + 1
          });
          this.stats.outOfOrderEntries += 1;
          continue;
        }

        
        const messageBuffer = Buffer.from(message, 'base64');

        
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

  
  _handleHeartbeat(data, socket) {
    this.logger.trace('Replication heartbeat received', data);
    
  }

  
  _sendAck(socket, topic, offset) {
    const ack = createReplicationMessage(REPLICATION_TYPES.APPEND_ACK, {
      followerId: this.brokerId,
      topic,
      offset,
      timestamp: Date.now()
    });

    const packet = encodePacket({
      opcode: OPCODES.REPLICATION_PACKET,
      payload: ack,
      timestamp: ack.timestamp
    });

    try {
      socket.write(packet, (err) => {
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

  
  _getOrCreateTopicState(topic) {
    if (!this.topicState.has(topic)) {
      this.topicState.set(topic, {
        lastOffset: -1,
        lastApplied: Date.now()
      });
    }
    return this.topicState.get(topic);
  }

  
  getReplicationLag(topic) {
    const state = this.topicState.get(topic);
    if (!state) {
      return -1;
    }

    return Date.now() - state.lastApplied;
  }

  
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

  
  resetLeader() {
    this.logger.info('Resetting leader connection', { oldLeader: this.leaderId });
    this.leaderSocket = null;
    this.leaderId = null;
  }

  
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

export { FollowerSync };