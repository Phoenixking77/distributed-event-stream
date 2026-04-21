'use strict';

import { createConnection } from 'net';
import { Logger } from '../../../shared/utils/logger.js';
import { PacketParser, encodePacket } from '../../../shared/protocol/codec.js';
import { OPCODES } from '../../../shared/protocol/packetTypes.js';
import { REPLICATION_TYPES, ISR_CONFIG, createReplicationMessage } from '../../../shared/protocol/clusterTypes.js';


class ReplicationSender {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'replicationSender' });
    
    this.brokerId = opts.brokerId;
    if (!this.brokerId) {
      throw new Error('brokerId required');
    }

    
    this.batchSize = opts.batchSize || 100;
    this.batchTimeoutMs = opts.batchTimeoutMs || 50;
    this.replicationTimeout = opts.replicationTimeout || ISR_CONFIG.REPLICATION_TIMEOUT;
    this.maxLagMs = opts.maxLagMs || ISR_CONFIG.MAX_LAG_MS;

    
    this.followers = new Map();

    
    this.topicState = new Map();

    
    this.replicationQueue = new Map();

    
    this.flushTimers = new Map();

    this.closed = false;

    this.stats = {
      totalBytesSent: 0,
      totalEntriesSent: 0,
      totalAcksReceived: 0,
      replicationErrors: 0
    };
  }

  
  addFollower(followerId, host, port) {
    if (this.followers.has(followerId)) {
      this.logger.debug('Follower already registered', { followerId });
      return;
    }

    const follower = {
      followerId,
      host,
      port,
      socket: null,
      connected: false,
      lastAckTime: Date.now(),
      lastAckOffset: 0,
      replicationLag: 0
    };

    this.followers.set(followerId, follower);
    this.logger.info('Follower registered for replication', { followerId, host, port });

    this._connectFollower(followerId);
  }

  
  removeFollower(followerId) {
    const follower = this.followers.get(followerId);
    if (!follower) {
      return;
    }

    this.logger.info('Removing follower', { followerId });

    if (follower.socket) {
      follower.socket.destroy();
    }

    this.followers.delete(followerId);
  }

  
  _connectFollower(followerId) {
    const follower = this.followers.get(followerId);
    if (!follower || follower.connected) {
      return;
    }

    this.logger.debug('Connecting to follower', {
      followerId,
      host: follower.host,
      port: follower.port
    });

    const socket = createConnection({
      host: follower.host,
      port: follower.port
    });

    const parser = new PacketParser({
      onPacket: (packet) => this._handleFollowerPacket(followerId, packet),
      onError: (err) => {
        this.logger.error('Follower packet parse error', {
          followerId,
          error: err.message
        });
        this.stats.replicationErrors += 1;
      }
    });
    follower.parser = parser;

    socket.on('connect', () => {
      this.logger.info('Connected to follower', { followerId });
      follower.connected = true;
      follower.socket = socket;
    });

    socket.on('error', (err) => {
      this.logger.error('Follower socket error', {
        followerId,
        error: err.message
      });
      follower.connected = false;
      this.stats.replicationErrors += 1;
      this._scheduleReconnect(followerId);
    });

    socket.on('close', () => {
      this.logger.warn('Follower connection closed', { followerId });
      follower.connected = false;
      follower.socket = null;
      this._scheduleReconnect(followerId);
    });

    socket.on('data', (data) => {
      try {
        parser.push(data);
      } catch (err) {
        this.logger.error('Failed to process follower data', {
          followerId,
          error: err.message
        });
        this.stats.replicationErrors += 1;
      }
    });

    follower.socket = socket;
  }

  
  _scheduleReconnect(followerId) {
    if (this.closed) {
      return;
    }

    setTimeout(() => {
      if (this.followers.has(followerId)) {
        this._connectFollower(followerId);
      }
    }, 5000);
  }

  
  _handleFollowerPacket(followerId, packet) {
    try {
      if (packet.opcode !== OPCODES.REPLICATION_PACKET) {
        return;
      }

      const response = JSON.parse(packet.payload.toString('utf8'));
      
      if (response.type === REPLICATION_TYPES.APPEND_ACK) {
        this._handleAck(followerId, response.data);
      }
    } catch (err) {
      this.logger.error('Failed to parse follower response', {
        followerId,
        error: err.message
      });
    }
  }

  
  _handleAck(followerId, data) {
    const { topic, offset } = data;
    
    const follower = this.followers.get(followerId);
    if (!follower) {
      return;
    }

    follower.lastAckTime = Date.now();
    follower.lastAckOffset = offset;

    const topicState = this.topicState.get(topic);
    if (topicState) {
      topicState.pendingAcks.delete(`${followerId}:${offset}`);
    }

    this.stats.totalAcksReceived += 1;

    this.logger.trace('Replication ACK received', {
      followerId,
      topic,
      offset
    });
  }

  
  replicateEntry(topic, offset, message, timestamp) {
    if (this.closed) {
      return;
    }

    if (!this.replicationQueue.has(topic)) {
      this.replicationQueue.set(topic, []);
    }

    const queue = this.replicationQueue.get(topic);
    queue.push({ topic, offset, message, timestamp });

    if (queue.length >= this.batchSize) {
      this._flushTopic(topic);
    } else {
      this._scheduleFlush(topic);
    }
  }

  
  _scheduleFlush(topic) {
    if (this.flushTimers.has(topic)) {
      return;
    }

    const timer = setTimeout(() => {
      this.flushTimers.delete(topic);
      this._flushTopic(topic);
    }, this.batchTimeoutMs);

    this.flushTimers.set(topic, timer);
  }

  
  _flushTopic(topic) {
    const queue = this.replicationQueue.get(topic);
    if (!queue || queue.length === 0) {
      return;
    }

    const timer = this.flushTimers.get(topic);
    if (timer) {
      clearTimeout(timer);
      this.flushTimers.delete(topic);
    }

    const entries = queue.splice(0, queue.length);
    
    this.logger.debug('Flushing replication batch', {
      topic,
      entryCount: entries.length
    });

    for (const follower of this.followers.values()) {
      if (follower.connected) {
        this._sendToFollower(follower, topic, entries);
      }
    }
  }

  
  _sendToFollower(follower, topic, entries) {
    if (!follower.socket || !follower.connected) {
      return;
    }

    const replicationData = createReplicationMessage(REPLICATION_TYPES.APPEND_ENTRIES, {
      leaderId: this.brokerId,
      topic,
      entries: entries.map(e => ({
        offset: e.offset,
        message: e.message.toString('base64'),
        timestamp: e.timestamp
      }))
    });

    const payload = Buffer.from(JSON.stringify(replicationData), 'utf8');
    const packet = encodePacket({
      opcode: OPCODES.REPLICATION_PACKET,
      payload,
      timestamp: Date.now()
    });

    try {
      follower.socket.write(packet, (err) => {
        if (err) {
          this.logger.error('Failed to send replication data', {
            followerId: follower.followerId,
            topic,
            error: err.message
          });
          this.stats.replicationErrors += 1;
        } else {
          this.stats.totalBytesSent += packet.length;
          this.stats.totalEntriesSent += entries.length;

          
          const topicState = this._getOrCreateTopicState(topic);
          for (const entry of entries) {
            topicState.pendingAcks.add(`${follower.followerId}:${entry.offset}`);
          }
          topicState.lastSent = Date.now();
        }
      });
    } catch (err) {
      this.logger.error('Exception sending replication data', {
        followerId: follower.followerId,
        error: err.message
      });
      this.stats.replicationErrors += 1;
    }
  }

  
  _getOrCreateTopicState(topic) {
    if (!this.topicState.has(topic)) {
      this.topicState.set(topic, {
        nextOffset: 0,
        pendingAcks: new Set(),
        lastSent: Date.now()
      });
    }
    return this.topicState.get(topic);
  }

  
  getStats() {
    const followerStats = [];

    for (const [followerId, follower] of this.followers.entries()) {
      followerStats.push({
        followerId,
        connected: follower.connected,
        lastAckTime: follower.lastAckTime,
        lastAckOffset: follower.lastAckOffset,
        replicationLag: follower.replicationLag
      });
    }

    return {
      ...this.stats,
      followerCount: this.followers.size,
      followers: followerStats
    };
  }

  
  getISR(topic) {
    const isr = [this.brokerId];
    const now = Date.now();

    for (const [followerId, follower] of this.followers.entries()) {
      if (!follower.connected) {
        continue;
      }

      const lag = now - follower.lastAckTime;
      if (lag < this.maxLagMs) {
        isr.push(followerId);
      }
    }

    return isr;
  }

  
  close() {
    if (this.closed) {
      return;
    }

    this.logger.info('Closing replication sender');
    this.closed = true;

    
    for (const timer of this.flushTimers.values()) {
      clearTimeout(timer);
    }
    this.flushTimers.clear();

    
    for (const follower of this.followers.values()) {
      if (follower.socket) {
        follower.socket.destroy();
      }
    }
    this.followers.clear();

    this.logger.info('Replication sender closed');
  }
}

export { ReplicationSender };