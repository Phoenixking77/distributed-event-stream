'use strict';

import { createSocket } from 'dgram';
import { Logger } from '../../../shared/utils/logger.js';
import { NODE_STATES, HEARTBEAT_INTERVALS, createHeartbeatMessage } from '../../../shared/protocol/clusterTypes.js';

/**
 * Heartbeat sends periodic health signals from broker to controller.
 * Includes broker state, topic metadata, and replication status.
 */
class Heartbeat {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'heartbeat' });
    
    this.brokerId = opts.brokerId;
    this.brokerHost = opts.brokerHost || 'localhost';
    this.brokerPort = opts.brokerPort || 9092;
    this.electionPort = opts.electionPort || 9093;
    
    if (!this.brokerId) {
      throw new Error('brokerId required');
    }

    this.controllerHost = opts.controllerHost || 'localhost';
    this.controllerPort = opts.controllerPort || 9091;
    
    this.interval = opts.interval || HEARTBEAT_INTERVALS.BROKER_TO_CONTROLLER;

    // Dependencies
    this.brokerState = opts.brokerState;
    this.replicationSender = opts.replicationSender;
    this.followerSync = opts.followerSync;

    // Current node state
    this.nodeState = NODE_STATES.FOLLOWER;
    this.isLeader = false;

    this.socket = null;
    this.timer = null;
    this.closed = false;

    this.stats = {
      heartbeatsSent: 0,
      errors: 0,
      lastSent: 0
    };
  }

  /**
   * Start sending heartbeats
   */
  start() {
    if (this.timer) {
      throw new Error('Heartbeat already started');
    }

    this.socket = createSocket('udp4');

    this.socket.on('error', (err) => {
      this.logger.error('Heartbeat socket error', err);
      this.stats.errors += 1;
    }); 

    this.logger.info('Starting heartbeat', {
      brokerId: this.brokerId,
      controllerHost: this.controllerHost,
      controllerPort: this.controllerPort,
      interval: this.interval
    });

    this._sendHeartbeat();

    this.timer = setInterval(() => {
      this._sendHeartbeat();
    }, this.interval);

    if (this.timer.unref) {
      this.timer.unref();
    }
  }

  /**
   * Update node state (follower/leader)
   */
  updateState(newState) {
    if (this.nodeState !== newState) {
      this.logger.info('Node state changed', {
        oldState: this.nodeState,
        newState
      });
      
      this.nodeState = newState;
      this.isLeader = (newState === NODE_STATES.LEADER);

      // Send immediate heartbeat on state change
      this._sendHeartbeat();
    }
  }

  /**
   * Send heartbeat to controller
   */
  _sendHeartbeat() {
    if (this.closed || !this.socket) {
      return;
    }

    const metadata = this._gatherMetadata();

    const heartbeat = createHeartbeatMessage(
      this.brokerId,
      'broker',
      metadata
    );

    const buffer = Buffer.from(JSON.stringify(heartbeat), 'utf8');

    this.socket.send(buffer, this.controllerPort, this.controllerHost, (err) => {
      if (err) {
        this.logger.error('Failed to send heartbeat', err);
        this.stats.errors += 1;
      } else {
        this.stats.heartbeatsSent += 1;
        this.stats.lastSent = Date.now();

        this.logger.trace('Heartbeat sent', {
          brokerId: this.brokerId,
          state: this.nodeState,
          topics: metadata.topics ? metadata.topics.length : 0
        });
      }
    });
  }

  /**
   * Gather broker metadata for heartbeat
   */
  _gatherMetadata() {
    const metadata = {
      host: this.brokerHost,
      port: this.brokerPort,
      electionPort: this.electionPort,
      state: this.nodeState,
      isLeader: this.isLeader,
      topics: []
    };

    if (!this.brokerState) {
      return metadata;
    }

    const topics = this.brokerState.listTopics();

    for (const topicName of topics) {
      const commitLog = this.brokerState.getTopic(topicName);
      if (!commitLog) {
        continue;
      }

      const topicData = {
        name: topicName,
        role: this.isLeader ? 'leader' : 'follower',
        offset: commitLog.getLatestOffset()
      };

      // Add replication lag if follower
      if (!this.isLeader && this.followerSync) {
        const lag = this.followerSync.getReplicationLag(topicName);
        if (lag >= 0) {
          topicData.lag = lag;
        }
      }

      metadata.topics.push(topicData);
    }

    // Add replication statistics if leader
    if (this.isLeader && this.replicationSender) {
      metadata.replication = this.replicationSender.getStats();
    }

    return metadata;
  }

  /**
   * Get heartbeat statistics
   */
  getStats() {
    return {
      ...this.stats,
      nodeState: this.nodeState,
      isLeader: this.isLeader
    };
  }

  /**
   * Stop sending heartbeats
   */
  close() {
    if (this.closed) {
      return;
    }

    this.logger.info('Closing heartbeat');
    this.closed = true;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    if (this.socket) {
      this.socket.close();
      this.socket = null;
    }

    this.logger.info('Heartbeat closed');
  }
}

export { Heartbeat };