'use strict';

import { TcpClient } from './connection/tcpClient.js';
import { encodePacket, decodePacket } from '../../shared/protocol/codec.js';
import { OPCODES } from '../../shared/protocol/packetTypes.js';
import { Logger } from '../../shared/utils/logger.js';
import { EventEmitter } from 'events';

/**
 * @typedef {Object} PublishResult
 * @property {string} topic
 * @property {number} offset
 * @property {number} timestamp
 * @property {string} [partition]
 */

/**
 * @typedef {Object} ConsumerGroupState
 * @property {string} groupId
 * @property {string} topic
 * @property {number} committedOffset
 * @property {number} lastUpdate
 */

/**
 * Production-grade Kafka-like client with full advanced features:
 * - Publish with compression options
 * - Subscribe with offset management
 * - Consumer groups with offset commits
 * - Automatic reconnection with exponential backoff
 * - Backpressure handling
 * - Connection pooling (single connection with multiplexing)
 * - Graceful shutdown
 * - Comprehensive metrics
 */
class Client extends EventEmitter {
  constructor(options) {
    super();

    const opts = options || {};

    this.host = opts.host || 'localhost';
    this.port = opts.port || 9092;
    this.logger = opts.logger || new Logger({ name: 'client' });

    // Reconnection strategy
    this.maxRetries = opts.maxRetries || 10;
    this.initialRetryDelay = opts.initialRetryDelay || 100; // ms
    this.maxRetryDelay = opts.maxRetryDelay || 30000; // ms
    this.backoffMultiplier = opts.backoffMultiplier || 2;

    // Connection pooling & timeout
    this.connectTimeout = opts.connectTimeout || 5000;
    this.requestTimeout = opts.requestTimeout || 10000;

    // State
    this.tcpClient = null;
    this.connected = false;
    this.closing = false;
    this.retryCount = 0;
    this.nextRetryDelay = this.initialRetryDelay;

    // Consumer groups & offset management
    this.consumerGroups = new Map(); // groupId -> { topic, committedOffset }
    this.subscriptions = new Map(); // topic -> { subscriberId, handler, offset, groupId, paused }
    this.pendingOffsetCommits = new Map(); // groupId -> { topic, offset, timestamp }

    // Metrics
    this.metrics = {
      messagesPublished: 0,
      messagesBytesPublished: 0,
      messagesReceived: 0,
      messagesBytesReceived: 0,
      reconnections: 0,
      errors: 0,
      offsetCommits: 0,
      offsetCommitErrors: 0,
      subscriberCount: 0
    };

    // Backpressure handling
    this.maxInflightRequests = opts.maxInflightRequests || 100;
    this.inflightRequests = 0;

    // Timers for cleanup
    this.offsetCommitInterval = opts.offsetCommitInterval || 5000;
    this.offsetCommitTimer = null;
  }

  /**
   * Connect to broker with automatic reconnection
   */
  async connect() {
    if (this.connected) {
      this.logger.warn('Client already connected');
      return;
    }

    return this._connectWithRetry();
  }

  /**
   * Internal connection with exponential backoff retry
   */
  async _connectWithRetry() {
    try {
      await this._attemptConnect();
      this.retryCount = 0;
      this.nextRetryDelay = this.initialRetryDelay;
      this.emit('connected');
      this.logger.info('Client connected successfully');
    } catch (err) {
      this.retryCount++;

      if (this.retryCount >= this.maxRetries) {
        this.logger.error('Client connection failed after max retries', {
          retries: this.retryCount,
          error: err.message
        });
        this.emit('error', err);
        throw err;
      }

      this.metrics.reconnections++;
      const delay = Math.min(
        this.nextRetryDelay,
        this.maxRetryDelay
      );

      this.logger.warn('Client connection failed, retrying', {
        retry: this.retryCount,
        delayMs: delay,
        error: err.message
      });

      await new Promise(resolve => setTimeout(resolve, delay));
      this.nextRetryDelay = Math.floor(delay * this.backoffMultiplier);

      return this._connectWithRetry();
    }
  }

  /**
   * Single connection attempt
   */
  async _attemptConnect() {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, this.connectTimeout);

      try {
        this.tcpClient = new TcpClient({
          host: this.host,
          port: this.port,
          logger: this.logger.child({ component: 'tcpClient' })
        });

        this.tcpClient.on('connect', () => {
          clearTimeout(timeout);
          this.connected = true;
          this._setupPacketHandler();
          this._startOffsetCommitTimer();
          resolve();
        });

        this.tcpClient.on('error', (err) => {
          clearTimeout(timeout);
          this.connected = false;
          this.metrics.errors++;
          this.emit('error', err);
          reject(err);
        });

        this.tcpClient.on('disconnect', () => {
          this.connected = false;
          this.emit('disconnected');
          if (!this.closing && this.retryCount < this.maxRetries) {
            this._connectWithRetry().catch(err => {
              this.logger.error('Reconnection failed', err);
            });
          }
        });

        this.tcpClient.connect();
      } catch (err) {
        clearTimeout(timeout);
        reject(err);
      }
    });
  }

  /**
   * Publish a message with options
   * @param {string} topic
   * @param {*} message
   * @param {Object} options - compression, key, partition
   * @returns {Promise<PublishResult>}
   */
  async publish(topic, message, options) {
    if (!this.connected) {
      throw new Error('Client not connected');
    }

    if (this.closing) {
      throw new Error('Client is shutting down');
    }

    // Backpressure: wait if too many inflight requests
    if (this.inflightRequests >= this.maxInflightRequests) {
      await new Promise((resolve) => {
        const interval = setInterval(() => {
          if (this.inflightRequests < this.maxInflightRequests) {
            clearInterval(interval);
            resolve();
          }
        }, 10);
      });
    }

    const opts = options || {};
    const payload = JSON.stringify({
      topic,
      message,
      key: opts.key,
      partition: opts.partition,
      compression: opts.compression || 'none'
    });

    const messageSize = Buffer.byteLength(payload);

    this.inflightRequests++;

    try {
      const packet = encodePacket({
        opcode: OPCODES.PUBLISH,
        payload
      });

      const response = await this._sendWithTimeout(packet);
      const ackData = JSON.parse(response.payload.toString('utf8'));

      if (ackData.error) {
        throw new Error(ackData.error);
      }

      this.metrics.messagesPublished++;
      this.metrics.messagesBytesPublished += messageSize;

      this.logger.debug('Published message', {
        topic,
        offset: ackData.offset,
        size: messageSize
      });

      return {
        topic: ackData.topic,
        offset: ackData.offset,
        timestamp: ackData.timestamp,
        partition: ackData.partition
      };
    } finally {
      this.inflightRequests--;
    }
  }

  /**
   * Subscribe to topic with consumer group support
   * @param {string} topic
   * @param {Function} handler
   * @param {Object} options - offset, groupId
   * @returns {Promise<string>} subscriberId
   */
  async subscribe(topic, handler, options) {
    if (!this.connected) {
      throw new Error('Client not connected');
    }

    if (this.closing) {
      throw new Error('Client is shutting down');
    }

    const opts = options || {};
    const groupId = opts.groupId || null;

    // If using consumer group, retrieve last committed offset
    let offset = opts.offset || 0;
    if (groupId && this.consumerGroups.has(groupId)) {
      const groupState = this.consumerGroups.get(groupId);
      if (groupState.topic === topic) {
        offset = groupState.committedOffset;
      }
    }

    const payload = JSON.stringify({
      topic,
      offset,
      groupId
    });

    const packet = encodePacket({
      opcode: OPCODES.SUBSCRIBE,
      payload
    });

    const response = await this._sendWithTimeout(packet);
    const ackData = JSON.parse(response.payload.toString('utf8'));

    if (ackData.error) {
      throw new Error(ackData.error);
    }

    const subscriberId = ackData.subscriberId;

    this.subscriptions.set(topic, {
      subscriberId,
      handler,
      offset: ackData.offset,
      groupId,
      paused: false,
      lastActivity: Date.now()
    });

    // Initialize consumer group if using one
    if (groupId && !this.consumerGroups.has(groupId)) {
      this.consumerGroups.set(groupId, {
        topic,
        committedOffset: offset,
        lastCommit: Date.now()
      });
    }

    this.metrics.subscriberCount++;

    this.logger.info('Subscribed to topic', {
      topic,
      subscriberId,
      offset: ackData.offset,
      groupId
    });

    return subscriberId;
  }

  /**
   * Commit offset for consumer group
   * @param {string} groupId
   * @param {string} topic
   * @param {number} offset
   */
  async commitOffset(groupId, topic, offset) {
    if (!this.connected) {
      throw new Error('Client not connected');
    }

    try {
      // Stage commit for batch
      this.pendingOffsetCommits.set(groupId, {
        topic,
        offset,
        timestamp: Date.now()
      });

      // Update local state immediately
      if (this.consumerGroups.has(groupId)) {
        const groupState = this.consumerGroups.get(groupId);
        groupState.committedOffset = offset;
        groupState.lastCommit = Date.now();
      }

      this.metrics.offsetCommits++;

      this.logger.debug('Offset commit staged', {
        groupId,
        topic,
        offset
      });
    } catch (err) {
      this.metrics.offsetCommitErrors++;
      this.logger.error('Failed to commit offset', {
        groupId,
        topic,
        offset,
        error: err.message
      });
      throw err;
    }
  }

  /**
   * Pause consuming from topic
   */
  pauseConsume(topic) {
    const subscription = this.subscriptions.get(topic);
    if (subscription) {
      subscription.paused = true;
      this.logger.info('Consumer paused', { topic });
    }
  }

  /**
   * Resume consuming from topic
   */
  resumeConsume(topic) {
    const subscription = this.subscriptions.get(topic);
    if (subscription) {
      subscription.paused = false;
      this.logger.info('Consumer resumed', { topic });
    }
  }

  /**
   * Get consumer group state
   */
  getConsumerGroupState(groupId) {
    return this.consumerGroups.get(groupId) || null;
  }

  /**
   * Get client metrics
   */
  getMetrics() {
    return {
      ...this.metrics,
      inflightRequests: this.inflightRequests,
      subscriptionCount: this.subscriptions.size,
      consumerGroupCount: this.consumerGroups.size,
      uptime: Date.now()
    };
  }

  /**
   * Send with timeout
   */
  _sendWithTimeout(packet) {
    return Promise.race([
      this.tcpClient.send(packet),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Request timeout')), this.requestTimeout)
      )
    ]);
  }

  /**
   * Setup packet handler for incoming messages
   */
  _setupPacketHandler() {
    this.tcpClient.parser.onPacket = (packet) => {
      if (packet.opcode === OPCODES.ACK) {
        this.tcpClient._onPacket(packet);
      } else if (packet.opcode === OPCODES.PUBLISH) {
        this._handleIncomingMessage(packet);
      }
    };
  }

  /**
   * Handle incoming message from broker
   */
  _handleIncomingMessage(packet) {
    try {
      const data = JSON.parse(packet.payload.toString('utf8'));
      const { topic, offset, timestamp, message } = data;

      const subscription = this.subscriptions.get(topic);
      if (!subscription || subscription.paused) {
        return;
      }

      subscription.offset = offset;
      subscription.lastActivity = Date.now();

      let parsedMessage;
      try {
        parsedMessage = JSON.parse(message);
      } catch {
        parsedMessage = message;
      }

      const messageEvent = {
        topic,
        offset,
        timestamp,
        message: parsedMessage,
        partition: data.partition
      };

      // Auto-commit if using consumer group
      if (subscription.groupId) {
        this.commitOffset(subscription.groupId, topic, offset).catch(err => {
          this.logger.error('Auto-commit failed', err);
        });
      }

      this.metrics.messagesReceived++;
      this.metrics.messagesBytesReceived += Buffer.byteLength(JSON.stringify(messageEvent));

      // Call handler
      subscription.handler(messageEvent);
    } catch (err) {
      this.metrics.errors++;
      this.logger.error('Failed to handle incoming message', err);
      this.emit('error', err);
    }
  }

  /**
   * Start periodic offset commit timer
   */
  _startOffsetCommitTimer() {
    this.offsetCommitTimer = setInterval(() => {
      this._flushOffsetCommits();
    }, this.offsetCommitInterval);
  }

  /**
   * Flush pending offset commits (stub - for future persistence)
   */
  async _flushOffsetCommits() {
    // This would persist offsets to a separate storage
    // For now, they're kept in memory and committed on message receive
  }

  /**
   * Graceful shutdown
   */
  async disconnect() {
    this.closing = true;

    if (this.offsetCommitTimer) {
      clearInterval(this.offsetCommitTimer);
    }

    await this._flushOffsetCommits();

    if (this.tcpClient) {
      this.tcpClient.disconnect();
    }

    this.connected = false;
    this.subscriptions.clear();
    this.consumerGroups.clear();
    this.pendingOffsetCommits.clear();

    this.logger.info('Client disconnected gracefully', {
      metricsSnapshot: this.getMetrics()
    });

    this.emit('disconnected');
  }
}

export {
  Client
};