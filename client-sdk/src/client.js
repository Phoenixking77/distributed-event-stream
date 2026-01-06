'use strict';

import { TcpClient } from './connection/tcpClient.js';
import { encodePacket, decodePacket } from '../../shared/protocol/codec.js';
import { OPCODES } from '../../shared/protocol/packetTypes.js';
import { Logger } from '../../shared/utils/logger.js';
import { EventEmitter } from 'events';

class Client extends EventEmitter {
  constructor(options) {
    super();

    const opts = options || {};

    this.host = opts.host || 'localhost';
    this.port = opts.port || 9092;
    this.logger = opts.logger || new Logger({ name: 'client' });

    this.maxRetries = opts.maxRetries || 10;
    this.initialRetryDelay = opts.initialRetryDelay || 100;
    this.maxRetryDelay = opts.maxRetryDelay || 30000;
    this.backoffMultiplier = opts.backoffMultiplier || 2;

    this.connectTimeout = opts.connectTimeout || 15000;
    this.requestTimeout = opts.requestTimeout || 15000;

    this.tcpClient = null;
    this.connected = false;
    this.closing = false;
    this.retryCount = 0;
    this.nextRetryDelay = this.initialRetryDelay;

    this.consumerGroups = new Map();
    this.subscriptions = new Map();
    this.pendingOffsetCommits = new Map();

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

    this.maxInflightRequests = opts.maxInflightRequests || 100;
    this.inflightRequests = 0;

    this.offsetCommitInterval = opts.offsetCommitInterval || 5000;
    this.offsetCommitTimer = null;
  }

  async connect() {
    if (this.connected) {
      this.logger.warn('Client already connected');
      return;
    }

    return this._connectWithRetry();
  }

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

  async _attemptConnect() {
    return new Promise((resolve, reject) => {
      let timeout = null;
      let resolved = false;

      const cleanup = () => {
        if (timeout) {
          clearTimeout(timeout);
          timeout = null;
        }
      };

      const onConnect = () => {
        if (resolved) return;
        resolved = true;
        cleanup();
        this.connected = true;
        this._setupPacketHandler();
        this._startOffsetCommitTimer();
        resolve();
      };

      const onError = (err) => {
        if (resolved) return;
        resolved = true;
        cleanup();
        this.connected = false;
        this.metrics.errors++;
        this.emit('error', err);
        reject(err);
      };

      try {
        this.tcpClient = new TcpClient({
          host: this.host,
          port: this.port,
          logger: this.logger.child({ component: 'tcpClient' })
        });

        this.tcpClient.on('connect', onConnect);
        this.tcpClient.on('error', onError);

        this.tcpClient.on('disconnect', () => {
          this.connected = false;
          this.emit('disconnected');
          if (!this.closing && this.retryCount < this.maxRetries) {
            this._connectWithRetry().catch(err => {
              this.logger.error('Reconnection failed', err);
            });
          }
        });

        timeout = setTimeout(() => {
          if (!resolved) {
            resolved = true;
            this.tcpClient.removeListener('connect', onConnect);
            this.tcpClient.removeListener('error', onError);
            if (this.tcpClient.socket) {
              this.tcpClient.socket.destroy();
            }
            reject(new Error('Connection timeout'));
          }
        }, this.connectTimeout);

        this.tcpClient.connect().catch(onError);
      } catch (err) {
        cleanup();
        reject(err);
      }
    });
  }

  async publish(topic, message, options) {
    if (!this.connected) {
      throw new Error('Client not connected');
    }

    if (this.closing) {
      throw new Error('Client is shutting down');
    }

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

  async subscribe(topic, handler, options) {
    if (!this.connected) {
      throw new Error('Client not connected');
    }

    if (this.closing) {
      throw new Error('Client is shutting down');
    }

    const opts = options || {};
    const groupId = opts.groupId || null;

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

  async commitOffset(groupId, topic, offset) {
    if (!this.connected) {
      throw new Error('Client not connected');
    }

    try {
      this.pendingOffsetCommits.set(groupId, {
        topic,
        offset,
        timestamp: Date.now()
      });

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

  pauseConsume(topic) {
    const subscription = this.subscriptions.get(topic);
    if (subscription) {
      subscription.paused = true;
      this.logger.info('Consumer paused', { topic });
    }
  }

  resumeConsume(topic) {
    const subscription = this.subscriptions.get(topic);
    if (subscription) {
      subscription.paused = false;
      this.logger.info('Consumer resumed', { topic });
    }
  }

  getConsumerGroupState(groupId) {
    return this.consumerGroups.get(groupId) || null;
  }

  getMetrics() {
    return {
      ...this.metrics,
      inflightRequests: this.inflightRequests,
      subscriptionCount: this.subscriptions.size,
      consumerGroupCount: this.consumerGroups.size,
      uptime: Date.now()
    };
  }

  _sendWithTimeout(packet) {
    return Promise.race([
      this.tcpClient.send(packet),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Request timeout')), this.requestTimeout)
      )
    ]);
  }

  _setupPacketHandler() {
    this.tcpClient.parser.onPacket = (packet) => {
      if (packet.opcode === OPCODES.ACK) {
        this.tcpClient._onPacket(packet);
      } else if (packet.opcode === OPCODES.PUBLISH) {
        this._handleIncomingMessage(packet);
      }
    };
  }

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

      if (subscription.groupId) {
        this.commitOffset(subscription.groupId, topic, offset).catch(err => {
          this.logger.error('Auto-commit failed', err);
        });
      }

      this.metrics.messagesReceived++;
      this.metrics.messagesBytesReceived += Buffer.byteLength(JSON.stringify(messageEvent));

      subscription.handler(messageEvent);
    } catch (err) {
      this.metrics.errors++;
      this.logger.error('Failed to handle incoming message', err);
      this.emit('error', err);
    }
  }

  _startOffsetCommitTimer() {
    this.offsetCommitTimer = setInterval(() => {
      this._flushOffsetCommits();
    }, this.offsetCommitInterval);
  }

  async _flushOffsetCommits() {
  }

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