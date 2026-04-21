'use strict';

import { OPCODES } from '../../../shared/protocol/packetTypes.js';
import { encodePacket } from '../../../shared/protocol/codec.js';
import { Logger } from '../../../shared/utils/logger.js';
import { ENTRY_HEADER_SIZE } from '../log/segmentManager.js';

class MessageRouter {
  constructor(options) {
    const opts = options || {};
    this.brokerState = opts.brokerState;
    this.followerSync = opts.followerSync;
    this.onPublish = opts.onPublish;
    this.logger = opts.logger || new Logger({ name: 'messageRouter' });
  }

  async route(packet, socket) {
    const { opcode, payload } = packet;

    try {
      switch (opcode) {
        case OPCODES.PUBLISH:
          await this._handlePublish(payload, socket);
          break;

        case OPCODES.SUBSCRIBE:
          await this._handleSubscribe(payload, socket);
          break;

        case OPCODES.HEARTBEAT:
          await this._handleHeartbeat(payload, socket);
          break;

        case OPCODES.REPLICATION_PACKET:
          await this._handleReplication(payload, socket);
          break;

        default:
          this.logger.warn('Unknown opcode', { opcode });
          break;
      }
    } catch (err) {
      this.logger.error('Route handler error', err);
      this._sendErrorResponse(socket, err.message);
    }
  }

  async _handlePublish(payload, socket) {
    const data = JSON.parse(payload.toString('utf8'));
    const { topic, message } = data;

    if (!topic || !message) {
      throw new Error('Missing topic or message');
    }

    const commitLog = await this.brokerState.getOrCreateTopic(topic);
    const messageBuffer = Buffer.isBuffer(message)
      ? message
      : Buffer.from(JSON.stringify(message), 'utf8');

    const result = commitLog.append(messageBuffer);

    this.logger.debug('Published message', {
      topic,
      offset: result.offset,
      size: result.size
    });

    const ackPayload = JSON.stringify({
      topic: result.topic,
      offset: result.offset,
      timestamp: result.timestamp
    });

    const ackPacket = encodePacket({
      opcode: OPCODES.ACK,
      payload: ackPayload
    });

    socket.write(ackPacket);

    
    if (this.onPublish) {
      this.onPublish(topic, result.offset, messageBuffer, result.timestamp);
    }

    this._notifySubscribers(topic, result);
  }

  async _handleSubscribe(payload, socket) {
    const data = JSON.parse(payload.toString('utf8'));
    const { topic, offset } = data;

    if (!topic) {
      throw new Error('Missing topic');
    }

    const startOffset = typeof offset === 'number' ? offset : 0;
    const subscriberId = this.brokerState.registerSubscriber(socket, topic, startOffset);

    socket._subscriberId = subscriberId;

    this.logger.info('Subscription registered', {
      subscriberId,
      topic,
      offset: startOffset
    });

    const ackPayload = JSON.stringify({
      subscriberId,
      topic,
      offset: startOffset
    });

    const ackPacket = encodePacket({
      opcode: OPCODES.ACK,
      payload: ackPayload
    });

    socket.write(ackPacket);

    this._streamToSubscriber(subscriberId);
  }

  async _handleHeartbeat(payload, socket) {
    const ackPacket = encodePacket({
      opcode: OPCODES.ACK,
      payload: JSON.stringify({ status: 'alive' })
    });
    socket.write(ackPacket);
  }

  _notifySubscribers(topic, publishResult) {
    const subscribers = this.brokerState.getSubscribersForTopic(topic);
    for (const subscriber of subscribers) {
      setImmediate(() => {
        this._streamToSubscriber(subscriber.id);
      });
    }
  }

  _streamToSubscriber(subscriberId) {
    const subscriber = this.brokerState.getSubscriber(subscriberId);
    if (!subscriber || !subscriber.active) {
      return;
    }

    const commitLog = this.brokerState.getTopic(subscriber.topic);
    if (!commitLog) {
      return;
    }

    const latestOffset = commitLog.getLatestOffset();
    if (subscriber.offset >= latestOffset) {
      return;
    }

    try {
      const messages = commitLog.readRange(subscriber.offset, 10);
      for (const msg of messages) {
        const dataPacket = encodePacket({
          opcode: OPCODES.PUBLISH,
          payload: JSON.stringify({
            topic: msg.topic,
            offset: msg.offset,
            timestamp: msg.timestamp,
            message: msg.message.toString('utf8')
          })
        });

        subscriber.socket.write(dataPacket);
        const nextOffset = msg.offset + ENTRY_HEADER_SIZE + msg.message.length;
        this.brokerState.updateSubscriberOffset(subscriberId, nextOffset);
      }
    } catch (err) {
      this.logger.error('Stream error', err);
    }
  }

  async _handleReplication(payload, socket) {
    if (!this.followerSync) {
      this.logger.warn('Replication received but follower sync not configured');
      return;
    }

    try {
      const message = JSON.parse(payload.toString('utf8'));
      const leaderId = message.data?.leaderId || 'unknown';
      
      this.followerSync.handleReplicationMessage(message, socket, leaderId);
      
      this.logger.trace('Replication message handled', {
        type: message.type,
        leaderId
      });
    } catch (err) {
      this.logger.error('Failed to handle replication', err);
    }
  }

  _sendErrorResponse(socket, errorMessage) {
    try {
      const errorPacket = encodePacket({
        opcode: OPCODES.ACK,
        payload: JSON.stringify({ error: errorMessage })
      });
      socket.write(errorPacket);
    } catch (err) {
      this.logger.error('Failed to send error response', err);
    }
  }
}

export { MessageRouter };
