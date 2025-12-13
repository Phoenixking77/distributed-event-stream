'use strict';

import { TcpClient } from './connection/tcpClient';
import { encodePacket, decodePacket } from '../../shared/protocol/codec';
import { OPCODES } from '../../shared/protocol/packetTypes';
import { Logger } from '../../shared/utils/logger';

class Client {
  constructor(options) {
    const opts = options || {};
    this.host = opts.host || 'localhost';
    this.port = opts.port || 9092;
    this.logger = opts.logger || new Logger({ name: 'client' });

    this.tcpClient = new TcpClient({
      host: this.host,
      port: this.port,
      logger: this.logger.child({ component: 'tcpClient' })
    });

    this.subscriptions = new Map();
    this.connected = false;
  }

  async connect() {
    await this.tcpClient.connect();
    this.connected = true;
    this._setupPacketHandler();
    this.logger.info('Client connected');
  }

  async publish(topic, message) {
    if (!this.connected) {
      throw new Error('Client not connected');
    }

    const payload = JSON.stringify({ topic, message });
    const packet = encodePacket({
      opcode: OPCODES.PUBLISH,
      payload
    });

    const response = await this.tcpClient.send(packet);
    const ackData = JSON.parse(response.payload.toString('utf8'));

    if (ackData.error) {
      throw new Error(ackData.error);
    }

    this.logger.debug('Published', {
      topic,
      offset: ackData.offset
    });

    return {
      topic: ackData.topic,
      offset: ackData.offset,
      timestamp: ackData.timestamp
    };
  }

  async subscribe(topic, handler, options) {
    if (!this.connected) {
      throw new Error('Client not connected');
    }

    const opts = options || {};
    const offset = typeof opts.offset === 'number' ? opts.offset : 0;

    const payload = JSON.stringify({ topic, offset });
    const packet = encodePacket({
      opcode: OPCODES.SUBSCRIBE,
      payload
    });

    const response = await this.tcpClient.send(packet);
    const ackData = JSON.parse(response.payload.toString('utf8'));

    if (ackData.error) {
      throw new Error(ackData.error);
    }

    const subscriberId = ackData.subscriberId;
    this.subscriptions.set(topic, {
      subscriberId,
      handler,
      offset: ackData.offset
    });

    this.logger.info('Subscribed', {
      topic,
      subscriberId,
      offset: ackData.offset
    });

    return subscriberId;
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
      if (subscription && subscription.handler) {
        subscription.offset = offset;

        let parsedMessage;
        try {
          parsedMessage = JSON.parse(message);
        } catch {
          parsedMessage = message;
        }

        subscription.handler({
          topic,
          offset,
          timestamp,
          message: parsedMessage
        });
      }
    } catch (err) {
      this.logger.error('Failed to handle incoming message', err);
    }
  }

  disconnect() {
    this.tcpClient.disconnect();
    this.connected = false;
    this.subscriptions.clear();
    this.logger.info('Client disconnected');
  }
}

export default {
  Client
};