'use strict';

import { PacketParser } from '../../../shared/protocol/codec.js';
import { Logger } from '../../../shared/utils/logger.js';

class ConnectionHandler {
  constructor(options) {
    const opts = options || {};
    this.socket = opts.socket;
    this.messageRouter = opts.messageRouter;
    this.brokerState = opts.brokerState;
    this.logger = opts.logger || new Logger({ name: 'connectionHandler' });

    this.remoteAddress = this.socket.remoteAddress;
    this.remotePort = this.socket.remotePort;
    this.connectionId = `${this.remoteAddress}:${this.remotePort}`;

    this.parser = new PacketParser({
      onPacket: (packet) => this._onPacket(packet),
      onError: (err) => this._onParseError(err)
    });

    this.closed = false;
  }

  initialize() {
    this.logger.info('Connection established', { connectionId: this.connectionId });

    this.socket.on('data', (chunk) => this._onData(chunk));
    this.socket.on('error', (err) => this._onError(err));
    this.socket.on('close', () => this._onClose());
    this.socket.on('end', () => this._onEnd());
  }

  _onData(chunk) {
    if (this.closed) {
      return;
    }

    try {
      this.parser.push(chunk);
    } catch (err) {
      this.logger.error('Parser push error', err);
      this.close();
    }
  }

  _onPacket(packet) {
    if (this.closed) {
      return;
    }

    this.logger.trace('Packet received', {
      connectionId: this.connectionId,
      opcode: packet.opcodeName
    });

    this.messageRouter.route(packet, this.socket).catch((err) => {
      this.logger.error('Message router error', err);
    });
  }

  _onParseError(err) {
    this.logger.error('Parse error', { connectionId: this.connectionId, err });
    this.close();
  }

  _onError(err) {
    this.logger.error('Socket error', { connectionId: this.connectionId, err });
    this.close();
  }

  _onEnd() {
    this.logger.debug('Socket ended', { connectionId: this.connectionId });
    this.close();
  }

  _onClose() {
    this.logger.info('Connection closed', { connectionId: this.connectionId });
    this.close();
  }

  close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    if (this.socket._subscriberId) {
      this.brokerState.unregisterSubscriber(this.socket._subscriberId);
    }

    if (!this.socket.destroyed) {
      this.socket.destroy();
    }
  }
}

export { ConnectionHandler };