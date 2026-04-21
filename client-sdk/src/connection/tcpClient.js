'use strict';

import { connect as _connect } from 'net';
import { EventEmitter } from 'events';
import { PacketParser, decodePacket, encodePacket } from '../../../shared/protocol/codec.js';
import { Logger } from '../../../shared/utils/logger.js';

class TcpClient extends EventEmitter {
  constructor(options) {
    super();
    const opts = options || {};
    this.host = opts.host || 'localhost';
    this.port = opts.port || 9092;
    this.logger = opts.logger || new Logger({ name: 'tcpClient' });

    this.socket = null;
    this.connected = false;
    this.connecting = false;

    this.parser = new PacketParser({
      onPacket: (packet) => this._onPacket(packet),
      onError: (err) => this._onParseError(err)
    });

    this.packetHandlers = new Map();
    this.nextRequestId = 1;
  }

  connect() {
    return new Promise((resolve, reject) => {
      if (this.connected) {
        return resolve();
      }

      if (this.connecting) {
        return reject(new Error('Connection already in progress'));
      }

      this.connecting = true;

      this.socket = _connect({
        host: this.host,
        port: this.port
      });

      let connectTimer = null;

      const onConnect = () => {
        if (connectTimer) clearTimeout(connectTimer);
        this.connected = true;
        this.connecting = false;
        this.logger.info('Connected', { host: this.host, port: this.port });
        this.emit('connect');
        resolve();
      };

      const onError = (err) => {
        if (connectTimer) clearTimeout(connectTimer);
        this.logger.error('Socket error', err);
        this._failPendingRequests(err);
        this.emit('error', err);
        this.connecting = false;
        reject(err);
      };

      const onClose = () => {
        if (connectTimer) clearTimeout(connectTimer);
      };

      connectTimer = setTimeout(() => {
        this.socket.removeListener('connect', onConnect);
        this.socket.removeListener('error', onError);
        this.socket.removeListener('close', onClose);
        this.socket.destroy();
        reject(new Error('Connection timeout'));
      }, 15000);
      
      this.socket.on('connect', onConnect);
      this.socket.on('error', onError);
      this.socket.on('close', onClose);

      this.socket.on('data', (chunk) => {
        try {
          this.parser.push(chunk);
        } catch (err) {
          this.logger.error('Parser error', err);
        }
      });

      this.socket.on('close', () => {
        this.logger.info('Connection closed');
        this.connected = false;
        this.connecting = false;
        this._failPendingRequests(new Error('Connection closed'));
        this.emit('disconnect');
      });
    });
  }

  send(packet, options) {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        return reject(new Error('Not connected'));
      }

      const opts = options || {};
      const expectResponse = opts.expectResponse !== false;
      const timeout = opts.timeout || 5000;

      let requestId = null;
      let outboundPacket = packet;

      if (expectResponse) {
        requestId = this.nextRequestId++;
        outboundPacket = this._attachRequestId(packet, requestId);

        const timer = setTimeout(() => {
          const pending = this.packetHandlers.get(requestId);
          if (pending) {
            this.packetHandlers.delete(requestId);
            pending.reject(new Error('Request timeout'));
          }
        }, timeout);

        this.packetHandlers.set(requestId, {
          resolve,
          reject,
          timer
        });
      }

      this.socket.write(outboundPacket, (err) => {
        if (err) {
          if (expectResponse && requestId !== null) {
            const pending = this.packetHandlers.get(requestId);
            if (pending) {
              clearTimeout(pending.timer);
              this.packetHandlers.delete(requestId);
            }
          }
          reject(err);
        } else if (!expectResponse) {
          resolve();
        }
      });
    });
  }

  on(event, handler) {
    if (event === 'packet') {
      this.parser.onPacket = (packet) => {
        this._onPacket(packet);
        handler(packet);
      };
    } else {
      super.on(event, handler);
    }
  }

  _onPacket(packet) {
    const requestId = this._extractRequestId(packet);

    if (requestId !== null && this.packetHandlers.has(requestId)) {
      const pending = this.packetHandlers.get(requestId);
      clearTimeout(pending.timer);
      this.packetHandlers.delete(requestId);
      pending.resolve(packet);
      return;
    }

    if (this.packetHandlers.size === 1) {
      const [pendingRequestId, pending] = this.packetHandlers.entries().next().value;
      clearTimeout(pending.timer);
      this.packetHandlers.delete(pendingRequestId);
      pending.resolve(packet);
      return;
    }

    if (this.packetHandlers.size > 1) {
      this.logger.warn('Received response without requestId and multiple requests are pending');
    }
  }

  _attachRequestId(packet, requestId) {
    try {
      const decoded = decodePacket(packet);
      const payloadText = decoded.payload.toString('utf8');

      if (!payloadText) {
        return packet;
      }

      const payload = JSON.parse(payloadText);
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
        return packet;
      }

      return encodePacket({
        opcode: decoded.opcode,
        timestamp: decoded.timestamp,
        payload: {
          ...payload,
          requestId
        }
      });
    } catch {
      return packet;
    }
  }

  _extractRequestId(packet) {
    try {
      const payloadText = packet.payload.toString('utf8');
      if (!payloadText) {
        return null;
      }

      const payload = JSON.parse(payloadText);
      if (payload && typeof payload === 'object' && !Array.isArray(payload) && payload.requestId !== undefined) {
        return payload.requestId;
      }
    } catch {
      return null;
    }

    return null;
  }

  _failPendingRequests(error) {
    for (const [requestId, pending] of this.packetHandlers.entries()) {
      clearTimeout(pending.timer);
      pending.reject(error);
      this.packetHandlers.delete(requestId);
    }
  }

  _onParseError(err) {
    this.logger.error('Parse error', err);
    this._failPendingRequests(err);
  }

  disconnect() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
    this.connecting = false;
    this._failPendingRequests(new Error('Connection closed'));
  }
}

export {
  TcpClient
};