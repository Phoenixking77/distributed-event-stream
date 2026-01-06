'use strict';

import { connect as _connect } from 'net';
import { EventEmitter } from 'events';
import { PacketParser } from '../../../shared/protocol/codec.js';
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

      if (expectResponse) {
        const requestId = this.nextRequestId++;
        let timer = null;

        const handler = (responsePacket) => {
          if (timer) {
            clearTimeout(timer);
          }
          this.packetHandlers.delete(requestId);
          resolve(responsePacket);
        };

        this.packetHandlers.set(requestId, handler);

        timer = setTimeout(() => {
          this.packetHandlers.delete(requestId);
          reject(new Error('Request timeout'));
        }, timeout);

        packet._requestId = requestId;
      }

      this.socket.write(packet, (err) => {
        if (err) {
          if (expectResponse && packet._requestId) {
            this.packetHandlers.delete(packet._requestId);
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
    // First, check if any handler is waiting for a response
    if (packet._requestId && this.packetHandlers.has(packet._requestId)) {
      const handler = this.packetHandlers.get(packet._requestId);
      handler(packet);
    } else if (this.packetHandlers.size > 0) {
      // If no request ID, give to the first waiting handler (FIFO)
      const [requestId, handler] = this.packetHandlers.entries().next().value;
      this.packetHandlers.delete(requestId);
      handler(packet);
    }
  }

  _onParseError(err) {
    this.logger.error('Parse error', err);
  }

  disconnect() {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
    this.connecting = false;
    this.packetHandlers.clear();
  }
}

export {
  TcpClient
};