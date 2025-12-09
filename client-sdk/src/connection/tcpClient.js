'use strict';

const net = require('net');
const { PacketParser } = require('../../../shared/protocol/codec');
const { Logger } = require('../../../shared/utils/logger');

class TcpClient {
  constructor(options) {
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

      this.socket = net.connect({
        host: this.host,
        port: this.port
      });

      this.socket.on('connect', () => {
        this.connected = true;
        this.connecting = false;
        this.logger.info('Connected', { host: this.host, port: this.port });
        resolve();
      });

      this.socket.on('data', (chunk) => {
        try {
          this.parser.push(chunk);
        } catch (err) {
          this.logger.error('Parser error', err);
        }
      });

      this.socket.on('error', (err) => {
        this.logger.error('Socket error', err);
        if (this.connecting) {
          this.connecting = false;
          reject(err);
        }
      });

      this.socket.on('close', () => {
        this.logger.info('Connection closed');
        this.connected = false;
        this.connecting = false;
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
        const requestId = this.nextRequestId;
        this.nextRequestId += 1;

        let timer = null;
        const handler = (responsePacket) => {
          if (timer) {
            clearTimeout(timer);
          }
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
    } else if (this.socket) {
      this.socket.on(event, handler);
    }
  }

  _onPacket(packet) {
    const handlers = Array.from(this.packetHandlers.values());
    if (handlers.length > 0) {
      const handler = handlers.shift();
      const requestId = Array.from(this.packetHandlers.keys())[0];
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

module.exports = {
  TcpClient
};