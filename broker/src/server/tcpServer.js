'use strict';

const net = require('net');
const { ConnectionHandler } = require('./connectionHandler');
const { MessageRouter } = require('./messageRouter');
const { Logger } = require('../../../shared/utils/logger');

class TcpServer {
  constructor(options) {
    const opts = options || {};
    this.host = opts.host || '0.0.0.0';
    this.port = opts.port || 9092;
    this.brokerState = opts.brokerState;
    this.logger = opts.logger || new Logger({ name: 'tcpServer' });

    this.messageRouter = new MessageRouter({
      brokerState: this.brokerState,
      logger: this.logger.child({ component: 'messageRouter' })
    });

    this.server = null;
    this.connections = new Set();
    this.listening = false;
  }

  start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        this._onConnection(socket);
      });

      this.server.on('error', (err) => {
        this.logger.error('Server error', err);
        reject(err);
      });

      this.server.listen(this.port, this.host, () => {
        this.listening = true;
        this.logger.info('TCP server listening', {
          host: this.host,
          port: this.port
        });
        resolve();
      });
    });
  }

  _onConnection(socket) {
    const handler = new ConnectionHandler({
      socket,
      messageRouter: this.messageRouter,
      brokerState: this.brokerState,
      logger: this.logger.child({ component: 'connectionHandler' })
    });

    this.connections.add(handler);
    handler.initialize();

    socket.on('close', () => {
      this.connections.delete(handler);
    });
  }

  stop() {
    return new Promise((resolve) => {
      this.logger.info('Stopping TCP server');
      this.listening = false;

      for (const handler of this.connections) {
        handler.close();
      }
      this.connections.clear();

      if (this.server) {
        this.server.close(() => {
          this.logger.info('TCP server stopped');
          resolve();
        });
      } else {
        resolve();
      }
    });
  }

  getConnectionCount() {
    return this.connections.size;
  }
}

module.exports = {
  TcpServer
};