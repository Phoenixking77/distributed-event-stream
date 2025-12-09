'use strict';

const { TcpServer } = require('./server/tcpServer');
const { BrokerState } = require('./state/brokerState');
const { Logger } = require('../../shared/utils/logger');
const { loadConfig } = require('../../shared/utils/configLoader');

class Broker {
  constructor(options) {
    const opts = options || {};
    this.config = opts.config || loadConfig();
    this.logger = opts.logger || new Logger({ name: 'broker' });

    this.brokerState = new BrokerState({
      dataDir: this.config.broker.dataDir,
      logConfig: this.config.log,
      logger: this.logger.child({ component: 'brokerState' })
    });

    this.tcpServer = new TcpServer({
      host: this.config.broker.host,
      port: this.config.broker.port,
      brokerState: this.brokerState,
      logger: this.logger.child({ component: 'tcpServer' })
    });

    this.running = false;
  }

  async start() {
    this.logger.info('Starting broker', {
      port: this.config.broker.port,
      dataDir: this.config.broker.dataDir
    });

    await this.tcpServer.start();
    this.running = true;

    this._setupSignalHandlers();

    this.logger.info('Broker started successfully');
  }

  async stop() {
    if (!this.running) {
      return;
    }

    this.logger.info('Stopping broker');
    this.running = false;

    await this.tcpServer.stop();
    this.brokerState.close();

    this.logger.info('Broker stopped');
  }

  _setupSignalHandlers() {
    const shutdown = async (signal) => {
      this.logger.info('Received shutdown signal', { signal });
      await this.stop();
      process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
  }

  getStats() {
    return {
      running: this.running,
      connections: this.tcpServer.getConnectionCount(),
      broker: this.brokerState.getStats()
    };
  }
}

if (require.main === module) {
  const broker = new Broker();
  broker.start().catch((err) => {
    console.error('Failed to start broker:', err);
    process.exit(1);
  });
}

module.exports = {
  Broker
};