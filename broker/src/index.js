'use strict';

import http from 'http';
import { TcpServer } from './server/tcpServer.js';
import { BrokerState } from './state/brokerState.js';
import { Heartbeat } from './state/heartbeat.js';
import { ReplicationSender } from './replication/replicationSender.js';
import { FollowerSync } from './replication/followerSync.js';
import { Logger } from '../../shared/utils/logger.js';
import { loadConfig } from '../../shared/utils/configLoader.js';
import { NODE_STATES } from '../../shared/protocol/clusterTypes.js';

class Broker {
  constructor(options) {
    const opts = options || {};
    this.config = opts.config || loadConfig();
    this.logger = opts.logger || new Logger({ name: 'broker' });

    this.brokerId = opts.brokerId || `broker-${Date.now()}`;
    this.nodeState = NODE_STATES.FOLLOWER;

    // Allow port override from options or environment
    if (opts.port) {
      this.config.broker.port = opts.port;
    } else if (process.env.BROKER_PORT) {
      this.config.broker.port = parseInt(process.env.BROKER_PORT, 10);
    }

    this.brokerState = new BrokerState({
      dataDir: this.config.broker.dataDir,
      logConfig: this.config.log,
      logger: this.logger.child({ component: 'brokerState' })
    });

    this.replicationSender = new ReplicationSender({
      brokerId: this.brokerId,
      logger: this.logger.child({ component: 'replicationSender' }),
      batchSize: opts.replicationBatchSize,
      batchTimeoutMs: opts.replicationBatchTimeout,
      maxLagMs: opts.maxReplicationLag
    });

    this.followerSync = new FollowerSync({
      brokerId: this.brokerId,
      brokerState: this.brokerState,
      logger: this.logger.child({ component: 'followerSync' })
    });

    this.heartbeat = new Heartbeat({
      brokerId: this.brokerId,
      brokerHost: this.config.broker.host,
      brokerPort: this.config.broker.port,
      controllerHost: process.env.CONTROLLER_HOST || this.config.controller.host,
      controllerPort: parseInt(process.env.CONTROLLER_HEARTBEAT_PORT || this.config.controller.heartbeatPort || 9091, 10),
      electionPort: opts.electionPort || 9093,
      interval: this.config.heartbeat?.interval,
      brokerState: this.brokerState,
      replicationSender: this.replicationSender,
      followerSync: this.followerSync,
      logger: this.logger.child({ component: 'heartbeat' })
    });

    this.tcpServer = new TcpServer({
      host: this.config.broker.host,
      port: this.config.broker.port,
      brokerState: this.brokerState,
      replicationSender: this.replicationSender,
      followerSync: this.followerSync,
      onPublish: (topic, offset, message, timestamp) => {
        this._handlePublish(topic, offset, message, timestamp);
      },
      logger: this.logger.child({ component: 'tcpServer' })
    });

    this.running = false;
  }

  async start() {
    this.logger.info('Starting broker', {
      brokerId: this.brokerId,
      port: this.config.broker.port,
      dataDir: this.config.broker.dataDir,
      nodeState: this.nodeState
    });

    await this.tcpServer.start();
    this.heartbeat.start();
    this.running = true;

    this._setupSignalHandlers();
    this._startLeaderStatusCheck();

    this.logger.info('Broker started successfully', {
      brokerId: this.brokerId,
      nodeState: this.nodeState
    });
  }

  _handlePublish(topic, offset, message, timestamp) {
    if (this.nodeState === NODE_STATES.LEADER) {
      this.replicationSender.replicateEntry(topic, offset, message, timestamp);
    }
  }

  async promoteToLeader() {
    if (this.nodeState === NODE_STATES.LEADER) {
      return;
    }

    this.logger.info('Promoting broker to leader', { brokerId: this.brokerId });
    this.nodeState = NODE_STATES.LEADER;
    this.heartbeat.updateState(NODE_STATES.LEADER);
    
    this.followerSync.resetLeader();

    this.logger.info('Broker promoted to leader', { brokerId: this.brokerId });
  }

  async demoteToFollower() {
    if (this.nodeState === NODE_STATES.FOLLOWER) {
      return;
    }

    this.logger.info('Demoting broker to follower', { brokerId: this.brokerId });
    this.nodeState = NODE_STATES.FOLLOWER;
    this.heartbeat.updateState(NODE_STATES.FOLLOWER);

    this.replicationSender.close();

    this.logger.info('Broker demoted to follower', { brokerId: this.brokerId });
  }

  addFollower(followerId, host, port) {
    if (this.nodeState !== NODE_STATES.LEADER) {
      this.logger.warn('Attempted to add follower but not leader', {
        brokerId: this.brokerId,
        nodeState: this.nodeState
      });
      return;
    }

    this.replicationSender.addFollower(followerId, host, port);
  }

  removeFollower(followerId) {
    this.replicationSender.removeFollower(followerId);
  }

  async stop() {
    if (!this.running) {
      return;
    }

    this.logger.info('Stopping broker');
    this.running = false;

    this.heartbeat.close();
    this.replicationSender.close();
    this.followerSync.close();
    await this.tcpServer.stop();
    this.brokerState.close();

    this.logger.info('Broker stopped');
  }

  _startLeaderStatusCheck() {
    // Check leader status every 5 seconds
    setInterval(() => {
      this._checkLeaderStatus();
    }, 5000);
  }

  async _checkLeaderStatus() {
    try {
      const controllerHost = this.config.controller.host || 'localhost';
      const controllerPort = this.config.controller.httpPort || 8080;

      const response = await new Promise((resolve, reject) => {
        const req = http.get(
          `http://${controllerHost}:${controllerPort}/cluster/nodes`,
          (res) => {
            let data = '';
            res.on('data', chunk => { data += chunk; });
            res.on('end', () => {
              try {
                resolve(JSON.parse(data));
              } catch (e) {
                reject(e);
              }
            });
          }
        );
        req.on('error', reject);
        req.setTimeout(2000, () => {
          req.destroy();
          reject(new Error('Request timeout'));
        });
      });

      const nodes = response.nodes || [];
      const myNode = nodes.find(node => node.nodeId === this.brokerId);
      if (!myNode) return;

      const isCurrentlyLeader = this.nodeState === NODE_STATES.LEADER;
      const shouldBeLeader = myNode.state === NODE_STATES.LEADER || myNode.isLeader === true;

      if (shouldBeLeader && !isCurrentlyLeader) {
        this.logger.info('Controller selected this broker as leader');
        await this.promoteToLeader();
      } else if (!shouldBeLeader && isCurrentlyLeader) {
        this.logger.info('Controller demoted this broker from leader');
        await this.demoteToFollower();
      }
    } catch (err) {
      // Silently ignore errors in status check
    }
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
      brokerId: this.brokerId,
      running: this.running,
      nodeState: this.nodeState,
      connections: this.tcpServer.getConnectionCount(),
      broker: this.brokerState.getStats(),
      replication: this.nodeState === NODE_STATES.LEADER
        ? this.replicationSender.getStats()
        : this.followerSync.getStats(),
      heartbeat: this.heartbeat.getStats()
    };
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const brokerId = process.env.BROKER_ID || `broker-${process.pid}`;
  const broker = new Broker({ brokerId });
  broker.start().catch((err) => {
    console.error('Failed to start broker:', err);
    process.exit(1);
  });

  const shutdown = async (signal) => {
    console.log(`\nReceived ${signal}, shutting down gracefully...`);
    await broker.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

export { Broker };
