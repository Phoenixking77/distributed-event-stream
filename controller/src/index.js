'use strict';

import { Logger } from '../../shared/utils/logger.js';
import { NodeRegistry } from './metadata/nodeRegistry.js';
import { TopicManager } from './metadata/topicManager.js';
import { ControllerRegistry } from './metadata/controllerRegistry.js';
import { LeaderElection } from './election/leaderElection.js';
import { UDPHeartbeatListener } from './server/udpHeartbeatListener.js';
import { HTTPApi } from './server/httpApi.js';

class Controller {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'controller' });
    
    this.config = {
      heartbeatPort: opts.heartbeatPort || 9091,
      electionPort: opts.electionPort || 9093,
      httpPort: opts.httpPort || 8080,
      host: opts.host || '0.0.0.0'
    };

    const peerString = opts.peers || process.env.CONTROLLER_PEERS || '';
    const peers = peerString
      .split(',')
      .map(s => s.trim())
      .filter(s => s.length > 0)
      .map(s => {
        const [host, port] = s.split(':');
        return { host, port: parseInt(port, 10) };
      });

    this.logger.debug('Parsed peer controllers', {
      peerString,
      peers: peers.map(p => `${p.host}:${p.port}`).join(','),
      count: peers.length
    });

    this.nodeRegistry = new NodeRegistry({
      logger: this.logger.child({ component: 'nodeRegistry' }),
      deadNodeTimeout: opts.deadNodeTimeout,
      heartbeatCheckInterval: opts.heartbeatCheckInterval
    });

    this.topicManager = new TopicManager({
      logger: this.logger.child({ component: 'topicManager' }),
      nodeRegistry: this.nodeRegistry,
      minIsr: opts.minIsr,
      maxLagMs: opts.maxLagMs
    });

    this.leaderElection = new LeaderElection({
      logger: this.logger.child({ component: 'leaderElection' }),
      nodeRegistry: this.nodeRegistry,
      electionPort: this.config.electionPort,
      host: this.config.host,
      peers: peers,
      electionTimeout: opts.electionTimeout,
      leaderCheckInterval: opts.leaderCheckInterval,
      forceLeader: (process.env.FORCE_LEADER === 'true') || !!opts.forceLeader,
      selfId: `${this.config.host}:${this.config.electionPort}`
    });

    this.controllerRegistry = new ControllerRegistry({
      logger: this.logger.child({ component: 'controllerRegistry' }),
      nodeRegistry: this.nodeRegistry,
      leaderElection: this.leaderElection,
      peers: peers
    });

    this.leaderElection.controllerRegistry = this.controllerRegistry;

    this.heartbeatListener = new UDPHeartbeatListener({
      logger: this.logger.child({ component: 'udpHeartbeatListener' }),
      nodeRegistry: this.nodeRegistry,
      topicManager: this.topicManager,
      port: this.config.heartbeatPort,
      host: this.config.host
    });

    this.httpApi = new HTTPApi({
      logger: this.logger.child({ component: 'httpApi' }),
      nodeRegistry: this.nodeRegistry,
      topicManager: this.topicManager,
      leaderElection: this.leaderElection,
      heartbeatListener: this.heartbeatListener,
      port: this.config.httpPort,
      host: this.config.host
    });

    this.started = false;
    this.closed = false;

    this._setupEventHandlers();
  }

  _setupEventHandlers() {
    this.nodeRegistry.on('nodeDied', ({ nodeId, oldState }) => {
      this.logger.warn('Node died, initiating failover', { nodeId, oldState });
      this.topicManager.handleNodeFailure(nodeId);
    });

    this.nodeRegistry.on('nodeRecovered', ({ nodeId }) => {
      this.logger.info('Node recovered', { nodeId });
    });

    this.nodeRegistry.on('nodeRegistered', ({ nodeId }) => {
      this.logger.info('New node joined cluster', { nodeId });
    });
  }

  async start() {
    if (this.started) {
      throw new Error('Controller already started');
    }
    if (this.closed) {
      throw new Error('Controller is closed');
    }

    this.logger.info('Starting controller', this.config);

    try {
      this.nodeRegistry.initialize();

      const advertisedHost = process.env.HOST ||
        (this.leaderElection && this.leaderElection.peers && this.leaderElection.peers[0] && this.leaderElection.peers[0].host) ||
        this.config.host ||
        'localhost';

      const selfId = `${advertisedHost}:${this.config.electionPort}`;
      
      this.controllerRegistry.initialize(selfId);

      if (this.leaderElection) {
        this.leaderElection.selfId = selfId;
      }

      await this.leaderElection.initialize();
      await this.heartbeatListener.start();
      await this.httpApi.start();

      this.started = true;

      this.logger.info('Controller started successfully', {
        heartbeatPort: this.config.heartbeatPort,
        electionPort: this.config.electionPort,
        httpPort: this.config.httpPort,
        selfId: selfId
      });

      this._logClusterState();

    } catch (err) {
      this.logger.error('Failed to start controller', err);
      await this.close();
      throw err;
    }
  }

  _logClusterState() {
    setTimeout(() => {
      if (this.closed) return;

      const nodeStats = this.nodeRegistry.getStats();
      const topicStats = this.topicManager.getClusterStats();
      const leader = this.nodeRegistry.getLeader();

      this.logger.info('Cluster state', {
        nodes: nodeStats,
        topics: {
          total: topicStats.totalTopics,
          underReplicated: topicStats.underReplicatedTopics
        },
        leader: leader ? leader.nodeId : 'none'
      });

      this._logClusterState();
    }, 30000);
  }

  getStatus() {
    return {
      started: this.started,
      closed: this.closed,
      hasQuorum: this.nodeRegistry.hasQuorum(),
      leader: this.nodeRegistry.getLeader(),
      nodes: this.nodeRegistry.getStats(),
      topics: this.topicManager.getClusterStats()
    };
  }

  async close() {
    if (this.closed) {
      return;
    }

    this.logger.info('Stopping controller');
    this.closed = true;

    const errors = [];

    try {
      await this.httpApi.close();
    } catch (err) {
      this.logger.error('Error closing HTTP API', err);
      errors.push(err);
    }

    try {
      this.leaderElection.close();
    } catch (err) {
      this.logger.error('Error closing leader election', err);
      errors.push(err);
    }

    try {
      this.heartbeatListener.close();
    } catch (err) {
      this.logger.error('Error closing heartbeat listener', err);
      errors.push(err);
    }

    try {
      this.nodeRegistry.close();
    } catch (err) {
      this.logger.error('Error closing node registry', err);
      errors.push(err);
    }

    if (errors.length > 0) {
      this.logger.warn('Controller closed with errors', { errorCount: errors.length });
    } else {
      this.logger.info('Controller stopped successfully');
    }
  }
}

export { Controller };

if (import.meta.url === `file://${process.argv[1]}`) {
  const controller = new Controller({
    heartbeatPort: process.env.HEARTBEAT_PORT || 9091,
    electionPort: process.env.ELECTION_PORT || 9093,
    httpPort: process.env.HTTP_PORT || 8080,
    host: process.env.HOST || '0.0.0.0',
    peers: process.env.CONTROLLER_PEERS
  });

  controller.start().catch(err => {
    console.error('Fatal error starting controller:', err);
    process.exit(1);
  });

  const shutdown = async (signal) => {
    console.log(`\nReceived ${signal}, shutting down gracefully...`);
    await controller.close();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}