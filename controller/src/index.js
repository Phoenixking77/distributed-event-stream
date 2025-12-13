'use strict';

import { Logger } from '../../shared/utils/logger';
import { NodeRegistry } from './metadata/nodeRegistry';
import { TopicManager } from './metadata/topicManager';
import { LeaderElection } from './election/leaderElection';
import { UDPHeartbeatListener } from './server/udpHeartbeatListener';
import { HTTPApi } from './server/httpApi';

/**
 * Controller is the central coordinator for the distributed event streaming cluster.
 * Responsibilities:
 * - Track all broker nodes and their health
 * - Manage topic metadata and partition assignments
 * - Coordinate leader election
 * - Expose HTTP API for cluster queries
 * - Handle failover scenarios
 */
class Controller {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'controller' });
    
    // Configuration
    this.config = {
      heartbeatPort: opts.heartbeatPort || 9091,
      electionPort: opts.electionPort || 9093,
      httpPort: opts.httpPort || 8080,
      host: opts.host || '0.0.0.0'
    };

    // Core components
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
      electionTimeout: opts.electionTimeout,
      leaderCheckInterval: opts.leaderCheckInterval
    });

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

  /**
   * Setup event handlers for node failures
   */
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

  /**
   * Start the controller
   */
  async start() {
    if (this.started) {
      throw new Error('Controller already started');
    }
    if (this.closed) {
      throw new Error('Controller is closed');
    }

    this.logger.info('Starting controller', this.config);

    try {
      // Initialize node registry
      this.nodeRegistry.initialize();

      // Start heartbeat listener
      await this.heartbeatListener.start();

      // Start leader election
      await this.leaderElection.initialize();

      // Start HTTP API
      await this.httpApi.start();

      this.started = true;

      this.logger.info('Controller started successfully', {
        heartbeatPort: this.config.heartbeatPort,
        electionPort: this.config.electionPort,
        httpPort: this.config.httpPort
      });

      this._logClusterState();

    } catch (err) {
      this.logger.error('Failed to start controller', err);
      await this.close();
      throw err;
    }
  }

  /**
   * Log current cluster state
   */
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

  /**
   * Get cluster status
   */
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

  /**
   * Stop the controller
   */
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

export default {
  Controller
};

if (require.main === module) {
  const controller = new Controller({
    heartbeatPort: process.env.HEARTBEAT_PORT || 9091,
    electionPort: process.env.ELECTION_PORT || 9093,
    httpPort: process.env.HTTP_PORT || 8080,
    host: process.env.HOST || '0.0.0.0'
  });

  controller.start().catch(err => {
    console.error('Fatal error starting controller:', err);
    process.exit(1);
  });

  process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT, shutting down gracefully...');
    await controller.close();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\nReceived SIGTERM, shutting down gracefully...');
    await controller.close();
    process.exit(0);
  });
}