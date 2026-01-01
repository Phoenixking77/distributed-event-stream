'use strict';

import { NODE_STATES } from '../../../shared/protocol/clusterTypes.js';
import { Logger } from '../../../shared/utils/logger.js';

/**
 * ControllerRegistry - Tracks controller peers as nodes in the cluster
 * 
 * Responsibilities:
 * - Register this controller and its peers in the node registry
 * - Update controller states based on election events
 * - Bridge between election system and node registry system
 * - Track which controller is the current leader
 */
class ControllerRegistry {
  constructor(options = {}) {
    this.logger = options.logger || new Logger({ name: 'controllerRegistry' });
    this.nodeRegistry = options.nodeRegistry;
    this.leaderElection = options.leaderElection;
    
    if (!this.nodeRegistry) {
      throw new Error('nodeRegistry required');
    }
    if (!this.leaderElection) {
      throw new Error('leaderElection required');
    }

    // Self identification
    this.selfId = options.selfId; // Will be set after initialization
    this.peers = options.peers || [];
    this.initialized = false;
  }

  /**
   * Initialize controller registration
   * Registers all peer controllers (NOT self, since controller doesn't send heartbeats)
   */
  initialize(selfId) {
    if (this.initialized) {
      return;
    }

    this.selfId = selfId;

    // Register self in nodeRegistry so controller state (leader) can be tracked
    // even though it doesn't send heartbeats to itself. This makes forced-leader
    // mode visible to the cluster health APIs.
    const [selfHost, selfPortStr] = String(this.selfId).split(':');
    const selfPort = parseInt(selfPortStr, 10) || null;

    const existingSelf = this.nodeRegistry.getNode(this.selfId);
    if (!existingSelf) {
      this.nodeRegistry.registerHeartbeat(this.selfId, {
        type: 'controller',
        role: 'self',
        host: selfHost,
        port: selfPort,
        registeredAt: Date.now()
      });
    }

    this.logger.info('Controller registry initialized', {
      selfId: this.selfId,
      peerCount: this.peers.length
    });

    // Register all peer controllers
    for (const peer of this.peers) {
      const peerId = `${peer.host}:${peer.port}`;
      this.nodeRegistry.registerHeartbeat(peerId, {
        type: 'controller',
        role: 'peer',
        host: peer.host,
        port: peer.port,
        registeredAt: Date.now()
      });

      this.logger.debug('Peer controller registered', {
        nodeId: peerId
      });
    }

    this.initialized = true;

    this.logger.info('Controller registry initialized', {
      selfId: this.selfId,
      peerCount: this.peers.length,
      totalControllers: 1 + this.peers.length
    });
  }

  /**
   * Handle leader election event
   * Updates node registry when a new leader is elected
   */
  handleLeaderElected(leaderId, term) {
    // Mark previous leader as follower
    const previousLeader = this.nodeRegistry.getLeader();
    if (previousLeader && previousLeader.nodeId !== leaderId) {
      this.nodeRegistry.updateNodeState(previousLeader.nodeId, NODE_STATES.FOLLOWER);
      this.logger.info('Previous leader stepped down', {
        previousLeader: previousLeader.nodeId
      });
    }

    // Update new leader state
    const node = this.nodeRegistry.getNode(leaderId);
    if (node) {
      this.nodeRegistry.updateNodeState(leaderId, NODE_STATES.LEADER);
      this.logger.info('Controller promoted to leader', {
        leaderId,
        term
      });

      // If this controller is elected as leader, promote a broker leader too
      if (leaderId === this.selfId) {
        this.promoteBrokerLeader();
      }
    } else {
      this.logger.warn('Unknown controller elected as leader', {
        leaderId,
        term
      });
    }
  }

  /**
   * Promote the first alive broker to be the broker leader
   */
  promoteBrokerLeader() {
    const aliveNodes = this.nodeRegistry.getAliveNodes();
    const brokers = aliveNodes.filter(node => 
      node.metadata && node.metadata.nodeType === 'broker'
    );

    if (brokers.length === 0) {
      this.logger.warn('No alive brokers to promote as leader');
      return;
    }

    // Select the first broker as leader
    const selectedBroker = brokers[0];
    const updated = this.nodeRegistry.updateNodeState(selectedBroker.nodeId, NODE_STATES.LEADER);

    if (updated) {
      this.logger.info('Broker promoted to leader', {
        brokerId: selectedBroker.nodeId,
        brokerPort: selectedBroker.metadata.port
      });
    }
  }

  /**
   * Update controller node based on heartbeat reception
   */
  handleHeartbeatReceived(leaderId) {
    const node = this.nodeRegistry.getNode(leaderId);
    if (node) {
      // Update heartbeat timestamp and state
      this.nodeRegistry.registerHeartbeat(leaderId, {
        type: 'controller',
        heartbeatReceived: Date.now()
      });

      // Ensure leader state is correct
      if (node.state !== NODE_STATES.LEADER) {
        this.nodeRegistry.updateNodeState(leaderId, NODE_STATES.LEADER);
      }
    }
  }

  /**
   * Get all controller nodes
   */
  getControllers() {
    return this.nodeRegistry.getAllNodes().filter(node => 
      node.metadata && node.metadata.type === 'controller'
    );
  }

  /**
   * Get current leader controller
   */
  getLeaderController() {
    return this.nodeRegistry.getLeader();
  }

  /**
   * Set peers (useful if peers change)
   */
  setPeers(peers) {
    this.peers = peers || [];
    
    if (this.initialized) {
      // Re-register peers if already initialized
      for (const peer of this.peers) {
        const peerId = `${peer.host}:${peer.port}`;
        const existing = this.nodeRegistry.getNode(peerId);
        if (!existing) {
          this.nodeRegistry.registerHeartbeat(peerId, {
            type: 'controller',
            role: 'peer',
            host: peer.host,
            port: peer.port
          });
        }
      }
    }
  }
}

export { ControllerRegistry };
