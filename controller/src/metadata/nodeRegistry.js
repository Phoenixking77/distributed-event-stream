'use strict';

import { Logger } from '../../../shared/utils/logger';
import { NODE_STATES, HEARTBEAT_INTERVALS } from '../../../shared/protocol/clusterTypes';

/**
 * NodeRegistry manages the lifecycle and health of all broker nodes in the cluster.
 * Responsibilities:
 * - Track node registration and metadata
 * - Monitor heartbeats and detect failures
 * - Maintain node state transitions (follower/candidate/leader/dead)
 * - Provide cluster topology queries
 */
class NodeRegistry {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'nodeRegistry' });
    
    // Map of nodeId -> node metadata
    this.nodes = new Map();
    
    // Heartbeat configuration
    this.deadNodeTimeout = opts.deadNodeTimeout || HEARTBEAT_INTERVALS.DEAD_NODE_TIMEOUT;
    this.heartbeatCheckInterval = opts.heartbeatCheckInterval || 2000;
    
    // Health check timer
    this.healthCheckTimer = null;
    this.closed = false;
    
    // Event listeners for node state changes
    this.listeners = {
      nodeRegistered: [],
      nodeStateChanged: [],
      nodeDied: [],
      nodeRecovered: []
    };
  }

  /**
   * Initialize the registry and start health monitoring
   */
  initialize() {
    if (this.healthCheckTimer) {
      return;
    }

    this.logger.info('Initializing node registry', {
      deadNodeTimeout: this.deadNodeTimeout,
      checkInterval: this.heartbeatCheckInterval
    });

    this._startHealthCheck();
  }

  /**
   * Register or update a node with heartbeat
   */
  registerHeartbeat(nodeId, metadata) {
    if (this.closed) {
      throw new Error('NodeRegistry is closed');
    }

    const now = Date.now();
    const existing = this.nodes.get(nodeId);

    if (existing) {
      // Update existing node
      const wasAlive = existing.state !== NODE_STATES.DEAD;
      existing.lastHeartbeat = now;
      existing.metadata = { ...existing.metadata, ...metadata };
      existing.heartbeatCount += 1;

      // Check if node recovered from dead state
      if (!wasAlive && existing.state === NODE_STATES.DEAD) {
        this.logger.info('Node recovered from dead state', { nodeId });
        existing.state = NODE_STATES.FOLLOWER;
        this._emit('nodeRecovered', { nodeId, node: existing });
      }

      this.logger.trace('Node heartbeat received', {
        nodeId,
        heartbeatCount: existing.heartbeatCount,
        state: existing.state
      });
    } else {
      // Register new node
      const node = {
        nodeId,
        state: NODE_STATES.FOLLOWER,
        registeredAt: now,
        lastHeartbeat: now,
        heartbeatCount: 1,
        metadata: metadata || {},
        isLeader: false
      };

      this.nodes.set(nodeId, node);
      this.logger.info('New node registered', { nodeId, metadata });
      this._emit('nodeRegistered', { nodeId, node });
    }

    return this.nodes.get(nodeId);
  }

  /**
   * Update node state (follower/candidate/leader)
   */
  updateNodeState(nodeId, newState) {
    const node = this.nodes.get(nodeId);
    if (!node) {
      this.logger.warn('Attempted to update state of unknown node', { nodeId });
      return false;
    }

    const oldState = node.state;
    if (oldState === newState) {
      return true;
    }

    this.logger.info('Node state transition', {
      nodeId,
      oldState,
      newState
    });

    node.state = newState;
    node.isLeader = (newState === NODE_STATES.LEADER);

    this._emit('nodeStateChanged', {
      nodeId,
      oldState,
      newState,
      node
    });

    return true;
  }

  /**
   * Get node by ID
   */
  getNode(nodeId) {
    return this.nodes.get(nodeId) || null;
  }

  /**
   * Get all nodes
   */
  getAllNodes() {
    return Array.from(this.nodes.values());
  }

  /**
   * Get alive nodes (not in DEAD state)
   */
  getAliveNodes() {
    const alive = [];
    for (const node of this.nodes.values()) {
      if (node.state !== NODE_STATES.DEAD) {
        alive.push(node);
      }
    }
    return alive;
  }

  /**
   * Get nodes by state
   */
  getNodesByState(state) {
    const nodes = [];
    for (const node of this.nodes.values()) {
      if (node.state === state) {
        nodes.push(node);
      }
    }
    return nodes;
  }

  /**
   * Get current leader node
   */
  getLeader() {
    for (const node of this.nodes.values()) {
      if (node.isLeader && node.state === NODE_STATES.LEADER) {
        return node;
      }
    }
    return null;
  }

  /**
   * Get cluster statistics
   */
  getStats() {
    const stats = {
      totalNodes: this.nodes.size,
      aliveNodes: 0,
      deadNodes: 0,
      followers: 0,
      candidates: 0,
      leaders: 0
    };

    for (const node of this.nodes.values()) {
      if (node.state === NODE_STATES.DEAD) {
        stats.deadNodes += 1;
      } else {
        stats.aliveNodes += 1;
      }

      switch (node.state) {
        case NODE_STATES.FOLLOWER:
          stats.followers += 1;
          break;
        case NODE_STATES.CANDIDATE:
          stats.candidates += 1;
          break;
        case NODE_STATES.LEADER:
          stats.leaders += 1;
          break;
      }
    }

    return stats;
  }

  /**
   * Check if cluster has quorum (majority alive)
   */
  hasQuorum() {
    const alive = this.getAliveNodes();
    const total = this.nodes.size;
    return total > 0 && alive.length > Math.floor(total / 2);
  }

  /**
   * Add event listener
   */
  on(event, callback) {
    if (this.listeners[event]) {
      this.listeners[event].push(callback);
    }
  }

  /**
   * Remove event listener
   */
  off(event, callback) {
    if (this.listeners[event]) {
      const index = this.listeners[event].indexOf(callback);
      if (index > -1) {
        this.listeners[event].splice(index, 1);
      }
    }
  }

  /**
   * Emit event to all listeners
   */
  _emit(event, data) {
    const callbacks = this.listeners[event];
    if (callbacks) {
      for (const callback of callbacks) {
        try {
          callback(data);
        } catch (err) {
          this.logger.error('Event listener error', { event, error: err.message });
        }
      }
    }
  }

  /**
   * Start periodic health check
   */
  _startHealthCheck() {
    this.healthCheckTimer = setInterval(() => {
      this._checkNodeHealth();
    }, this.heartbeatCheckInterval);

    if (this.healthCheckTimer.unref) {
      this.healthCheckTimer.unref();
    }

    this.logger.debug('Health check timer started');
  }

  /**
   * Check health of all nodes and mark dead ones
   */
  _checkNodeHealth() {
    if (this.closed) {
      return;
    }

    const now = Date.now();
    const threshold = now - this.deadNodeTimeout;

    for (const [nodeId, node] of this.nodes.entries()) {
      if (node.state === NODE_STATES.DEAD) {
        continue;
      }

      if (node.lastHeartbeat < threshold) {
        this.logger.warn('Node marked as dead (heartbeat timeout)', {
          nodeId,
          lastHeartbeat: new Date(node.lastHeartbeat).toISOString(),
          timeoutMs: this.deadNodeTimeout
        });

        const oldState = node.state;
        node.state = NODE_STATES.DEAD;
        node.isLeader = false;

        this._emit('nodeDied', {
          nodeId,
          oldState,
          node
        });
      }
    }
  }

  /**
   * Stop health monitoring
   */
  _stopHealthCheck() {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
      this.logger.debug('Health check timer stopped');
    }
  }

  /**
   * Close the registry
   */
  close() {
    if (this.closed) {
      return;
    }

    this.logger.info('Closing node registry');
    this.closed = true;

    this._stopHealthCheck();
    this.nodes.clear();

    // Clear all listeners
    for (const key in this.listeners) {
      this.listeners[key] = [];
    }

    this.logger.info('Node registry closed');
  }
}

export default {
  NodeRegistry
};