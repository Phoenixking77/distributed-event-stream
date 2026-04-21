'use strict';

import { Logger } from '../../../shared/utils/logger.js';
import { NODE_STATES, HEARTBEAT_INTERVALS } from '../../../shared/protocol/clusterTypes.js';


class NodeRegistry {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'nodeRegistry' });
    
    
    this.nodes = new Map();
    
    
    this.deadNodeTimeout = opts.deadNodeTimeout || HEARTBEAT_INTERVALS.DEAD_NODE_TIMEOUT;
    this.heartbeatCheckInterval = opts.heartbeatCheckInterval || 2000;
    
    
    this.healthCheckTimer = null;
    this.closed = false;
    
    
    this.listeners = {
      nodeRegistered: [],
      nodeStateChanged: [],
      nodeDied: [],
      nodeRecovered: []
    };
  }

  
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

  
  registerHeartbeat(nodeId, metadata) {
    if (this.closed) {
      throw new Error('NodeRegistry is closed');
    }

    const now = Date.now();
    const existing = this.nodes.get(nodeId);

    if (existing) {
      const wasAlive = existing.state !== NODE_STATES.DEAD;
      existing.lastHeartbeat = now;
      existing.metadata = { ...existing.metadata, ...metadata };
      existing.metadata.lastSeen = now;
      if (metadata && metadata.heartbeatType) {
        if (metadata.heartbeatType === 'leader') {
          existing.metadata.lastLeaderHeartbeat = now;
        } else if (metadata.heartbeatType === 'broker') {
          existing.metadata.lastBrokerHeartbeat = now;
        } else if (metadata.heartbeatType === 'registration') {
          existing.metadata.lastRegistrationHeartbeat = now;
        }
      }
      existing.heartbeatCount += 1;

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
      const node = {
        nodeId,
        state: NODE_STATES.FOLLOWER,
        registeredAt: now,
        lastHeartbeat: now,
        heartbeatCount: 1,
        metadata: metadata || {},
        isLeader: false
      };
      node.metadata.lastSeen = now;
      if (metadata && metadata.heartbeatType) {
        if (metadata.heartbeatType === 'leader') {
          node.metadata.lastLeaderHeartbeat = now;
        } else if (metadata.heartbeatType === 'broker') {
          node.metadata.lastBrokerHeartbeat = now;
        } else if (metadata.heartbeatType === 'registration') {
          node.metadata.lastRegistrationHeartbeat = now;
        }
      }

      this.nodes.set(nodeId, node);
      this.logger.info('New node registered', { nodeId, metadata });
      this._emit('nodeRegistered', { nodeId, node });
    }

    return this.nodes.get(nodeId);
  }

  
  updateNodeState(nodeId, newState, term = null) {
    const node = this.nodes.get(nodeId);
    if (!node) {
      this.logger.warn('Attempted to update state of unknown node', { nodeId, newState, term });
      return false;
    }

    const oldState = node.state;
    if (oldState === newState) {
      return true;
    }

    
    if (term !== null && node.metadata && node.metadata.type === 'controller') {
      if (!node.metadata.term || term > node.metadata.term) {
        node.metadata.term = term;
      } else if (term < node.metadata.term) {
        this.logger.warn('Rejecting stale term for node state update', {
          nodeId,
          newState,
          term,
          currentTerm: node.metadata.term
        });
        return false;
      }
    }

    this.logger.info('Node state transition', {
      nodeId,
      oldState,
      newState,
      term: term || undefined
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

  
  getNode(nodeId) {
    return this.nodes.get(nodeId) || null;
  }

  
  getAllNodes() {
    return Array.from(this.nodes.values());
  }

  
  getAliveNodes() {
    const alive = [];
    for (const node of this.nodes.values()) {
      if (node.state !== NODE_STATES.DEAD) {
        alive.push(node);
      }
    }
    return alive;
  }

  
  getNodesByState(state) {
    const nodes = [];
    for (const node of this.nodes.values()) {
      if (node.state === state) {
        nodes.push(node);
      }
    }
    return nodes;
  }

  
  getLeader() {
    for (const node of this.nodes.values()) {
      if (node.isLeader && node.state === NODE_STATES.LEADER) {
        return node;
      }
    }
    return null;
  }

  
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

  
  hasQuorum() {
    const alive = this.getAliveNodes();
    const total = this.nodes.size;
    return total > 0 && alive.length > Math.floor(total / 2);
  }

  
  on(event, callback) {
    if (this.listeners[event]) {
      this.listeners[event].push(callback);
    }
  }

  
  off(event, callback) {
    if (this.listeners[event]) {
      const index = this.listeners[event].indexOf(callback);
      if (index > -1) {
        this.listeners[event].splice(index, 1);
      }
    }
  }

  
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

  
  _startHealthCheck() {
    this.healthCheckTimer = setInterval(() => {
      this._checkNodeHealth();
    }, this.heartbeatCheckInterval);

    if (this.healthCheckTimer.unref) {
      this.healthCheckTimer.unref();
    }

    this.logger.debug('Health check timer started');
  }

  
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

      const lastActivity = Math.max(
        node.metadata?.lastLeaderHeartbeat || 0,
        node.metadata?.lastBrokerHeartbeat || 0,
        node.metadata?.lastRegistrationHeartbeat || 0,
        node.metadata?.lastSeen || 0,
        node.lastHeartbeat || 0
      );

      if (lastActivity < threshold) {
        this.logger.warn('Node marked as dead (heartbeat timeout)', {
          nodeId,
          lastHeartbeat: new Date(lastActivity).toISOString(),
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

  
  _stopHealthCheck() {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
      this.logger.debug('Health check timer stopped');
    }
  }

  
  close() {
    if (this.closed) {
      return;
    }

    this.logger.info('Closing node registry');
    this.closed = true;

    this._stopHealthCheck();
    this.nodes.clear();

    
    for (const key in this.listeners) {
      this.listeners[key] = [];
    }

    this.logger.info('Node registry closed');
  }
}

export {
  NodeRegistry
};