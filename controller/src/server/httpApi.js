'use strict';

import { createServer } from 'http';
import { Logger } from '../../../shared/utils/logger';

/**
 * HTTPApi provides REST endpoints for querying cluster state,
 * topic metadata, and node health.
 */
class HTTPApi {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'httpApi' });
    
    this.nodeRegistry = opts.nodeRegistry;
    this.topicManager = opts.topicManager;
    this.leaderElection = opts.leaderElection;
    this.heartbeatListener = opts.heartbeatListener;
    
    if (!this.nodeRegistry) {
      throw new Error('nodeRegistry required');
    }
    if (!this.topicManager) {
      throw new Error('topicManager required');
    }

    this.port = opts.port || 8080;
    this.host = opts.host || '0.0.0.0';
    this.server = null;
    this.closed = false;

    this.stats = {
      totalRequests: 0,
      errors: 0,
      startedAt: Date.now()
    };
  }

  /**
   * Start the HTTP server
   */
  async start() {
    if (this.server) {
      throw new Error('HTTP API already started');
    }

    return new Promise((resolve, reject) => {
      this.server = createServer((req, res) => {
        this._handleRequest(req, res);
      });

      this.server.on('error', (err) => {
        this.logger.error('HTTP server error', err);
        if (!this.closed) {
          reject(err);
        }
      });

      this.server.listen(this.port, this.host, () => {
        this.logger.info('HTTP API started', {
          host: this.host,
          port: this.port
        });
        resolve();
      });
    });
  }

  /**
   * Handle incoming HTTP request
   */
  _handleRequest(req, res) {
    if (this.closed) {
      this._sendResponse(res, 503, { error: 'Server is closing' });
      return;
    }

    this.stats.totalRequests += 1;

    const { method, url } = req;
    this.logger.debug('HTTP request', { method, url });

    try {
      if (method === 'GET') {
        this._handleGetRequest(url, req, res);
      } else if (method === 'OPTIONS') {
        this._handleCORS(res);
      } else {
        this._sendResponse(res, 405, { error: 'Method not allowed' });
      }
    } catch (err) {
      this.logger.error('Request handler error', { url, error: err.message });
      this.stats.errors += 1;
      this._sendResponse(res, 500, { error: 'Internal server error' });
    }
  }

  /**
   * Handle GET requests
   */
  _handleGetRequest(url, req, res) {
    // Remove query string
    const path = url.split('?')[0];

    if (path === '/health') {
      this._handleHealth(req, res);
    } else if (path === '/cluster') {
      this._handleClusterInfo(req, res);
    } else if (path === '/cluster/nodes') {
      this._handleNodes(req, res);
    } else if (path === '/cluster/leader') {
      this._handleLeader(req, res);
    } else if (path === '/cluster/stats') {
      this._handleClusterStats(req, res);
    } else if (path === '/topics') {
      this._handleTopics(req, res);
    } else if (path.startsWith('/topics/')) {
      const topicName = path.substring(8);
      this._handleTopicDetail(topicName, req, res);
    } else if (path === '/metrics') {
      this._handleMetrics(req, res);
    } else {
      this._sendResponse(res, 404, { error: 'Not found' });
    }
  }

  /**
   * Health check endpoint
   */
  _handleHealth(req, res) {
    const hasQuorum = this.nodeRegistry.hasQuorum();
    const aliveNodes = this.nodeRegistry.getAliveNodes().length;
    
    const health = {
      status: hasQuorum ? 'healthy' : 'degraded',
      hasQuorum,
      aliveNodes,
      timestamp: Date.now()
    };

    this._sendResponse(res, 200, health);
  }

  /**
   * Cluster information endpoint
   */
  _handleClusterInfo(req, res) {
    const stats = this.nodeRegistry.getStats();
    const leader = this.nodeRegistry.getLeader();
    const hasQuorum = this.nodeRegistry.hasQuorum();

    const info = {
      cluster: {
        hasQuorum,
        totalNodes: stats.totalNodes,
        aliveNodes: stats.aliveNodes,
        deadNodes: stats.deadNodes
      },
      nodes: {
        leaders: stats.leaders,
        followers: stats.followers,
        candidates: stats.candidates
      },
      leader: leader ? {
        nodeId: leader.nodeId,
        metadata: leader.metadata,
        registeredAt: leader.registeredAt,
        lastHeartbeat: leader.lastHeartbeat
      } : null,
      term: this.leaderElection ? this.leaderElection.getCurrentTerm() : null,
      timestamp: Date.now()
    };

    this._sendResponse(res, 200, info);
  }

  /**
   * All nodes endpoint
   */
  _handleNodes(req, res) {
    const nodes = this.nodeRegistry.getAllNodes().map(node => ({
      nodeId: node.nodeId,
      state: node.state,
      isLeader: node.isLeader,
      metadata: node.metadata,
      registeredAt: node.registeredAt,
      lastHeartbeat: node.lastHeartbeat,
      heartbeatCount: node.heartbeatCount
    }));

    this._sendResponse(res, 200, { nodes, count: nodes.length });
  }

  /**
   * Current leader endpoint
   */
  _handleLeader(req, res) {
    const leader = this.nodeRegistry.getLeader();
    
    if (!leader) {
      this._sendResponse(res, 404, { error: 'No leader elected' });
      return;
    }

    this._sendResponse(res, 200, {
      nodeId: leader.nodeId,
      state: leader.state,
      metadata: leader.metadata,
      registeredAt: leader.registeredAt,
      lastHeartbeat: leader.lastHeartbeat,
      term: this.leaderElection ? this.leaderElection.getCurrentTerm() : null
    });
  }

  /**
   * Cluster statistics endpoint
   */
  _handleClusterStats(req, res) {
    const nodeStats = this.nodeRegistry.getStats();
    const topicStats = this.topicManager.getClusterStats();

    this._sendResponse(res, 200, {
      nodes: nodeStats,
      topics: topicStats,
      timestamp: Date.now()
    });
  }

  /**
   * All topics endpoint
   */
  _handleTopics(req, res) {
    const topics = this.topicManager.getAllTopics().map(topic => ({
      name: topic.topicName,
      leader: topic.leader,
      followers: topic.followers,
      isr: topic.isr,
      stats: topic.stats,
      createdAt: topic.createdAt,
      updatedAt: topic.updatedAt
    }));

    this._sendResponse(res, 200, { topics, count: topics.length });
  }

  /**
   * Topic detail endpoint
   */
  _handleTopicDetail(topicName, req, res) {
    const topic = this.topicManager.getTopic(topicName);
    
    if (!topic) {
      this._sendResponse(res, 404, { error: `Topic '${topicName}' not found` });
      return;
    }

    const leaderNode = this.nodeRegistry.getNode(topic.leader);
    const followerNodes = topic.followers.map(id => this.nodeRegistry.getNode(id)).filter(n => n);

    this._sendResponse(res, 200, {
      name: topic.topicName,
      leader: {
        nodeId: topic.leader,
        state: leaderNode ? leaderNode.state : 'unknown',
        metadata: leaderNode ? leaderNode.metadata : null
      },
      followers: followerNodes.map(node => ({
        nodeId: node.nodeId,
        state: node.state,
        metadata: node.metadata
      })),
      isr: topic.isr,
      stats: topic.stats,
      hasSufficientISR: this.topicManager.hasSufficientISR(topicName),
      createdAt: topic.createdAt,
      updatedAt: topic.updatedAt
    });
  }

  /**
   * Metrics endpoint
   */
  _handleMetrics(req, res) {
    const metrics = {
      api: this.stats,
      heartbeat: this.heartbeatListener ? this.heartbeatListener.getStats() : null,
      timestamp: Date.now()
    };

    this._sendResponse(res, 200, metrics);
  }

  /**
   * Handle CORS preflight
   */
  _handleCORS(res) {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    res.end();
  }

  /**
   * Send JSON response
   */
  _sendResponse(res, statusCode, data) {
    const body = JSON.stringify(data, null, 2);
    
    res.writeHead(statusCode, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Content-Length': Buffer.byteLength(body)
    });
    
    res.end(body);
  }

  /**
   * Stop the HTTP server
   */
  close() {
    if (this.closed) {
      return;
    }

    return new Promise((resolve) => {
      this.logger.info('Closing HTTP API');
      this.closed = true;

      if (this.server) {
        this.server.close(() => {
          this.logger.info('HTTP API closed');
          resolve();
        });
      } else {
        resolve();
      }
    });
  }
}

export default {
  HTTPApi
};