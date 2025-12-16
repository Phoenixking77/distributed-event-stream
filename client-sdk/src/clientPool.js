'use strict';

import { Client } from './client.js';
import { Logger } from '../../shared/utils/logger.js';

/**
 * Client pool for managing connections to multiple brokers
 * Implements intelligent routing and failover
 */
class ClientPool {
  constructor(brokers, options) {
    if (!Array.isArray(brokers) || brokers.length === 0) {
      throw new Error('At least one broker must be specified');
    }

    const opts = options || {};

    this.brokers = brokers;
    this.logger = opts.logger || new Logger({ name: 'clientPool' });

    this.clients = new Map(); // brokerId -> Client
    this.loadBalancingStrategy = opts.loadBalancingStrategy || 'round-robin'; // round-robin, least-connections, random
    this.currentIndex = 0;
    this.clientOptions = opts.clientOptions || {};

    // Metrics
    this.metrics = {
      routedRequests: 0,
      failovers: 0,
      clientErrors: 0
    };
  }

  /**
   * Connect all brokers
   */
  async connect() {
    const promises = this.brokers.map((broker, idx) => {
      const brokerId = `${broker.host}:${broker.port}`;
      const client = new Client({
        ...broker,
        ...this.clientOptions,
        logger: this.logger.child({ broker: brokerId })
      });

      this.clients.set(brokerId, client);
      return client.connect();
    });

    return Promise.all(promises);
  }

  /**
   * Publish to least-connected broker
   */
  async publish(topic, message, options) {
    const client = this._selectClient();
    
    try {
      this.metrics.routedRequests++;
      return await client.publish(topic, message, options);
    } catch (err) {
      this.metrics.failovers++;
      this.logger.warn('Publish failed, attempting failover', { error: err.message });
      
      // Try next broker
      const nextClient = this._selectNextClient();
      if (nextClient === client) {
        throw err; // All brokers failed
      }

      return nextClient.publish(topic, message, options);
    }
  }

  /**
   * Subscribe to topic via any broker
   */
  async subscribe(topic, handler, options) {
    const client = this._selectClient();
    return client.subscribe(topic, handler, options);
  }

  /**
   * Commit offset via any broker
   */
  async commitOffset(groupId, topic, offset) {
    const client = this._selectClient();
    return client.commitOffset(groupId, topic, offset);
  }

  /**
   * Select client based on load balancing strategy
   */
  _selectClient() {
    switch (this.loadBalancingStrategy) {
      case 'round-robin':
        return this._selectRoundRobin();
      case 'least-connections':
        return this._selectLeastConnections();
      case 'random':
        return this._selectRandom();
      default:
        return this._selectRoundRobin();
    }
  }

  /**
   * Select next client (for failover)
   */
  _selectNextClient() {
    this.currentIndex = (this.currentIndex + 1) % this.clients.size;
    return Array.from(this.clients.values())[this.currentIndex];
  }

  /**
   * Round-robin selection
   */
  _selectRoundRobin() {
    const clients = Array.from(this.clients.values());
    const client = clients[this.currentIndex];
    this.currentIndex = (this.currentIndex + 1) % clients.length;
    return client;
  }

  /**
   * Select client with least active connections
   */
  _selectLeastConnections() {
    const clients = Array.from(this.clients.values());
    return clients.reduce((min, client) => {
      const minInflight = min.inflightRequests;
      const clientInflight = client.inflightRequests;
      return clientInflight < minInflight ? client : min;
    });
  }

  /**
   * Random selection
   */
  _selectRandom() {
    const clients = Array.from(this.clients.values());
    return clients[Math.floor(Math.random() * clients.length)];
  }

  /**
   * Get health status of all brokers
   */
  getHealthStatus() {
    const status = {};
    for (const [brokerId, client] of this.clients) {
      status[brokerId] = {
        connected: client.connected,
        inflightRequests: client.inflightRequests,
        metrics: client.getMetrics()
      };
    }
    return status;
  }

  /**
   * Get all client metrics
   */
  getMetrics() {
    const clientMetrics = {};
    for (const [brokerId, client] of this.clients) {
      clientMetrics[brokerId] = client.getMetrics();
    }
    return {
      ...this.metrics,
      clientMetrics,
      brokerCount: this.clients.size
    };
  }

  /**
   * Disconnect all clients
   */
  async disconnect() {
    const promises = Array.from(this.clients.values()).map(client => client.disconnect());
    await Promise.all(promises);
    this.clients.clear();
  }
}

export { ClientPool };
