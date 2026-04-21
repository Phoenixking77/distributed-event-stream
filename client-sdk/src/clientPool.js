'use strict';

import { Client } from './client.js';
import { Logger } from '../../shared/utils/logger.js';


class ClientPool {
  constructor(brokers, options) {
    if (!Array.isArray(brokers) || brokers.length === 0) {
      throw new Error('At least one broker must be specified');
    }

    const opts = options || {};

    this.brokers = brokers;
    this.logger = opts.logger || new Logger({ name: 'clientPool' });

    this.clients = new Map(); 
    this.loadBalancingStrategy = opts.loadBalancingStrategy || 'round-robin'; 
    this.currentIndex = 0;
    this.clientOptions = opts.clientOptions || {};

    
    this.metrics = {
      routedRequests: 0,
      failovers: 0,
      clientErrors: 0
    };
  }

  
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

  
  async publish(topic, message, options) {
    const client = this._selectClient();
    
    try {
      this.metrics.routedRequests++;
      return await client.publish(topic, message, options);
    } catch (err) {
      this.metrics.failovers++;
      this.logger.warn('Publish failed, attempting failover', { error: err.message });
      
      
      const nextClient = this._selectNextClient();
      if (nextClient === client) {
        throw err; 
      }

      return nextClient.publish(topic, message, options);
    }
  }

  
  async subscribe(topic, handler, options) {
    const client = this._selectClient();
    return client.subscribe(topic, handler, options);
  }

  
  async commitOffset(groupId, topic, offset) {
    const client = this._selectClient();
    return client.commitOffset(groupId, topic, offset);
  }

  
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

  
  _selectNextClient() {
    this.currentIndex = (this.currentIndex + 1) % this.clients.size;
    return Array.from(this.clients.values())[this.currentIndex];
  }

  
  _selectRoundRobin() {
    const clients = Array.from(this.clients.values());
    const client = clients[this.currentIndex];
    this.currentIndex = (this.currentIndex + 1) % clients.length;
    return client;
  }

  
  _selectLeastConnections() {
    const clients = Array.from(this.clients.values());
    return clients.reduce((min, client) => {
      const minInflight = min.inflightRequests;
      const clientInflight = client.inflightRequests;
      return clientInflight < minInflight ? client : min;
    });
  }

  
  _selectRandom() {
    const clients = Array.from(this.clients.values());
    return clients[Math.floor(Math.random() * clients.length)];
  }

  
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

  
  async disconnect() {
    const promises = Array.from(this.clients.values()).map(client => client.disconnect());
    await Promise.all(promises);
    this.clients.clear();
  }
}

export { ClientPool };
