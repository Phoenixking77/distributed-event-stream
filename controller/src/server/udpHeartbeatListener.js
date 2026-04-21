'use strict';

import { createSocket } from 'dgram';
import { Logger } from '../../../shared/utils/logger.js';
import { createHeartbeatMessage } from '../../../shared/protocol/clusterTypes.js';


class UDPHeartbeatListener {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'udpHeartbeatListener' });
    
    this.nodeRegistry = opts.nodeRegistry;
    this.topicManager = opts.topicManager;
    
    if (!this.nodeRegistry) {
      throw new Error('nodeRegistry required');
    }
    
    if (!this.topicManager) {
      throw new Error('topicManager required');
    }

    this.port = opts.port || 9091;
    this.host = opts.host || '0.0.0.0';
    this.socket = null;
    this.closed = false;

    this.stats = {
      totalHeartbeats: 0,
      failedParses: 0,
      startedAt: Date.now()
    };
  }

  
  async start() {
    if (this.socket) {
      throw new Error('Heartbeat listener already started');
    }

    return new Promise((resolve, reject) => {
      this.socket = createSocket('udp4');

      this.socket.on('message', (msg, rinfo) => {
        this._handleHeartbeat(msg, rinfo);
      });

      this.socket.on('error', (err) => {
        
        if (err.code === 'EADDRINUSE') {
          this.logger.info('Heartbeat port already in use by another controller, delegating to peer', {
            host: this.host,
            port: this.port,
            error: err.message
          });
          this.socket = null; 
          resolve();
          return;
        }

        this.logger.error('Heartbeat socket error', err);
        if (!this.closed) {
          reject(err);
        }
      });

      this.socket.bind(this.port, this.host, () => {
        this.logger.info('Heartbeat listener started', {
          host: this.host,
          port: this.port
        });
        resolve();
      });
    });
  }

  
  _handleHeartbeat(buffer, rinfo) {
    if (this.closed) {
      return;
    }

    this.stats.totalHeartbeats += 1;

    try {
      const data = JSON.parse(buffer.toString('utf8'));
      
      if (!data.nodeId || !data.nodeType) {
        this.logger.warn('Invalid heartbeat message (missing required fields)', { from: rinfo.address });
        this.stats.failedParses += 1;
        return;
      }

      const { nodeId, nodeType, metadata, timestamp } = data;

      this.logger.trace('Heartbeat received', {
        nodeId,
        nodeType,
        from: `${rinfo.address}:${rinfo.port}`,
        latency: Date.now() - timestamp
      });

      
      const enrichedMetadata = {
        ...metadata,
        lastSeenHost: rinfo.address,
        lastSeenPort: rinfo.port,
        nodeType
      };

      
      this.nodeRegistry.registerHeartbeat(nodeId, enrichedMetadata);

      
      if (metadata && metadata.topics) {
        this._processTopicMetadata(nodeId, metadata.topics);
      }

    } catch (err) {
      this.logger.error('Failed to parse heartbeat message', {
        error: err.message,
        from: `${rinfo.address}:${rinfo.port}`
      });
      this.stats.failedParses += 1;
    }
  }

  
  _processTopicMetadata(nodeId, topics) {
    if (!Array.isArray(topics)) {
      return;
    }

    for (const topicData of topics) {
      const { name, role, offset, lag } = topicData;
      
      if (!name || !role) {
        continue;
      }

      
      if (role === 'leader') {
        const existingTopic = this.topicManager.getTopic(name);
        if (!existingTopic) {
          try {
            this.topicManager.registerTopic(name, nodeId);
          } catch (err) {
            this.logger.warn('Failed to register topic from heartbeat', {
              topicName: name,
              nodeId,
              error: err.message
            });
          }
        }

        
        if (typeof offset === 'number') {
          this.topicManager.updateStats(name, { latestOffset: offset });
        }
      }

      
      if (role === 'follower' && typeof lag === 'number' && typeof offset === 'number') {
        this.topicManager.updateReplicationLag(name, nodeId, lag, offset);
      }
    }
  }

  
  getStats() {
    return {
      ...this.stats,
      uptime: Date.now() - this.stats.startedAt
    };
  }

  
  close() {
    if (this.closed) {
      return;
    }

    this.logger.info('Closing heartbeat listener');
    this.closed = true;

    if (this.socket) {
      this.socket.close();
      this.socket = null;
    }

    this.logger.info('Heartbeat listener closed');
  }
}

export {
  UDPHeartbeatListener
};