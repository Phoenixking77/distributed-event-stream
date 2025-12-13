'use strict';

import { createSocket } from 'dgram';
import { Logger } from '../../../shared/utils/logger';
import { createHeartbeatMessage } from '../../../shared/protocol/clusterTypes';

/**
 * UDPHeartbeatListener receives heartbeats from broker nodes.
 * Parses heartbeat messages and updates the node registry.
 */
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

  /**
   * Start the UDP heartbeat listener
   */
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

  /**
   * Handle incoming heartbeat message
   */
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

      // Enrich metadata with connection info
      const enrichedMetadata = {
        ...metadata,
        lastSeenHost: rinfo.address,
        lastSeenPort: rinfo.port,
        nodeType
      };

      // Register/update node in registry
      this.nodeRegistry.registerHeartbeat(nodeId, enrichedMetadata);

      // Process topic metadata if included
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

  /**
   * Process topic metadata from heartbeat
   */
  _processTopicMetadata(nodeId, topics) {
    if (!Array.isArray(topics)) {
      return;
    }

    for (const topicData of topics) {
      const { name, role, offset, lag } = topicData;
      
      if (!name || !role) {
        continue;
      }

      // Register topic if it doesn't exist
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

        // Update topic stats
        if (typeof offset === 'number') {
          this.topicManager.updateStats(name, { latestOffset: offset });
        }
      }

      // Update replication lag for followers
      if (role === 'follower' && typeof lag === 'number' && typeof offset === 'number') {
        this.topicManager.updateReplicationLag(name, nodeId, lag, offset);
      }
    }
  }

  /**
   * Get listener statistics
   */
  getStats() {
    return {
      ...this.stats,
      uptime: Date.now() - this.stats.startedAt
    };
  }

  /**
   * Stop the heartbeat listener
   */
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

export default {
  UDPHeartbeatListener
};