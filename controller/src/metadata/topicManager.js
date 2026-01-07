'use strict';

import { Logger } from '../../../shared/utils/logger.js';
import { ISR_CONFIG } from '../../../shared/protocol/clusterTypes.js';

/**
 * TopicManager handles topic metadata, partition assignments,
 * and In-Sync Replica (ISR) tracking.
 */
class TopicManager {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'topicManager' });
    this.nodeRegistry = opts.nodeRegistry;
    
    if (!this.nodeRegistry) {
      throw new Error('nodeRegistry required');
    }

    this.topics = new Map();
    this.minIsr = opts.minIsr || ISR_CONFIG.MIN_ISR;
    this.maxLagMs = opts.maxLagMs || ISR_CONFIG.MAX_LAG_MS;
  }

  /**
   * Register or update topic metadata
   */
  registerTopic(topicName, leaderNodeId) {
    if (!leaderNodeId) {
      throw new Error('leaderNodeId required');
    }

    const leader = this.nodeRegistry.getNode(leaderNodeId);
    if (!leader) {
      throw new Error(`Leader node ${leaderNodeId} not found`);
    }

    const existing = this.topics.get(topicName);
    
    if (existing) {
      this.logger.debug('Topic already exists', { topicName, currentLeader: existing.leader });
      return existing;
    }

    const topic = {
      topicName,
      leader: leaderNodeId,
      followers: [],
      isr: [leaderNodeId],
      createdAt: Date.now(),
      updatedAt: Date.now(),
      stats: {
        totalMessages: 0,
        latestOffset: 0,
        replicationLag: {}
      }
    };

    this.topics.set(topicName, topic);
    this.logger.info('Topic registered', { topicName, leader: leaderNodeId });

    return topic;
  }

  /**
   * Add follower to topic
   */
  addFollower(topicName, followerNodeId) {
    const topic = this.topics.get(topicName);
    if (!topic) {
      throw new Error(`Topic ${topicName} not found`);
    }

    const follower = this.nodeRegistry.getNode(followerNodeId);
    if (!follower) {
      throw new Error(`Follower node ${followerNodeId} not found`);
    }

    if (topic.followers.includes(followerNodeId)) {
      this.logger.debug('Follower already registered', { topicName, followerNodeId });
      return;
    }

    topic.followers.push(followerNodeId);
    topic.updatedAt = Date.now();

    this.logger.info('Follower added to topic', {
      topicName,
      followerNodeId,
      totalFollowers: topic.followers.length
    });
  }

  /**
   * Remove follower from topic
   */
  removeFollower(topicName, followerNodeId) {
    const topic = this.topics.get(topicName);
    if (!topic) {
      return;
    }

    const index = topic.followers.indexOf(followerNodeId);
    if (index > -1) {
      topic.followers.splice(index, 1);
      topic.updatedAt = Date.now();

      this.logger.info('Follower removed from topic', {
        topicName,
        followerNodeId,
        remainingFollowers: topic.followers.length
      });
    }

    this._updateISR(topicName, followerNodeId, false);
  }

  /**
   * Update topic leader
   */
  updateLeader(topicName, newLeaderId) {
    const topic = this.topics.get(topicName);
    if (!topic) {
      throw new Error(`Topic ${topicName} not found`);
    }

    const newLeader = this.nodeRegistry.getNode(newLeaderId);
    if (!newLeader) {
      throw new Error(`New leader node ${newLeaderId} not found`);
    }

    const oldLeader = topic.leader;
    if (oldLeader === newLeaderId) {
      return;
    }

    this.logger.info('Topic leader changed', {
      topicName,
      oldLeader,
      newLeader: newLeaderId
    });

    // Move old leader to followers if still alive
    if (oldLeader && !topic.followers.includes(oldLeader)) {
      const oldLeaderNode = this.nodeRegistry.getNode(oldLeader);
      if (oldLeaderNode && oldLeaderNode.state !== 'dead') {
        topic.followers.push(oldLeader);
      }
    }

    // Remove new leader from followers
    const followerIndex = topic.followers.indexOf(newLeaderId);
    if (followerIndex > -1) {
      topic.followers.splice(followerIndex, 1);
    }

    topic.leader = newLeaderId;
    topic.isr = [newLeaderId];
    topic.updatedAt = Date.now();
  }

  /**
   * Update replication lag for a follower
   */
  updateReplicationLag(topicName, followerNodeId, lagMs, offset) {
    const topic = this.topics.get(topicName);
    if (!topic) {
      return;
    }

    if (!topic.stats.replicationLag) {
      topic.stats.replicationLag = {};
    }

    topic.stats.replicationLag[followerNodeId] = {
      lagMs,
      offset,
      updatedAt: Date.now()
    };

    const isInSync = lagMs < this.maxLagMs;
    this._updateISR(topicName, followerNodeId, isInSync);

    this.logger.trace('Replication lag updated', {
      topicName,
      followerNodeId,
      lagMs,
      offset,
      isInSync
    });
  }

  /**
   * Update In-Sync Replica set
   */
  _updateISR(topicName, nodeId, isInSync) {
    const topic = this.topics.get(topicName);
    if (!topic) {
      return;
    }

    const isCurrentlyInISR = topic.isr.includes(nodeId);

    if (isInSync && !isCurrentlyInISR) {
      topic.isr.push(nodeId);
      this.logger.info('Node added to ISR', {
        topicName,
        nodeId,
        isrSize: topic.isr.length
      });
    } else if (!isInSync && isCurrentlyInISR) {
      const index = topic.isr.indexOf(nodeId);
      if (index > -1) {
        topic.isr.splice(index, 1);
        this.logger.warn('Node removed from ISR', {
          topicName,
          nodeId,
          isrSize: topic.isr.length
        });
      }
    }
  }

  /**
   * Update topic statistics
   */
  updateStats(topicName, stats) {
    const topic = this.topics.get(topicName);
    if (!topic) {
      return;
    }

    if (typeof stats.totalMessages === 'number') {
      topic.stats.totalMessages = stats.totalMessages;
    }
    if (typeof stats.latestOffset === 'number') {
      topic.stats.latestOffset = stats.latestOffset;
    }

    topic.updatedAt = Date.now();
  }

  /**
   * Get topic metadata
   */
  getTopic(topicName) {
    return this.topics.get(topicName) || null;
  }

  /**
   * Get all topics
   */
  getAllTopics() {
    return Array.from(this.topics.values());
  }

  /**
   * Get topics by leader
   */
  getTopicsByLeader(leaderNodeId) {
    const topics = [];
    for (const topic of this.topics.values()) {
      if (topic.leader === leaderNodeId) {
        topics.push(topic);
      }
    }
    return topics;
  }

  /**
   * Get topics where node is a follower
   */
  getTopicsByFollower(followerNodeId) {
    const topics = [];
    for (const topic of this.topics.values()) {
      if (topic.followers.includes(followerNodeId)) {
        topics.push(topic);
      }
    }
    return topics;
  }

  /**
   * Check if topic has sufficient ISR
   */
  hasSufficientISR(topicName) {
    const topic = this.topics.get(topicName);
    if (!topic) {
      return false;
    }

    return topic.isr.length >= this.minIsr;
  }

  /**
   * Get cluster-wide topic statistics
   */
  getClusterStats() {
    const stats = {
      totalTopics: this.topics.size,
      totalPartitions: this.topics.size,
      underReplicatedTopics: 0,
      topics: []
    };

    for (const topic of this.topics.values()) {
      const hasSufficientISR = topic.isr.length >= this.minIsr;
      if (!hasSufficientISR) {
        stats.underReplicatedTopics += 1;
      }

      stats.topics.push({
        name: topic.topicName,
        leader: topic.leader,
        followers: topic.followers.length,
        isr: topic.isr.length,
        messages: topic.stats.totalMessages,
        latestOffset: topic.stats.latestOffset,
        underReplicated: !hasSufficientISR
      });
    }

    return stats;
  }

  /**
   * Reassign topic leadership (called during failover)
   */
  reassignLeader(topicName) {
    const topic = this.topics.get(topicName);
    if (!topic) {
      throw new Error(`Topic ${topicName} not found`);
    }

    // Find new leader from ISR first
    for (const nodeId of topic.isr) {
      const node = this.nodeRegistry.getNode(nodeId);
      if (node && node.state !== 'dead') {
        this.updateLeader(topicName, nodeId);
        return nodeId;
      }
    }

    // If no ISR members available, try any follower
    for (const nodeId of topic.followers) {
      const node = this.nodeRegistry.getNode(nodeId);
      if (node && node.state !== 'dead') {
        this.updateLeader(topicName, nodeId);
        this.logger.warn('Leader reassigned to follower outside ISR', {
          topicName,
          newLeader: nodeId
        });
        return nodeId;
      }
    }

    this.logger.error('No viable leader candidate found for topic', { topicName });
    return null;
  }

  /**
   * Handle node failure - reassign topics if needed
   */
  handleNodeFailure(nodeId) {
    this.logger.info('Handling node failure for topics', { nodeId });

    // Find all topics where this node was leader
    const leaderTopics = this.getTopicsByLeader(nodeId);
    for (const topic of leaderTopics) {
      this.logger.warn('Reassigning leader due to node failure', {
        topicName: topic.topicName,
        failedLeader: nodeId
      });
      this.reassignLeader(topic.topicName);
    }

    for (const topic of this.topics.values()) {
      this.removeFollower(topic.topicName, nodeId);
    }
  }
}

export {
  TopicManager
};