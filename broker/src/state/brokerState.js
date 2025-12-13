'use strict';

const { CommitLog } = require('../log/commitLog').default;
const { Logger } = require('../../../shared/utils/logger');

class BrokerState {
  constructor(options) {
    const opts = options || {};
    this.dataDir = opts.dataDir || './data';
    this.logger = opts.logger || new Logger({ name: 'brokerState' });
    this.logConfig = opts.logConfig || {};

    this.topics = new Map();
    this.subscribers = new Map();
    this.nextSubscriberId = 1;
  }

  async getOrCreateTopic(topicName) {
    if (this.topics.has(topicName)) {
      return this.topics.get(topicName);
    }

    this.logger.info('Creating new topic', { topic: topicName });

    const commitLog = new CommitLog({
      topic: topicName,
      dataDir: this.dataDir,
      maxSegmentSize: this.logConfig.maxSegmentSize,
      syncInterval: this.logConfig.syncInterval,
      logger: this.logger.child({ topic: topicName })
    });

    await commitLog.initialize();
    this.topics.set(topicName, commitLog);

    this.logger.info('Topic created', {
      topic: topicName,
      latestOffset: commitLog.getLatestOffset()
    });

    return commitLog;
  }

  getTopic(topicName) {
    return this.topics.get(topicName) || null;
  }

  listTopics() {
    return Array.from(this.topics.keys());
  }

  registerSubscriber(socket, topic, startOffset) {
    const subscriberId = this.nextSubscriberId;
    this.nextSubscriberId += 1;

    const subscriber = {
      id: subscriberId,
      socket,
      topic,
      offset: typeof startOffset === 'number' ? startOffset : 0,
      active: true,
      registeredAt: Date.now()
    };

    this.subscribers.set(subscriberId, subscriber);

    this.logger.debug('Subscriber registered', {
      subscriberId,
      topic,
      offset: subscriber.offset
    });

    return subscriberId;
  }

  unregisterSubscriber(subscriberId) {
    const subscriber = this.subscribers.get(subscriberId);
    if (subscriber) {
      subscriber.active = false;
      this.subscribers.delete(subscriberId);
      this.logger.debug('Subscriber unregistered', { subscriberId });
    }
  }

  getSubscriber(subscriberId) {
    return this.subscribers.get(subscriberId) || null;
  }

  getSubscribersForTopic(topicName) {
    const subscribers = [];
    for (const subscriber of this.subscribers.values()) {
      if (subscriber.topic === topicName && subscriber.active) {
        subscribers.push(subscriber);
      }
    }
    return subscribers;
  }

  updateSubscriberOffset(subscriberId, offset) {
    const subscriber = this.subscribers.get(subscriberId);
    if (subscriber) {
      subscriber.offset = offset;
    }
  }

  getStats() {
    const topicStats = [];
    for (const [topicName, commitLog] of this.topics.entries()) {
      const subscribers = this.getSubscribersForTopic(topicName);
      topicStats.push({
        topic: topicName,
        latestOffset: commitLog.getLatestOffset(),
        subscriberCount: subscribers.length
      });
    }

    return {
      topicCount: this.topics.size,
      subscriberCount: this.subscribers.size,
      topics: topicStats
    };
  }

  close() {
    this.logger.info('Closing broker state');

    for (const [topicName, commitLog] of this.topics.entries()) {
      this.logger.debug('Closing topic', { topic: topicName });
      commitLog.close();
    }

    this.topics.clear();
    this.subscribers.clear();

    this.logger.info('Broker state closed');
  }
}

module.exports = {
  BrokerState
};