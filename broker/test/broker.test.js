'use strict';

import { describe, it, beforeEach, afterEach } from 'mocha';
import assert from 'assert';
import { mkdir, rm } from 'fs/promises';
import { join } from 'path';
import { Broker } from '../src/index.js';
import { CommitLog } from '../src/log/commitLog.js';
import { BrokerState } from '../src/state/brokerState.js';
import { SegmentManager, ENTRY_HEADER_SIZE } from '../src/log/segmentManager.js';
import { TcpServer } from '../src/server/tcpServer.js';
import { Logger } from '../../shared/utils/logger.js';
import { NODE_STATES } from '../../shared/protocol/clusterTypes.js';

describe('Broker', () => {
  let testDataDir;
  let broker;

  beforeEach(async () => {
    testDataDir = join(process.cwd(), 'test-data', `broker-${Date.now()}`);
    await mkdir(testDataDir, { recursive: true });
  });

  afterEach(async () => {
    if (broker) {
      await broker.stop();
      broker = null;
    }
    try {
      await rm(testDataDir, { recursive: true, force: true });
    } catch (err) {
      // Ignore cleanup errors
    }
  });

  describe('Broker Construction', () => {
    it('should create broker with default config', () => {
      broker = new Broker({
        brokerId: 'test-broker',
        config: {
          broker: { host: '0.0.0.0', port: 0, dataDir: testDataDir },
          log: { maxSegmentSize: 1024 * 1024 }
        }
      });
      assert.strictEqual(broker.brokerId, 'test-broker');
      assert.strictEqual(broker.nodeState, NODE_STATES.FOLLOWER);
    });

    it('should create broker with custom port', () => {
      broker = new Broker({
        brokerId: 'test-broker',
        port: 9095,
        config: {
          broker: { host: '0.0.0.0', port: 9095, dataDir: testDataDir },
          log: {}
        }
      });
      assert.strictEqual(broker.config.broker.port, 9095);
    });
  });

  describe('Broker State Management', () => {
    it('should promote broker to leader', async () => {
      broker = new Broker({
        brokerId: 'test-broker',
        config: {
          broker: { host: '0.0.0.0', port: 0, dataDir: testDataDir },
          log: {}
        }
      });

      await broker.promoteToLeader();
      assert.strictEqual(broker.nodeState, NODE_STATES.LEADER);
    });

    it('should demote broker to follower', async () => {
      broker = new Broker({
        brokerId: 'test-broker',
        config: {
          broker: { host: '0.0.0.0', port: 0, dataDir: testDataDir },
          log: {}
        }
      });

      await broker.promoteToLeader();
      assert.strictEqual(broker.nodeState, NODE_STATES.LEADER);

      await broker.demoteToFollower();
      assert.strictEqual(broker.nodeState, NODE_STATES.FOLLOWER);
    });

    it('should add and remove followers', async () => {
      broker = new Broker({
        brokerId: 'test-broker',
        config: {
          broker: { host: '0.0.0.0', port: 0, dataDir: testDataDir },
          log: {}
        }
      });

      await broker.promoteToLeader();
      broker.addFollower('follower-1', 'localhost', 9093);
      broker.removeFollower('follower-1');

      const stats = broker.getStats();
      assert.ok(stats.replication);
    });
  });

  describe('Broker Stats', () => {
    it('should return broker statistics', () => {
      broker = new Broker({
        brokerId: 'test-broker',
        config: {
          broker: { host: '0.0.0.0', port: 0, dataDir: testDataDir },
          log: {}
        }
      });

      const stats = broker.getStats();
      assert.strictEqual(stats.brokerId, 'test-broker');
      assert.strictEqual(stats.running, false);
      assert.strictEqual(stats.nodeState, NODE_STATES.FOLLOWER);
      assert.ok(typeof stats.connections === 'number');
    });
  });
});

describe('BrokerState', () => {
  let testDataDir;
  let brokerState;

  beforeEach(async () => {
    testDataDir = join(process.cwd(), 'test-data', `broker-state-${Date.now()}`);
    await mkdir(testDataDir, { recursive: true });
  });

  afterEach(async () => {
    if (brokerState) {
      brokerState.close();
    }
    try {
      await rm(testDataDir, { recursive: true, force: true });
    } catch (err) {
      // Ignore cleanup errors
    }
  });

  it('should create broker state', () => {
    brokerState = new BrokerState({ dataDir: testDataDir });
    assert.ok(brokerState);
    assert.strictEqual(brokerState.topics.size, 0);
    assert.strictEqual(brokerState.subscribers.size, 0);
  });

  it('should create or get topic', async () => {
    brokerState = new BrokerState({ dataDir: testDataDir });
    const topic1 = await brokerState.getOrCreateTopic('test-topic');
    const topic2 = await brokerState.getOrCreateTopic('test-topic');

    assert.strictEqual(topic1, topic2);
    assert.strictEqual(brokerState.topics.size, 1);
  });

  it('should register and unregister subscribers', () => {
    brokerState = new BrokerState({ dataDir: testDataDir });
    const mockSocket = { write: () => {}, end: () => {} };

    const subId1 = brokerState.registerSubscriber(mockSocket, 'topic1', 0);
    const subId2 = brokerState.registerSubscriber(mockSocket, 'topic2', 10);

    assert.strictEqual(subId1, 1);
    assert.strictEqual(subId2, 2);
    assert.strictEqual(brokerState.subscribers.size, 2);

    brokerState.unregisterSubscriber(subId1);
    assert.strictEqual(brokerState.subscribers.size, 1);
  });

  it('should get subscribers for topic', () => {
    brokerState = new BrokerState({ dataDir: testDataDir });
    const mockSocket = { write: () => {}, end: () => {} };

    brokerState.registerSubscriber(mockSocket, 'topic1', 0);
    brokerState.registerSubscriber(mockSocket, 'topic1', 10);
    brokerState.registerSubscriber(mockSocket, 'topic2', 0);

    const subs = brokerState.getSubscribersForTopic('topic1');
    assert.strictEqual(subs.length, 2);
  });

  it('should update subscriber offset', () => {
    brokerState = new BrokerState({ dataDir: testDataDir });
    const mockSocket = { write: () => {}, end: () => {} };

    const subId = brokerState.registerSubscriber(mockSocket, 'topic1', 0);
    brokerState.updateSubscriberOffset(subId, 100);

    const sub = brokerState.getSubscriber(subId);
    assert.strictEqual(sub.offset, 100);
  });

  it('should get stats', async () => {
    brokerState = new BrokerState({ dataDir: testDataDir });
    await brokerState.getOrCreateTopic('test-topic');
    const mockSocket = { write: () => {}, end: () => {} };
    brokerState.registerSubscriber(mockSocket, 'test-topic', 0);

    const stats = brokerState.getStats();
    assert.strictEqual(stats.topicCount, 1);
    assert.strictEqual(stats.subscriberCount, 1);
    assert.strictEqual(stats.topics.length, 1);
  });
});

describe('CommitLog', () => {
  let testDataDir;
  let commitLog;

  beforeEach(async () => {
    testDataDir = join(process.cwd(), 'test-data', `commit-log-${Date.now()}`);
    await mkdir(join(testDataDir, 'topics', 'test-topic'), { recursive: true });
  });

  afterEach(async () => {
    if (commitLog) {
      commitLog.close();
    }
    try {
      await rm(testDataDir, { recursive: true, force: true });
    } catch (err) {
      // Ignore cleanup errors
    }
  });

  it('should create commit log', async () => {
    commitLog = new CommitLog({
      topic: 'test-topic',
      dataDir: testDataDir
    });
    await commitLog.initialize();
    assert.strictEqual(commitLog.topic, 'test-topic');
  });

  it('should append and read messages', async () => {
    commitLog = new CommitLog({
      topic: 'test-topic',
      dataDir: testDataDir
    });
    await commitLog.initialize();

    const message = Buffer.from('test message');
    const result = commitLog.append(message);

    assert.ok(result.offset >= 0);
    assert.strictEqual(result.topic, 'test-topic');
    assert.ok(result.timestamp > 0);

    const read = commitLog.read(result.offset);
    assert.strictEqual(read.topic, 'test-topic');
    assert.deepStrictEqual(read.message, message);
  });

  it('should read range of messages', async () => {
    commitLog = new CommitLog({
      topic: 'test-topic',
      dataDir: testDataDir
    });
    await commitLog.initialize();

    const messages = [];
    for (let i = 0; i < 5; i++) {
      const msg = Buffer.from(`message-${i}`);
      const result = commitLog.append(msg);
      messages.push({ offset: result.offset, message: msg });
    }

    const read = commitLog.readRange(messages[0].offset, 10);
    assert.ok(read.length >= 1);
  });

  it('should verify checksums', async () => {
    commitLog = new CommitLog({
      topic: 'test-topic',
      dataDir: testDataDir
    });
    await commitLog.initialize();

    const message = Buffer.from('test message');
    const result = commitLog.append(message);

    const read = commitLog.read(result.offset);
    assert.deepStrictEqual(read.message, message);
  });

  it('should get latest offset', async () => {
    commitLog = new CommitLog({
      topic: 'test-topic',
      dataDir: testDataDir
    });
    await commitLog.initialize();

    const initialOffset = commitLog.getLatestOffset();
    const message = Buffer.from('test');
    commitLog.append(message);
    const newOffset = commitLog.getLatestOffset();

    assert.ok(newOffset > initialOffset);
  });

  it('should throw on append after close', async () => {
    commitLog = new CommitLog({
      topic: 'test-topic',
      dataDir: testDataDir
    });
    await commitLog.initialize();
    commitLog.close();

    assert.throws(() => {
      commitLog.append(Buffer.from('test'));
    }, /closed/);
  });
});

describe('SegmentManager', () => {
  let testDataDir;
  let segmentManager;

  beforeEach(async () => {
    testDataDir = join(process.cwd(), 'test-data', `segment-manager-${Date.now()}`);
    await mkdir(testDataDir, { recursive: true });
  });

  afterEach(async () => {
    if (segmentManager) {
      segmentManager.close();
    }
    try {
      await rm(testDataDir, { recursive: true, force: true });
    } catch (err) {
      // Ignore cleanup errors
    }
  });

  it('should initialize segment manager', async () => {
    segmentManager = new SegmentManager({
      topicPath: testDataDir,
      maxSegmentSize: 1024 * 1024
    });
    await segmentManager.initialize();
    assert.ok(segmentManager);
  });

  it('should append and read entries', async () => {
    segmentManager = new SegmentManager({
      topicPath: testDataDir,
      maxSegmentSize: 1024 * 1024
    });
    await segmentManager.initialize();

    const message = Buffer.from('test message');
    const offset = segmentManager.append(message, Date.now(), 12345);

    const entry = segmentManager.readEntry(offset);
    assert.ok(entry);
    assert.deepStrictEqual(entry.message, message);
    assert.strictEqual(entry.crc, 12345);
  });

  it('should read range of entries', async () => {
    segmentManager = new SegmentManager({
      topicPath: testDataDir,
      maxSegmentSize: 1024 * 1024
    });
    await segmentManager.initialize();

    const offsets = [];
    for (let i = 0; i < 5; i++) {
      const msg = Buffer.from(`message-${i}`);
      const offset = segmentManager.append(msg, Date.now(), 0);
      offsets.push(offset);
    }

    const entries = segmentManager.readRange(offsets[0], offsets[4] + 100);
    assert.ok(entries.length >= 1);
  });

  it('should get latest offset', async () => {
    segmentManager = new SegmentManager({
      topicPath: testDataDir,
      maxSegmentSize: 1024 * 1024
    });
    await segmentManager.initialize();

    const initialOffset = segmentManager.getLatestOffset();
    segmentManager.append(Buffer.from('test'), Date.now(), 0);
    const newOffset = segmentManager.getLatestOffset();

    assert.ok(newOffset > initialOffset);
  });
});

describe('TcpServer', () => {
  let tcpServer;
  let mockBrokerState;

  beforeEach(() => {
    mockBrokerState = {
      getOrCreateTopic: async () => ({ append: () => ({ offset: 0, timestamp: Date.now() }) }),
      getTopic: () => null,
      registerSubscriber: () => 1,
      unregisterSubscriber: () => {},
      getSubscribersForTopic: () => []
    };
  });

  afterEach(async () => {
    if (tcpServer && tcpServer.listening) {
      await tcpServer.stop();
    }
  });

  it('should create TCP server', () => {
    tcpServer = new TcpServer({
      host: '0.0.0.0',
      port: 0,
      brokerState: mockBrokerState,
      followerSync: {},
      onPublish: () => {},
      logger: new Logger({ name: 'test' })
    });
    assert.ok(tcpServer);
  });

  it('should start and stop TCP server', async () => {
    tcpServer = new TcpServer({
      host: '0.0.0.0',
      port: 0,
      brokerState: mockBrokerState,
      followerSync: {},
      onPublish: () => {},
      logger: new Logger({ name: 'test' })
    });

    await tcpServer.start();
    assert.strictEqual(tcpServer.listening, true);

    await tcpServer.stop();
    assert.strictEqual(tcpServer.listening, false);
  });

  it('should track connection count', async () => {
    tcpServer = new TcpServer({
      host: '0.0.0.0',
      port: 0,
      brokerState: mockBrokerState,
      followerSync: {},
      onPublish: () => {},
      logger: new Logger({ name: 'test' })
    });

    const count = tcpServer.getConnectionCount();
    assert.strictEqual(typeof count, 'number');
    assert.ok(count >= 0);
  });
});
