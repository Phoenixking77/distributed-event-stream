'use strict';

import { describe, it, beforeEach, afterEach } from 'mocha';
import assert from 'assert';
import { Controller } from '../src/index.js';
import { NodeRegistry } from '../src/metadata/nodeRegistry.js';
import { TopicManager } from '../src/metadata/topicManager.js';
import { LeaderElection } from '../src/election/leaderElection.js';
import { Logger } from '../../shared/utils/logger.js';
import { NODE_STATES, ELECTION_TYPES } from '../../shared/protocol/clusterTypes.js';

describe('Controller', () => {
  let controller;

  afterEach(async () => {
    if (controller && !controller.closed) {
      await controller.close();
    }
  });

  it('should create controller', () => {
    controller = new Controller({
      heartbeatPort: 9091,
      electionPort: 9093,
      httpPort: 8080,
      host: '0.0.0.0'
    });
    assert.ok(controller);
    assert.strictEqual(controller.started, false);
    assert.strictEqual(controller.closed, false);
  });

  it('should initialize controller components', () => {
    controller = new Controller({
      heartbeatPort: 9091,
      electionPort: 9093,
      httpPort: 8080
    });
    assert.ok(controller.nodeRegistry);
    assert.ok(controller.topicManager);
    assert.ok(controller.leaderElection);
    assert.ok(controller.heartbeatListener);
    assert.ok(controller.httpApi);
  });

  it('should get status', () => {
    controller = new Controller({
      heartbeatPort: 9091,
      electionPort: 9093,
      httpPort: 8080
    });
    const status = controller.getStatus();
    assert.strictEqual(status.started, false);
    assert.strictEqual(status.closed, false);
    assert.ok(typeof status.hasQuorum === 'boolean');
  });
});

describe('NodeRegistry', () => {
  let nodeRegistry;

  beforeEach(() => {
    nodeRegistry = new NodeRegistry({
      logger: new Logger({ name: 'test' }),
      deadNodeTimeout: 15000,
      heartbeatCheckInterval: 2000
    });
  });

  afterEach(() => {
    if (nodeRegistry) {
      nodeRegistry.close();
    }
  });

  it('should create node registry', () => {
    assert.ok(nodeRegistry);
    assert.strictEqual(nodeRegistry.nodes.size, 0);
  });

  it('should register node heartbeat', () => {
    nodeRegistry.initialize();
    const node = nodeRegistry.registerHeartbeat('node-1', { host: 'localhost', port: 9092 });

    assert.ok(node);
    assert.strictEqual(node.nodeId, 'node-1');
    assert.strictEqual(node.state, NODE_STATES.FOLLOWER);
    assert.strictEqual(nodeRegistry.nodes.size, 1);
  });

  it('should update node on heartbeat', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});
    const node1 = nodeRegistry.getNode('node-1');

    nodeRegistry.registerHeartbeat('node-1', { updated: true });
    const node2 = nodeRegistry.getNode('node-1');

    assert.strictEqual(node2.heartbeatCount, 2);
    assert.strictEqual(node2.metadata.updated, true);
  });

  it('should update node state', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});
    const updated = nodeRegistry.updateNodeState('node-1', NODE_STATES.LEADER);

    assert.strictEqual(updated, true);
    const node = nodeRegistry.getNode('node-1');
    assert.strictEqual(node.state, NODE_STATES.LEADER);
    assert.strictEqual(node.isLeader, true);
  });

  it('should get node by ID', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});

    const node = nodeRegistry.getNode('node-1');
    assert.ok(node);
    assert.strictEqual(node.nodeId, 'node-1');

    const missing = nodeRegistry.getNode('missing');
    assert.strictEqual(missing, null);
  });

  it('should get all nodes', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});
    nodeRegistry.registerHeartbeat('node-2', {});

    const nodes = nodeRegistry.getAllNodes();
    assert.strictEqual(nodes.length, 2);
  });

  it('should get alive nodes', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});
    nodeRegistry.registerHeartbeat('node-2', {});

    const alive = nodeRegistry.getAliveNodes();
    assert.strictEqual(alive.length, 2);
  });

  it('should get nodes by state', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});
    nodeRegistry.registerHeartbeat('node-2', {});

    nodeRegistry.updateNodeState('node-1', NODE_STATES.LEADER);

    const leaders = nodeRegistry.getNodesByState(NODE_STATES.LEADER);
    assert.strictEqual(leaders.length, 1);
    assert.strictEqual(leaders[0].nodeId, 'node-1');
  });

  it('should get leader', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});
    nodeRegistry.registerHeartbeat('node-2', {});

    nodeRegistry.updateNodeState('node-1', NODE_STATES.LEADER);

    const leader = nodeRegistry.getLeader();
    assert.ok(leader);
    assert.strictEqual(leader.nodeId, 'node-1');
  });

  it('should get stats', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});
    nodeRegistry.registerHeartbeat('node-2', {});

    nodeRegistry.updateNodeState('node-1', NODE_STATES.LEADER);

    const stats = nodeRegistry.getStats();
    assert.strictEqual(stats.totalNodes, 2);
    assert.strictEqual(stats.aliveNodes, 2);
    assert.strictEqual(stats.leaders, 1);
    assert.strictEqual(stats.followers, 1);
  });

  it('should check quorum', () => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});
    nodeRegistry.registerHeartbeat('node-2', {});
    nodeRegistry.registerHeartbeat('node-3', {});

    const hasQuorum = nodeRegistry.hasQuorum();
    assert.strictEqual(hasQuorum, true);
  });

  it('should emit node registered event', (done) => {
    nodeRegistry.initialize();
    nodeRegistry.on('nodeRegistered', ({ nodeId }) => {
      assert.strictEqual(nodeId, 'node-1');
      done();
    });

    nodeRegistry.registerHeartbeat('node-1', {});
  });

  it('should emit node state changed event', (done) => {
    nodeRegistry.initialize();
    nodeRegistry.registerHeartbeat('node-1', {});

    nodeRegistry.on('nodeStateChanged', ({ nodeId, oldState, newState }) => {
      assert.strictEqual(nodeId, 'node-1');
      assert.strictEqual(oldState, NODE_STATES.FOLLOWER);
      assert.strictEqual(newState, NODE_STATES.LEADER);
      done();
    });

    nodeRegistry.updateNodeState('node-1', NODE_STATES.LEADER);
  });
});

describe('TopicManager', () => {
  let nodeRegistry;
  let topicManager;

  beforeEach(() => {
    nodeRegistry = new NodeRegistry({
      logger: new Logger({ name: 'test' })
    });
    nodeRegistry.initialize();

    topicManager = new TopicManager({
      logger: new Logger({ name: 'test' }),
      nodeRegistry
    });
  });

  afterEach(() => {
    if (nodeRegistry) {
      nodeRegistry.close();
    }
  });

  it('should create topic manager', () => {
    assert.ok(topicManager);
    assert.strictEqual(topicManager.topics.size, 0);
  });

  it('should register topic', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    const topic = topicManager.registerTopic('test-topic', 'leader-1');

    assert.ok(topic);
    assert.strictEqual(topic.topicName, 'test-topic');
    assert.strictEqual(topic.leader, 'leader-1');
    assert.strictEqual(topic.isr.length, 1);
    assert.ok(topic.isr.includes('leader-1'));
  });

  it('should add follower to topic', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    nodeRegistry.registerHeartbeat('follower-1', {});
    topicManager.registerTopic('test-topic', 'leader-1');
    topicManager.addFollower('test-topic', 'follower-1');

    const topic = topicManager.getTopic('test-topic');
    assert.ok(topic.followers.includes('follower-1'));
  });

  it('should remove follower from topic', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    nodeRegistry.registerHeartbeat('follower-1', {});
    topicManager.registerTopic('test-topic', 'leader-1');
    topicManager.addFollower('test-topic', 'follower-1');
    topicManager.removeFollower('test-topic', 'follower-1');

    const topic = topicManager.getTopic('test-topic');
    assert.ok(!topic.followers.includes('follower-1'));
  });

  it('should update topic leader', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    nodeRegistry.registerHeartbeat('leader-2', {});
    topicManager.registerTopic('test-topic', 'leader-1');
    topicManager.updateLeader('test-topic', 'leader-2');

    const topic = topicManager.getTopic('test-topic');
    assert.strictEqual(topic.leader, 'leader-2');
  });

  it('should update replication lag', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    nodeRegistry.registerHeartbeat('follower-1', {});
    topicManager.registerTopic('test-topic', 'leader-1');
    topicManager.addFollower('test-topic', 'follower-1');
    topicManager.updateReplicationLag('test-topic', 'follower-1', 100, 1000);

    const topic = topicManager.getTopic('test-topic');
    assert.ok(topic.stats.replicationLag['follower-1']);
    assert.strictEqual(topic.stats.replicationLag['follower-1'].lagMs, 100);
  });

  it('should update topic stats', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    topicManager.registerTopic('test-topic', 'leader-1');
    topicManager.updateStats('test-topic', {
      totalMessages: 100,
      latestOffset: 1000
    });

    const topic = topicManager.getTopic('test-topic');
    assert.strictEqual(topic.stats.totalMessages, 100);
    assert.strictEqual(topic.stats.latestOffset, 1000);
  });

  it('should get topic', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    topicManager.registerTopic('test-topic', 'leader-1');

    const topic = topicManager.getTopic('test-topic');
    assert.ok(topic);
    assert.strictEqual(topic.topicName, 'test-topic');

    const missing = topicManager.getTopic('missing');
    assert.strictEqual(missing, null);
  });

  it('should get all topics', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    topicManager.registerTopic('topic-1', 'leader-1');
    topicManager.registerTopic('topic-2', 'leader-1');

    const topics = topicManager.getAllTopics();
    assert.strictEqual(topics.length, 2);
  });

  it('should get topics by leader', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    nodeRegistry.registerHeartbeat('leader-2', {});
    topicManager.registerTopic('topic-1', 'leader-1');
    topicManager.registerTopic('topic-2', 'leader-1');
    topicManager.registerTopic('topic-3', 'leader-2');

    const topics = topicManager.getTopicsByLeader('leader-1');
    assert.strictEqual(topics.length, 2);
  });

  it('should check sufficient ISR', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    topicManager.registerTopic('test-topic', 'leader-1');

    const hasSufficientISR = topicManager.hasSufficientISR('test-topic');
    assert.strictEqual(hasSufficientISR, true);
  });

  it('should get cluster stats', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    topicManager.registerTopic('topic-1', 'leader-1');
    topicManager.registerTopic('topic-2', 'leader-1');

    const stats = topicManager.getClusterStats();
    assert.strictEqual(stats.totalTopics, 2);
    assert.ok(stats.topics.length >= 0);
  });

  it('should reassign leader on failure', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    nodeRegistry.registerHeartbeat('follower-1', {});
    topicManager.registerTopic('test-topic', 'leader-1');
    topicManager.addFollower('test-topic', 'follower-1');
    topicManager._updateISR('test-topic', 'follower-1', true);

    nodeRegistry.updateNodeState('leader-1', NODE_STATES.DEAD);
    const newLeader = topicManager.reassignLeader('test-topic');

    assert.ok(newLeader);
    const topic = topicManager.getTopic('test-topic');
    assert.notStrictEqual(topic.leader, 'leader-1');
  });

  it('should handle node failure', () => {
    nodeRegistry.registerHeartbeat('leader-1', {});
    nodeRegistry.registerHeartbeat('follower-1', {});
    topicManager.registerTopic('test-topic', 'leader-1');
    topicManager.addFollower('test-topic', 'follower-1');

    nodeRegistry.updateNodeState('leader-1', NODE_STATES.DEAD);
    topicManager.handleNodeFailure('leader-1');

    const topic = topicManager.getTopic('test-topic');
    assert.notStrictEqual(topic.leader, 'leader-1');
  });
});

describe('LeaderElection', () => {
  let nodeRegistry;
  let leaderElection;

  beforeEach(() => {
    nodeRegistry = new NodeRegistry({
      logger: new Logger({ name: 'test' })
    });
    nodeRegistry.initialize();

    leaderElection = new LeaderElection({
      logger: new Logger({ name: 'test' }),
      nodeRegistry,
      electionPort: 0,
      host: '0.0.0.0',
      peers: [],
      selfId: 'controller-1'
    });
  });

  afterEach(() => {
    if (leaderElection) {
      leaderElection.close();
    }
    if (nodeRegistry) {
      nodeRegistry.close();
    }
  });

  it('should create leader election', () => {
    assert.ok(leaderElection);
    assert.strictEqual(leaderElection.currentTerm, 0);
    assert.strictEqual(leaderElection.state, NODE_STATES.FOLLOWER);
  });

  it('should handle vote request', () => {
    const granted = leaderElection.handleVoteRequest('candidate-1', 1);
    assert.strictEqual(granted, true);
    assert.strictEqual(leaderElection.currentTerm, 1);
    assert.strictEqual(leaderElection.votedFor, 'candidate-1');
  });

  it('should reject vote request for lower term', () => {
    leaderElection.currentTerm = 5;
    const granted = leaderElection.handleVoteRequest('candidate-1', 3);
    assert.strictEqual(granted, false);
    assert.strictEqual(leaderElection.currentTerm, 5);
  });

  it('should handle vote response', () => {
    leaderElection.currentTerm = 1;
    leaderElection.state = NODE_STATES.CANDIDATE;
    leaderElection.electionInProgress = true;
    leaderElection.votes.add('controller-1');

    leaderElection.handleVoteResponse('controller-2', 1, true);
    assert.ok(leaderElection.votes.has('controller-2'));
  });

  it('should handle leader heartbeat', () => {
    leaderElection.handleLeaderHeartbeat('leader-1', 1);
    assert.strictEqual(leaderElection.currentTerm, 1);
    assert.strictEqual(leaderElection.leaderId, 'leader-1');
    assert.strictEqual(leaderElection.state, NODE_STATES.FOLLOWER);
  });

  it('should step down from leadership', () => {
    leaderElection.currentTerm = 1;
    leaderElection.state = NODE_STATES.LEADER;
    leaderElection.leaderId = 'controller-1';

    leaderElection.stepDown();
    assert.strictEqual(leaderElection.state, NODE_STATES.FOLLOWER);
    assert.strictEqual(leaderElection.leaderId, null);
  });

  it('should get state', () => {
    const state = leaderElection.getState();
    assert.strictEqual(state.term, 0);
    assert.strictEqual(state.state, NODE_STATES.FOLLOWER);
    assert.strictEqual(state.closed, false);
  });

  it('should check if leader', () => {
    assert.strictEqual(leaderElection.isLeader(), false);
    leaderElection.state = NODE_STATES.LEADER;
    assert.strictEqual(leaderElection.isLeader(), true);
  });

  it('should calculate required votes for quorum', () => {
    leaderElection.peers = [
      { host: 'localhost', port: 9093 },
      { host: 'localhost', port: 9094 }
    ];
    const required = leaderElection._getRequiredVotes();
    assert.strictEqual(required, 2);
  });
});
