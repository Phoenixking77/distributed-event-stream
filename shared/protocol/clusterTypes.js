'use strict';

// Node states for cluster membership
const NODE_STATES = Object.freeze({
  FOLLOWER: 'follower',
  CANDIDATE: 'candidate',
  LEADER: 'leader',
  DEAD: 'dead'
});

// Replication message types
const REPLICATION_TYPES = Object.freeze({
  LOG_ENTRY: 'log_entry',
  HEARTBEAT: 'heartbeat',
  APPEND_ENTRIES: 'append_entries',
  APPEND_ACK: 'append_ack'
});

// Election message types
const ELECTION_TYPES = Object.freeze({
  VOTE_REQUEST: 'vote_request',
  VOTE_RESPONSE: 'vote_response',
  LEADER_ANNOUNCEMENT: 'leader_announcement',
  LEADER_HEARTBEAT: 'leader_heartbeat'  // New: periodic heartbeat from leader to followers
});

// Heartbeat intervals (ms)
// Election timeouts are randomized to avoid split-brain:
//   timeout = ELECTION_TIMEOUT_BASE + rand(0..ELECTION_TIMEOUT_RANGE)
const HEARTBEAT_INTERVALS = Object.freeze({
  BROKER_TO_CONTROLLER: 5000,     // 5s
  LEADER_TO_FOLLOWERS: 1000,      // 1s
  ELECTION_TIMEOUT_BASE: 1200,    // 1.2s base
  ELECTION_TIMEOUT_RANGE: 800,    // +0..0.8s jitter (net 1.2s-2.0s)
  DEAD_NODE_TIMEOUT: 15000        // 15s
});

// ISR (In-Sync Replica) settings
const ISR_CONFIG = Object.freeze({
  MIN_ISR: 2,                    // Minimum replicas for writes
  MAX_LAG_MS: 5000,             // 5s max replication lag
  REPLICATION_TIMEOUT: 3000      // 3s timeout for replication
});

function createReplicationMessage(type, data) {
  return {
    type,
    timestamp: Date.now(),
    data: data || {}
  };
}

function createElectionMessage(type, term, candidateId, data = {}) {
  return {
    type,
    term,
    candidateId,
    timestamp: Date.now(),
    ...data
  };
}

function createHeartbeatMessage(nodeId, nodeType, metadata = {}) {
  return {
    nodeId,
    nodeType,
    timestamp: Date.now(),
    metadata
  };
}

export {
  NODE_STATES,
  REPLICATION_TYPES,
  ELECTION_TYPES,
  HEARTBEAT_INTERVALS,
  ISR_CONFIG,
  createReplicationMessage,
  createElectionMessage,
  createHeartbeatMessage
};