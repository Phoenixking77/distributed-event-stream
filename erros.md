# Distributed Event Streaming System - Error Documentation

**Analysis Date:** January 6, 2026  
**Log Timestamp:** 2026-01-06T02:53:42 - 2026-01-06T02:54:01  
**System Version:** [Your Version]

---

## Critical Errors

### ERROR-001: Split-Brain Condition (CRITICAL)
**Severity:** CRITICAL  
**Component:** Controller Election Module  
**Status:** ACTIVE

**Description:**  
Multiple controllers simultaneously elected themselves as leaders in the same term, violating the Raft consensus protocol's safety guarantee of at most one leader per term.

**Evidence from Logs:**
```
{"ts":"2026-01-06T02:53:44.617Z","level":"info","msg":"Elected as leader","pid":24181,"logger":"controller","term":1,"votes":2,"required":2}
{"ts":"2026-01-06T02:53:44.617Z","level":"info","msg":"Elected as leader","pid":24211,"logger":"controller","term":1,"votes":2,"required":2}
```

**Affected Components:**
- Controller 0 (localhost:9010, PID 24181)
- Controller 2 (localhost:9012, PID 24211)

**Root Cause Analysis:**
1. All three controllers started elections simultaneously (within 1ms)
2. Vote splitting occurred:
   - Controller 1 (9011) voted for Controller 2 (9012)
   - Controller 0 (9010) received vote from Controller 1
   - Controller 2 (9012) received votes from Controller 0 and itself
   - Both reached 2/3 majority simultaneously
3. No tie-breaking mechanism exists
4. Lack of randomized election timeouts

**Impact:**
- Data inconsistency risk
- Conflicting cluster state
- Potential for divergent topic/partition assignments
- Undefined behavior for client requests

**Reproduction Steps:**
1. Start all 3 controllers simultaneously
2. Observe immediate election triggers
3. Monitor for multiple leader announcements

**Suggested Fix:**
```javascript
// In election initialization
class ElectionManager {
  constructor(config) {
    // Add randomized timeout to prevent simultaneous elections
    this.electionTimeout = config.baseTimeout + 
                          Math.random() * config.timeoutRange;
    // Example: 150ms base + (0-150ms random) = 150-300ms range
  }

  startElection() {
    // Before starting election, add random delay
    const jitter = Math.random() * 50; // 0-50ms jitter
    setTimeout(() => {
      this.initiateVoteRequest();
    }, jitter);
  }

  handleMultipleLeaders(msg) {
    // Split-brain detection and resolution
    if (msg.term === this.currentTerm && 
        this.state === 'leader' && 
        msg.type === 'leader_announcement') {
      
      // Use deterministic tie-breaking (e.g., lexicographic node ID)
      if (msg.leaderId > this.nodeId) {
        this.stepDownToFollower();
        this.currentLeader = msg.leaderId;
      }
    }
  }
}
```

**References:**
- Raft Paper Section 5.2 (Leader Election)
- Log lines: 24181:02:53:44.617Z, 24211:02:53:44.617Z

---

### ERROR-002: False Positive Node Death Detection (CRITICAL)
**Severity:** CRITICAL  
**Component:** Node Registry / Heartbeat Monitor  
**Status:** ACTIVE

**Description:**  
Controllers are incorrectly marking each other as dead due to heartbeat timeouts, despite active leader heartbeats being exchanged continuously.

**Evidence from Logs:**
```
{"ts":"2026-01-06T02:53:58.616Z","level":"warn","msg":"Node marked as dead (heartbeat timeout)","pid":24181,"nodeId":"localhost:9010","lastHeartbeat":"2026-01-06T02:53:42.609Z","timeoutMs":15000}

// Yet heartbeats continue:
{"ts":"2026-01-06T02:53:58.631Z","level":"info","msg":"Election message received","type":"leader_heartbeat","candidateId":"localhost:9010"}
```

**Affected Components:**
- All controller nodes
- Node Registry module
- Heartbeat monitoring system

**Root Cause Analysis:**
1. Controller registration heartbeat (`lastHeartbeat: 2026-01-06T02:53:42.609Z`) is from initial registration
2. This timestamp is never updated despite ongoing leader heartbeats
3. Dead node detection checks the wrong heartbeat timestamp
4. Two separate heartbeat mechanisms exist:
   - Registration heartbeat (checked for liveness)
   - Election heartbeat (sent but not checked)
5. 15-second timeout expires on stale registration heartbeat

**Impact:**
- Unnecessary failover procedures triggered
- Cluster instability
- Resource waste from repeated elections
- Potential cascading failures

**Affected Timestamps:**
- Registration: `2026-01-06T02:53:42.609Z`
- Death detection: `2026-01-06T02:53:58.616Z` (16 seconds later)
- Active heartbeats continuing throughout

**Suggested Fix:**
```javascript
// In NodeRegistry class
class NodeRegistry {
  handleHeartbeat(nodeId, heartbeatType) {
    const node = this.nodes.get(nodeId);
    if (!node) return;

    // Update appropriate heartbeat timestamp based on type
    switch (heartbeatType) {
      case 'broker_heartbeat':
        node.metadata.lastBrokerHeartbeat = Date.now();
        break;
      case 'leader_heartbeat':
        node.metadata.lastLeaderHeartbeat = Date.now();
        break;
      case 'registration':
        node.metadata.lastRegistrationHeartbeat = Date.now();
        break;
    }

    // Update general lastSeen for any heartbeat
    node.metadata.lastSeen = Date.now();
  }

  checkNodeHealth(nodeId) {
    const node = this.nodes.get(nodeId);
    const now = Date.now();
    
    // Use the most recent heartbeat regardless of type
    const lastActivity = Math.max(
      node.metadata.lastBrokerHeartbeat || 0,
      node.metadata.lastLeaderHeartbeat || 0,
      node.metadata.lastRegistrationHeartbeat || 0,
      node.metadata.lastSeen || 0
    );

    const timeSinceLastActivity = now - lastActivity;
    return timeSinceLastActivity < this.deadNodeTimeout;
  }
}

// In election message handler
handleElectionMessage(msg) {
  if (msg.type === 'leader_heartbeat') {
    // Update heartbeat timestamp for the sender
    this.registry.handleHeartbeat(msg.candidateId, 'leader_heartbeat');
  }
}
```

**Alternative Fix (Simpler):**
```javascript
// Controllers should not use broker heartbeat mechanism
// Remove inter-controller heartbeat checking entirely
checkDeadNodes() {
  this.nodes.forEach((node, nodeId) => {
    // Only check broker nodes for heartbeat timeout
    if (node.metadata.nodeType === 'broker') {
      const timeSinceHeartbeat = Date.now() - node.metadata.lastSeen;
      if (timeSinceHeartbeat > this.deadNodeTimeout) {
        this.handleDeadNode(nodeId);
      }
    }
    // Controllers use election heartbeats for liveness
    // Handled separately in election module
  });
}
```

**References:**
- Log lines: 24181:02:53:58.616Z through 02:54:01.634Z

---

### ERROR-003: Concurrent Vote Granting Violation (HIGH)
**Severity:** HIGH  
**Component:** Election Voting Logic  
**Status:** ACTIVE

**Description:**  
Controllers are granting votes to multiple candidates in the same term, violating the Raft protocol's "one vote per term" rule.

**Evidence from Logs:**
```
// Controller 1 (9011) voting in term 1:
{"ts":"2026-01-06T02:53:44.616Z","msg":"Vote decision","candidateId":"localhost:9012","term":1,"voteGranted":true,"votedFor":"localhost:9012"}

// Same controller, same term, different candidate:
{"ts":"2026-01-06T02:53:44.617Z","msg":"Vote decision","candidateId":"localhost:9010","term":1,"voteGranted":false,"votedFor":"localhost:9012"}
```

**Root Cause Analysis:**
1. Vote requests arrive nearly simultaneously
2. First vote grants before `votedFor` is persisted
3. Race condition in vote processing
4. Missing atomic vote-granting mechanism

**Impact:**
- Enables split-brain condition (ERROR-001)
- Violates Raft safety guarantees
- Multiple leaders can be elected

**Suggested Fix:**
```javascript
class ElectionManager {
  constructor() {
    this.voteLock = new AsyncLock();
    this.persistedVote = null;
  }

  async handleVoteRequest(request) {
    // Acquire lock to ensure atomic voting
    return await this.voteLock.acquire('vote', async () => {
      // Check persisted vote state
      const currentVote = await this.loadPersistedVote(request.term);
      
      if (currentVote && currentVote.term === request.term) {
        // Already voted in this term
        return {
          voteGranted: currentVote.votedFor === request.candidateId,
          term: this.currentTerm,
          votedFor: currentVote.votedFor
        };
      }

      // Grant vote and persist BEFORE responding
      if (this.shouldGrantVote(request)) {
        await this.persistVote(request.term, request.candidateId);
        this.currentVote = {
          term: request.term,
          votedFor: request.candidateId
        };
        
        return {
          voteGranted: true,
          term: request.term,
          votedFor: request.candidateId
        };
      }

      return { voteGranted: false, term: this.currentTerm };
    });
  }
}
```

**References:**
- Raft Paper Section 5.2, Figure 2
- Log lines: 24196:02:53:44.616Z through 02:53:44.619Z

---

## High Priority Errors

### ERROR-004: Missing Pre-Vote Phase (HIGH)
**Severity:** HIGH  
**Component:** Election Module  
**Status:** ACTIVE - DESIGN ISSUE

**Description:**  
The system lacks a pre-vote phase, causing unnecessary elections and term inflation when nodes have network partitions or temporary disconnections.

**Impact:**
- Increased election frequency
- Term number inflation
- Cluster instability during network issues
- Unnecessary load on the system

**Suggested Fix:**
Implement the Pre-Vote optimization from Raft:
```javascript
class ElectionManager {
  async startPreVote() {
    this.state = 'pre-candidate';
    const preVoteRequest = {
      type: 'pre_vote_request',
      term: this.currentTerm + 1, // Hypothetical next term
      candidateId: this.nodeId,
      lastLogIndex: this.log.lastIndex,
      lastLogTerm: this.log.lastTerm
    };

    const preVotes = await this.requestPreVotes(preVoteRequest);
    
    // Only start real election if pre-vote succeeds
    if (preVotes >= this.quorum) {
      this.startElection();
    } else {
      this.state = 'follower';
    }
  }
}
```

**References:**
- Raft PhD Dissertation Section 9.6

---

### ERROR-005: No Election Term Persistence (HIGH)
**Severity:** HIGH  
**Component:** Election State Management  
**Status:** UNKNOWN - NEEDS VERIFICATION

**Description:**  
Cannot verify from logs whether election terms and votes are persisted to disk. If not persisted, node restarts could violate safety guarantees.

**Required Verification:**
Check if the following are persisted:
- Current term
- Voted for (candidate ID)
- Log entries

**Suggested Fix:**
```javascript
class PersistentState {
  constructor(dataDir) {
    this.stateFile = path.join(dataDir, 'election-state.json');
  }

  async saveState(term, votedFor) {
    const state = {
      currentTerm: term,
      votedFor: votedFor,
      timestamp: Date.now()
    };
    
    // Atomic write with fsync
    const tempFile = `${this.stateFile}.tmp`;
    await fs.writeFile(tempFile, JSON.stringify(state));
    await fs.fsync(tempFile);
    await fs.rename(tempFile, this.stateFile);
  }

  async loadState() {
    try {
      const data = await fs.readFile(this.stateFile, 'utf8');
      return JSON.parse(data);
    } catch (err) {
      return { currentTerm: 0, votedFor: null };
    }
  }
}
```

---

### ERROR-006: Broker Leader Election Warning (HIGH)
**Severity:** HIGH  
**Component:** Controller-Broker Coordination  
**Status:** ACTIVE

**Description:**  
Controllers attempted to promote a broker to leader but found no alive brokers immediately after broker registration.

**Evidence from Logs:**
```
{"ts":"2026-01-06T02:53:44.618Z","level":"warn","msg":"No alive brokers to promote as leader","pid":24181}
{"ts":"2026-01-06T02:53:44.618Z","level":"warn","msg":"No alive brokers to promote as leader","pid":24211}

// But brokers registered successfully at 02:53:47.672Z:
{"ts":"2026-01-06T02:53:47.672Z","level":"info","msg":"New node registered","nodeId":"broker-0"}
```

**Root Cause Analysis:**
1. Controller leader election completes at 02:53:44.618Z
2. Controllers try to assign broker leaders immediately
3. Brokers don't start until 02:53:47.664Z (3 seconds later)
4. Race condition in startup sequence

**Impact:**
- Initial broker leader assignment fails
- Topics created early might not be assigned leaders
- Requires manual intervention or retry logic

**Suggested Fix:**
```javascript
// Option 1: Lazy broker leader assignment
class Controller {
  async assignBrokerLeader(topicPartition) {
    const aliveBrokers = this.getAliveBrokers();
    
    if (aliveBrokers.length === 0) {
      this.logger.info('No brokers available, deferring leader assignment');
      // Add to pending assignments queue
      this.pendingLeaderAssignments.push(topicPartition);
      return null;
    }
    
    return this.selectLeader(aliveBrokers, topicPartition);
  }

  onBrokerRegistered(brokerId) {
    this.logger.info('Broker registered, processing pending assignments');
    // Process pending leader assignments
    this.processPendingAssignments();
  }
}

// Option 2: Wait for brokers before completing startup
async startController() {
  await this.startElection();
  
  // Wait for minimum broker count
  if (this.isLeader()) {
    await this.waitForMinimumBrokers(this.config.minBrokers, 30000);
  }
}
```

**References:**
- Log lines: 24181:02:53:44.618Z, broker registration at 02:53:47.672Z

---

## Medium Priority Errors

### ERROR-007: Rapid Heartbeat Message Flooding (MEDIUM)
**Severity:** MEDIUM  
**Component:** Election Heartbeat System  
**Status:** ACTIVE

**Description:**  
Leader heartbeats are being sent and logged at 1-second intervals, creating excessive log volume and potential network overhead.

**Evidence from Logs:**
```
{"ts":"2026-01-06T02:53:45.620Z","msg":"Election message received","type":"leader_heartbeat"}
{"ts":"2026-01-06T02:53:46.620Z","msg":"Election message received","type":"leader_heartbeat"}
{"ts":"2026-01-06T02:53:47.620Z","msg":"Election message received","type":"leader_heartbeat"}
// Continues every second...
```

**Impact:**
- Log file bloat (100+ entries per minute per controller)
- Difficult debugging due to noise
- Unnecessary CPU cycles for logging
- Minimal network impact (messages are small)

**Suggested Fix:**
```javascript
class ElectionManager {
  handleElectionMessage(msg) {
    if (msg.type === 'leader_heartbeat') {
      // Only log at debug level, or aggregate
      if (this.shouldLogHeartbeat()) {
        this.logger.debug('Leader heartbeat received', {
          candidateId: msg.candidateId,
          term: msg.term
        });
      }
      
      this.lastHeartbeatTime = Date.now();
      return;
    }
    
    // Log other election messages at info level
    this.logger.info('Election message received', msg);
  }

  shouldLogHeartbeat() {
    // Log every 10th heartbeat, or once per 10 seconds
    return this.heartbeatCount++ % 10 === 0;
  }
}

// Or use log aggregation
class HeartbeatLogger {
  constructor(logger) {
    this.logger = logger;
    this.heartbeatCounts = new Map();
    
    setInterval(() => this.flushHeartbeatStats(), 10000);
  }

  recordHeartbeat(nodeId) {
    this.heartbeatCounts.set(
      nodeId, 
      (this.heartbeatCounts.get(nodeId) || 0) + 1
    );
  }

  flushHeartbeatStats() {
    this.heartbeatCounts.forEach((count, nodeId) => {
      this.logger.info('Heartbeat summary', { nodeId, count });
    });
    this.heartbeatCounts.clear();
  }
}
```

---

### ERROR-008: Deprecation Warning - url.parse() (MEDIUM)
**Severity:** MEDIUM  
**Component:** Dashboard HTTP Server  
**Status:** ACTIVE

**Description:**  
The dashboard is using the deprecated `url.parse()` API, which has security implications and is not standardized.

**Evidence from Logs:**
```
(node:24277) [DEP0169] DeprecationWarning: `url.parse()` behavior is not standardized and prone to errors that have security implications. Use the WHATWG URL API instead.
```

**Impact:**
- Security vulnerabilities in URL parsing
- Future Node.js version incompatibility
- Non-standard behavior

**Suggested Fix:**
```javascript
// Replace this:
const url = require('url');
const parsed = url.parse(req.url, true);

// With this:
const parsed = new URL(req.url, `http://${req.headers.host}`);
const pathname = parsed.pathname;
const query = Object.fromEntries(parsed.searchParams);

// Or use a modern web framework
const express = require('express');
const app = express();

app.get('/api/topics', (req, res) => {
  // req.query automatically parsed
  const limit = req.query.limit;
});
```

**References:**
- Node.js DEP0169 deprecation
- Log line: Dashboard PID 24277

---

### ERROR-009: Missing Node State Validation (MEDIUM)
**Severity:** MEDIUM  
**Component:** Node Registry  
**Status:** POTENTIAL ISSUE

**Description:**  
Node state transitions lack validation. System shows multiple nodes simultaneously transitioning to leader state without proper conflict resolution.

**Evidence from Logs:**
```
{"ts":"2026-01-06T02:53:44.618Z","msg":"Node state transition","nodeId":"localhost:9010","oldState":"follower","newState":"leader"}
{"ts":"2026-01-06T02:53:44.618Z","msg":"Node state transition","nodeId":"localhost:9012","oldState":"follower","newState":"leader"}
```

**Impact:**
- Related to ERROR-001 (split-brain)
- No validation prevents invalid state transitions
- No automatic conflict detection

**Suggested Fix:**
```javascript
class NodeRegistry {
  transitionNodeState(nodeId, newState) {
    const node = this.nodes.get(nodeId);
    if (!node) throw new Error(`Node ${nodeId} not found`);

    const oldState = node.metadata.state;
    
    // Validate state transition
    if (newState === 'leader') {
      const currentLeader = this.findLeader();
      if (currentLeader && currentLeader !== nodeId) {
        this.logger.error('Attempted leader transition with existing leader', {
          nodeId,
          existingLeader: currentLeader,
          term: node.metadata.term
        });
        
        // Trigger split-brain resolution
        this.resolveLeaderConflict(nodeId, currentLeader);
        return false;
      }
    }

    node.metadata.state = newState;
    this.emit('stateChange', { nodeId, oldState, newState });
    
    return true;
  }

  resolveLeaderConflict(nodeId1, nodeId2) {
    // Implement deterministic conflict resolution
    const winner = nodeId1 > nodeId2 ? nodeId1 : nodeId2;
    const loser = winner === nodeId1 ? nodeId2 : nodeId1;
    
    this.logger.warn('Resolving leader conflict', { winner, loser });
    this.transitionNodeState(loser, 'follower');
  }
}
```

---

## Low Priority Issues

### ERROR-010: Insufficient Election Timeout Configuration (LOW)
**Severity:** LOW  
**Component:** Configuration  
**Status:** POTENTIAL ISSUE

**Description:**  
Cannot verify from logs what the election timeout values are. If too short, could contribute to split-brain issues.

**Recommended Configuration:**
```javascript
const ELECTION_CONFIG = {
  // Base timeout should be >> network RTT
  BASE_TIMEOUT: 150,      // ms
  TIMEOUT_RANGE: 150,     // ms (adds randomness)
  // Result: 150-300ms range
  
  // Heartbeat interval should be << election timeout
  HEARTBEAT_INTERVAL: 50, // ms (1/3 of min election timeout)
  
  // Dead node timeout for brokers
  DEAD_NODE_TIMEOUT: 15000, // ms (10x heartbeat interval)
};
```

---

### ERROR-011: Missing Metrics and Observability (LOW)
**Severity:** LOW  
**Component:** System-wide  
**Status:** ENHANCEMENT NEEDED

**Description:**  
System lacks proper metrics for monitoring cluster health and detecting issues proactively.

**Suggested Metrics to Add:**
```javascript
class ClusterMetrics {
  constructor() {
    this.metrics = {
      // Election metrics
      electionCount: 0,
      electionDuration: [],
      leaderChanges: 0,
      splitBrainEvents: 0,
      
      // Heartbeat metrics
      heartbeatsSent: 0,
      heartbeatsReceived: 0,
      heartbeatLatency: [],
      
      // Node health
      deadNodeDetections: 0,
      falsePositiveDeaths: 0,
      nodeRecoveries: 0,
      
      // Vote metrics
      votesGranted: 0,
      votesDenied: 0,
      duplicateVotes: 0,
    };
  }

  recordElection(duration, success) {
    this.metrics.electionCount++;
    this.metrics.electionDuration.push(duration);
    if (!success) this.metrics.splitBrainEvents++;
  }

  getMetricsSummary() {
    return {
      avgElectionDuration: this.avg(this.metrics.electionDuration),
      splitBrainRate: this.metrics.splitBrainEvents / this.metrics.electionCount,
      avgHeartbeatLatency: this.avg(this.metrics.heartbeatLatency),
    };
  }
}
```

---

## Testing Recommendations

### Test Case 1: Split-Brain Prevention
```javascript
describe('Election - Split-Brain Prevention', () => {
  it('should elect only one leader per term', async () => {
    // Start 3 controllers simultaneously
    const controllers = await startControllers(3);
    
    // Wait for election to complete
    await sleep(5000);
    
    // Verify only one leader exists
    const leaders = controllers.filter(c => c.isLeader());
    expect(leaders).toHaveLength(1);
    
    // Verify all have same term
    const terms = controllers.map(c => c.currentTerm);
    expect(new Set(terms).size).toBe(1);
  });
});
```

### Test Case 2: Heartbeat Updates
```javascript
describe('Heartbeat - Timestamp Updates', () => {
  it('should update node heartbeat on leader messages', async () => {
    const controller = await startController();
    const initialHeartbeat = controller.registry.getNode('peer').lastSeen;
    
    await sleep(2000);
    
    // Leader heartbeat should have updated timestamp
    const updatedHeartbeat = controller.registry.getNode('peer').lastSeen;
    expect(updatedHeartbeat).toBeGreaterThan(initialHeartbeat);
  });
});
```

### Test Case 3: Concurrent Vote Requests
```javascript
describe('Election - Vote Granting', () => {
  it('should grant vote to only one candidate per term', async () => {
    const follower = await startController({ role: 'follower' });
    
    // Simulate concurrent vote requests
    const [vote1, vote2] = await Promise.all([
      follower.handleVoteRequest({ candidateId: 'A', term: 1 }),
      follower.handleVoteRequest({ candidateId: 'B', term: 1 })
    ]);
    
    // Only one should be granted
    const grantedCount = [vote1, vote2].filter(v => v.voteGranted).length;
    expect(grantedCount).toBe(1);
  });
});
```

---

## Priority Matrix

| Error ID | Severity | Component | Fix Complexity | Priority |
|----------|----------|-----------|----------------|----------|
| ERROR-001 | CRITICAL | Election | HIGH | 1 |
| ERROR-002 | CRITICAL | Heartbeat | MEDIUM | 2 |
| ERROR-003 | HIGH | Voting | MEDIUM | 3 |
| ERROR-006 | HIGH | Startup | LOW | 4 |
| ERROR-004 | HIGH | Election | HIGH | 5 |
| ERROR-005 | HIGH | Persistence | MEDIUM | 6 |
| ERROR-007 | MEDIUM | Logging | LOW | 7 |
| ERROR-008 | MEDIUM | Dashboard | LOW | 8 |
| ERROR-009 | MEDIUM | State Mgmt | MEDIUM | 9 |
| ERROR-010 | LOW | Config | LOW | 10 |
| ERROR-011 | LOW | Observability | HIGH | 11 |

---

## Next Steps

1. **Immediate Actions:**
   - Implement randomized election timeouts (ERROR-001)
   - Fix heartbeat timestamp updates (ERROR-002)
   - Add vote persistence and locking (ERROR-003)

2. **Short-term (1-2 weeks):**
   - Add broker startup coordination (ERROR-006)
   - Implement pre-vote phase (ERROR-004)
   - Add state persistence (ERROR-005)

3. **Medium-term (1 month):**
   - Reduce logging verbosity (ERROR-007)
   - Update deprecated APIs (ERROR-008)
   - Add state transition validation (ERROR-009)

4. **Long-term (2+ months):**
   - Build comprehensive metrics system (ERROR-011)
   - Create full test suite
   - Performance optimization

---

## Additional Notes

- All timestamps in logs are UTC
- System is running on localhost, production deployment may reveal additional network-related issues
- Consider implementing a formal incident response process
- Set up continuous monitoring for split-brain detection
- Review Raft paper sections 5.2 (Leader Election) and 5.4 (Safety) for reference

**Document Version:** 1.0  
**Last Updated:** January 6, 2026  
**Maintained By:** [Your Team]