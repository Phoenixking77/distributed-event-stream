'use strict';

import { NODE_STATES, ELECTION_TYPES, HEARTBEAT_INTERVALS, createElectionMessage } from '../../../shared/protocol/clusterTypes.js';
import { Logger } from '../../../shared/utils/logger.js';

class LeaderElection {
  constructor(options = {}) {
    if (!options.nodeRegistry) {
      throw new Error('nodeRegistry required');
    }

    this.nodeRegistry = options.nodeRegistry;
    this.logger = options.logger || new Logger({ name: 'leaderElection' });
    
    this.currentTerm = 0;
    this.votedFor = null;
    this.state = NODE_STATES.FOLLOWER;
    this.leaderId = null;
    this.lastLeaderHeartbeat = Date.now();
    
    this.electionTimeout = null;
    this.heartbeatInterval = null;
    
    this.votes = new Set();
    this.electionInProgress = false;
    this.closed = false;
    
    this._startElectionTimer();
  }

  getCurrentTerm() {
    return this.currentTerm;
  }

  startElection() {
    if (this.closed) {
      return;
    }

    if (this.electionInProgress) {
      this.logger.debug('Election already in progress, skipping');
      return;
    }

    this.currentTerm++;
    this.votedFor = null;
    this.state = NODE_STATES.CANDIDATE;
    this.electionInProgress = true;
    this.votes.clear();
    this.leaderId = null;
    
    this.votes.add('self');
    
    this.logger.info('Starting leader election', {
      term: this.currentTerm,
      candidateId: 'self'
    });

    this._requestVotes();
    this._setElectionTimeoutTimer();
  }

  handleVoteRequest(candidateId, term, lastLogIndex = 0, lastLogTerm = 0) {
    if (this.closed) {
      return false;
    }

    if (!candidateId || typeof term !== 'number') {
      this.logger.warn('Invalid vote request parameters', { candidateId, term });
      return false;
    }

    this.logger.debug('Received vote request', {
      candidateId,
      term,
      currentTerm: this.currentTerm
    });

    let voteGranted = false;
    
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.state = NODE_STATES.FOLLOWER;
      this.leaderId = null;
      this.electionInProgress = false;
    }
    
    if (term >= this.currentTerm && 
        (this.votedFor === null || this.votedFor === candidateId)) {
      voteGranted = true;
      this.votedFor = candidateId;
      this.lastLeaderHeartbeat = Date.now();
      this._resetElectionTimer();
    }

    this.logger.info('Vote decision', {
      candidateId,
      term,
      voteGranted,
      votedFor: this.votedFor
    });

    return voteGranted;
  }

  handleVoteResponse(nodeId, term, voteGranted) {
    if (this.closed || !this.electionInProgress || this.state !== NODE_STATES.CANDIDATE) {
      return;
    }

    if (typeof term !== 'number' || typeof voteGranted !== 'boolean') {
      this.logger.warn('Invalid vote response parameters', { nodeId, term, voteGranted });
      return;
    }

    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.state = NODE_STATES.FOLLOWER;
      this.electionInProgress = false;
      this.leaderId = null;
      this._startElectionTimer();
      return;
    }

    if (term === this.currentTerm && voteGranted) {
      this.votes.add(nodeId);
      
      this.logger.debug('Vote received', {
        nodeId,
        totalVotes: this.votes.size,
        required: this._getRequiredVotes()
      });

      if (this.votes.size >= this._getRequiredVotes()) {
        this._becomeLeader();
      }
    }
  }

  handleLeaderHeartbeat(leaderId, term) {
    if (this.closed) {
      return;
    }

    if (!leaderId || typeof term !== 'number') {
      this.logger.warn('Invalid heartbeat parameters', { leaderId, term });
      return;
    }

    if (term >= this.currentTerm) {
      if (term > this.currentTerm) {
        this.currentTerm = term;
        this.votedFor = null;
      }
      
      this.state = NODE_STATES.FOLLOWER;
      this.leaderId = leaderId;
      this.lastLeaderHeartbeat = Date.now();
      this.electionInProgress = false;
      
      this._resetElectionTimer();
      
      try {
        this.nodeRegistry.updateNodeState(leaderId, NODE_STATES.LEADER, term);
      } catch (err) {
        this.logger.error('Failed to update node registry', err);
      }
      
      this.logger.debug('Leader heartbeat received', {
        leaderId,
        term
      });
    }
  }

  stepDown() {
    if (this.closed) {
      return;
    }

    if (this.state === NODE_STATES.LEADER) {
      this.logger.info('Stepping down from leadership');
      this.state = NODE_STATES.FOLLOWER;
      this.leaderId = null;
      this.electionInProgress = false;
      
      this._clearHeartbeatTimer();
      this._startElectionTimer();
    }
  }

  getState() {
    return {
      term: this.currentTerm,
      state: this.state,
      leaderId: this.leaderId,
      votedFor: this.votedFor,
      lastLeaderHeartbeat: this.lastLeaderHeartbeat,
      electionInProgress: this.electionInProgress,
      votes: this.votes.size,
      closed: this.closed
    };
  }

  isLeader() {
    return this.state === NODE_STATES.LEADER;
  }

  getCurrentLeader() {
    return this.leaderId;
  }

  _requestVotes() {
    const brokers = this.nodeRegistry.getAllNodes();
    
    this.logger.debug('Requesting votes from brokers', {
      brokerCount: brokers.length,
      term: this.currentTerm
    });
    
    for (const broker of brokers) {
      if (broker.nodeId === 'self') {
        continue;
      }
      
      this.logger.trace('Vote request', {
        nodeId: broker.nodeId,
        term: this.currentTerm
      });
    }
  }

  _becomeLeader() {
    this.logger.info('Elected as leader', {
      term: this.currentTerm,
      votes: this.votes.size,
      required: this._getRequiredVotes()
    });

    this.state = NODE_STATES.LEADER;
    this.leaderId = 'self';
    this.electionInProgress = false;
    
    this._clearElectionTimer();
    this._startLeaderHeartbeats();
    this._announceLeadership();
  }

  _startLeaderHeartbeats() {
    this._clearHeartbeatTimer();
    
    this.heartbeatInterval = setInterval(() => {
      if (!this.closed && this.state === NODE_STATES.LEADER) {
        this._sendHeartbeats();
      }
    }, HEARTBEAT_INTERVALS.LEADER_TO_FOLLOWERS);

    if (this.heartbeatInterval.unref) {
      this.heartbeatInterval.unref();
    }
  }

  _sendHeartbeats() {
    const brokers = this.nodeRegistry.getBrokerNodes();
    
    this.logger.trace('Sending leader heartbeats', {
      term: this.currentTerm,
      brokerCount: brokers.length
    });
    
    for (const broker of brokers) {
      if (broker.nodeId === 'self') {
        continue;
      }

      try {
        this.nodeRegistry.updateNodeState(broker.nodeId, NODE_STATES.FOLLOWER, this.currentTerm);
      } catch (err) {
        this.logger.error('Failed to update broker state', { nodeId: broker.nodeId, err });
      }
    }
  }

  _announceLeadership() {
    const brokers = this.nodeRegistry.getBrokerNodes();
    
    this.logger.info('Announcing leadership to brokers', {
      term: this.currentTerm,
      brokerCount: brokers.length
    });
    
    for (const broker of brokers) {
      if (broker.nodeId !== 'self') {
        try {
          this.nodeRegistry.updateNodeState(broker.nodeId, NODE_STATES.FOLLOWER, this.currentTerm);
        } catch (err) {
          this.logger.error('Failed to announce leadership to broker', { nodeId: broker.nodeId, err });
        }
      }
    }
  }

  _getRequiredVotes() {
    const totalNodes = this.nodeRegistry.getBrokerNodes().length;
    return Math.floor(totalNodes / 2) + 1;
  }

  _startElectionTimer() {
    this._clearElectionTimer();
    
    const timeout = HEARTBEAT_INTERVALS.ELECTION_TIMEOUT + 
                   Math.random() * 4000;
    
    this.electionTimeout = setTimeout(() => {
      if (!this.closed &&
          this.state === NODE_STATES.FOLLOWER && 
          Date.now() - this.lastLeaderHeartbeat > HEARTBEAT_INTERVALS.ELECTION_TIMEOUT) {
        this.logger.info('Election timeout, no leader heartbeat received');
        this.startElection();
      }
    }, timeout);

    if (this.electionTimeout.unref) {
      this.electionTimeout.unref();
    }
  }

  _resetElectionTimer() {
    this._startElectionTimer();
  }

  _setElectionTimeoutTimer() {
    this._clearElectionTimer();

    const timeout = HEARTBEAT_INTERVALS.ELECTION_TIMEOUT;
    
    this.electionTimeout = setTimeout(() => {
      if (!this.closed && this.electionInProgress && this.state === NODE_STATES.CANDIDATE) {
        this.logger.warn('Election timed out, restarting');
        this.electionInProgress = false;
        this._startElectionTimer();
      }
    }, timeout);

    if (this.electionTimeout.unref) {
      this.electionTimeout.unref();
    }
  }

  _clearElectionTimer() {
    if (this.electionTimeout) {
      clearTimeout(this.electionTimeout);
      this.electionTimeout = null;
    }
  }

  _clearHeartbeatTimer() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  close() {
    this.closed = true;
    this._clearElectionTimer();
    this._clearHeartbeatTimer();
    this.votes.clear();
    this.logger.debug('LeaderElection closed');
  }
}

export {
  LeaderElection
};