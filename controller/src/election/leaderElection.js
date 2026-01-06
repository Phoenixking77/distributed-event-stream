'use strict';

import { createSocket } from 'dgram';
import { NODE_STATES, ELECTION_TYPES, HEARTBEAT_INTERVALS, createElectionMessage } from '../../../shared/protocol/clusterTypes.js';
import { Logger } from '../../../shared/utils/logger.js';
import { HeartbeatSender } from '../state/heartbeatSender.js';

class LeaderElection {
  constructor(options = {}) {
    if (!options.nodeRegistry) {
      throw new Error('nodeRegistry required');
    }

    this.nodeRegistry = options.nodeRegistry;
    this.controllerRegistry = options.controllerRegistry; // New: bridge to node registry
    this.logger = options.logger || new Logger({ name: 'leaderElection' });
    
    // Election communication config
    this.electionPort = options.electionPort || 9093;
    this.host = options.host || '0.0.0.0';
    this.peers = options.peers || []; // Array of {host, port} for peer controllers
    
    this.currentTerm = 0;
    this.votedFor = null;
    this.votedForTerm = 0;
    this.state = NODE_STATES.FOLLOWER;
    this.leaderId = null;
    this.lastLeaderHeartbeat = Date.now();
    
    this.electionTimeout = null;
    this.socket = null;
    
    this.votes = new Set();
    this.electionInProgress = false;
    this.closed = false;
    // Self identifier used in election messages (host:port)
    this.selfId = options.selfId || `${this.host}:${this.electionPort}`;
    
    // Initialize heartbeat sender (NEW)
    this.heartbeatSender = new HeartbeatSender({
      logger: this.logger.child({ component: 'heartbeatSender' }),
      peers: this.peers,
      onSendHeartbeat: () => this._getHeartbeatData()
    });

    // Force this controller to be leader when configured (useful for single static leader)
    this.forceLeader = !!options.forceLeader;
    this.electionTimeoutBase = options.electionTimeoutBase || HEARTBEAT_INTERVALS.ELECTION_TIMEOUT_BASE;
    this.electionTimeoutRange = options.electionTimeoutRange || HEARTBEAT_INTERVALS.ELECTION_TIMEOUT_RANGE;

    if (!this.forceLeader) {
      this._startElectionTimer();
    }
  }

  /**
   * Initialize UDP socket for election communication
   */
  async initialize() {
    if (this.socket) {
      return;
    }

    return new Promise((resolve, reject) => {
      this.socket = createSocket('udp4');

      this.socket.on('message', (msg, rinfo) => {
        this._handleElectionMessage(msg, rinfo);
      });

      this.socket.on('error', (err) => {
        this.logger.error('Election socket error', err);
        if (!this.closed) {
          reject(err);
        }
      });

      this.socket.bind(this.electionPort, this.host, () => {
        this.logger.info('Election socket initialized', {
          host: this.host,
          port: this.electionPort,
          peersCount: this.peers.length
        });
        
        // Share socket with heartbeat sender (NEW)
        this.heartbeatSender.setSocket(this.socket);
        
        // If we have peers (multi-controller setup), trigger election immediately
        if (this.peers.length > 0 && !this.forceLeader) {
          this.logger.info('Multi-controller setup detected, starting election', {
            peersCount: this.peers.length,
            peerAddresses: this.peers.map(p => `${p.host}:${p.port}`).join(',')
          });
          
          // Start election after a short delay to allow all controllers to initialize
          setTimeout(() => {
            if (!this.closed && !this.electionInProgress) {
              this.startElection();
            }
          }, 2000);
        }

        // If configured to force leader, immediately become leader and announce
        if (this.forceLeader) {
          this.logger.info('Force-leader mode enabled, becoming leader immediately', { selfId: this.selfId });
          // ensure term increments so followers see a higher term
          this.currentTerm = Math.max(this.currentTerm + 1, 1);
          this._becomeLeader();
        }
        
        resolve();
      });
    });
  }

  /**
   * Set peer controllers for election communication
   */
  setPeers(peers) {
    this.peers = peers || [];
    this.heartbeatSender.setPeers(this.peers); // NEW: update heartbeat sender too
    this.logger.debug('Peers configured for election', {
      peerCount: this.peers.length,
      peers: this.peers.map(p => `${p.host}:${p.port}`).join(',')
    });
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
    
    this.votes.add(this.selfId);
    
    this.logger.info('Starting leader election', {
      term: this.currentTerm,
      candidateId: this.selfId
    });

    // Add small jitter before requesting votes to reduce simultaneous broadcasts
    // Add jitter before broadcasting vote requests to reduce simultaneous elections
    const jitter = Math.random() * Math.max(this.electionTimeoutRange * 0.1, 50);
    setTimeout(() => {
      this._requestVotes();
    
      // Check if we already have quorum (single controller or no peers)
      if (this.votes.size >= this._getRequiredVotes()) {
        this._becomeLeader();
      } else {
        this._setElectionTimeoutTimer();
      }
    }, jitter);
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
      
      this.logger.info('Vote response received', {
        nodeId,
        voteGranted,
        totalVotes: this.votes.size,
        required: this._getRequiredVotes(),
        term: this.currentTerm
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
      // Split-brain guard: if we are leader with same term and see another leader, tie-break
      if (term === this.currentTerm && this.state === NODE_STATES.LEADER && leaderId !== this.selfId) {
        const preferredLeader = [leaderId, this.selfId].sort()[0];
        if (preferredLeader !== this.selfId) {
          this.logger.warn('Detected competing leader, stepping down (tie-break)', {
            currentLeader: this.selfId,
            incomingLeader: leaderId,
            term
          });
          this.stepDown();
        } else {
          this.logger.warn('Detected competing leader, retaining leadership via tie-break', {
            currentLeader: this.selfId,
            incomingLeader: leaderId,
            term
          });
          return;
        }
      }

      if (term > this.currentTerm) {
        this.currentTerm = term;
        this.votedFor = null;
        this.votedForTerm = 0;
      }
      
      this.state = NODE_STATES.FOLLOWER;
      this.leaderId = leaderId;
      this.lastLeaderHeartbeat = Date.now();
      this.electionInProgress = false;
      
      this._resetElectionTimer();
      
      // Update controller registry with new leader (NEW)
      if (this.controllerRegistry) {
        this.controllerRegistry.handleHeartbeatReceived(leaderId);
      }
      // Update node registry heartbeat to avoid false dead detection
      if (this.nodeRegistry) {
        this.nodeRegistry.registerHeartbeat(leaderId, {
          type: 'controller',
          heartbeatType: 'leader'
        });
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
      
      this.heartbeatSender.stopHeartbeats(); // NEW: stop sending heartbeats
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
    if (!this.socket) {
      this.logger.warn('Election socket not initialized, cannot request votes');
      return;
    }

    const message = createElectionMessage(
      ELECTION_TYPES.VOTE_REQUEST,
      this.currentTerm,
      this.selfId,
      {
        lastLogIndex: 0,
        lastLogTerm: 0
      }
    );

    const buffer = Buffer.from(JSON.stringify(message));

    this.logger.info('Requesting votes from peers', {
      peerCount: this.peers.length,
      term: this.currentTerm,
      peers: this.peers.map(p => `${p.host}:${p.port}`).join(',')
    });

    if (this.peers.length === 0) {
      this.logger.warn('No peers configured, cannot request votes');
      return;
    }

    for (const peer of this.peers) {
      try {
        this.socket.send(buffer, 0, buffer.length, peer.port, peer.host, (err) => {
          if (err) {
            this.logger.warn('Failed to send vote request', {
              peer: `${peer.host}:${peer.port}`,
              error: err.message
            });
          } else {
            this.logger.trace('Vote request sent', {
              peer: `${peer.host}:${peer.port}`,
              term: this.currentTerm
            });
          }
        });
      } catch (err) {
        this.logger.error('Error sending vote request', {
          peer: `${peer.host}:${peer.port}`,
          error: err.message
        });
      }
    }
  }

  _becomeLeader() {
    // If another leader is already recorded for this term, apply deterministic tie-break
    if (this.nodeRegistry) {
      const currentLeader = this.nodeRegistry.getLeader();
      if (currentLeader && currentLeader.nodeId !== this.selfId) {
        const preferredLeader = [currentLeader.nodeId, this.selfId].sort()[0];
        if (preferredLeader !== this.selfId) {
          this.logger.warn('Detected existing leader, stepping down due to tie-break', {
            currentLeader: currentLeader.nodeId,
            selfId: this.selfId,
            term: this.currentTerm
          });
          this.state = NODE_STATES.FOLLOWER;
          this.leaderId = currentLeader.nodeId;
          this.electionInProgress = false;
          this._resetElectionTimer();
          return;
        }
      }
    }

    this.logger.info('Elected as leader', {
      term: this.currentTerm,
      votes: this.votes.size,
      required: this._getRequiredVotes()
    });

    this.state = NODE_STATES.LEADER;
    this.leaderId = this.selfId;
    this.electionInProgress = false;
    
    this._clearElectionTimer();
    
    // Start sending heartbeats to followers (NEW - CRITICAL FIX)
    this.heartbeatSender.startHeartbeats();
    
    this._announceLeadership();
    
    // Update controller registry (NEW)
    if (this.controllerRegistry) {
      this.controllerRegistry.handleLeaderElected(this.leaderId, this.currentTerm);
    }
  }

  _startLeaderHeartbeats() {
    // REMOVED - Now using HeartbeatSender.startHeartbeats()
    // This method is no longer used
  }

  _sendHeartbeats() {
    // REMOVED - Now using HeartbeatSender
    // This method is no longer used
  }

  _announceLeadership() {
    if (!this.socket) {
      this.logger.warn('Election socket not initialized, cannot announce leadership');
      return;
    }

    // Announce leadership to all peer controllers
    const message = createElectionMessage(
      ELECTION_TYPES.LEADER_ANNOUNCEMENT,
      this.currentTerm,
      this.selfId
    );

    const buffer = Buffer.from(JSON.stringify(message));

    this.logger.info('Announcing leadership to peer controllers', {
      term: this.currentTerm,
      peerCount: this.peers.length
    });

    for (const peer of this.peers) {
      try {
        this.socket.send(buffer, 0, buffer.length, peer.port, peer.host, (err) => {
          if (err) {
            this.logger.warn('Failed to announce leadership', {
              peer: `${peer.host}:${peer.port}`,
              error: err.message
            });
          } else {
            this.logger.trace('Leadership announced', {
              peer: `${peer.host}:${peer.port}`,
              term: this.currentTerm
            });
          }
        });
      } catch (err) {
        this.logger.error('Error announcing leadership', {
          peer: `${peer.host}:${peer.port}`,
          error: err.message
        });
      }
    }
  }

  _getRequiredVotes() {
    // For controllers, we need quorum among controllers, not brokers
    // Total controllers = number of peers + 1 (self)
    const totalControllers = this.peers.length + 1;
    return Math.floor(totalControllers / 2) + 1;
  }

  _startElectionTimer() {
    this._clearElectionTimer();
    
    const timeout = this.electionTimeoutBase +
                   Math.random() * this.electionTimeoutRange;
    
    this.electionTimeout = setTimeout(() => {
      if (!this.closed &&
          this.state === NODE_STATES.FOLLOWER && 
          Date.now() - this.lastLeaderHeartbeat > timeout) {
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

    const timeout = this.electionTimeoutBase +
                   Math.random() * this.electionTimeoutRange;
    
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

  /**
   * Get current heartbeat data for leader to send to followers
   */
  _getHeartbeatData() {
    return {
      term: this.currentTerm,
      leaderId: this.selfId,
      timestamp: Date.now()
    };
  }

  /**
   * Handle incoming election message from peer controller
   */
  _handleElectionMessage(buffer, rinfo) {
    if (this.closed) {
      return;
    }

    try {
      const message = JSON.parse(buffer.toString('utf-8'));
      const { type, term, candidateId } = message;

      if (!type || typeof term !== 'number' || !candidateId) {
        this.logger.warn('Invalid election message format', { rinfo });
        return;
      }

      this.logger.info('Election message received', {
        type,
        candidateId,
        term,
        electionState: this.state,
        electionInProgress: this.electionInProgress
      });

      if (type === ELECTION_TYPES.VOTE_REQUEST) {
        const voteGranted = this.handleVoteRequest(candidateId, term, message.lastLogIndex, message.lastLogTerm);
        
        // Send vote response
        const response = createElectionMessage(
          ELECTION_TYPES.VOTE_RESPONSE,
          this.currentTerm,
          this.selfId,
          { voteGranted }
        );
        
        const responseBuffer = Buffer.from(JSON.stringify(response));
        this.socket.send(responseBuffer, 0, responseBuffer.length, rinfo.port, rinfo.address, (err) => {
          if (err) {
            this.logger.warn('Failed to send vote response', {
              peer: `${rinfo.address}:${rinfo.port}`,
              error: err.message
            });
          } else {
            this.logger.info('Vote response sent', {
              peer: `${rinfo.address}:${rinfo.port}`,
              voteGranted,
              term: this.currentTerm
            });
          }
        });
      } else if (type === ELECTION_TYPES.VOTE_RESPONSE) {
        // Prefer explicit candidateId from message (responder selfId); fall back to source addr
        const peerId = message.candidateId || `${rinfo.address}:${rinfo.port}`;
        this.handleVoteResponse(peerId, term, message.voteGranted);
      } else if (type === ELECTION_TYPES.LEADER_ANNOUNCEMENT) {
        // Use announced leader id when available
        const leaderId = message.leaderId || message.candidateId || `${rinfo.address}:${rinfo.port}`;
        this.handleLeaderHeartbeat(leaderId, term);
      } else if (type === ELECTION_TYPES.LEADER_HEARTBEAT) {
        // Handle periodic leader heartbeats (NEW)
        const leaderId = message.leaderId || message.candidateId || `${rinfo.address}:${rinfo.port}`;
        this.handleLeaderHeartbeat(leaderId, term);
      } else {
        this.logger.warn('Unknown election message type', { type });
      }
    } catch (err) {
      this.logger.error('Error processing election message', { error: err.message });
    }
  }

  close() {
    this.closed = true;
    this._clearElectionTimer();
    this.heartbeatSender.close(); // NEW: close heartbeat sender
    
    if (this.socket) {
      try {
        this.socket.close();
      } catch (err) {
        this.logger.error('Error closing election socket', err);
      }
      this.socket = null;
    }
    
    this.votes.clear();
    this.logger.debug('LeaderElection closed');
  }
}

export {
  LeaderElection
};