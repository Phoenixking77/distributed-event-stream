'use strict';

import { createSocket } from 'dgram';
import { ELECTION_TYPES, HEARTBEAT_INTERVALS, createElectionMessage } from '../../../shared/protocol/clusterTypes.js';
import { Logger } from '../../../shared/utils/logger.js';


class HeartbeatSender {
  constructor(options = {}) {
    const baseLogger = options.logger || new Logger({ name: 'heartbeatSender' });
    this.logger = baseLogger.child ? baseLogger.child({ component: 'heartbeatSender' }) : baseLogger;
    this.peers = options.peers || [];
    this.socket = null;
    this.heartbeatInterval = null;
    this.closed = false;
    
    
    this.onSendHeartbeat = options.onSendHeartbeat || (() => ({}));
  }

  
  setSocket(socket) {
    this.socket = socket;
    this.logger.debug('Heartbeat sender socket configured');
  }

  
  setPeers(peers) {
    this.peers = peers || [];
  }

  
  startHeartbeats() {
    if (this.closed) {
      return;
    }

    this._clearHeartbeatInterval();

    this.heartbeatInterval = setInterval(() => {
      if (!this.closed) {
        this._sendHeartbeats();
      }
    }, HEARTBEAT_INTERVALS.LEADER_TO_FOLLOWERS);

    if (this.heartbeatInterval.unref) {
      this.heartbeatInterval.unref();
    }

    this.logger.info('Leader heartbeat sender started', {
      interval: HEARTBEAT_INTERVALS.LEADER_TO_FOLLOWERS,
      peerCount: this.peers.length
    });
  }

  
  stopHeartbeats() {
    this._clearHeartbeatInterval();
    this.logger.debug('Leader heartbeat sender stopped');
  }

  
  _sendHeartbeats() {
    if (!this.socket || this.peers.length === 0) {
      return;
    }

    
    const heartbeatData = this.onSendHeartbeat();

    
    
    const leaderIdForMsg = heartbeatData.leaderId || heartbeatData.candidateId || 'unknown';

    const message = createElectionMessage(
      ELECTION_TYPES.LEADER_HEARTBEAT,
      heartbeatData.term,
      leaderIdForMsg,
      {
        leaderId: heartbeatData.leaderId,
        timestamp: Date.now()
      }
    );

    const buffer = Buffer.from(JSON.stringify(message));

    this.logger.trace('Sending leader heartbeats', {
      term: heartbeatData.term,
      peerCount: this.peers.length
    });

    for (const peer of this.peers) {
      try {
        this.socket.send(buffer, 0, buffer.length, peer.port, peer.host, (err) => {
          if (err) {
            this.logger.warn('Failed to send heartbeat to peer', {
              peer: `${peer.host}:${peer.port}`,
              error: err.message
            });
          } else {
            this.logger.trace('Heartbeat sent to peer', {
              peer: `${peer.host}:${peer.port}`,
              term: heartbeatData.term
            });
          }
        });
      } catch (err) {
        this.logger.error('Error sending heartbeat', {
          peer: `${peer.host}:${peer.port}`,
          error: err.message
        });
      }
    }
  }

  
  _clearHeartbeatInterval() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  
  close() {
    this.closed = true;
    this._clearHeartbeatInterval();
    this.logger.debug('HeartbeatSender closed');
  }
}

export { HeartbeatSender };
