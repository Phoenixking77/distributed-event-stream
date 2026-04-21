'use strict';

import { NODE_STATES } from '../../../shared/protocol/clusterTypes.js';
import { Logger } from '../../../shared/utils/logger.js';


class ControllerRegistry {
  constructor(options = {}) {
    this.logger = options.logger || new Logger({ name: 'controllerRegistry' });
    this.nodeRegistry = options.nodeRegistry;
    this.leaderElection = options.leaderElection;
    
    if (!this.nodeRegistry) {
      throw new Error('nodeRegistry required');
    }
    if (!this.leaderElection) {
      throw new Error('leaderElection required');
    }

    
    this.selfId = options.selfId; 
    this.peers = options.peers || [];
    this.initialized = false;
  }

  
  initialize(selfId) {
    if (this.initialized) {
      return;
    }

    this.selfId = selfId;

    
    
    
    const [selfHost, selfPortStr] = String(this.selfId).split(':');
    const selfPort = parseInt(selfPortStr, 10) || null;

    const existingSelf = this.nodeRegistry.getNode(this.selfId);
    if (!existingSelf) {
      this.nodeRegistry.registerHeartbeat(this.selfId, {
        type: 'controller',
        role: 'self',
        host: selfHost,
        port: selfPort,
        registeredAt: Date.now()
      });
    }

    this.logger.info('Controller registry initialized', {
      selfId: this.selfId,
      peerCount: this.peers.length
    });

    
    for (const peer of this.peers) {
      const peerId = `${peer.host}:${peer.port}`;
      this.nodeRegistry.registerHeartbeat(peerId, {
        type: 'controller',
        role: 'peer',
        host: peer.host,
        port: peer.port,
        registeredAt: Date.now()
      });

      this.logger.debug('Peer controller registered', {
        nodeId: peerId
      });
    }

    this.initialized = true;

    this.logger.info('Controller registry initialized', {
      selfId: this.selfId,
      peerCount: this.peers.length,
      totalControllers: 1 + this.peers.length
    });
  }

  
  handleLeaderElected(leaderId, term) {
    
    const previousLeader = this.nodeRegistry.getLeader();
    if (previousLeader && previousLeader.nodeId !== leaderId) {
      this.nodeRegistry.updateNodeState(previousLeader.nodeId, NODE_STATES.FOLLOWER);
      this.logger.info('Previous leader stepped down', {
        previousLeader: previousLeader.nodeId
      });
    }

    
    const node = this.nodeRegistry.getNode(leaderId);
    if (node) {
      this.nodeRegistry.updateNodeState(leaderId, NODE_STATES.LEADER);
      this.logger.info('Controller promoted to leader', {
        leaderId,
        term
      });

      
      if (leaderId === this.selfId) {
        this.promoteBrokerLeader();
      }
    } else {
      this.logger.warn('Unknown controller elected as leader', {
        leaderId,
        term
      });
    }
  }

  
  promoteBrokerLeader() {
    const aliveNodes = this.nodeRegistry.getAliveNodes();
    const brokers = aliveNodes.filter(node => 
      node.metadata && node.metadata.nodeType === 'broker'
    );

    if (brokers.length === 0) {
      this.logger.warn('No alive brokers to promote as leader');
      return;
    }

    
    const selectedBroker = brokers[0];
    const updated = this.nodeRegistry.updateNodeState(selectedBroker.nodeId, NODE_STATES.LEADER);

    if (updated) {
      this.logger.info('Broker promoted to leader', {
        brokerId: selectedBroker.nodeId,
        brokerPort: selectedBroker.metadata.port
      });
    }
  }

  
  handleHeartbeatReceived(leaderId) {
    const node = this.nodeRegistry.getNode(leaderId);
    if (node) {
      
      this.nodeRegistry.registerHeartbeat(leaderId, {
        type: 'controller',
        heartbeatReceived: Date.now(),
        heartbeatType: 'leader'
      });

      
      if (node.state !== NODE_STATES.LEADER) {
        this.nodeRegistry.updateNodeState(leaderId, NODE_STATES.LEADER);
      }
    }
  }

  
  getControllers() {
    return this.nodeRegistry.getAllNodes().filter(node => 
      node.metadata && node.metadata.type === 'controller'
    );
  }

  
  getLeaderController() {
    return this.nodeRegistry.getLeader();
  }

  
  setPeers(peers) {
    this.peers = peers || [];
    
    if (this.initialized) {
      
      for (const peer of this.peers) {
        const peerId = `${peer.host}:${peer.port}`;
        const existing = this.nodeRegistry.getNode(peerId);
        if (!existing) {
          this.nodeRegistry.registerHeartbeat(peerId, {
            type: 'controller',
            role: 'peer',
            host: peer.host,
            port: peer.port
          });
        }
      }
    }
  }
}

export { ControllerRegistry };
