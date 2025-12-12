'use strict';

const { Logger } = require('../../../shared/utils/logger');

class Quorum {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'quorum' });
    
    this.currentTerm = 0;
    this.votedFor = null;
    this.votes = new Map();
  }

  startNewTerm(term) {
    if (term <= this.currentTerm) {
      this.logger.warn('Attempted to start term that is not greater than current', {
        requestedTerm: term,
        currentTerm: this.currentTerm
      });
      return false;
    }

    this.logger.info('Starting new election term', { term, previousTerm: this.currentTerm });
    this.currentTerm = term;
    this.votedFor = null;

    return true;
  }

  getCurrentTerm() {
    return this.currentTerm;
  }

  recordVote(term, voterId, candidateId) {
    if (term < this.currentTerm) {
      this.logger.debug('Ignoring vote from old term', { term, currentTerm: this.currentTerm });
      return false;
    }

    if (term > this.currentTerm) {
      this.logger.info('Received vote for future term, updating', { term, currentTerm: this.currentTerm });
      this.currentTerm = term;
      this.votedFor = null;
    }

    if (this.votedFor && this.votedFor !== candidateId) {
      this.logger.debug('Voter already voted for different candidate', {
        term,
        voterId,
        votedFor: this.votedFor,
        requestedCandidate: candidateId
      });
      return false;
    }

    if (!this.votes.has(term)) {
      this.votes.set(term, new Map());
    }

    const termVotes = this.votes.get(term);
    if (!termVotes.has(candidateId)) {
      termVotes.set(candidateId, new Set());
    }

    const candidateVotes = termVotes.get(candidateId);
    candidateVotes.add(voterId);

    if (term === this.currentTerm) {
      this.votedFor = candidateId;
    }

    this.logger.debug('Vote recorded', {
      term,
      voterId,
      candidateId,
      totalVotesForCandidate: candidateVotes.size
    });

    return true;
  }

  /**
   * Get vote count for a candidate in a specific term
   */
  getVoteCount(term, candidateId) {
    const termVotes = this.votes.get(term);
    if (!termVotes) {
      return 0;
    }

    const candidateVotes = termVotes.get(candidateId);
    return candidateVotes ? candidateVotes.size : 0;
  }

  hasMajority(term, candidateId, totalNodes) {
    const voteCount = this.getVoteCount(term, candidateId);
    const required = Math.floor(totalNodes / 2) + 1;

    this.logger.trace('Checking majority', {
      term,
      candidateId,
      voteCount,
      required,
      totalNodes
    });

    return voteCount >= required;
  }

  getVotedFor() {
    return this.votedFor;
  }

  hasVotedInCurrentTerm() {
    return this.votedFor !== null;
  }

  getTermResults(term) {
    const termVotes = this.votes.get(term);
    if (!termVotes) {
      return {};
    }

    const results = {};
    for (const [candidateId, voters] of termVotes.entries()) {
      results[candidateId] = {
        voteCount: voters.size,
        voters: Array.from(voters)
      };
    }

    return results;
  }

  clearOldVotes(currentTerm) {
    const termsToDelete = [];
    
    for (const term of this.votes.keys()) {
      if (term < currentTerm) {
        termsToDelete.push(term);
      }
    }

    for (const term of termsToDelete) {
      this.votes.delete(term);
    }

    if (termsToDelete.length > 0) {
      this.logger.debug('Cleared old vote data', {
        clearedTerms: termsToDelete.length,
        currentTerm
      });
    }
  }

  reset() {
    this.logger.info('Resetting quorum state');
    this.currentTerm = 0;
    this.votedFor = null;
    this.votes.clear();
  }
}

module.exports = {
  Quorum
};