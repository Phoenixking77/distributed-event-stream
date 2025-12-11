'use strict';

const { Logger } = require('../../../shared/utils/logger');

/**
 * Quorum manages vote counting and consensus decisions.
 * Implements majority-based quorum logic for leader election.
 */
class Quorum {
  constructor(options) {
    const opts = options || {};
    this.logger = opts.logger || new Logger({ name: 'quorum' });
    
    // Current term and vote tracking
    this.currentTerm = 0;
    this.votedFor = null;
    this.votes = new Map(); // term -> { candidateId -> Set(voterIds) }
  }

  /**
   * Start a new election term
   */
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

  /**
   * Get current term
   */
  getCurrentTerm() {
    return this.currentTerm;
  }

  /**
   * Record a vote in the current term
   */
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

    // Check if this voter has already voted in this term
    if (this.votedFor && this.votedFor !== candidateId) {
      this.logger.debug('Voter already voted for different candidate', {
        term,
        voterId,
        votedFor: this.votedFor,
        requestedCandidate: candidateId
      });
      return false;
    }

    // Initialize votes map for this term
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

  /**
   * Check if candidate has majority votes
   */
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

  /**
   * Get who was voted for in current term
   */
  getVotedFor() {
    return this.votedFor;
  }

  /**
   * Check if already voted in current term
   */
  hasVotedInCurrentTerm() {
    return this.votedFor !== null;
  }

  /**
   * Get all candidates and their votes for a term
   */
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

  /**
   * Clear votes older than specified term (garbage collection)
   */
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

  /**
   * Reset all state (for testing or restart)
   */
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