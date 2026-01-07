'use strict';

import { Logger } from '../../../shared/utils/logger.js';


class ReconnectionManager {
  constructor(options) {
    const opts = options || {};
    
    this.minDelay = opts.minDelay || 100;           // ms
    this.maxDelay = opts.maxDelay || 30000;         // ms
    this.backoffMultiplier = opts.backoffMultiplier || 2;
    this.maxJitter = opts.maxJitter || 0.1;         // 10% jitter
    this.maxRetries = opts.maxRetries || 10;
    this.failureThreshold = opts.failureThreshold || 5;
    this.resetTimeout = opts.resetTimeout || 60000; // ms
    
    this.logger = opts.logger || new Logger({ name: 'reconnectManager' });
    
    // State
    this.retryCount = 0;
    this.currentDelay = this.minDelay;
    this.lastFailureTime = null;
    this.consecutiveFailures = 0;
    this.circuitOpen = false;
    this.resetTimer = null;
  }

  /**
   * Get next retry delay with exponential backoff and jitter
   */
  getNextDelay() {
    if (this.circuitOpen) {
      return this.maxDelay;
    }

    const jitter = Math.random() * this.maxJitter;
    const delayWithJitter = this.currentDelay * (1 + jitter);
    return Math.min(Math.ceil(delayWithJitter), this.maxDelay);
  }


  recordFailure() {
    this.retryCount++;
    this.lastFailureTime = Date.now();
    this.consecutiveFailures++;

    if (this.consecutiveFailures >= this.failureThreshold) {
      this.circuitOpen = true;
      this.logger.warn('Circuit breaker opened', {
        consecutiveFailures: this.consecutiveFailures,
        retryCount: this.retryCount
      });
      this._scheduleCircuitReset();
    }

    this.currentDelay = Math.min(
      this.currentDelay * this.backoffMultiplier,
      this.maxDelay
    );

    return {
      retryCount: this.retryCount,
      delay: this.getNextDelay(),
      circuitOpen: this.circuitOpen
    };
  }


  recordSuccess() {
    this.retryCount = 0;
    this.consecutiveFailures = 0;
    this.currentDelay = this.minDelay;
    
    if (this.circuitOpen) {
      this.circuitOpen = false;
      this.logger.info('Circuit breaker closed');
    }

    if (this.resetTimer) {
      clearTimeout(this.resetTimer);
      this.resetTimer = null;
    }
  }


  shouldGiveUp() {
    return this.retryCount >= this.maxRetries;
  }


  getState() {
    return {
      retryCount: this.retryCount,
      consecutiveFailures: this.consecutiveFailures,
      currentDelay: this.currentDelay,
      circuitOpen: this.circuitOpen,
      lastFailureTime: this.lastFailureTime
    };
  }

  
  reset() {
    this.retryCount = 0;
    this.currentDelay = this.minDelay;
    this.consecutiveFailures = 0;
    this.circuitOpen = false;
    this.lastFailureTime = null;
    
    if (this.resetTimer) {
      clearTimeout(this.resetTimer);
      this.resetTimer = null;
    }
  }

 
  _scheduleCircuitReset() {
    if (this.resetTimer) {
      clearTimeout(this.resetTimer);
    }

    this.resetTimer = setTimeout(() => {
      this.logger.info('Circuit breaker reset timeout, attempting reconnection');
      this.circuitOpen = false;
      this.consecutiveFailures = 0;
      this.resetTimer = null;
    }, this.resetTimeout);
  }

  
  destroy() {
    if (this.resetTimer) {
      clearTimeout(this.resetTimer);
      this.resetTimer = null;
    }
  }
}

export { ReconnectionManager };
