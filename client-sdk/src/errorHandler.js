'use strict';

import { Logger } from '../../shared/utils/logger.js';


class RetryPolicy {
  constructor(options) {
    const opts = options || {};
    
    this.maxAttempts = opts.maxAttempts || 3;
    this.initialDelayMs = opts.initialDelayMs || 100;
    this.maxDelayMs = opts.maxDelayMs || 10000;
    this.backoffMultiplier = opts.backoffMultiplier || 2;
    this.jitterFactor = opts.jitterFactor || 0.1;
    
    
    this.retryableErrors = new Set(opts.retryableErrors || [
      'ECONNREFUSED',
      'ECONNRESET',
      'ETIMEDOUT',
      'EHOSTUNREACH',
      'ENETUNREACH',
      'RequestTimeout',
      'ConnectionTimeout'
    ]);

    this.logger = opts.logger || new Logger({ name: 'retryPolicy' });
  }

  
  isRetryable(error) {
    if (!error) return false;

    
    if (error.code && this.retryableErrors.has(error.code)) {
      return true;
    }

    
    const message = error.message || '';
    return this.retryableErrors.has(message);
  }

  
  async executeWithRetry(fn, context) {
    let lastError;

    for (let attempt = 0; attempt < this.maxAttempts; attempt++) {
      try {
        return await fn();
      } catch (err) {
        lastError = err;

        if (!this.isRetryable(err) || attempt === this.maxAttempts - 1) {
          throw err;
        }

        const delayMs = this._calculateDelay(attempt);
        this.logger.warn('Retrying after error', {
          attempt: attempt + 1,
          maxAttempts: this.maxAttempts,
          delayMs,
          error: err.message,
          context
        });

        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }

    throw lastError;
  }

  
  _calculateDelay(attempt) {
    const exponentialDelay = this.initialDelayMs * Math.pow(this.backoffMultiplier, attempt);
    const cappedDelay = Math.min(exponentialDelay, this.maxDelayMs);
    const jitter = cappedDelay * this.jitterFactor * Math.random();
    return Math.ceil(cappedDelay + jitter);
  }
}


class ErrorHandler {
  constructor(options) {
    const opts = options || {};
    
    this.logger = opts.logger || new Logger({ name: 'errorHandler' });
    this.retryPolicy = opts.retryPolicy || new RetryPolicy(opts);

    
    this.errorCategories = {
      NETWORK: 'NETWORK',
      TIMEOUT: 'TIMEOUT',
      VALIDATION: 'VALIDATION',
      UNKNOWN: 'UNKNOWN',
      CIRCUIT_BREAK: 'CIRCUIT_BREAK'
    };

    
    this.metrics = {
      totalErrors: 0,
      errorsByCategory: {},
      retriedErrors: 0,
      retriedSuccessfully: 0
    };

    for (const category of Object.values(this.errorCategories)) {
      this.metrics.errorsByCategory[category] = 0;
    }
  }

  
  categorizeError(error) {
    const message = error.message || '';
    const code = error.code || '';

    if (code.includes('ECONNREFUSED') || code.includes('ECONNRESET')) {
      return this.errorCategories.NETWORK;
    }
    if (code.includes('ETIMEDOUT') || message.includes('timeout')) {
      return this.errorCategories.TIMEOUT;
    }
    if (message.includes('Missing') || message.includes('Invalid')) {
      return this.errorCategories.VALIDATION;
    }
    if (message.includes('Circuit')) {
      return this.errorCategories.CIRCUIT_BREAK;
    }

    return this.errorCategories.UNKNOWN;
  }

  
  recordError(error, context) {
    const category = this.categorizeError(error);

    this.metrics.totalErrors++;
    this.metrics.errorsByCategory[category]++;

    this.logger.error('Error recorded', {
      category,
      message: error.message,
      code: error.code,
      context,
      stack: error.stack
    });

    return {
      category,
      isRetryable: this.retryPolicy.isRetryable(error)
    };
  }

  
  async handleWithRetry(fn, context) {
    try {
      return await this.retryPolicy.executeWithRetry(fn, context);
    } catch (err) {
      const errorInfo = this.recordError(err, context);
      throw err;
    }
  }

  
  formatError(error, context) {
    return {
      category: this.categorizeError(error),
      message: error.message,
      code: error.code,
      context,
      timestamp: new Date().toISOString(),
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    };
  }

  
  getMetrics() {
    return {
      ...this.metrics,
      retrySuccessRate: this.metrics.retriedSuccessfully / Math.max(this.metrics.retriedErrors, 1)
    };
  }

  
  resetMetrics() {
    this.metrics.totalErrors = 0;
    this.metrics.retriedErrors = 0;
    this.metrics.retriedSuccessfully = 0;
    
    for (const category of Object.values(this.errorCategories)) {
      this.metrics.errorsByCategory[category] = 0;
    }
  }
}

export { RetryPolicy, ErrorHandler };
