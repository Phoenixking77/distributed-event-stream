'use strict';

class BufferPool {
  constructor(options) {
    const opts = options || {};
    const size = opts.bufferSize || 64 * 1024;
    const max = opts.maxPoolSize || 1024;

    if (!Number.isInteger(size) || size <= 0) {
      throw new Error('bufferSize must be a positive integer');
    }
    if (!Number.isInteger(max) || max <= 0) {
      throw new Error('maxPoolSize must be a positive integer');
    }

    this.bufferSize = size;
    this.maxPoolSize = max;
    this.pool = [];
  }

  acquire() {
    if (this.pool.length > 0) {
      return this.pool.pop();
    }
    return Buffer.allocUnsafe(this.bufferSize);
  }

  release(buffer) {
    if (!Buffer.isBuffer(buffer)) {
      return;
    }
    if (buffer.length !== this.bufferSize) {
      return;
    }
    if (this.pool.length >= this.maxPoolSize) {
      return;
    }
    this.pool.push(buffer);
  }

  clear() {
    this.pool.length = 0;
  }
}

let sharedPool = null;

function getSharedBufferPool() {
  if (!sharedPool) {
    sharedPool = new BufferPool({});
  }
  return sharedPool;
}

module.exports = {
  BufferPool,
  getSharedBufferPool
};