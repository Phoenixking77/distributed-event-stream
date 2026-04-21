'use strict';

import { describe, it, beforeEach } from 'mocha';
import assert from 'assert';
import { Logger, LEVELS } from '../utils/logger.js';
import { BufferPool, getSharedBufferPool } from '../utils/bufferPool.js';
import { loadConfig } from '../utils/configLoader.js';
import { crc32, writeCrc32BE, readCrc32BE } from '../protocol/checksum.js';

describe('Logger', () => {
  it('should create logger with default level', () => {
    const logger = new Logger({ name: 'test' });
    assert.ok(logger);
    assert.strictEqual(logger.name, 'test');
    assert.strictEqual(logger.level, LEVELS.info);
  });

  it('should create logger with custom level', () => {
    const logger = new Logger({ name: 'test', level: 'debug' });
    assert.strictEqual(logger.level, LEVELS.debug);
  });

  it('should create child logger', () => {
    const parent = new Logger({ name: 'parent' });
    const child = parent.child({ name: 'child', component: 'test' });
    
    assert.ok(child);
    assert.notStrictEqual(child, parent);
  });

  it('should log at different levels', () => {
    const logger = new Logger({ name: 'test', level: 'trace' });
    logger.trace('trace message');
    logger.debug('debug message');
    logger.info('info message');
    logger.warn('warn message');
    logger.error('error message');
    logger.fatal('fatal message');
  });

  it('should handle error objects', () => {
    const logger = new Logger({ name: 'test' });
    const err = new Error('test error');
    logger.error('Error occurred', err);
    logger.error(err);
  });

  it('should respect log level', () => {
    const logger = new Logger({ name: 'test', level: 'warn' });
    logger.info('should not log');
    logger.warn('should log');
    logger.error('should log');
  });
});

describe('BufferPool', () => {
  it('should create buffer pool with defaults', () => {
    const pool = new BufferPool({});
    assert.ok(pool);
    assert.strictEqual(pool.bufferSize, 64 * 1024);
    assert.strictEqual(pool.maxPoolSize, 1024);
  });

  it('should create buffer pool with custom config', () => {
    const pool = new BufferPool({
      bufferSize: 1024,
      maxPoolSize: 10
    });
    assert.strictEqual(pool.bufferSize, 1024);
    assert.strictEqual(pool.maxPoolSize, 10);
  });

  it('should throw on invalid buffer size', () => {
    assert.throws(() => {
      new BufferPool({ bufferSize: -1 });
    }, /positive integer/);
  });

  it('should throw on invalid max pool size', () => {
    assert.throws(() => {
      new BufferPool({ maxPoolSize: 0 });
    }, /positive integer/);
  });

  it('should acquire buffer', () => {
    const pool = new BufferPool({ bufferSize: 1024 });
    const buffer = pool.acquire();
    
    assert.ok(Buffer.isBuffer(buffer));
    assert.strictEqual(buffer.length, 1024);
  });

  it('should release buffer back to pool', () => {
    const pool = new BufferPool({ bufferSize: 1024, maxPoolSize: 10 });
    const buffer = pool.acquire();
    pool.release(buffer);
    
    const buffer2 = pool.acquire();
    assert.ok(Buffer.isBuffer(buffer2));
  });

  it('should not release invalid buffers', () => {
    const pool = new BufferPool({ bufferSize: 1024 });
    const buffer = Buffer.alloc(512);
    pool.release(buffer);
    
    assert.strictEqual(pool.pool.length, 0);
  });

  it('should not release when pool is full', () => {
    const pool = new BufferPool({ bufferSize: 1024, maxPoolSize: 2 });
    pool.release(Buffer.alloc(1024));
    pool.release(Buffer.alloc(1024));
    pool.release(Buffer.alloc(1024));
    
    assert.strictEqual(pool.pool.length, 2);
  });

  it('should clear pool', () => {
    const pool = new BufferPool({ bufferSize: 1024 });
    pool.release(Buffer.alloc(1024));
    pool.clear();
    
    assert.strictEqual(pool.pool.length, 0);
  });

  it('should get shared buffer pool', () => {
    const pool1 = getSharedBufferPool();
    const pool2 = getSharedBufferPool();
    
    assert.strictEqual(pool1, pool2);
  });
});

describe('ConfigLoader', () => {
  it('should load config with defaults', () => {
    const config = loadConfig({});
    assert.ok(config);
    assert.strictEqual(typeof config, 'object');
  });

  it('should handle missing config files gracefully', () => {
    const config = loadConfig({
      defaultFile: 'non-existent-file.json'
    });
    assert.ok(config);
    assert.strictEqual(typeof config, 'object');
  });
});

describe('Checksum', () => {
  it('should compute CRC32 checksum', () => {
    const buffer = Buffer.from('hello world');
    const checksum = crc32(buffer);
    
    assert.ok(typeof checksum === 'number');
    assert.ok(checksum > 0);
  });

  it('should compute CRC32 with offset and length', () => {
    const buffer = Buffer.from('hello world');
    const checksum1 = crc32(buffer, 0, 5);
    const checksum2 = crc32(buffer, 0, 11);
    
    assert.ok(checksum1 !== checksum2);
  });

  it('should throw on invalid range', () => {
    const buffer = Buffer.from('hello');
    assert.throws(() => {
      crc32(buffer, -1, 10);
    }, /range/);
  });

  it('should write and read CRC32 BE', () => {
    const buffer = Buffer.alloc(8);
    const value = 0x12345678;
    
    writeCrc32BE(buffer, value, 0);
    const read = readCrc32BE(buffer, 0);
    
    assert.strictEqual(read, value >>> 0);
  });

  it('should handle different messages correctly', () => {
    const msg1 = Buffer.from('message 1');
    const msg2 = Buffer.from('message 2');
    
    const crc1 = crc32(msg1);
    const crc2 = crc32(msg2);
    
    assert.ok(crc1 !== crc2);
  });

  it('should verify checksum for same message', () => {
    const msg = Buffer.from('test message');
    const crc1 = crc32(msg);
    const crc2 = crc32(msg);
    
    assert.strictEqual(crc1, crc2);
  });
});
