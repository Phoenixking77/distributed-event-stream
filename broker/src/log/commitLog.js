'use strict';

const path = require('path');
const { SegmentManager } = require('./segmentManager');
const { crc32 } = require('../../../shared/protocol/checksum');
const { Logger } = require('../../../shared/utils/logger');

class CommitLog {
  constructor(options) {
    const opts = options || {};
    if (!opts.topic) {
      throw new Error('topic name required');
    }
    if (!opts.dataDir) {
      throw new Error('dataDir required');
    }

    this.topic = opts.topic;
    this.dataDir = opts.dataDir;
    this.topicPath = path.join(this.dataDir, 'topics', this.topic);
    this.logger = opts.logger || new Logger({ name: 'commitLog' });

    const maxSegmentSize = opts.maxSegmentSize || 64 * 1024 * 1024;
    this.segmentManager = new SegmentManager({
      topicPath: this.topicPath,
      maxSegmentSize,
      logger: this.logger.child({ component: 'segmentManager' })
    });

    this.syncInterval = opts.syncInterval || 1000;
    this.syncTimer = null;
    this.closed = false;
  }

  async initialize() {
    this.logger.info('Initializing commit log', { topic: this.topic });
    await this.segmentManager.initialize();

    if (this.syncInterval > 0) {
      this._startSyncTimer();
    }

    this.logger.info('Commit log initialized', {
      topic: this.topic,
      latestOffset: this.segmentManager.getLatestOffset()
    });
  }

  append(message) {
    if (this.closed) {
      throw new Error('Commit log is closed');
    }

    if (!Buffer.isBuffer(message)) {
      throw new TypeError('Message must be a Buffer');
    }

    const timestamp = Date.now();
    const checksum = crc32(message, 0, message.length);

    const offset = this.segmentManager.append(message, timestamp, checksum);

    this.logger.trace('Appended message', {
      topic: this.topic,
      offset,
      size: message.length
    });

    return {
      topic: this.topic,
      offset,
      timestamp,
      size: message.length
    };
  }

  read(offset) {
    if (this.closed) {
      throw new Error('Commit log is closed');
    }

    const entry = this.segmentManager.readEntry(offset);
    if (!entry) {
      return null;
    }

    const computedCrc = crc32(entry.message, 0, entry.message.length);
    if (computedCrc !== entry.crc) {
      this.logger.error('Checksum mismatch on read', {
        topic: this.topic,
        offset,
        expected: entry.crc,
        computed: computedCrc
      });
      throw new Error('Checksum verification failed');
    }

    return {
      topic: this.topic,
      offset: entry.offset,
      timestamp: entry.timestamp,
      message: entry.message
    };
  }

  readRange(startOffset, count) {
    if (this.closed) {
      throw new Error('Commit log is closed');
    }

    const maxCount = typeof count === 'number' && count > 0 ? count : 100;
    const latestOffset = this.segmentManager.getLatestOffset();
    const endOffset = Math.min(startOffset + maxCount * 16384, latestOffset);

    const entries = this.segmentManager.readRange(startOffset, endOffset);
    const messages = [];

    for (const entry of entries) {
      const computedCrc = crc32(entry.message, 0, entry.message.length);
      if (computedCrc !== entry.crc) {
        this.logger.warn('Skipping entry with invalid checksum', {
          topic: this.topic,
          offset: entry.offset
        });
        continue;
      }

      messages.push({
        topic: this.topic,
        offset: entry.offset,
        timestamp: entry.timestamp,
        message: entry.message
      });

      if (messages.length >= maxCount) {
        break;
      }
    }

    return messages;
  }

  getLatestOffset() {
    return this.segmentManager.getLatestOffset();
  }

  sync() {
    if (!this.closed) {
      this.segmentManager.sync();
    }
  }

  _startSyncTimer() {
    this.syncTimer = setInterval(() => {
      if (!this.closed) {
        try {
          this.sync();
        } catch (err) {
          this.logger.error('Sync failed', err);
        }
      }
    }, this.syncInterval);

    if (this.syncTimer.unref) {
      this.syncTimer.unref();
    }
  }

  _stopSyncTimer() {
    if (this.syncTimer) {
      clearInterval(this.syncTimer);
      this.syncTimer = null;
    }
  }

  close() {
    if (this.closed) {
      return;
    }

    this.logger.info('Closing commit log', { topic: this.topic });
    this.closed = true;

    this._stopSyncTimer();
    this.segmentManager.close();

    this.logger.info('Commit log closed', { topic: this.topic });
  }
}

module.exports = {
  CommitLog
};