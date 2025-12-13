'use strict';

const path = require('path');
const {
  listLogSegments,
  openSegmentForAppend,
  openSegmentForRead,
  closeFile,
  writeToFile,
  fsyncFile,
  readFromFile,
  getFileSize,
  deleteFile
} = require('./fsUtils').default;
const { Logger } = require('../../../shared/utils/logger');

const DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024;
const ENTRY_HEADER_SIZE = 4 + 8 + 4;

class SegmentManager {
  constructor(options) {
    const opts = options || {};
    if (!opts.topicPath) {
      throw new Error('topicPath required');
    }

    this.topicPath = opts.topicPath;
    this.maxSegmentSize = opts.maxSegmentSize || DEFAULT_SEGMENT_SIZE;
    this.logger = opts.logger || new Logger({ name: 'segmentManager' });

    this.activeSegmentId = 0;
    this.activeSegmentFd = null;
    this.activeSegmentSize = 0;
    this.globalOffset = 0;

    this.segments = [];
  }

  async initialize() {
    this.logger.info('Initializing segment manager', { topicPath: this.topicPath });

    const existingSegments = listLogSegments(this.topicPath);
    this.segments = existingSegments;

    if (existingSegments.length === 0) {
      this.logger.info('No existing segments found, creating new segment');
      this._createNewSegment();
    } else {
      const lastSegment = existingSegments[existingSegments.length - 1];
      this.activeSegmentId = lastSegment.id;
      this.activeSegmentFd = openSegmentForAppend(lastSegment.path);
      this.activeSegmentSize = getFileSize(this.activeSegmentFd);

      this.globalOffset = this._calculateGlobalOffset();
      this.logger.info('Recovered from existing segments', {
        segmentCount: existingSegments.length,
        activeSegmentId: this.activeSegmentId,
        activeSegmentSize: this.activeSegmentSize,
        globalOffset: this.globalOffset
      });
    }
  }

  _createNewSegment() {
    const segmentPath = path.join(this.topicPath, `segment-${this.activeSegmentId}.log`);
    this.activeSegmentFd = openSegmentForAppend(segmentPath);
    this.activeSegmentSize = 0;

    this.segments.push({
      id: this.activeSegmentId,
      path: segmentPath,
      size: 0
    });

    this.logger.debug('Created new segment', {
      segmentId: this.activeSegmentId,
      path: segmentPath
    });
  }

  _calculateGlobalOffset() {
    let offset = 0;
    for (const segment of this.segments) {
      if (segment.id < this.activeSegmentId) {
        offset += segment.size;
      }
    }
    offset += this.activeSegmentSize;
    return offset;
  }

  append(messageBuffer, timestamp, crc) {
    const ts = typeof timestamp === 'number' ? timestamp : Date.now();
    const checksum = typeof crc === 'number' ? crc : 0;

    const messageSize = messageBuffer.length;
    const entrySize = ENTRY_HEADER_SIZE + messageSize;
    const entryBuffer = Buffer.allocUnsafe(entrySize);

    let offset = 0;
    entryBuffer.writeUInt32BE(messageSize, offset);
    offset += 4;

    entryBuffer.writeBigInt64BE(BigInt(ts), offset);
    offset += 8;

    entryBuffer.writeUInt32BE(checksum, offset);
    offset += 4;

    messageBuffer.copy(entryBuffer, offset);

    if (this.activeSegmentSize + entrySize > this.maxSegmentSize) {
      this.logger.info('Rotating segment', {
        currentSegmentId: this.activeSegmentId,
        currentSize: this.activeSegmentSize
      });
      this._rotateSegment();
    }

    writeToFile(this.activeSegmentFd, entryBuffer);
    this.activeSegmentSize += entrySize;
    const entryOffset = this.globalOffset;
    this.globalOffset += entrySize;

    this.segments[this.segments.length - 1].size = this.activeSegmentSize;

    return entryOffset;
  }

  sync() {
    if (this.activeSegmentFd !== null) {
      fsyncFile(this.activeSegmentFd);
    }
  }

  _rotateSegment() {
    if (this.activeSegmentFd !== null) {
      fsyncFile(this.activeSegmentFd);
      closeFile(this.activeSegmentFd);
    }

    this.activeSegmentId += 1;
    this._createNewSegment();
  }

  readEntry(globalOffset) {
    const location = this._offsetToSegment(globalOffset);
    if (!location) {
      return null;
    }

    const fd = openSegmentForRead(location.segmentPath);
    try {
      const headerBuffer = Buffer.allocUnsafe(ENTRY_HEADER_SIZE);
      const headerRead = readFromFile(fd, headerBuffer, 0, ENTRY_HEADER_SIZE, location.segmentOffset);
      if (headerRead < ENTRY_HEADER_SIZE) {
        return null;
      }

      let offset = 0;
      const messageSize = headerBuffer.readUInt32BE(offset);
      offset += 4;

      const timestamp = headerBuffer.readBigInt64BE(offset);
      offset += 8;

      const crc = headerBuffer.readUInt32BE(offset);

      const messageBuffer = Buffer.allocUnsafe(messageSize);
      const messageRead = readFromFile(
        fd,
        messageBuffer,
        0,
        messageSize,
        location.segmentOffset + ENTRY_HEADER_SIZE
      );

      if (messageRead < messageSize) {
        return null;
      }

      return {
        offset: globalOffset,
        timestamp: Number(timestamp),
        crc,
        message: messageBuffer
      };
    } finally {
      closeFile(fd);
    }
  }

  readRange(startOffset, endOffset) {
    const entries = [];
    let currentOffset = startOffset;

    while (currentOffset < endOffset) {
      const entry = this.readEntry(currentOffset);
      if (!entry) {
        break;
      }
      entries.push(entry);
      currentOffset += ENTRY_HEADER_SIZE + entry.message.length;
    }

    return entries;
  }

  _offsetToSegment(globalOffset) {
    let cumulativeOffset = 0;

    for (const segment of this.segments) {
      const segmentEnd = cumulativeOffset + segment.size;
      if (globalOffset >= cumulativeOffset && globalOffset < segmentEnd) {
        return {
          segmentId: segment.id,
          segmentPath: segment.path,
          segmentOffset: globalOffset - cumulativeOffset
        };
      }
      cumulativeOffset = segmentEnd;
    }

    return null;
  }

  getLatestOffset() {
    return this.globalOffset;
  }

  deleteSegmentsBefore(segmentId) {
    const toDelete = [];
    for (const segment of this.segments) {
      if (segment.id < segmentId) {
        toDelete.push(segment);
      }
    }

    for (const segment of toDelete) {
      this.logger.info('Deleting old segment', { segmentId: segment.id });
      deleteFile(segment.path);
      const index = this.segments.indexOf(segment);
      if (index >= 0) {
        this.segments.splice(index, 1);
      }
    }
  }

  close() {
    if (this.activeSegmentFd !== null) {
      fsyncFile(this.activeSegmentFd);
      closeFile(this.activeSegmentFd);
      this.activeSegmentFd = null;
    }
    this.logger.info('Segment manager closed');
  }
}

module.exports = {
  SegmentManager,
  ENTRY_HEADER_SIZE
};