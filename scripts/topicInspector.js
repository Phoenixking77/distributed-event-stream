'use strict';

import { promises as fs } from 'fs';
import { join, basename } from 'path';
import { createReadStream } from 'fs';
import { Logger } from '../shared/utils/logger.js';
import { decodePacket } from '../shared/protocol/codec.js';

/**
 * Topic Inspector: Examine and analyze commit log files
 * Useful for debugging, recovery, and understanding data
 * 
 * Usage:
 *   node scripts/topicInspector.js --topic orders
 *   node scripts/topicInspector.js --topic events --data-dir ./data
 *   node scripts/topicInspector.js --topic logs --segment 0 --limit 100
 */

const logger = new Logger({ name: 'topic-inspector' });

class TopicInspector {
  constructor(options) {
    const opts = options || {};
    this.topic = opts.topic || 'default';
    this.dataDir = opts.dataDir || './data';
    this.topicsDir = join(this.dataDir, 'topics');
    this.topicDir = join(this.topicsDir, this.topic);
    this.segmentId = opts.segmentId;
    this.limit = opts.limit || 100;
  }

  /**
   * Run inspection
   */
  async run() {
    logger.info('Starting topic inspection', {
      topic: this.topic,
      topicDir: this.topicDir
    });

    try {
      await this.inspect();
    } catch (err) {
      logger.error('Inspection failed', err);
      process.exit(1);
    }
  }

  /**
   * Main inspection logic
   */
  async inspect() {
    // Check if topic exists
    try {
      await fs.access(this.topicDir);
    } catch (err) {
      console.log(`❌ Topic "${this.topic}" not found at ${this.topicDir}`);
      console.log(`Available topics in ${this.topicsDir}:`);
      try {
        const topics = await fs.readdir(this.topicsDir);
        topics.forEach(t => console.log(`  - ${t}`));
      } catch (err) {
        console.log(`  (No topics directory found)`);
      }
      return;
    }

    console.log(`\n${'='.repeat(70)}`);
    console.log(`📖 TOPIC INSPECTION: ${this.topic}`);
    console.log(`${'='.repeat(70)}\n`);

    // List segments
    const segments = await this._listSegments();
    await this._inspectSegments(segments);
  }

  /**
   * List all segments for topic
   */
  async _listSegments() {
    const files = await fs.readdir(this.topicDir);
    const segments = files
      .filter(f => f.endsWith('.log'))
      .sort((a, b) => {
        const numA = parseInt(a.replace('segment-', '').replace('.log', ''), 10);
        const numB = parseInt(b.replace('segment-', '').replace('.log', ''), 10);
        return numA - numB;
      });

    return segments;
  }

  /**
   * Inspect segments
   */
  async _inspectSegments(segments) {
    if (segments.length === 0) {
      console.log('No segments found');
      return;
    }

    console.log(`📁 SEGMENTS`);
    console.log(`${'─'.repeat(70)}`);

    let totalMessages = 0;
    let totalBytes = 0;

    for (const segment of segments) {
      const segmentPath = join(this.topicDir, segment);
      const stats = await fs.stat(segmentPath);

      console.log(`\nSegment: ${segment}`);
      console.log(`  Path:       ${segmentPath}`);
      console.log(`  Size:       ${(stats.size / 1024).toFixed(2)} KB`);
      console.log(`  Modified:   ${stats.mtime.toISOString()}`);

      // Count messages in segment
      const count = await this._countMessages(segmentPath);
      totalMessages += count;
      totalBytes += stats.size;

      console.log(`  Messages:   ${count}`);
    }

    console.log(`\n${'─'.repeat(70)}`);
    console.log(`📊 AGGREGATE STATS`);
    console.log(`${'─'.repeat(70)}`);
    console.log(`Total Segments: ${segments.length}`);
    console.log(`Total Messages: ${totalMessages}`);
    console.log(`Total Size:     ${(totalBytes / 1024 / 1024).toFixed(2)} MB`);
    console.log(`Avg Size/Msg:   ${(totalBytes / totalMessages).toFixed(2)} bytes`);

    // Show samples if specified
    if (this.segmentId !== undefined) {
      await this._showSegmentSamples(segments);
    } else {
      console.log(`\nTo inspect individual messages, use:`);
      console.log(`  node scripts/topicInspector.js --topic ${this.topic} --segment 0 --limit 20`);
    }

    console.log(`\n${'='.repeat(70)}\n`);
  }

  /**
   * Show sample messages from segment
   */
  async _showSegmentSamples(segments) {
    if (this.segmentId >= segments.length) {
      console.log(`\n❌ Segment ${this.segmentId} not found`);
      return;
    }

    const segmentFile = segments[this.segmentId];
    const segmentPath = join(this.topicDir, segmentFile);

    console.log(`\n${'─'.repeat(70)}`);
    console.log(`📝 SEGMENT ${this.segmentId} MESSAGES (limit: ${this.limit})`);
    console.log(`${'─'.repeat(70)}`);

    const messages = await this._readSegmentMessages(segmentPath, this.limit);

    messages.forEach((msg, idx) => {
      console.log(`\nMessage ${idx + 1}:`);
      console.log(`  Offset:     ${msg.offset}`);
      console.log(`  Timestamp:  ${new Date(msg.timestamp).toISOString()}`);
      console.log(`  Size:       ${msg.size} bytes`);
      console.log(`  Checksum:   ${msg.checksum}`);
      console.log(`  Payload:    ${msg.payload.substring(0, 200)}${msg.payload.length > 200 ? '...' : ''}`);
    });

    console.log(`\n${'─'.repeat(70)}`);
  }

  /**
   * Count messages in segment
   */
  async _countMessages(segmentPath) {
    const buffer = await fs.readFile(segmentPath);
    let count = 0;
    let offset = 0;

    while (offset < buffer.length) {
      if (offset + 4 > buffer.length) break;

      const size = buffer.readUInt32BE(offset);
      if (size === 0 || offset + 4 + size > buffer.length) break;

      count++;
      offset += 4 + size;
    }

    return count;
  }

  /**
   * Read segment messages
   */
  async _readSegmentMessages(segmentPath, limit) {
    const buffer = await fs.readFile(segmentPath);
    const messages = [];
    let offset = 0;
    let messageOffset = 0;

    while (offset < buffer.length && messages.length < limit) {
      if (offset + 4 > buffer.length) break;

      const size = buffer.readUInt32BE(offset);
      if (size === 0 || offset + 4 + size > buffer.length) break;

      const messageBuffer = buffer.slice(offset + 4, offset + 4 + size);

      try {
        // Parse message structure: [timestamp 8][crc32 4][opcode 1][payload...]
        const timestamp = Number(messageBuffer.readBigUInt64BE(0));
        const checksum = messageBuffer.readUInt32BE(8).toString(16);
        const opcode = messageBuffer.readUInt8(12);
        const payload = messageBuffer.slice(13).toString('utf8');

        messages.push({
          offset: messageOffset,
          timestamp,
          checksum,
          opcode,
          size,
          payload
        });

        messageOffset++;
      } catch (err) {
        logger.warn('Failed to parse message', { offset, size, error: err.message });
      }

      offset += 4 + size;
    }

    return messages;
  }
}

/**
 * Parse command-line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    topic: 'default',
    dataDir: './data',
    limit: 100
  };

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace('--', '').replace(/-/g, '');
    const value = args[i + 1];

    if (key === 'topic') options.topic = value;
    if (key === 'datadir') options.dataDir = value;
    if (key === 'segment') options.segmentId = parseInt(value, 10);
    if (key === 'limit') options.limit = parseInt(value, 10);
  }

  return options;
}

/**
 * Main
 */
async function main() {
  const options = parseArgs();
  const inspector = new TopicInspector(options);
  await inspector.run();
}

main().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});
