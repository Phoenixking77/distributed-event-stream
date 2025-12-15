'use strict';

import { promises as fs } from 'fs';
import { join } from 'path';
import { Logger } from '../shared/utils/logger.js';

/**
 * Log cleanup utility for maintenance
 * - Delete old segments
 * - Compact log files
 * - Purge topics
 * - Free disk space
 * 
 * Usage:
 *   node scripts/cleanupLogs.js --mode list
 *   node scripts/cleanupLogs.js --mode delete-old --days 7
 *   node scripts/cleanupLogs.js --mode delete-topic --topic orders
 *   node scripts/cleanupLogs.js --mode compact
 */

const logger = new Logger({ name: 'log-cleanup' });

class LogCleanup {
  constructor(options) {
    const opts = options || {};
    this.mode = opts.mode || 'list'; // list, delete-old, delete-topic, compact, purge-all
    this.dataDir = opts.dataDir || './data';
    this.topicsDir = join(this.dataDir, 'topics');
    this.daysOld = opts.daysOld || 7; // For delete-old
    this.topic = opts.topic; // For delete-topic
    this.dryRun = opts.dryRun !== false; // Default to dry-run
  }

  /**
   * Run cleanup
   */
  async run() {
    logger.info('Starting log cleanup', {
      mode: this.mode,
      dryRun: this.dryRun
    });

    try {
      await this.cleanup();
    } catch (err) {
      logger.error('Cleanup failed', err);
      process.exit(1);
    }
  }

  /**
   * Main cleanup logic
   */
  async cleanup() {
    switch (this.mode) {
      case 'list':
        await this._listTopics();
        break;
      case 'delete-old':
        await this._deleteOldSegments();
        break;
      case 'delete-topic':
        await this._deleteTopic();
        break;
      case 'compact':
        await this._compactSegments();
        break;
      case 'purge-all':
        await this._purgeAll();
        break;
      default:
        logger.error(`Unknown mode: ${this.mode}`);
        process.exit(1);
    }
  }

  /**
   * List all topics and segments
   */
  async _listTopics() {
    console.log('\n' + '='.repeat(70));
    console.log('📋 TOPICS AND SEGMENTS');
    console.log('='.repeat(70) + '\n');

    try {
      const topics = await fs.readdir(this.topicsDir);
      let totalSize = 0;
      let totalSegments = 0;

      for (const topic of topics) {
        const topicDir = join(this.topicsDir, topic);
        const stats = await fs.stat(topicDir);

        if (!stats.isDirectory()) continue;

        const files = await fs.readdir(topicDir);
        const segments = files.filter(f => f.endsWith('.log'));
        let topicSize = 0;

        console.log(`\n📁 Topic: ${topic}`);
        console.log(`   Segments: ${segments.length}`);

        for (const segment of segments) {
          const segmentPath = join(topicDir, segment);
          const segStats = await fs.stat(segmentPath);
          const sizeKB = (segStats.size / 1024).toFixed(2);
          const modified = segStats.mtime.toISOString();

          console.log(`     - ${segment} (${sizeKB} KB, modified: ${modified})`);

          topicSize += segStats.size;
        }

        console.log(`   Total Size: ${(topicSize / 1024 / 1024).toFixed(2)} MB`);
        totalSize += topicSize;
        totalSegments += segments.length;
      }

      console.log('\n' + '─'.repeat(70));
      console.log(`Total Segments: ${totalSegments}`);
      console.log(`Total Size:     ${(totalSize / 1024 / 1024).toFixed(2)} MB`);
      console.log('='.repeat(70) + '\n');
    } catch (err) {
      console.log(`No topics directory found at ${this.topicsDir}`);
    }
  }

  /**
   * Delete segments older than N days
   */
  async _deleteOldSegments() {
    console.log(`\n${'='.repeat(70)}`);
    console.log(`🗑️  DELETE SEGMENTS OLDER THAN ${this.daysOld} DAYS`);
    console.log(`${this.dryRun ? '[DRY RUN]' : '[LIVE]'}`);
    console.log(`${'='.repeat(70)}\n`);

    const now = Date.now();
    const oldestTime = now - (this.daysOld * 24 * 60 * 60 * 1000);

    let deletedCount = 0;
    let freedBytes = 0;

    try {
      const topics = await fs.readdir(this.topicsDir);

      for (const topic of topics) {
        const topicDir = join(this.topicsDir, topic);
        const stats = await fs.stat(topicDir);

        if (!stats.isDirectory()) continue;

        const files = await fs.readdir(topicDir);
        const segments = files.filter(f => f.endsWith('.log')).sort();

        // Keep at least one segment
        if (segments.length <= 1) continue;

        for (const segment of segments.slice(0, -1)) {
          const segmentPath = join(topicDir, segment);
          const segStats = await fs.stat(segmentPath);

          if (segStats.mtime.getTime() < oldestTime) {
            console.log(`  Deleting: ${topic}/${segment} (${(segStats.size / 1024).toFixed(2)} KB)`);

            if (!this.dryRun) {
              await fs.unlink(segmentPath);
            }

            deletedCount++;
            freedBytes += segStats.size;
          }
        }
      }

      console.log(`\n${'─'.repeat(70)}`);
      console.log(`Segments deleted: ${deletedCount}`);
      console.log(`Space freed:      ${(freedBytes / 1024 / 1024).toFixed(2)} MB`);
      if (this.dryRun) {
        console.log(`\nTo execute: node scripts/cleanupLogs.js --mode delete-old --days ${this.daysOld} --no-dry-run`);
      }
      console.log(`${'='.repeat(70)}\n`);
    } catch (err) {
      logger.error('Delete old segments failed', err);
    }
  }

  /**
   * Delete entire topic
   */
  async _deleteTopic() {
    if (!this.topic) {
      console.log('Error: --topic required for delete-topic mode');
      process.exit(1);
    }

    console.log(`\n${'='.repeat(70)}`);
    console.log(`⚠️  DELETE TOPIC: ${this.topic}`);
    console.log(`${this.dryRun ? '[DRY RUN]' : '[LIVE]'}`);
    console.log(`${'='.repeat(70)}\n`);

    const topicDir = join(this.topicsDir, this.topic);

    try {
      const stats = await fs.stat(topicDir);
      if (!stats.isDirectory()) {
        console.log(`Error: ${this.topic} is not a directory`);
        return;
      }

      const files = await fs.readdir(topicDir);
      let totalSize = 0;

      console.log(`Files to delete:`);
      for (const file of files) {
        const filePath = join(topicDir, file);
        const fileStats = await fs.stat(filePath);
        totalSize += fileStats.size;
        console.log(`  - ${file} (${(fileStats.size / 1024).toFixed(2)} KB)`);
      }

      console.log(`\nTotal space: ${(totalSize / 1024 / 1024).toFixed(2)} MB`);

      if (!this.dryRun) {
        for (const file of files) {
          await fs.unlink(join(topicDir, file));
        }
        await fs.rmdir(topicDir);
        console.log(`\n✅ Topic deleted`);
      } else {
        console.log(`\nTo execute: node scripts/cleanupLogs.js --mode delete-topic --topic ${this.topic} --no-dry-run`);
      }

      console.log(`${'='.repeat(70)}\n`);
    } catch (err) {
      logger.error('Delete topic failed', err);
    }
  }

  /**
   * Compact segments (future: implement log compaction)
   */
  async _compactSegments() {
    console.log(`\n${'='.repeat(70)}`);
    console.log('🔧 COMPACT SEGMENTS');
    console.log(`${'='.repeat(70)}\n`);

    console.log('Log compaction is not yet implemented.');
    console.log('This would remove duplicate keys and old values.');
  }

  /**
   * Purge all data
   */
  async _purgeAll() {
    console.log(`\n${'='.repeat(70)}`);
    console.log('💣 PURGE ALL DATA');
    console.log(`${this.dryRun ? '[DRY RUN]' : '[LIVE]'}`);
    console.log(`${'='.repeat(70)}\n`);

    console.log('⚠️  WARNING: This will delete ALL topic data');

    if (!this.dryRun) {
      try {
        const topics = await fs.readdir(this.topicsDir);

        for (const topic of topics) {
          const topicDir = join(this.topicsDir, topic);
          const stats = await fs.stat(topicDir);

          if (!stats.isDirectory()) continue;

          const files = await fs.readdir(topicDir);
          for (const file of files) {
            await fs.unlink(join(topicDir, file));
          }
          await fs.rmdir(topicDir);
        }

        console.log('✅ All data purged');
      } catch (err) {
        logger.error('Purge failed', err);
      }
    } else {
      console.log('\nTo execute: node scripts/cleanupLogs.js --mode purge-all --no-dry-run');
    }

    console.log(`${'='.repeat(70)}\n`);
  }
}

/**
 * Parse command-line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'list',
    dataDir: './data',
    daysOld: 7,
    dryRun: true
  };

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace('--', '').replace(/-/g, '');
    const value = args[i + 1];

    if (key === 'mode') options.mode = value;
    if (key === 'datadir') options.dataDir = value;
    if (key === 'daysold') options.daysOld = parseInt(value, 10);
    if (key === 'topic') options.topic = value;
    if (key === 'nodryrun') options.dryRun = false;
  }

  return options;
}

/**
 * Main
 */
async function main() {
  const options = parseArgs();
  const cleanup = new LogCleanup(options);
  await cleanup.run();
}

main().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});
