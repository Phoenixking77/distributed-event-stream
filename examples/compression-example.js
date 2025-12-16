#!/usr/bin/env node
'use strict';

/**
 * Example: Message compression
 * Demonstrates encoding/decoding with compression support
 */

import { MessageEncoder, MessageDecoder } from '../client-sdk/src/index.js';
import { Logger } from '../shared/utils/logger.js';

const logger = new Logger({ name: 'compression-example' });

async function main() {
  logger.info('Message Compression Examples\n');

  // Example 1: Simple message compression
  logger.info('=== Example 1: Simple Message Encoding ===');
  const simpleMessage = {
    userId: '12345',
    action: 'login',
    timestamp: Date.now()
  };

  const encoded1 = await MessageEncoder.encode(simpleMessage, {
    compression: 'none',
    key: 'user-action-1'
  });

  logger.info('Encoded message', {
    originalSize: JSON.stringify(simpleMessage).length + ' bytes',
    encodedSize: encoded1.length + ' bytes'
  });

  const decoded1 = await MessageDecoder.decode(encoded1);
  logger.info('Decoded message', decoded1);

  // Example 2: Large message with gzip compression
  logger.info('\n=== Example 2: Large Message with Gzip Compression ===');
  const largeMessage = {
    id: '67890',
    data: 'x'.repeat(10000),  // 10KB of data
    metadata: {
      version: '1.0',
      source: 'api',
      timestamp: Date.now()
    }
  };

  const encodedUncompressed = await MessageEncoder.encode(largeMessage, {
    compression: 'none'
  });

  const encodedCompressed = await MessageEncoder.encode(largeMessage, {
    compression: 'gzip'
  });

  const compressionRatio = (1 - encodedCompressed.length / encodedUncompressed.length) * 100;

  logger.info('Large message compression', {
    uncompressed: encodedUncompressed.length + ' bytes',
    gzipCompressed: encodedCompressed.length + ' bytes',
    compressionRatio: `${compressionRatio.toFixed(2)}%`
  });

  // Example 3: Batch encoding/decoding
  logger.info('\n=== Example 3: Batch Encoding/Decoding ===');
  const messages = [];
  for (let i = 0; i < 10; i++) {
    messages.push({
      id: i,
      timestamp: Date.now(),
      value: Math.random(),
      tags: ['tag1', 'tag2', 'tag3']
    });
  }

  const batch = await MessageEncoder.encodeBatch(messages, {
    compression: 'gzip'
  });

  logger.info('Batch encoding', {
    messageCount: messages.length,
    originalSize: JSON.stringify(messages).length + ' bytes',
    batchSize: batch.length + ' bytes'
  });

  const decodedBatch = await MessageDecoder.decodeBatch(batch);
  logger.info('Batch decoding', {
    decodedMessageCount: decodedBatch.length,
    firstMessage: decodedBatch[0].message,
    compression: decodedBatch[0].compression
  });

  // Example 4: Compression comparison
  logger.info('\n=== Example 4: Compression Algorithm Comparison ===');
  const testMessage = {
    data: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. '.repeat(100)
  };

  const algorithms = ['none', 'gzip', 'deflate'];
  const results = [];

  for (const algo of algorithms) {
    const encoded = await MessageEncoder.encode(testMessage, {
      compression: algo
    });
    results.push({
      algorithm: algo,
      size: encoded.length + ' bytes'
    });
  }

  logger.info('Compression algorithm comparison:');
  for (const result of results) {
    logger.info(`  ${result.algorithm}: ${result.size}`);
  }

  logger.info('\n✓ All compression examples completed');
}

main().catch(err => {
  logger.error('Error in compression example', err);
  process.exit(1);
});
