'use strict';

import { Client } from '../client-sdk/src/client.js';
import { Logger } from '../shared/utils/logger.js';

/**
 * Production-grade benchmarking suite for distributed event stream
 * Measures:
 * - Throughput (messages/sec, bytes/sec)
 * - Latency (p50, p95, p99)
 * - Resource usage
 * - Replication lag
 * - Error rates
 * 
 * Usage:
 *   node scripts/benchmark.js --mode throughput --duration 30 --batch-size 1000
 *   node scripts/benchmark.js --mode latency --samples 10000
 *   node scripts/benchmark.js --mode sustained --duration 300 --connections 10
 */

const logger = new Logger({ name: 'benchmark' });

class Benchmark {
  constructor(options) {
    const opts = options || {};
    this.mode = opts.mode || 'throughput';
    this.duration = opts.duration || 30; // seconds
    this.topic = opts.topic || 'benchmark-topic';
    this.batchSize = opts.batchSize || 1000;
    this.messageSize = opts.messageSize || 1024; // bytes
    this.connections = opts.connections || 1;
    this.brokerHost = opts.brokerHost || 'localhost';
    this.brokerPort = opts.brokerPort || 9092;

    this.results = {
      mode: this.mode,
      startTime: null,
      endTime: null,
      durationMs: 0,
      published: 0,
      received: 0,
      errors: 0,
      messageSize: this.messageSize,
      latencies: [],
      throughput: {
        messagesPerSec: 0,
        bytesPerSec: 0,
        totalBytes: 0
      },
      latency: {
        min: Infinity,
        max: 0,
        avg: 0,
        p50: 0,
        p95: 0,
        p99: 0
      },
      cpuUsage: {},
      memoryUsage: {}
    };
  }

  /**
   * Run benchmark
   */
  async run() {
    logger.info('Starting benchmark', {
      mode: this.mode,
      duration: this.duration,
      topic: this.topic,
      batchSize: this.batchSize,
      messageSize: this.messageSize,
      connections: this.connections
    });

    this.results.startTime = Date.now();

    try {
      switch (this.mode) {
        case 'throughput':
          await this._runThroughputTest();
          break;
        case 'latency':
          await this._runLatencyTest();
          break;
        case 'sustained':
          await this._runSustainedTest();
          break;
        case 'subscribe':
          await this._runSubscribeTest();
          break;
        default:
          throw new Error(`Unknown benchmark mode: ${this.mode}`);
      }
    } catch (err) {
      logger.error('Benchmark failed', err);
      this.results.errors++;
    } finally {
      this.results.endTime = Date.now();
      this.results.durationMs = this.results.endTime - this.results.startTime;
      this._captureSystemMetrics();
      this._printResults();
    }
  }

  /**
   * Throughput test: Max messages/sec
   */
  async _runThroughputTest() {
    const clients = await this._createClients(this.connections);

    const testEndTime = Date.now() + (this.duration * 1000);
    let batchCount = 0;

    logger.info('Running throughput test', {
      clients: this.connections,
      duration: this.duration,
      batchSize: this.batchSize
    });

    while (Date.now() < testEndTime) {
      const batch = [];

      for (let i = 0; i < this.batchSize; i++) {
        const clientIdx = i % clients.length;
        const client = clients[clientIdx];

        const message = this._generateMessage();

        batch.push(
          client.publish(this.topic, message)
            .then(() => {
              this.results.published++;
            })
            .catch((err) => {
              this.results.errors++;
              logger.error('Publish failed', err);
            })
        );
      }

      await Promise.all(batch);
      batchCount++;

      if (batchCount % 10 === 0) {
        const elapsed = (Date.now() - this.results.startTime) / 1000;
        const throughput = (this.results.published / elapsed).toFixed(0);
        process.stdout.write(
          `\rPublished: ${this.results.published} msgs (${throughput} msg/s)`
        );
      }
    }

    process.stdout.write('\n');

    // Disconnect
    await Promise.all(clients.map(c => c.disconnect()));
  }

  /**
   * Latency test: Measure round-trip times
   */
  async _runLatencyTest() {
    const client = await this._createClient();
    const samples = 10000;

    logger.info('Running latency test', { samples });

    for (let i = 0; i < samples; i++) {
      const message = this._generateMessage();
      const startTime = process.hrtime.bigint();

      try {
        await client.publish(this.topic, message);
        const endTime = process.hrtime.bigint();
        const latencyMs = Number(endTime - startTime) / 1_000_000;

        this.results.latencies.push(latencyMs);
        this.results.published++;
      } catch (err) {
        this.results.errors++;
        logger.error('Publish failed', err);
      }

      if ((i + 1) % 1000 === 0) {
        process.stdout.write(`\rLatency samples: ${i + 1}/${samples}`);
      }
    }

    process.stdout.write('\n');

    await client.disconnect();

    // Calculate latency stats
    this._computeLatencyStats();
  }

  /**
   * Sustained load test: Multiple clients over longer duration
   */
  async _runSustainedTest() {
    const clients = await this._createClients(this.connections);
    const testEndTime = Date.now() + (this.duration * 1000);
    let publishCount = 0;

    logger.info('Running sustained load test', {
      clients: this.connections,
      duration: this.duration
    });

    // Publish loop
    const publishPromise = (async () => {
      while (Date.now() < testEndTime) {
        const promises = [];

        for (let i = 0; i < this.batchSize; i++) {
          const clientIdx = i % clients.length;
          const client = clients[clientIdx];
          const message = this._generateMessage();

          promises.push(
            client.publish(this.topic, message)
              .then(() => {
                this.results.published++;
              })
              .catch(() => {
                this.results.errors++;
              })
          );
        }

        await Promise.all(promises);
        publishCount++;

        if (publishCount % 5 === 0) {
          const elapsed = (Date.now() - this.results.startTime) / 1000;
          const throughput = (this.results.published / elapsed).toFixed(0);
          process.stdout.write(
            `\rPublished: ${this.results.published} msgs (${throughput} msg/s)`
          );
        }
      }
    })();

    // Subscribe loop (on separate connection)
    const subscriberClient = await this._createClient();
    let subscribeCount = 0;

    const subscribePromise = subscriberClient.subscribe(
      this.topic,
      (msg) => {
        this.results.received++;
      },
      { offset: 0 }
    ).catch(err => {
      logger.error('Subscribe failed', err);
    });

    await Promise.all([publishPromise, subscribePromise]);

    // Give subscriber time to catch up
    await new Promise(resolve => setTimeout(resolve, 2000));

    await Promise.all(
      clients.map(c => c.disconnect()).concat([subscriberClient.disconnect()])
    );

    process.stdout.write('\n');
  }

  /**
   * Subscribe performance test
   */
  async _runSubscribeTest() {
    // First, publish test messages
    const publishClient = await this._createClient();
    const messageCount = 10000;

    logger.info('Pre-populating topic with messages', { count: messageCount });

    for (let i = 0; i < messageCount; i++) {
      const message = this._generateMessage();
      await publishClient.publish(this.topic, message);

      if ((i + 1) % 1000 === 0) {
        process.stdout.write(`\rPublished: ${i + 1}/${messageCount}`);
      }
    }

    process.stdout.write('\n');
    await publishClient.disconnect();

    // Now test subscription throughput
    const clients = await this._createClients(this.connections);
    const testEndTime = Date.now() + (this.duration * 1000);

    logger.info('Running subscribe throughput test', {
      clients: this.connections,
      duration: this.duration,
      prePopulatedMessages: messageCount
    });

    const subscriptions = await Promise.all(
      clients.map((client, idx) =>
        client.subscribe(
          this.topic,
          (msg) => {
            this.results.received++;
          },
          { offset: Math.floor((messageCount / this.connections) * idx) }
        )
      )
    );

    // Let subscribers consume
    while (Date.now() < testEndTime) {
      process.stdout.write(
        `\rReceived: ${this.results.received} msgs (lag: ${messageCount - this.results.received})`
      );
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    process.stdout.write('\n');

    await Promise.all(clients.map(c => c.disconnect()));
  }

  /**
   * Create client
   */
  async _createClient() {
    const client = new Client({
      host: this.brokerHost,
      port: this.brokerPort,
      logger: logger.child({ component: 'client' })
    });

    await client.connect();
    return client;
  }

  /**
   * Create multiple clients
   */
  async _createClients(count) {
    const clients = [];

    for (let i = 0; i < count; i++) {
      const client = await this._createClient();
      clients.push(client);
    }

    return clients;
  }

  /**
   * Generate test message
   */
  _generateMessage() {
    const payload = Buffer.alloc(this.messageSize);
    payload.write(JSON.stringify({
      timestamp: Date.now(),
      random: Math.random(),
      seq: this.results.published
    }));

    return payload.toString('base64');
  }

  /**
   * Compute latency percentiles
   */
  _computeLatencyStats() {
    const sorted = this.results.latencies.sort((a, b) => a - b);

    this.results.latency.min = sorted[0];
    this.results.latency.max = sorted[sorted.length - 1];
    this.results.latency.avg = sorted.reduce((a, b) => a + b, 0) / sorted.length;
    this.results.latency.p50 = sorted[Math.floor(sorted.length * 0.50)];
    this.results.latency.p95 = sorted[Math.floor(sorted.length * 0.95)];
    this.results.latency.p99 = sorted[Math.floor(sorted.length * 0.99)];
  }

  /**
   * Capture system metrics
   */
  _captureSystemMetrics() {
    const usage = process.cpuUsage();
    const memUsage = process.memoryUsage();

    this.results.cpuUsage = {
      user: (usage.user / 1000).toFixed(2),
      system: (usage.system / 1000).toFixed(2)
    };

    this.results.memoryUsage = {
      rss: (memUsage.rss / 1024 / 1024).toFixed(2),
      heapUsed: (memUsage.heapUsed / 1024 / 1024).toFixed(2),
      heapTotal: (memUsage.heapTotal / 1024 / 1024).toFixed(2),
      external: (memUsage.external / 1024 / 1024).toFixed(2)
    };

    // Calculate throughput
    const durationSeconds = this.results.durationMs / 1000;
    this.results.throughput.messagesPerSec =
      (this.results.published / durationSeconds).toFixed(2);
    this.results.throughput.totalBytes = this.results.published * this.messageSize;
    this.results.throughput.bytesPerSec =
      (this.results.throughput.totalBytes / durationSeconds / 1024 / 1024).toFixed(2);
  }

  /**
   * Print results
   */
  _printResults() {
    console.log('\n' + '='.repeat(70));
    console.log(`BENCHMARK RESULTS: ${this.mode.toUpperCase()}`);
    console.log('='.repeat(70));

    console.log('\n📊 SUMMARY');
    console.log('-'.repeat(70));
    console.log(`Duration:          ${this.results.durationMs}ms (${(this.results.durationMs / 1000).toFixed(2)}s)`);
    console.log(`Messages Published: ${this.results.published}`);
    console.log(`Messages Received:  ${this.results.received}`);
    console.log(`Errors:             ${this.results.errors}`);

    if (this.results.throughput.messagesPerSec > 0) {
      console.log('\n🚀 THROUGHPUT');
      console.log('-'.repeat(70));
      console.log(`Messages/sec:    ${this.results.throughput.messagesPerSec} msg/s`);
      console.log(`Bytes/sec:       ${this.results.throughput.bytesPerSec} MB/s`);
      console.log(`Total data:      ${(this.results.throughput.totalBytes / 1024 / 1024).toFixed(2)} MB`);
    }

    if (this.results.latencies.length > 0) {
      console.log('\n⏱️  LATENCY');
      console.log('-'.repeat(70));
      console.log(`Min:             ${this.results.latency.min.toFixed(2)}ms`);
      console.log(`Max:             ${this.results.latency.max.toFixed(2)}ms`);
      console.log(`Average:         ${this.results.latency.avg.toFixed(2)}ms`);
      console.log(`p50:             ${this.results.latency.p50.toFixed(2)}ms`);
      console.log(`p95:             ${this.results.latency.p95.toFixed(2)}ms`);
      console.log(`p99:             ${this.results.latency.p99.toFixed(2)}ms`);
    }

    console.log('\n💾 RESOURCE USAGE');
    console.log('-'.repeat(70));
    console.log(`CPU User:        ${this.results.cpuUsage.user}ms`);
    console.log(`CPU System:      ${this.results.cpuUsage.system}ms`);
    console.log(`Heap Used:       ${this.results.memoryUsage.heapUsed}MB`);
    console.log(`Heap Total:      ${this.results.memoryUsage.heapTotal}MB`);
    console.log(`RSS:             ${this.results.memoryUsage.rss}MB`);

    console.log('\n' + '='.repeat(70) + '\n');
  }
}

/**
 * Parse command-line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    mode: 'throughput',
    duration: 30,
    topic: 'benchmark-topic',
    batchSize: 1000,
    messageSize: 1024,
    connections: 1,
    brokerHost: 'localhost',
    brokerPort: 9092
  };

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace('--', '').replace(/-/g, '');
    const value = args[i + 1];

    if (key === 'mode') options.mode = value;
    if (key === 'duration') options.duration = parseInt(value, 10);
    if (key === 'topic') options.topic = value;
    if (key === 'batchsize') options.batchSize = parseInt(value, 10);
    if (key === 'messagesize') options.messageSize = parseInt(value, 10);
    if (key === 'connections') options.connections = parseInt(value, 10);
    if (key === 'brokerhost') options.brokerHost = value;
    if (key === 'brokerport') options.brokerPort = parseInt(value, 10);
  }

  return options;
}

/**
 * Main
 */
async function main() {
  const options = parseArgs();

  logger.info('Benchmark Configuration', options);

  const benchmark = new Benchmark(options);
  await benchmark.run();

  process.exit(0);
}

main().catch((err) => {
  logger.error('Fatal error', err);
  process.exit(1);
});
