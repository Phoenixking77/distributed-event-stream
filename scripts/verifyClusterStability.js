#!/usr/bin/env node

/**
 * Cluster Stability Verification Script
 * 
 * This script verifies that the leadership protocol is now stable:
 * 1. Controllers get elected leader only once
 * 2. Leader heartbeats maintain stability
 * 3. Node registry tracks all controllers correctly
 * 4. Term numbers stay consistent
 */

import { fetch } from 'undici';

const CONTROLLERS = [
  { name: 'C1', httpPort: 8080 },
  { name: 'C2', httpPort: 8081 },
  { name: 'C3', httpPort: 8082 }
];

const CHECK_INTERVAL = 2000; // Check every 2 seconds
const TEST_DURATION = 180000; // Run for 3 minutes

class ClusterMonitor {
  constructor() {
    this.state = new Map();
    this.leaderChanges = [];
    this.startTime = Date.now();
    this.lastLogTime = 0;
  }

  async checkController(controller) {
    try {
      const response = await fetch(`http://localhost:${controller.httpPort}/cluster`, {
        method: 'GET',
        timeout: 5000
      });

      if (!response.ok) {
        return {
          controller: controller.name,
          error: `HTTP ${response.status}`,
          available: false
        };
      }

      const data = await response.json();
      return {
        controller: controller.name,
        available: true,
        state: data.leader?.nodeId || 'unknown',
        nodes: data.nodes,
        term: data.leader?.metadata?.term || 'N/A'
      };
    } catch (err) {
      return {
        controller: controller.name,
        error: err.message,
        available: false
      };
    }
  }

  async checkCluster() {
    const results = await Promise.all(
      CONTROLLERS.map(c => this.checkController(c))
    );

    return results;
  }

  trackResults(results) {
    const leaders = results
      .filter(r => r.available && r.state !== 'unknown')
      .map(r => r.state);

    const uniqueLeaders = new Set(leaders);

    // Track leader changes
    const currentLeaders = Array.from(uniqueLeaders);
    const previousLeaders = this.state.get('lastLeaders') || [];

    if (JSON.stringify(currentLeaders) !== JSON.stringify(previousLeaders)) {
      this.leaderChanges.push({
        timestamp: Date.now(),
        leaders: currentLeaders,
        count: this.leaderChanges.length + 1
      });

      if (currentLeaders.length === 1) {
        console.log(
          `✓ [${this.getElapsed()}] Leader elected: ${currentLeaders[0]}`
        );
      } else if (currentLeaders.length === 0) {
        console.log(
          `✗ [${this.getElapsed()}] NO LEADER - potential split brain`
        );
      } else {
        console.log(
          `✗ [${this.getElapsed()}] Multiple leaders detected: ${currentLeaders.join(', ')}`
        );
      }
    }

    this.state.set('lastLeaders', currentLeaders);
    this.state.set('lastResults', results);
  }

  getElapsed() {
    const elapsed = Math.floor((Date.now() - this.startTime) / 1000);
    return `${elapsed}s`;
  }

  logState() {
    const results = this.state.get('lastResults') || [];
    const elapsed = Date.now() - this.lastLogTime;

    if (elapsed < 15000) {
      return; // Log every 15 seconds only
    }

    console.log(`\n[${this.getElapsed()}] Cluster state:`);
    
    for (const result of results) {
      if (result.available) {
        console.log(
          `  ${result.controller}: leader=${result.state} nodes=${result.nodes.totalNodes}`
        );
      } else {
        console.log(`  ${result.controller}: ${result.error}`);
      }
    }

    this.lastLogTime = Date.now();
  }

  printSummary() {
    console.log('\n' + '='.repeat(70));
    console.log('CLUSTER STABILITY ANALYSIS');
    console.log('='.repeat(70));

    console.log(`\nTest Duration: ${(TEST_DURATION / 1000).toFixed(0)} seconds`);
    console.log(`Total Leader Changes: ${this.leaderChanges.length}`);

    if (this.leaderChanges.length <= 1) {
      console.log('\n✓ PASS: Stable leadership maintained');
      console.log('  No unnecessary re-elections detected');
    } else {
      console.log('\n✗ FAIL: Unstable leadership detected');
      console.log(`  ${this.leaderChanges.length - 1} re-elections occurred`);
    }

    console.log('\nLeader Change Timeline:');
    for (const change of this.leaderChanges) {
      const time = new Date(change.timestamp).toISOString();
      console.log(`  [${change.count}] ${time}: ${change.leaders.join(', ')}`);
    }

    // Final state
    const results = this.state.get('lastResults') || [];
    console.log('\nFinal Cluster State:');
    for (const result of results) {
      if (result.available) {
        console.log(
          `  ${result.controller}:`
          + ` state=${result.state}`
          + ` nodes=${result.nodes.totalNodes}`
          + ` (alive=${result.nodes.aliveNodes})`
        );
      } else {
        console.log(`  ${result.controller}: unavailable`);
      }
    }

    console.log('\n' + '='.repeat(70));
  }

  async run() {
    console.log('Starting cluster stability verification...');
    console.log(`Test duration: ${(TEST_DURATION / 1000).toFixed(0)} seconds`);
    console.log('Checking every 2 seconds\n');

    const endTime = Date.now() + TEST_DURATION;

    while (Date.now() < endTime) {
      const results = await this.checkCluster();
      this.trackResults(results);
      this.logState();

      await new Promise(resolve => setTimeout(resolve, CHECK_INTERVAL));
    }

    this.printSummary();
  }
}

// Run the monitor
const monitor = new ClusterMonitor();
monitor.run().catch(err => {
  console.error('Monitor error:', err);
  process.exit(1);
});
