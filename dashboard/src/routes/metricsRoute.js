'use strict';

/**
 * metricsRoute provides thin helpers that can be used by
 * the dashboard HTTP server. Keeping them as pure
 * functions makes them easy to test in isolation.
 */

async function getNodeMetrics(controllerClient) {
  return controllerClient.getJson('/metrics');
}

async function getClusterState(controllerClient) {
  const [cluster, stats, metrics] = await Promise.all([
    controllerClient.getJson('/cluster'),
    controllerClient.getJson('/cluster/stats'),
    controllerClient.getJson('/metrics')
  ]);

  return {
    cluster,
    stats,
    metrics,
    generatedAt: Date.now()
  };
}

module.exports = {
  getNodeMetrics,
  getClusterState
};