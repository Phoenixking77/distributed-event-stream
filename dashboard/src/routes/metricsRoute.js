'use strict';



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