'use strict';

async function getTopics(controllerClient) {
  return controllerClient.getJson('/topics');
}

async function getTopic(controllerClient, topicName) {
  if (!topicName || typeof topicName !== 'string') {
    throw new Error('topicName is required');
  }
  const encoded = encodeURIComponent(topicName);
  return controllerClient.getJson(`/topics/${encoded}`);
}

async function getTopicLag(controllerClient, topicName) {
  const detail = await getTopic(controllerClient, topicName);

  const leaderOffset = detail.stats ? detail.stats.latestOffset : 0;
  const replicationLag = detail.stats && detail.stats.replicationLag
    ? detail.stats.replicationLag
    : {};

  const followers = (detail.followers || []).map((follower) => {
    const lagInfo = replicationLag[follower.nodeId] || null;
    return {
      nodeId: follower.nodeId,
      state: follower.state,
      metadata: follower.metadata,
      lagMs: lagInfo ? lagInfo.lagMs : null,
      offset: lagInfo ? lagInfo.offset : null,
      leaderOffset
    };
  });

  return {
    topic: detail.name,
    leader: detail.leader,
    followers,
    isr: detail.isr,
    hasSufficientISR: detail.hasSufficientISR,
    stats: detail.stats,
    generatedAt: Date.now()
  };
}

module.exports = {
  getTopics,
  getTopic,
  getTopicLag
};