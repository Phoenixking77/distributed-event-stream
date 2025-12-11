'use strict';

async function getNodes(controllerClient) {
  const data = await controllerClient.getJson('/cluster/nodes');
  return data;
}

module.exports = {
  getNodes
};