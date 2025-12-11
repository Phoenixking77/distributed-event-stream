(function () {
  'use strict';

  const REFRESH_INTERVAL_MS = 5000;

  function $(id) {
    return document.getElementById(id);
  }

  async function fetchJson(path) {
    const res = await fetch(path, {
      headers: {
        'Accept': 'application/json'
      }
    });

    if (!res.ok) {
      throw new Error('Request failed: ' + res.status);
    }

    return res.json();
  }

  function formatUptime(ms) {
    if (!ms || ms < 0) return '-';
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) return days + 'd ' + (hours % 24) + 'h';
    if (hours > 0) return hours + 'h ' + (minutes % 60) + 'm';
    if (minutes > 0) return minutes + 'm ' + (seconds % 60) + 's';
    return seconds + 's';
  }

  function setHealth(status, hasQuorum, aliveNodes, leaderId, uptimeMs) {
    const el = $('health-status');
    const quorumEl = $('health-quorum');
    const aliveEl = $('health-alive-nodes');
    const leaderEl = $('health-leader');
    const uptimeEl = $('health-uptime');

    el.textContent = status;
    el.classList.remove('health-healthy', 'health-degraded', 'health-down', 'health-unknown');

    if (status === 'healthy') {
      el.classList.add('health-healthy');
    } else if (status === 'degraded') {
      el.classList.add('health-degraded');
    } else if (status === 'down') {
      el.classList.add('health-down');
    } else {
      el.classList.add('health-unknown');
    }

    quorumEl.textContent = hasQuorum ? 'Yes' : 'No';
    aliveEl.textContent = String(aliveNodes);
    leaderEl.textContent = leaderId || '-';
    uptimeEl.textContent = formatUptime(uptimeMs);
  }

  function renderNodes(nodes) {
    const tbody = $('nodes-table').querySelector('tbody');
    tbody.innerHTML = '';

    if (!nodes || nodes.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 4;
      td.textContent = 'No nodes registered';
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    nodes.forEach((node) => {
      const tr = document.createElement('tr');

      const role = node.isLeader ? 'leader' : 'follower';

      const lastHeartbeat = node.lastHeartbeat
        ? new Date(node.lastHeartbeat).toLocaleTimeString()
        : '-';

      tr.innerHTML = [
        '<td>' + node.nodeId + '</td>',
        '<td>' + node.state + '</td>',
        '<td>' + role + '</td>',
        '<td>' + lastHeartbeat + '</td>'
      ].join('');

      tbody.appendChild(tr);
    });
  }

  function renderTopics(topics) {
    const tbody = $('topics-table').querySelector('tbody');
    const select = $('topic-select');
    tbody.innerHTML = '';
    select.innerHTML = '';

    if (!topics || topics.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 7;
      td.textContent = 'No topics';
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    topics.forEach((topic) => {
      const tr = document.createElement('tr');

      const underReplicated = topic.underReplicated ? 'Yes' : 'No';

      tr.innerHTML = [
        '<td>' + topic.name + '</td>',
        '<td>' + topic.leader + '</td>',
        '<td>' + topic.followers + '</td>',
        '<td>' + topic.isr + '</td>',
        '<td>' + topic.messages + '</td>',
        '<td>' + topic.latestOffset + '</td>',
        '<td>' + underReplicated + '</td>'
      ].join('');

      tbody.appendChild(tr);

      const opt = document.createElement('option');
      opt.value = topic.name;
      opt.textContent = topic.name;
      select.appendChild(opt);
    });
  }

  function renderTopicLag(lag) {
    const tbody = $('topic-lag-table').querySelector('tbody');
    tbody.innerHTML = '';

    if (!lag || !lag.followers || lag.followers.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 5;
      td.textContent = 'No follower data for topic';
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    lag.followers.forEach((follower) => {
      const tr = document.createElement('tr');

      tr.innerHTML = [
        '<td>' + follower.nodeId + '</td>',
        '<td>' + follower.state + '</td>',
        '<td>' + (follower.offset != null ? follower.offset : '-') + '</td>',
        '<td>' + (follower.leaderOffset != null ? follower.leaderOffset : '-') + '</td>',
        '<td>' + (follower.lagMs != null ? follower.lagMs + ' ms' : '-') + '</td>'
      ].join('');

      tbody.appendChild(tr);
    });
  }

  async function refreshOnce() {
    try {
      const [clusterState, topics, nodes] = await Promise.all([
        fetchJson('/cluster/state'),
        fetchJson('/cluster/stats'),
        fetchJson('/cluster/nodes')
      ]);

      const cluster = clusterState.cluster || {};
      const nodeMetrics = clusterState.metrics && clusterState.metrics.heartbeat
        ? clusterState.metrics.heartbeat
        : null;

      const status = cluster.hasQuorum ? 'healthy' : 'degraded';
      const leaderNodeId = clusterState.cluster && clusterState.cluster.leader
        ? clusterState.cluster.leader.nodeId
        : null;

      const uptimeMs = nodeMetrics ? nodeMetrics.uptime : null;

      setHealth(status, cluster.hasQuorum, cluster.aliveNodes || 0, leaderNodeId, uptimeMs);

      renderNodes(nodes.nodes || []);
      renderTopics(topics.topics || []);

      const topicSelect = $('topic-select');
      const selectedTopic = topicSelect.value || (topics.topics && topics.topics[0] && topics.topics[0].name);

      if (selectedTopic) {
        const lag = await fetchJson('/topic/' + encodeURIComponent(selectedTopic) + '/lag');
        renderTopicLag(lag);
      }

      $('last-refresh').textContent = 'Last refresh: ' + new Date().toLocaleTimeString();
    } catch (err) {
      console.error('Failed to refresh dashboard', err);
      setHealth('down', false, 0, null, null);
      $('last-refresh').textContent = 'Last refresh failed: ' + new Date().toLocaleTimeString();
    }
  }

  function scheduleRefresh() {
    refreshOnce().finally(() => {
      setTimeout(scheduleRefresh, REFRESH_INTERVAL_MS);
    });
  }

  window.addEventListener('DOMContentLoaded', () => {
    const select = $('topic-select');
    select.addEventListener('change', async () => {
      const topic = select.value;
      if (!topic) return;
      try {
        const lag = await fetchJson('/topic/' + encodeURIComponent(topic) + '/lag');
        renderTopicLag(lag);
      } catch (err) {
        console.error('Failed to fetch topic lag', err);
      }
    });

    scheduleRefresh();
  });
}());