'use strict';

import { createServer, request } from 'http';
import { promises } from 'fs';
import { join, extname, dirname } from 'path';
import { parse } from 'url';
import { fileURLToPath } from 'url';

import { Logger } from '../../shared/utils/logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);


class DashboardHTTPServer {
  constructor(options) {
    const opts = options || {};

    this.logger = opts.logger || new Logger({ name: 'dashboard' });

    this.host = opts.host || '0.0.0.0';
    this.port = opts.port || 8090;

    this.controllerHost = opts.controllerHost || '127.0.0.1';
    this.controllerPort = opts.controllerPort || 8080;

    this.server = null;
    this.closed = false;

    const uiDir = opts.uiDir || join(__dirname, 'ui');
    this.assets = {
      '/': join(uiDir, 'index.html'),
      '/index.html': join(uiDir, 'index.html'),
      '/dashboard.css': join(uiDir, 'dashboard.css'),
      '/metrics.js': join(uiDir, 'metrics.js')
    };

    this.assetCache = new Map();
  }

  async start() {
    if (this.server) {
      throw new Error('Dashboard HTTP server already started');
    }

    await this._warmAssetCache();

    return new Promise((resolve, reject) => {
      this.server = createServer((req, res) => {
        this._handleRequest(req, res);
      });

      this.server.on('error', (err) => {
        this.logger.error('Dashboard HTTP server error', err);
        if (!this.closed) {
          reject(err);
        }
      });

      this.server.listen(this.port, this.host, () => {
        this.logger.info('Dashboard HTTP server started', {
          host: this.host,
          port: this.port,
          controllerHost: this.controllerHost,
          controllerPort: this.controllerPort
        });
        resolve();
      });
    });
  }

  async close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    return new Promise((resolve) => {
      if (!this.server) {
        resolve();
        return;
      }

      this.logger.info('Closing dashboard HTTP server');
      this.server.close(() => {
        this.logger.info('Dashboard HTTP server closed');
        resolve();
      });
    });
  }

  async _warmAssetCache() {
    for (const [routePath, filePath] of Object.entries(this.assets)) {
      try {
        const content = await promises.readFile(filePath);
        const contentType = this._guessContentType(filePath);
        this.assetCache.set(routePath, { content, contentType });
      } catch (err) {
        // Missing UI files should not crash the server, but should be logged.
        this.logger.warn('Failed to preload UI asset', {
          routePath,
          filePath,
          error: err.message
        });
      }
    }
  }

  _handleRequest(req, res) {
    if (this.closed) {
      this._sendJson(res, 503, { error: 'Dashboard is shutting down' });
      return;
    }

    const parsed = parse(req.url, true);
    const pathname = parsed.pathname || '/';
    const method = req.method || 'GET';

    this.logger.debug('Dashboard request', { method, path: pathname });

    if (method === 'GET' && this.assets[pathname]) {
      this._serveStatic(pathname, res);
      return;
    }

    if (method === 'OPTIONS') {
      this._handleCORS(res);
      return;
    }

    if (method !== 'GET') {
      this._sendJson(res, 405, { error: 'Method not allowed' });
      return;
    }

    if (pathname === '/health') {
      this._proxySimple('/health', res);
      return;
    }

    if (pathname === '/cluster/state') {
      this._handleClusterState(res);
      return;
    }

    if (pathname === '/metrics/node') {
      this._proxySimple('/metrics', res);
      return;
    }

    if (pathname === '/topics') {
      this._proxySimple('/topics', res);
      return;
    }

    if (pathname.startsWith('/metrics/topic/')) {
      const topic = decodeURIComponent(pathname.substring('/metrics/topic/'.length));
      this._proxySimple(`/topics/${encodeURIComponent(topic)}`, res);
      return;
    }

    if (pathname.startsWith('/topic/') && pathname.endsWith('/lag')) {
      const topic = decodeURIComponent(
        pathname.substring('/topic/'.length, pathname.length - '/lag'.length)
      );
      this._handleTopicLag(topic, res);
      return;
    }

    if (pathname === '/cluster/nodes') {
      this._proxySimple('/cluster/nodes', res);
      return;
    }

    this._sendJson(res, 404, { error: 'Not found' });
  }

  _serveStatic(routePath, res) {
    const cached = this.assetCache.get(routePath);
    if (!cached) {
      this._sendJson(res, 404, { error: 'Asset not found' });
      return;
    }

    res.writeHead(200, {
      'Content-Type': cached.contentType,
      'Content-Length': cached.content.length,
      'Cache-Control': 'no-cache',
      'X-Content-Type-Options': 'nosniff'
    });
    res.end(cached.content);
  }

  async _handleClusterState(res) {
    try {
      const [cluster, stats, metrics] = await Promise.all([
        this._httpGetJson('/cluster'),
        this._httpGetJson('/cluster/stats'),
        this._httpGetJson('/metrics')
      ]);

      const payload = {
        cluster,
        stats,
        metrics,
        generatedAt: Date.now()
      };

      this._sendJson(res, 200, payload);
    } catch (err) {
      this.logger.error('Failed to fetch cluster state for dashboard', err);
      this._sendJson(res, 502, { error: 'Failed to query controller API' });
    }
  }

  async _handleTopicLag(topic, res) {
    try {
      const topicDetail = await this._httpGetJson(`/topics/${encodeURIComponent(topic)}`);

      const leaderOffset = topicDetail.stats ? topicDetail.stats.latestOffset : 0;
      const replicationLag = topicDetail.stats && topicDetail.stats.replicationLag
        ? topicDetail.stats.replicationLag
        : {};

      const followers = (topicDetail.followers || []).map((follower) => {
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

      const payload = {
        topic: topicDetail.name,
        leader: topicDetail.leader,
        followers,
        isr: topicDetail.isr,
        hasSufficientISR: topicDetail.hasSufficientISR,
        stats: topicDetail.stats,
        generatedAt: Date.now()
      };

      this._sendJson(res, 200, payload);
    } catch (err) {
      this.logger.error('Failed to fetch topic lag for dashboard', {
        topic,
        error: err.message
      });
      this._sendJson(res, 502, { error: 'Failed to query controller for topic lag' });
    }
  }

  _handleCORS(res) {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    res.end();
  }

  _sendJson(res, statusCode, payload) {
    const body = JSON.stringify(payload, null, 2);
    res.writeHead(statusCode, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Content-Length': Buffer.byteLength(body)
    });
    res.end(body);
  }

  _guessContentType(filePath) {
    const ext = extname(filePath).toLowerCase();
    switch (ext) {
      case '.html':
        return 'text/html; charset=utf-8';
      case '.css':
        return 'text/css; charset=utf-8';
      case '.js':
        return 'application/javascript; charset=utf-8';
      default:
        return 'application/octet-stream';
    }
  }

  async _proxySimple(controllerPath, res) {
    try {
      const data = await this._httpGetJson(controllerPath);
      this._sendJson(res, 200, data);
    } catch (err) {
      this.logger.error('Dashboard proxy error', {
        path: controllerPath,
        error: err.message
      });
      this._sendJson(res, 502, { error: 'Failed to query controller API' });
    }
  }

  _httpGetJson(controllerPath) {
    const options = {
      host: this.controllerHost,
      port: this.controllerPort,
      path: controllerPath,
      method: 'GET',
      timeout: 3000,
      headers: {
        'Accept': 'application/json'
      }
    };

    return new Promise((resolve, reject) => {
      const req = request(options, (res) => {
        const chunks = [];

        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => {
          const buffer = Buffer.concat(chunks);

          if (res.statusCode && res.statusCode >= 400) {
            reject(new Error(`Controller responded with status ${res.statusCode}`));
            return;
          }

          try {
            const parsed = buffer.length ? JSON.parse(buffer.toString('utf8')) : {};
            resolve(parsed);
          } catch (err) {
            reject(err);
          }
        });
      });

      req.on('error', (err) => reject(err));
      req.on('timeout', () => {
        req.destroy(new Error('Controller request timeout'));
      });

      req.end();
    });
  }
}

export {
  DashboardHTTPServer
};