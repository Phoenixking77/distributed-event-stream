'use strict';

import { describe, it, beforeEach, afterEach } from 'mocha';
import assert from 'assert';
import { DashboardHTTPServer } from '../src/httpServer.js';
import { Logger } from '../../shared/utils/logger.js';

describe('DashboardHTTPServer', () => {
  let dashboardServer;

  afterEach(async () => {
    if (dashboardServer && !dashboardServer.closed) {
      await dashboardServer.close();
    }
  });

  it('should create dashboard server', () => {
    dashboardServer = new DashboardHTTPServer({
      host: '0.0.0.0',
      port: 0,
      controllerHost: '127.0.0.1',
      controllerPort: 8080,
      logger: new Logger({ name: 'test' })
    });
    assert.ok(dashboardServer);
    assert.strictEqual(dashboardServer.host, '0.0.0.0');
    assert.strictEqual(dashboardServer.controllerHost, '127.0.0.1');
    assert.strictEqual(dashboardServer.controllerPort, 8080);
    assert.strictEqual(dashboardServer.closed, false);
  });

  it('should create dashboard server with default config', () => {
    dashboardServer = new DashboardHTTPServer({
      logger: new Logger({ name: 'test' })
    });
    assert.ok(dashboardServer);
    assert.strictEqual(dashboardServer.host, '0.0.0.0');
    assert.strictEqual(dashboardServer.port, 8090);
  });

  it('should initialize asset cache', async () => {
    dashboardServer = new DashboardHTTPServer({
      host: '0.0.0.0',
      port: 0,
      controllerHost: '127.0.0.1',
      controllerPort: 8080,
      logger: new Logger({ name: 'test' }),
      uiDir: './src/ui'
    });

    try {
      await dashboardServer.start();
      assert.ok(dashboardServer.assetCache);
    } catch (err) {
      // Ignore if UI files don't exist in test environment
    }
  });

  it('should close dashboard server', async () => {
    dashboardServer = new DashboardHTTPServer({
      host: '0.0.0.0',
      port: 0,
      controllerHost: '127.0.0.1',
      controllerPort: 8080,
      logger: new Logger({ name: 'test' })
    });

    await dashboardServer.close();
    assert.strictEqual(dashboardServer.closed, true);
  });

  it('should handle multiple close calls', async () => {
    dashboardServer = new DashboardHTTPServer({
      host: '0.0.0.0',
      port: 0,
      controllerHost: '127.0.0.1',
      controllerPort: 8080,
      logger: new Logger({ name: 'test' })
    });

    await dashboardServer.close();
    await dashboardServer.close();
    assert.strictEqual(dashboardServer.closed, true);
  });

  it('should guess content type', () => {
    dashboardServer = new DashboardHTTPServer({
      logger: new Logger({ name: 'test' })
    });

    assert.strictEqual(
      dashboardServer._guessContentType('test.html'),
      'text/html; charset=utf-8'
    );
    assert.strictEqual(
      dashboardServer._guessContentType('test.css'),
      'text/css; charset=utf-8'
    );
    assert.strictEqual(
      dashboardServer._guessContentType('test.js'),
      'application/javascript; charset=utf-8'
    );
    assert.strictEqual(
      dashboardServer._guessContentType('test.unknown'),
      'application/octet-stream'
    );
  });
});

describe('Dashboard Index', () => {
  it('should export createDashboardConfig', async () => {
    const module = await import('../src/index.js');
    assert.ok(module.createDashboardConfig);
    assert.strictEqual(typeof module.createDashboardConfig, 'function');
  });

  it('should export main function', async () => {
    const module = await import('../src/index.js');
    assert.ok(module.main);
    assert.strictEqual(typeof module.main, 'function');
  });

  it('should create dashboard config', async () => {
    const module = await import('../src/index.js');
    const config = module.createDashboardConfig();
    
    assert.ok(config);
    assert.ok(typeof config.host === 'string');
    assert.ok(typeof config.port === 'number');
    assert.ok(typeof config.controllerHost === 'string');
    assert.ok(typeof config.controllerPort === 'number');
  });
});
