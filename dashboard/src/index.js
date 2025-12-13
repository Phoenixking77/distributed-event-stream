'use strict';

import { join } from 'path';
import { Logger } from '../../shared/utils/logger';
import { loadConfig } from '../../shared/utils/configLoader';
import { DashboardHTTPServer } from './httpServer';

function createDashboardConfig() {
  const baseConfig = loadConfig({});

  const dashboardConfig = {
    host: process.env.DASHBOARD_HOST || '0.0.0.0',
    port: Number(process.env.DASHBOARD_PORT) || 8090,
    controllerHost: process.env.CONTROLLER_HOST || '127.0.0.1',
    controllerPort: Number(process.env.CONTROLLER_HTTP_PORT) || 8080
  };

  if (baseConfig && baseConfig.dashboard) {
    if (typeof baseConfig.dashboard.host === 'string') {
      dashboardConfig.host = baseConfig.dashboard.host;
    }
    if (typeof baseConfig.dashboard.port === 'number') {
      dashboardConfig.port = baseConfig.dashboard.port;
    }
    if (typeof baseConfig.dashboard.controllerHost === 'string') {
      dashboardConfig.controllerHost = baseConfig.dashboard.controllerHost;
    }
    if (typeof baseConfig.dashboard.controllerPort === 'number') {
      dashboardConfig.controllerPort = baseConfig.dashboard.controllerPort;
    }
  }

  return dashboardConfig;
}

async function main() {
  const logger = new Logger({ name: 'dashboard-main' });
  const config = createDashboardConfig();

  const uiDir = join(__dirname, 'ui');

  const server = new DashboardHTTPServer({
    logger,
    host: config.host,
    port: config.port,
    controllerHost: config.controllerHost,
    controllerPort: config.controllerPort,
    uiDir
  });

  try {
    await server.start();
  } catch (err) {
    logger.error('Failed to start dashboard server', err);
    process.exit(1);
  }

  const shutdown = async () => {
    logger.info('Shutting down dashboard server');
    try {
      await server.close();
    } catch (err) {
      logger.error('Error during dashboard shutdown', err);
    }
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) {
  // eslint-disable-next-line no-floating-promises
  main();
}

export default {
  createDashboardConfig,
  main
};