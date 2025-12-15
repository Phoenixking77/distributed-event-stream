'use strict';

import { Client } from './client.js';
import { TcpClient } from './connection/tcpClient.js';

export {
  Client,
  TcpClient
};

export const createClient = (options) => new Client(options);