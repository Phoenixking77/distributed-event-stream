'use strict';

import { Client } from './client';
import { TcpClient } from './connection/tcpClient';

export default {
  Client,
  TcpClient,
  createClient: (options) => new Client(options)
};