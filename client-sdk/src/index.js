'use strict';

import { Client } from './client.js';
import { TcpClient } from './connection/tcpClient.js';
import { ReconnectionManager } from './connection/reconnect.js';
import { MessageEncoder, MessageDecoder } from './protocol/messageEncoder.js';
import { BatchPublisher } from './batchPublisher.js';
import { ConsumerGroupManager } from './consumerGroupManager.js';
import { ClientPool } from './clientPool.js';
import { RetryPolicy, ErrorHandler } from './errorHandler.js';

export {
  Client,
  TcpClient,
  ReconnectionManager,
  MessageEncoder,
  MessageDecoder,
  BatchPublisher,
  ConsumerGroupManager,
  ClientPool,
  RetryPolicy,
  ErrorHandler
};

export const createClient = (options) => new Client(options);
export const createClientPool = (brokers, options) => new ClientPool(brokers, options);
export const createBatchPublisher = (client, options) => new BatchPublisher(client, options);
export const createConsumerGroup = (client, options) => new ConsumerGroupManager(client, options);