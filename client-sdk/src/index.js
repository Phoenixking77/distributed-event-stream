'use strict';

const { Client } = require('./client');
const { TcpClient } = require('./connection/tcpClient');

module.exports = {
  Client,
  TcpClient,
  createClient: (options) => new Client(options)
};