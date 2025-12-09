'use strict';

const MAGIC = 0xda7a;
const PROTOCOL_VERSION = 1;

const OPCODES = Object.freeze({
  PUBLISH: 0x01,
  SUBSCRIBE: 0x02,
  ACK: 0x03,
  HEARTBEAT: 0x04,
  REPLICATION_PACKET: 0x05
});

const OPCODE_NAMES = Object.freeze(
  Object.keys(OPCODES).reduce((acc, key) => {
    acc[OPCODES[key]] = key;
    return acc;
  }, {})
);

const HEADER_LENGTH = 2 + 1 + 1 + 8 + 4;
const FOOTER_LENGTH = 4;
const MIN_PACKET_LENGTH = HEADER_LENGTH + FOOTER_LENGTH;
const MAX_PAYLOAD_LENGTH = 16 * 1024 * 1024;

function opcodeFromName(name) {
  const value = OPCODES[name];
  if (typeof value !== 'number') {
    throw new Error(`Unknown opcode name: ${name}`);
  }
  return value;
}

function opcodeNameFromValue(value) {
  return OPCODE_NAMES[value] || 'UNKNOWN';
}

module.exports = {
  MAGIC,
  PROTOCOL_VERSION,
  OPCODES,
  OPCODE_NAMES,
  HEADER_LENGTH,
  FOOTER_LENGTH,
  MIN_PACKET_LENGTH,
  MAX_PAYLOAD_LENGTH,
  opcodeFromName,
  opcodeNameFromValue
};