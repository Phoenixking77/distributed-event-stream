'use strict';

import { MAGIC, PROTOCOL_VERSION, OPCODES, OPCODE_NAMES, HEADER_LENGTH, FOOTER_LENGTH, MIN_PACKET_LENGTH, MAX_PAYLOAD_LENGTH } from './packetTypes';
import { crc32, readCrc32BE } from './checksum';

function encodePacket({ opcode, payload, timestamp }) {
  if (typeof opcode === 'string') {
    const code = OPCODES[opcode];
    if (typeof code !== 'number') {
      throw new Error(`Unknown opcode: ${opcode}`);
    }
    opcode = code;
  }

  let body;
  if (Buffer.isBuffer(payload)) {
    body = payload;
  } else if (typeof payload === 'string') {
    body = Buffer.from(payload, 'utf8');
  } else if (payload == null) {
    body = Buffer.alloc(0);
  } else {
    body = Buffer.from(JSON.stringify(payload), 'utf8');
  }

  if (body.length > MAX_PAYLOAD_LENGTH) {
    throw new Error(`Payload too large: ${body.length} bytes`);
  }

  const ts = BigInt(typeof timestamp === 'number' ? timestamp : Date.now());
  const totalLength = HEADER_LENGTH + body.length + FOOTER_LENGTH;
  const buffer = Buffer.allocUnsafe(totalLength);

  let offset = 0;
  buffer.writeUInt16BE(MAGIC, offset);
  offset += 2;

  buffer.writeUInt8(PROTOCOL_VERSION, offset);
  offset += 1;

  buffer.writeUInt8(opcode, offset);
  offset += 1;

  buffer.writeBigInt64BE(ts, offset);
  offset += 8;

  buffer.writeUInt32BE(body.length, offset);
  offset += 4;

  body.copy(buffer, offset);
  offset += body.length;

  const checksum = crc32(buffer, HEADER_LENGTH, body.length);
  buffer.writeUInt32BE(checksum, offset);

  return buffer;
}

function decodePacket(buffer) {
  if (!Buffer.isBuffer(buffer)) {
    throw new TypeError('decodePacket expects a Buffer');
  }
  if (buffer.length < MIN_PACKET_LENGTH) {
    throw new Error('Buffer too small for packet');
  }

  let offset = 0;

  const magic = buffer.readUInt16BE(offset);
  offset += 2;
  if (magic !== MAGIC) {
    throw new Error('Invalid magic');
  }

  const version = buffer.readUInt8(offset);
  offset += 1;
  if (version !== PROTOCOL_VERSION) {
    throw new Error(`Unsupported protocol version: ${version}`);
  }

  const opcode = buffer.readUInt8(offset);
  offset += 1;

  const timestamp = buffer.readBigInt64BE(offset);
  offset += 8;

  const payloadLength = buffer.readUInt32BE(offset);
  offset += 4;

  if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_LENGTH) {
    throw new Error(`Invalid payload length: ${payloadLength}`);
  }

  const expectedLength = HEADER_LENGTH + payloadLength + FOOTER_LENGTH;
  if (buffer.length !== expectedLength) {
    throw new Error('Buffer length does not match packet length');
  }

  const payloadStart = offset;
  const payloadEnd = payloadStart + payloadLength;
  const payload = buffer.slice(payloadStart, payloadEnd);

  const checksumOffset = payloadEnd;
  const expectedChecksum = readCrc32BE(buffer, checksumOffset);
  const computedChecksum = crc32(buffer, payloadStart, payloadLength);

  if (computedChecksum !== expectedChecksum) {
    throw new Error('Checksum mismatch');
  }

  return {
    opcode,
    opcodeName: OPCODES[opcode] ? OPCODE_NAMES[opcode] : 'UNKNOWN',
    timestamp: Number(timestamp),
    payload
  };
}

class PacketParser {
  constructor(options) {
    const opts = options || {};
    this.maxPayloadLength = typeof opts.maxPayloadLength === 'number' && opts.maxPayloadLength > 0
      ? opts.maxPayloadLength
      : MAX_PAYLOAD_LENGTH;
    this.buffer = Buffer.alloc(0);
    this.offset = 0;
    this.onPacket = typeof opts.onPacket === 'function' ? opts.onPacket : null;
    this.onError = typeof opts.onError === 'function' ? opts.onError : null;
  }

  push(chunk) {
    if (!Buffer.isBuffer(chunk)) {
      throw new TypeError('PacketParser.push expects a Buffer');
    }

    if (this.offset === 0 && this.buffer.length === 0) {
      this.buffer = chunk;
    } else if (this.offset === 0) {
      this.buffer = Buffer.concat([this.buffer, chunk], this.buffer.length + chunk.length);
    } else {
      const remaining = this.buffer.length - this.offset;
      const next = Buffer.allocUnsafe(remaining + chunk.length);
      this.buffer.copy(next, 0, this.offset);
      chunk.copy(next, remaining);
      this.buffer = next;
      this.offset = 0;
    }

    this._drain();
  }

  _emitPacket(packet) {
    if (this.onPacket) {
      this.onPacket(packet);
    }
  }

  _emitError(err) {
    if (this.onError) {
      this.onError(err);
    }
  }

  _drain() {
    while (true) {
      const available = this.buffer.length - this.offset;
      if (available < MIN_PACKET_LENGTH) {
        break;
      }

      const start = this.offset;
      const magic = this.buffer.readUInt16BE(start);
      if (magic !== MAGIC) {
        this._emitError(new Error('Invalid magic, dropping buffer'));
        this.buffer = Buffer.alloc(0);
        this.offset = 0;
        return;
      }

      const version = this.buffer.readUInt8(start + 2);
      if (version !== PROTOCOL_VERSION) {
        const err = new Error(`Unsupported protocol version: ${version}`);
        this._emitError(err);
        this.buffer = Buffer.alloc(0);
        this.offset = 0;
        return;
      }

      const payloadLength = this.buffer.readUInt32BE(start + HEADER_LENGTH - 4);
      if (payloadLength < 0 || payloadLength > this.maxPayloadLength) {
        const err = new Error(`Invalid payload length: ${payloadLength}`);
        this._emitError(err);
        this.buffer = Buffer.alloc(0);
        this.offset = 0;
        return;
      }

      const packetLength = HEADER_LENGTH + payloadLength + FOOTER_LENGTH;
      if (available < packetLength) {
        break;
      }

      const payloadStart = start + HEADER_LENGTH;
      const payloadEnd = payloadStart + payloadLength;
      const checksumOffset = payloadEnd;

      const expectedChecksum = readCrc32BE(this.buffer, checksumOffset);
      const computedChecksum = crc32(this.buffer, payloadStart, payloadLength);

      if (computedChecksum !== expectedChecksum) {
        const err = new Error('Checksum mismatch');
        this._emitError(err);
        this.offset += packetLength;
        continue;
      }

      const opcode = this.buffer.readUInt8(start + 3);
      const timestamp = this.buffer.readBigInt64BE(start + 4);
      const payload = this.buffer.slice(payloadStart, payloadEnd);

      this._emitPacket({
        opcode,
        opcodeName: OPCODES[opcode] ? OPCODE_NAMES[opcode] : 'UNKNOWN',
        timestamp: Number(timestamp),
        payload
      });

      this.offset += packetLength;
    }

    if (this.offset === this.buffer.length) {
      this.buffer = Buffer.alloc(0);
      this.offset = 0;
    } else if (this.offset > 0) {
      this.buffer = this.buffer.slice(this.offset);
      this.offset = 0;
    }
  }
}

export default {
  encodePacket,
  decodePacket,
  PacketParser
};