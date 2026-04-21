'use strict';

import zlib from 'zlib';
import { promisify } from 'util';

const gunzip = promisify(zlib.gunzip);
const inflate = promisify(zlib.inflate);

class MessageDecoder {
  
  static async decode(frame) {
    if (!Buffer.isBuffer(frame) || frame.length < 9) {
      throw new Error('Invalid frame: too small');
    }

    let offset = 0;

    const compressionType = frame.readUInt8(offset);
    offset += 1;

    const keyLen = frame.readUInt32BE(offset);
    offset += 4;

    if (offset + keyLen > frame.length) {
      throw new Error('Invalid frame: key length out of bounds');
    }

    const key =
      keyLen > 0 ? frame.toString('utf8', offset, offset + keyLen) : null;
    offset += keyLen;

    const payloadLen = frame.readUInt32BE(offset);
    offset += 4;

    if (offset + payloadLen !== frame.length) {
      throw new Error('Invalid frame: payload length mismatch');
    }

    const payload = frame.subarray(offset, offset + payloadLen);

    const compression = MessageDecoder._getCompressionName(compressionType);

    let decompressed = payload;
    if (compressionType !== 0) {
      decompressed = await MessageDecoder._decompress(payload, compression);
    }

    let message;
    try {
      message = JSON.parse(decompressed.toString('utf8'));
    } catch {
      message = decompressed.toString('utf8');
    }

    return {
      message,
      key,
      compression
    };
  }

  
  static async decodeBatch(batch) {
    if (!Buffer.isBuffer(batch) || batch.length < 4) {
      throw new Error('Invalid batch: too small');
    }

    let offset = 0;
    const count = batch.readUInt32BE(offset);
    offset += 4;

    const messages = [];

    for (let i = 0; i < count; i++) {
      if (offset + 4 > batch.length) {
        throw new Error(`Invalid batch: message ${i} length marker missing`);
      }

      const msgLen = batch.readUInt32BE(offset);
      offset += 4;

      if (offset + msgLen > batch.length) {
        throw new Error(`Invalid batch: message ${i} out of bounds`);
      }

      const frame = batch.subarray(offset, offset + msgLen);
      offset += msgLen;

      messages.push(await MessageDecoder.decode(frame));
    }

    return messages;
  }

  static async _decompress(payload, compression) {
    switch (compression) {
      case 'gzip':
        return gunzip(payload);
      case 'deflate':
        return inflate(payload);
      default:
        return payload;
    }
  }

  static _getCompressionName(code) {
    return {
      0: 'none',
      1: 'gzip',
      2: 'deflate',
      3: 'snappy'
    }[code] || 'unknown';
  }
}

export { MessageDecoder };
