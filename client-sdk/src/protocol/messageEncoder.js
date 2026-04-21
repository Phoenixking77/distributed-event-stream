'use strict';

import zlib from 'zlib';
import { promisify } from 'util';

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);
const deflate = promisify(zlib.deflate);
const inflate = promisify(zlib.inflate);


class MessageEncoder {
  
  static async encode(message, options) {
    const opts = options || {};
    const compression = opts.compression || 'none';
    
    let payload;
    if (Buffer.isBuffer(message)) {
      payload = message;
    } else if (typeof message === 'string') {
      payload = Buffer.from(message, 'utf8');
    } else {
      payload = Buffer.from(JSON.stringify(message), 'utf8');
    }

    let compressed = payload;
    let compressionType = 0; 

    if (compression !== 'none') {
      compressed = await MessageEncoder._compress(payload, compression);
      compressionType = MessageEncoder._getCompressionCode(compression);
    }

    
    const keyBuffer = opts.key ? Buffer.from(opts.key, 'utf8') : Buffer.alloc(0);
    const keyLen = keyBuffer.length;
    
    const frame = Buffer.allocUnsafe(1 + 4 + keyLen + 4 + compressed.length);
    let offset = 0;
    
    frame.writeUInt8(compressionType, offset);
    offset += 1;
    
    frame.writeUInt32BE(keyLen, offset);
    offset += 4;
    
    if (keyLen > 0) {
      keyBuffer.copy(frame, offset);
      offset += keyLen;
    }
    
    frame.writeUInt32BE(compressed.length, offset);
    offset += 4;
    
    compressed.copy(frame, offset);
    
    return frame;
  }

  
  static async encodeBatch(messages, options) {
    const encoded = await Promise.all(
      messages.map(msg => MessageEncoder.encode(msg, options))
    );

    
    const totalSize = 4 + encoded.reduce((sum, buf) => sum + 4 + buf.length, 0);
    const batch = Buffer.allocUnsafe(totalSize);
    
    let offset = 0;
    batch.writeUInt32BE(messages.length, offset);
    offset += 4;
    
    for (const buf of encoded) {
      batch.writeUInt32BE(buf.length, offset);
      offset += 4;
      buf.copy(batch, offset);
      offset += buf.length;
    }
    
    return batch;
  }

  static async _compress(payload, compression) {
    switch (compression.toLowerCase()) {
      case 'gzip':
        return gzip(payload);
      case 'deflate':
        return deflate(payload);
      case 'snappy':
        
        return payload;
      default:
        return payload;
    }
  }

  static _getCompressionCode(compression) {
    const codes = {
      'none': 0,
      'gzip': 1,
      'deflate': 2,
      'snappy': 3
    };
    return codes[compression.toLowerCase()] || 0;
  }

  static _getCompressionName(code) {
    const names = {
      0: 'none',
      1: 'gzip',
      2: 'deflate',
      3: 'snappy'
    };
    return names[code] || 'unknown';
  }
}


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
    
    if (keyLen > 0) {
      if (offset + keyLen > frame.length) {
        throw new Error('Invalid frame: key length exceeds frame');
      }
    }
    
    const key = keyLen > 0 ? frame.toString('utf8', offset, offset + keyLen) : null;
    offset += keyLen;
    
    const payloadLen = frame.readUInt32BE(offset);
    offset += 4;
    
    if (offset + payloadLen !== frame.length) {
      throw new Error('Invalid frame: payload length mismatch');
    }
    
    const payload = frame.subarray(offset, offset + payloadLen);
    const compression = MessageEncoder._getCompressionName(compressionType);
    
    let decompressed = payload;
    let wasCompressed = false;
    
    if (compressionType !== 0) {
      decompressed = await MessageDecoder._decompress(payload, compression);
      wasCompressed = true;
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
      compression,
      decompressed: wasCompressed
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
        throw new Error(`Invalid batch: message ${i} length marker out of bounds`);
      }
      
      const msgLen = batch.readUInt32BE(offset);
      offset += 4;
      
      if (offset + msgLen > batch.length) {
        throw new Error(`Invalid batch: message ${i} data out of bounds`);
      }
      
      const msgFrame = batch.subarray(offset, offset + msgLen);
      offset += msgLen;
      
      const decoded = await MessageDecoder.decode(msgFrame);
      messages.push(decoded);
    }
    
    return messages;
  }

  static async _decompress(payload, compression) {
    switch (compression.toLowerCase()) {
      case 'gzip':
        return gunzip(payload);
      case 'deflate':
        return inflate(payload);
      case 'snappy':
        
        return payload;
      default:
        return payload;
    }
  }
}

export { MessageEncoder, MessageDecoder };
