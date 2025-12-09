'use strict';

const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i += 1) {
    let c = i;
    for (let j = 0; j < 8; j += 1) {
      const mask = -(c & 1);
      c = (c >>> 1) ^ (0xedb88320 & mask);
    }
    table[i] = c >>> 0;
  }
  return table;
})();

function crc32(buffer, offset, length) {
  let start = typeof offset === 'number' ? offset : 0;
  let len = typeof length === 'number' ? length : buffer.length - start;

  if (start < 0 || len < 0 || start + len > buffer.length) {
    throw new RangeError('Invalid crc32 range');
  }

  let crc = 0xffffffff;
  const end = start + len;

  for (let i = start; i < end; i += 1) {
    const byte = buffer[i];
    crc = CRC32_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }

  return (crc ^ 0xffffffff) >>> 0;
}

function writeCrc32BE(buffer, value, offset) {
  buffer.writeUInt32BE(value >>> 0, offset);
}

function readCrc32BE(buffer, offset) {
  return buffer.readUInt32BE(offset) >>> 0;
}

module.exports = {
  crc32,
  writeCrc32BE,
  readCrc32BE
};