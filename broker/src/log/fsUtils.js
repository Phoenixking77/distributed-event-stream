'use strict';

const fs = require('fs');
const path = require('path');

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function listLogSegments(topicDir) {
  if (!fs.existsSync(topicDir)) {
    return [];
  }

  const files = fs.readdirSync(topicDir);
  const segments = [];

  for (const file of files) {
    const match = /^segment-(\d+)\.log$/.exec(file);
    if (match) {
      const segmentId = parseInt(match[1], 10);
      const fullPath = path.join(topicDir, file);
      const stat = fs.statSync(fullPath);
      segments.push({
        id: segmentId,
        path: fullPath,
        size: stat.size
      });
    }
  }

  segments.sort((a, b) => a.id - b.id);
  return segments;
}

function openSegmentForAppend(segmentPath) {
  ensureDir(path.dirname(segmentPath));
  const fd = fs.openSync(segmentPath, 'a+');
  return fd;
}

function openSegmentForRead(segmentPath) {
  if (!fs.existsSync(segmentPath)) {
    throw new Error(`Segment does not exist: ${segmentPath}`);
  }
  const fd = fs.openSync(segmentPath, 'r');
  return fd;
}

function closeFile(fd) {
  if (typeof fd === 'number') {
    fs.closeSync(fd);
  }
}

function writeToFile(fd, buffer) {
  const written = fs.writeSync(fd, buffer);
  if (written !== buffer.length) {
    throw new Error(`Partial write: expected ${buffer.length}, wrote ${written}`);
  }
  return written;
}

function fsyncFile(fd) {
  fs.fsyncSync(fd);
}

function readFromFile(fd, buffer, offset, length, position) {
  const bytesRead = fs.readSync(fd, buffer, offset, length, position);
  return bytesRead;
}

function getFileSize(fd) {
  const stat = fs.fstatSync(fd);
  return stat.size;
}

function renameFile(oldPath, newPath) {
  fs.renameSync(oldPath, newPath);
}

function deleteFile(filePath) {
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
}

module.exports = {
  ensureDir,
  listLogSegments,
  openSegmentForAppend,
  openSegmentForRead,
  closeFile,
  writeToFile,
  fsyncFile,
  readFromFile,
  getFileSize,
  renameFile,
  deleteFile
};