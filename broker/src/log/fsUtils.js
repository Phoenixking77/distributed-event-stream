'use strict';

import {
  existsSync,
  mkdirSync,
  readdirSync,
  statSync,
  openSync,
  closeSync,
  writeSync,
  fsyncSync,
  readSync,
  fstatSync,
  renameSync,
  unlinkSync
} from 'fs';
import { join, dirname } from 'path';

function ensureDir(dirPath) {
  if (!existsSync(dirPath)) {
    mkdirSync(dirPath, { recursive: true });
  }
}

function listLogSegments(topicDir) {
  if (!existsSync(topicDir)) {
    return [];
  }

  const files = readdirSync(topicDir);
  const segments = [];

  for (const file of files) {
    const match = /^segment-(\d+)\.log$/.exec(file);
    if (match) {
      const segmentId = parseInt(match[1], 10);
      const fullPath = join(topicDir, file);
      const stat = statSync(fullPath);
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
  ensureDir(dirname(segmentPath));
  const fd = openSync(segmentPath, 'a+');
  return fd;
}

function openSegmentForRead(segmentPath) {
  if (!existsSync(segmentPath)) {
    throw new Error(`Segment does not exist: ${segmentPath}`);
  }
  const fd = openSync(segmentPath, 'r');
  return fd;
}

function closeFile(fd) {
  if (typeof fd === 'number') {
    closeSync(fd);
  }
}

function writeToFile(fd, buffer) {
  const written = writeSync(fd, buffer);
  if (written !== buffer.length) {
    throw new Error(`Partial write: expected ${buffer.length}, wrote ${written}`);
  }
  return written;
}

function fsyncFile(fd) {
  fsyncSync(fd);
}

function readFromFile(fd, buffer, offset, length, position) {
  const bytesRead = readSync(fd, buffer, offset, length, position);
  return bytesRead;
}

function getFileSize(fd) {
  const stat = fstatSync(fd);
  return stat.size;
}

function renameFile(oldPath, newPath) {
  renameSync(oldPath, newPath);
}

function deleteFile(filePath) {
  if (existsSync(filePath)) {
    unlinkSync(filePath);
  }
}

export {
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