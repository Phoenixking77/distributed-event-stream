'use strict';

const fs = require('fs');
const path = require('path');

function loadJsonFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(content);
}

function deepMerge(target, source) {
  const result = Object.assign({}, target);
  if (!source || typeof source !== 'object') {
    return result;
  }

  for (const key of Object.keys(source)) {
    const value = source[key];
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      result[key] = deepMerge(result[key] || {}, value);
    } else {
      result[key] = value;
    }
  }

  return result;
}

function loadConfig(options) {
  const opts = options || {};
  const baseDir = opts.baseDir || process.cwd();
  const defaultFile = opts.defaultFile || path.join('broker', 'src', 'config', 'default.json');
  const env = opts.env || process.env.NODE_ENV || 'development';
  const envFile = path.join(
    path.dirname(defaultFile),
    `${env}.json`
  );

  const defaultPath = path.isAbsolute(defaultFile) ? defaultFile : path.join(baseDir, defaultFile);
  const envPath = path.isAbsolute(envFile) ? envFile : path.join(baseDir, envFile);

  let config = {};
  if (fs.existsSync(defaultPath)) {
    config = loadJsonFile(defaultPath);
  }

  if (fs.existsSync(envPath)) {
    config = deepMerge(config, loadJsonFile(envPath));
  }

  const envOverrides = extractEnvOverrides(config);
  return deepMerge(config, envOverrides);
}

function extractEnvOverrides(template, prefix) {
  const rootKey = prefix || '';
  const result = {};

  if (!template || typeof template !== 'object') {
    return result;
  }

  const stack = [{ obj: template, path: rootKey }];

  while (stack.length > 0) {
    const { obj, path: currentPath } = stack.pop();
    for (const key of Object.keys(obj)) {
      const value = obj[key];
      const newPath = currentPath ? `${currentPath}_${key}` : key;
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        stack.push({ obj: value, path: newPath });
      } else {
        const envKey = newPath.toUpperCase();
        if (Object.prototype.hasOwnProperty.call(process.env, envKey)) {
          resultPathSet(result, newPath.split('_'), parseEnvValue(process.env[envKey]));
        }
      }
    }
  }

  return result;
}

function resultPathSet(target, pathSegments, value) {
  let cursor = target;
  for (let i = 0; i < pathSegments.length - 1; i += 1) {
    const seg = pathSegments[i];
    if (!cursor[seg] || typeof cursor[seg] !== 'object') {
      cursor[seg] = {};
    }
    cursor = cursor[seg];
  }
  cursor[pathSegments[pathSegments.length - 1]] = value;
}

function parseEnvValue(raw) {
  const trimmed = raw.trim().toLowerCase();
  if (trimmed === 'true') return true;
  if (trimmed === 'false') return false;
  if (!Number.isNaN(Number(trimmed))) return Number(trimmed);
  return raw;
}

module.exports = {
  loadConfig
};