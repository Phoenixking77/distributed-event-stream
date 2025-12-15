'use strict';

import { readFileSync, existsSync } from 'fs';
import { join, dirname, isAbsolute } from 'path';

function loadJsonFile(filePath) {
  const content = readFileSync(filePath, 'utf8');
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
  const defaultFile = opts.defaultFile || join('broker', 'src', 'config', 'default.json');
  const env = opts.env || process.env.NODE_ENV || 'development';
  const envFile = join(
    dirname(defaultFile),
    `${env}.json`
  );

  const defaultPath = isAbsolute(defaultFile) ? defaultFile : join(baseDir, defaultFile);
  const envPath = isAbsolute(envFile) ? envFile : join(baseDir, envFile);

  let config = {};
  if (existsSync(defaultPath)) {
    config = loadJsonFile(defaultPath);
  }

  if (existsSync(envPath)) {
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

export { loadConfig };