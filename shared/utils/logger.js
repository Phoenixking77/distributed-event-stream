'use strict';

const LEVELS = Object.freeze({
  trace: 10,
  debug: 20,
  info: 30,
  warn: 40,
  error: 50,
  fatal: 60
});

const LEVEL_NAMES = Object.freeze(
  Object.keys(LEVELS).reduce((acc, key) => {
    acc[LEVELS[key]] = key;
    return acc;
  }, {})
);

function resolveLevel(envLevel) {
  if (!envLevel) return LEVELS.info;
  const normalized = String(envLevel).toLowerCase();
  const value = LEVELS[normalized];
  return typeof value === 'number' ? value : LEVELS.info;
}

class Logger {
  constructor(options) {
    const opts = options || {};
    this.level = resolveLevel(opts.level || process.env.LOG_LEVEL);
    this.name = opts.name || null;
    this.stream = opts.stream || process.stdout;
    this.errorStream = opts.errorStream || process.stderr;
  }

  child(bindings) {
    const base = Object.assign({}, bindings || {});
    const name = base.name || this.name;
    delete base.name;

    return new Logger({
      level: LEVEL_NAMES[this.level],
      name,
      stream: this.stream,
      errorStream: this.errorStream,
      bindings: base
    });
  }

  log(levelName, msg, context) {
    const level = LEVELS[levelName];
    if (level < this.level) {
      return;
    }

    const record = {
      ts: new Date().toISOString(),
      level: levelName,
      msg,
      pid: process.pid
    };

    if (this.name) {
      record.logger = this.name;
    }

    if (context && typeof context === 'object') {
      Object.assign(record, context);
    }

    const line = JSON.stringify(record);

    if (level >= LEVELS.error) {
      this.errorStream.write(line + '\n');
    } else {
      this.stream.write(line + '\n');
    }
  }

  trace(msg, context) {
    this.log('trace', msg, context);
  }

  debug(msg, context) {
    this.log('debug', msg, context);
  }

  info(msg, context) {
    this.log('info', msg, context);
  }

  warn(msg, context) {
    this.log('warn', msg, context);
  }

  error(msg, context) {
    this.log('error', context instanceof Error ? { err: serializeError(context), msg } : context || { err: msg });
  }

  fatal(msg, context) {
    this.log('fatal', context instanceof Error ? { err: serializeError(context), msg } : context || { err: msg });
  }
}

function serializeError(err) {
  if (!err || typeof err !== 'object') {
    return { message: String(err) };
  }

  const base = {
    message: err.message,
    name: err.name,
    stack: err.stack
  };

  for (const key of Object.keys(err)) {
    if (key === 'message' || key === 'name' || key === 'stack') continue;
    base[key] = err[key];
  }

  return base;
}

const defaultLogger = new Logger({ name: 'root' });

export {
  Logger,
  LEVELS,
  defaultLogger
};