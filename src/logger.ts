import crypto from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';

export interface StructuredLogEntry {
  ts: string;
  level: 'info' | 'warn' | 'error';
  event: string;
  request_id?: string;
  [key: string]: unknown;
}

const REDACTED_KEYS = new Set([
  'authorization',
  'cookie',
  'password',
  'token',
  'api_key',
  'apikey',
  'secret',
  'database_url',
  'database_pooler_url',
  'init_data',
  'x-telegram-init-data',
  'x-guest-token',
]);

function sanitizeMeta(meta: Record<string, unknown>): Record<string, unknown> {
  const clean: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(meta)) {
    if (REDACTED_KEYS.has(key.toLowerCase())) {
      clean[key] = '[redacted]';
    } else if (value instanceof Error) {
      clean[key] = { name: value.name, message: value.message };
    } else {
      clean[key] = value;
    }
  }
  return clean;
}

export function logEvent(
  level: 'info' | 'warn' | 'error',
  event: string,
  meta: Record<string, unknown> = {},
): void {
  const entry: StructuredLogEntry = {
    ts: new Date().toISOString(),
    level,
    event,
    ...sanitizeMeta(meta),
  };
  const line = JSON.stringify(entry);
  if (level === 'error') {
    console.error(line);
  } else if (level === 'warn') {
    console.warn(line);
  } else {
    console.log(line);
  }
}

export function attachRequestContext(req: Request, res: Response, next: NextFunction): void {
  const incoming = String(req.header('X-Request-Id') || '').trim();
  const requestId = /^[A-Za-z0-9._-]{8,64}$/.test(incoming)
    ? incoming
    : crypto.randomBytes(8).toString('hex');

  res.locals.requestId = requestId;
  res.setHeader('X-Request-Id', requestId);

  const startedAt = Date.now();
  res.on('finish', () => {
    const durationMs = Date.now() - startedAt;
    const isStatic = req.path.startsWith('/static/');
    if (isStatic && res.statusCode < 400) return;

    const level = res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info';
    logEvent(level, 'http_request', {
      request_id: requestId,
      method: req.method,
      path: req.path,
      status: res.statusCode,
      duration_ms: durationMs,
    });
  });

  next();
}
