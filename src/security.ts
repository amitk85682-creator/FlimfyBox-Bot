import crypto from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';
import { sendApiError } from './contracts.ts';
import { logEvent } from './logger.ts';
import type { AppUser } from './store.ts';

export type AuthMode = 'telegram' | 'guest' | 'anonymous';

export interface ResolvedIdentity extends AppUser {
  auth_mode: AuthMode;
  can_mutate: boolean;
}

interface RateBucket {
  count: number;
  resetAt: number;
}

export interface RateLimitOptions {
  bucketName: string;
  windowMs: number;
  maxRequests: number;
  keyFn?: (req: Request) => string;
}

const rateBuckets = new Map<string, RateBucket>();

export function clearRateLimitBuckets(): void {
  rateBuckets.clear();
}

export function getClientIp(req: Request): string {
  const forwarded = String(req.header('X-Forwarded-For') || '').split(',')[0].trim();
  return forwarded || req.ip || req.socket.remoteAddress || 'unknown';
}

export function createRateLimiter(options: RateLimitOptions) {
  return (req: Request, res: Response, next: NextFunction): void => {
    if (process.env.DISABLE_RATE_LIMIT === 'true') {
      next();
      return;
    }

    const now = Date.now();
    const rawKey = options.keyFn ? options.keyFn(req) : getClientIp(req);
    const key = `${options.bucketName}:${rawKey}`;

    if (rateBuckets.size > 5000) {
      for (const [entryKey, entry] of rateBuckets.entries()) {
        if (entry.resetAt <= now) rateBuckets.delete(entryKey);
      }
    }

    let bucket = rateBuckets.get(key);
    if (!bucket || bucket.resetAt <= now) {
      bucket = { count: 0, resetAt: now + options.windowMs };
      rateBuckets.set(key, bucket);
    }

    bucket.count += 1;
    const remaining = Math.max(0, options.maxRequests - bucket.count);
    res.setHeader('X-RateLimit-Limit', String(options.maxRequests));
    res.setHeader('X-RateLimit-Remaining', String(remaining));
    res.setHeader('X-RateLimit-Reset', String(Math.ceil(bucket.resetAt / 1000)));

    if (bucket.count > options.maxRequests) {
      const retryAfterSec = Math.max(1, Math.ceil((bucket.resetAt - now) / 1000));
      res.setHeader('Retry-After', String(retryAfterSec));
      logEvent('warn', 'rate_limit_exceeded', {
        request_id: res.locals.requestId,
        bucket: options.bucketName,
        path: req.path,
        retry_after_sec: retryAfterSec,
      });
      sendApiError(
        res,
        429,
        'RATE_LIMITED',
        `Too many requests. Please wait ${retryAfterSec}s before trying again.`,
      );
      return;
    }

    next();
  };
}

export function verifyTelegramInitData(initData: string, botToken: string): AppUser | null {
  if (!initData || !botToken) return null;
  const params = new URLSearchParams(initData);
  const receivedHash = params.get('hash') || '';
  const authDateRaw = params.get('auth_date') || '';
  const authDate = Number(authDateRaw);
  const maxAge = Number(process.env.TELEGRAM_AUTH_MAX_AGE_SECONDS || '86400');
  if (!receivedHash || !/^[0-9a-f]{64}$/i.test(receivedHash) || !Number.isFinite(authDate) || maxAge <= 0) {
    return null;
  }
  const nowSec = Math.floor(Date.now() / 1000);
  if (authDate > nowSec + 60 || nowSec - authDate > maxAge) return null;

  params.delete('hash');
  const entries = [...params.entries()].sort(([a], [b]) => a.localeCompare(b));
  const dataCheckString = entries.map(([k, v]) => `${k}=${v}`).join('\n');
  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(botToken).digest();
  const expectedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');
  const expectedBuf = Buffer.from(expectedHash, 'hex');
  const receivedBuf = Buffer.from(receivedHash, 'hex');
  if (expectedBuf.length !== receivedBuf.length || !crypto.timingSafeEqual(expectedBuf, receivedBuf)) {
    return null;
  }
  try {
    const parsed = JSON.parse(params.get('user') || '{}') as Record<string, unknown>;
    const id = Number(parsed.id);
    if (!Number.isFinite(id) || id <= 0) return null;
    return {
      id,
      username: String(parsed.username || ''),
      first_name: String(parsed.first_name || 'Member'),
      photo_url: typeof parsed.photo_url === 'string' ? parsed.photo_url : '',
      is_guest: false,
    };
  } catch {
    return null;
  }
}

export function deriveIsolatedGuestId(guestToken: string): number {
  const salt = process.env.GUEST_TOKEN_SALT || 'flimfybox-guest-v2';
  const digest = crypto.createHmac('sha256', salt).update(guestToken).digest();
  // Negative 31-bit integer space strictly disjoint from positive Telegram user IDs
  const numeric = Number(digest.readBigUInt64BE(0) & BigInt(0x7ffffffe)) + 1;
  return -numeric;
}

export function resolveIdentityFromRequest(req: Request, botToken: string): ResolvedIdentity {
  const initData = String(req.header('X-Telegram-Init-Data') || '').trim();
  const tgUser = verifyTelegramInitData(initData, botToken);
  if (tgUser) {
    return {
      ...tgUser,
      auth_mode: 'telegram',
      can_mutate: true,
    };
  }

  const guestToken = String(req.header('X-Guest-Token') || '').trim();
  if (/^[A-Za-z0-9_-]{16,128}$/.test(guestToken)) {
    return {
      id: deriveIsolatedGuestId(guestToken),
      username: 'guest',
      first_name: 'Guest',
      photo_url: '',
      is_guest: true,
      auth_mode: 'guest',
      can_mutate: process.env.REQUIRE_TELEGRAM_AUTH !== 'true',
    };
  }

  // Never collapse unauthenticated browsers onto a shared mutable user ID (previously 1001).
  // Read-only anonymous identity has id = 0; mutations require Telegram auth or a client X-Guest-Token.
  return {
    id: 0,
    username: 'anonymous',
    first_name: 'Guest',
    photo_url: '',
    is_guest: true,
    auth_mode: 'anonymous',
    can_mutate: false,
  };
}

export function requireMutationIdentity(
  req: Request,
  res: Response,
  botToken: string,
  options: { requireTelegram?: boolean } = {},
): ResolvedIdentity | null {
  const identity = resolveIdentityFromRequest(req, botToken);
  if (options.requireTelegram && identity.auth_mode !== 'telegram') {
    sendApiError(res, 401, 'TELEGRAM_AUTH_REQUIRED', 'Sign in inside Telegram to perform this action.');
    return null;
  }
  if (!identity.can_mutate || identity.id === 0) {
    sendApiError(
      res,
      401,
      'AUTH_REQUIRED',
      'Valid Telegram session or guest session token (X-Guest-Token) is required.',
    );
    return null;
  }
  return identity;
}

export function buildAllowedOrigins(): Set<string> {
  const allowed = new Set<string>();
  const rawList = [
    process.env.ALLOWED_ORIGINS || '',
    process.env.PUBLIC_URL || '',
    process.env.WEB_APP_URL || '',
  ];
  for (const raw of rawList) {
    for (const part of raw.split(',')) {
      const trimmed = part.trim();
      if (!trimmed) continue;
      try {
        allowed.add(new URL(trimmed).origin);
      } catch {
        // Ignore malformed entries
      }
    }
  }
  return allowed;
}

export function isOriginAllowed(origin: string | undefined, hostHeader: string | undefined, allowlist: Set<string>): boolean {
  // Same-origin / non-browser requests often omit Origin
  if (!origin) return true;
  if (allowlist.has(origin)) return true;
  try {
    const parsed = new URL(origin);
    if (hostHeader && parsed.host === hostHeader) return true;
    if (parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1') return true;
    if (parsed.hostname.endsWith('.run.app') || parsed.hostname.endsWith('.onrender.com')) return true;
  } catch {
    return false;
  }
  return false;
}

export function securityHeadersMiddleware(_req: Request, res: Response, next: NextFunction): void {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  next();
}
