import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { resolveDataMode } from '../src/contracts.ts';
import {
  clearRateLimitBuckets,
  createRateLimiter,
  deriveIsolatedGuestId,
  isOriginAllowed,
  requireMutationIdentity,
  resolveIdentityFromRequest,
  verifyTelegramInitData,
} from '../src/security.ts';
import { escapeTelegramHtml, formatGlobalChatTelegramText } from '../src/telegramRelay.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '..');

function makeMockReq(headers = {}, method = 'GET', reqPath = '/api/test') {
  const lower = Object.fromEntries(
    Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]),
  );
  return {
    method,
    path: reqPath,
    ip: '203.0.113.10',
    socket: { remoteAddress: '203.0.113.10' },
    header(name) {
      return lower[name.toLowerCase()];
    },
  };
}

function makeMockRes() {
  const headers = {};
  const res = {
    statusCode: 200,
    body: null,
    locals: { requestId: 'test-req-1234' },
    headers,
    setHeader(name, value) {
      headers[name.toLowerCase()] = value;
    },
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };
  return res;
}

function buildSignedTelegramInitData(userObj, botToken, authDateSec) {
  const params = new URLSearchParams();
  params.set('auth_date', String(authDateSec));
  params.set('query_id', 'AAHdF6IQAAAAAN0XohDhrOrc');
  params.set('user', JSON.stringify(userObj));
  const entries = [...params.entries()].sort(([a], [b]) => a.localeCompare(b));
  const checkString = entries.map(([k, v]) => `${k}=${v}`).join('\n');
  const secret = crypto.createHmac('sha256', 'WebAppData').update(botToken).digest();
  const hash = crypto.createHmac('sha256', secret).update(checkString).digest('hex');
  params.set('hash', hash);
  return params.toString();
}

test('guest identity isolates each client token and never falls back to shared user 1001', () => {
  const anon = resolveIdentityFromRequest(makeMockReq({}), '');
  assert.equal(anon.id, 0);
  assert.equal(anon.auth_mode, 'anonymous');
  assert.equal(anon.can_mutate, false);

  const tokenA = 'guest-token-alpha-1234567890';
  const tokenB = 'guest-token-beta-9876543210';
  const guestA = resolveIdentityFromRequest(makeMockReq({ 'X-Guest-Token': tokenA }), '');
  const guestB = resolveIdentityFromRequest(makeMockReq({ 'X-Guest-Token': tokenB }), '');

  assert.equal(guestA.auth_mode, 'guest');
  assert.equal(guestB.auth_mode, 'guest');
  assert.ok(guestA.id < 0 && guestB.id < 0);
  assert.notEqual(guestA.id, guestB.id);
  assert.equal(guestA.id, deriveIsolatedGuestId(tokenA));
});

test('requireMutationIdentity rejects anonymous callers with 401 AUTH_REQUIRED', () => {
  const req = makeMockReq({}, 'POST', '/api/global-chat');
  const res = makeMockRes();
  const identity = requireMutationIdentity(req, res, 'bot-secret');
  assert.equal(identity, null);
  assert.equal(res.statusCode, 401);
  assert.equal(res.body.code, 'AUTH_REQUIRED');
  assert.equal(res.body.request_id, 'test-req-1234');
});

test('verifyTelegramInitData accepts valid HMAC signature and rejects tampered or expired payloads', () => {
  const botToken = '123456:TEST_BOT_TOKEN_FOR_UNIT_TESTS';
  const nowSec = Math.floor(Date.now() / 1000);
  const validInit = buildSignedTelegramInitData(
    { id: 424242, username: 'cinema_fan', first_name: 'Amit' },
    botToken,
    nowSec,
  );
  const verified = verifyTelegramInitData(validInit, botToken);
  assert.ok(verified);
  assert.equal(verified.id, 424242);
  assert.equal(verified.is_guest, false);

  const tampered = validInit.replace('cinema_fan', 'attacker');
  assert.equal(verifyTelegramInitData(tampered, botToken), null);

  const expired = buildSignedTelegramInitData(
    { id: 424242, username: 'cinema_fan', first_name: 'Amit' },
    botToken,
    nowSec - 200_000,
  );
  assert.equal(verifyTelegramInitData(expired, botToken), null);
});

test('sliding-window rate limiter blocks excess requests with 429 and Retry-After', () => {
  clearRateLimitBuckets();
  const prevDisable = process.env.DISABLE_RATE_LIMIT;
  delete process.env.DISABLE_RATE_LIMIT;
  try {
    const limiter = createRateLimiter({
      bucketName: 'unit_test_bucket',
      windowMs: 60_000,
      maxRequests: 2,
    });
    let nextCalls = 0;
    const req = makeMockReq({ 'X-Forwarded-For': '198.51.100.44' }, 'POST', '/api/global-chat');

    limiter(req, makeMockRes(), () => { nextCalls += 1; });
    limiter(req, makeMockRes(), () => { nextCalls += 1; });
    assert.equal(nextCalls, 2);

    const blockedRes = makeMockRes();
    limiter(req, blockedRes, () => { nextCalls += 1; });
    assert.equal(nextCalls, 2);
    assert.equal(blockedRes.statusCode, 429);
    assert.equal(blockedRes.body.code, 'RATE_LIMITED');
    assert.ok(Number(blockedRes.headers['retry-after']) >= 1);
  } finally {
    if (prevDisable !== undefined) process.env.DISABLE_RATE_LIMIT = prevDisable;
  }
});

test('CORS origin allowlist blocks untrusted origins and permits configured hosts', () => {
  const allowlist = new Set(['https://flimfybox-bot-yht0.onrender.com']);
  assert.equal(isOriginAllowed('https://flimfybox-bot-yht0.onrender.com', undefined, allowlist), true);
  assert.equal(isOriginAllowed('https://evil-phishing.example', 'flimfybox.app', allowlist), false);
  assert.equal(isOriginAllowed(undefined, undefined, allowlist), true);
});

test('Global Chat Telegram relay safely escapes HTML entities', () => {
  assert.equal(escapeTelegramHtml('<script>alert("x") & y</script>'), '&lt;script&gt;alert("x") &amp; y&lt;/script&gt;');
  const formatted = formatGlobalChatTelegramText(
    { id: 1, username: 'amit_k', first_name: 'Amit <Admin>' },
    'Check <b>Dune</b> & Interstellar',
  );
  assert.ok(formatted.includes('Amit &lt;Admin&gt;'));
  assert.ok(formatted.includes('Check &lt;b&gt;Dune&lt;/b&gt; &amp; Interstellar'));
});

test('data mode cleanly separates production and demo modes', () => {
  const prev = process.env.APP_DATA_MODE;
  try {
    delete process.env.APP_DATA_MODE;
    assert.equal(resolveDataMode(true), 'production');
    assert.equal(resolveDataMode(false), 'demo');

    process.env.APP_DATA_MODE = 'demo';
    assert.equal(resolveDataMode(true), 'demo');
  } finally {
    if (prev === undefined) delete process.env.APP_DATA_MODE;
    else process.env.APP_DATA_MODE = prev;
  }
});

test('server.ts uses shared PostgreSQL temp_links, /readyz, and avoids innerHTML in watch page', () => {
  const src = fs.readFileSync(path.join(ROOT, 'server.ts'), 'utf8');
  assert.ok(src.includes('persistDownloadTokenInDb'));
  assert.ok(src.includes("app.get('/readyz'"));
  assert.ok(src.includes('relayGlobalChatToTelegram'));
  assert.ok(!src.includes('document.body.innerHTML'));
});
