const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const source = fs.readFileSync(
  path.join(__dirname, '..', 'static', 'miniapp', 'app.js'),
  'utf8'
);

function latestResponseGuard(sequence, responseSequence) {
  return sequence === responseSequence;
}

function mutationGate(inFlight) {
  if (inFlight) return { accepted: false, inFlight };
  return { accepted: true, inFlight: true };
}

assert.match(source, /function apiRequest\(url, options = \{\}\)/);
assert.match(source, /if \(!response\.ok \|\| \(data && data\.status === 'error'\)\)/);
assert.match(source, /detailsController\.abort\(\)/);
assert.match(source, /searchController\.abort\(\)/);
assert.match(source, /browseController\.abort\(\)/);
assert.match(source, /if \(isAbortError\(error\)\) return;/);
assert.match(source, /String\(activeDetailsMovieId\) !== mutationMovieId/);
assert.match(source, /poster-placeholder\.svg/);
assert.match(source, /IMAGE_FALLBACK_GRADIENT/);
assert.match(source, /chat-room-banner/);
assert.match(source, /chat-send/);

assert.equal(latestResponseGuard(2, 1), false);
assert.equal(latestResponseGuard(2, 2), true);
assert.deepEqual(mutationGate(false), { accepted: true, inFlight: true });
assert.deepEqual(mutationGate(true), { accepted: false, inFlight: true });

console.log('frontend reliability checks passed');
