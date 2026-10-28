const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const source = fs.readFileSync(
    path.join(__dirname, '..', 'static', 'miniapp', 'app.js'),
    'utf8'
);

assert.match(source, /function normalizeTrailerKey\(value\)/);
assert.match(source, /window\.playCurrentTrailer = async function\(\)/);
assert.match(source, /fetchWithTimeout\(`\/api\/movie\/\$\{encodeURIComponent\(movieId\)\}`\)/);
assert.match(source, /const fetchedKey = normalizeTrailerKey\(details\?\.trailer_key\)/);
assert.match(source, /playTrailer\(fetchedKey\)/);

const fn = source.match(/function normalizeTrailerKey\(value\) \{[\s\S]*?\n        \}/)[0];
const normalize = new Function(`${fn}; return normalizeTrailerKey;`)();
assert.equal(normalize('NOT_FOUND'), '');
assert.equal(normalize('bad-key'), '');
assert.equal(normalize('AbCdEf12345'), 'AbCdEf12345');
assert.equal(normalize('https://youtu.be/AbCdEf12345?t=5'), 'AbCdEf12345');
