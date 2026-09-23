const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const source = fs.readFileSync(
    path.join(__dirname, '..', 'static', 'miniapp', 'app.js'),
    'utf8'
);

assert.match(source, /function normalizeTrailerKey\(value\)/);
assert.match(source, /window\.playCurrentTrailer = async function\(\)/);
assert.match(source, /fetch\(`\/api\/movie\/\$\{encodeURIComponent\(movieId\)\}`\)/);
assert.match(source, /const fetchedKey = normalizeTrailerKey\(details\?\.trailer_key\)/);
assert.match(source, /playTrailer\(fetchedKey\)/);
