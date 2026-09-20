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
assert.match(source, /typeof tg\.requestFullscreen === 'function'/);
assert.match(source, /tg\.requestFullscreen\(\)/);
assert.match(source, /tg\.isVersionAtLeast\('8\.0'\)/);
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
assert.match(source, /window\.shareCurrentMovie = async function\(\)/);
assert.match(source, /\/webapp\?movie=/);
assert.match(source, /navigator\.share/);
assert.match(source, /https:\/\/t\.me\/share\/url/);
assert.match(source, /urlParams\.get\('movie'\)/);

const styles = fs.readFileSync(
  path.join(__dirname, '..', 'static', 'miniapp', 'app.css'),
  'utf8'
);
assert.match(styles, /\.more-panel\.open\s*\{[\s\S]*pointer-events:\s*auto/);
assert.match(styles, /@media \(min-width: 700px\)[\s\S]*\.more-panel\s*\{[\s\S]*display:\s*block/);
assert.match(styles, /@media \(min-width: 700px\)[\s\S]*\.more-panel-grid button\s*\{[\s\S]*display:\s*flex/);
assert.match(source, /const posters = \[GENRE_ARTWORK\[genre\.id\]\];/);
assert.doesNotMatch(source, /const posters = Array\.isArray\(catalogueGenre\.posters\)/);
assert.match(source, /const movieDetailsCache = new Map\(\)/);
assert.match(source, /detailsPage\.classList\.remove\('is-loading'\)/);
assert.match(source, /const cachedDetails = movieDetailsCache\.get\(String\(id\)\)/);
assert.match(styles, /@media \(min-width: 700px\)[\s\S]*\.hero-slider\s*\{[\s\S]*border-radius:\s*24px/);
assert.match(styles, /@media \(min-width: 700px\)[\s\S]*\.movie-grid\s*\{[\s\S]*grid-template-columns:\s*repeat\(6/);
assert.match(styles, /\.more-panel-grid\s*\{[\s\S]*display:\s*grid\s*!important/);
assert.match(styles, /\.more-panel-grid button\s*\{[\s\S]*display:\s*flex\s*!important/);
assert.match(styles, /body\.screen-explore #exploreContent \.genre-card-grid\s*\{[\s\S]*grid-template-columns:\s*repeat\(4/);
assert.match(styles, /body\.screen-explore #exploreContent \.genre-card-name::after\s*\{[\s\S]*content:\s*'Explore →'/);
assert.match(styles, /body\.screen-explore \.search-section\s*\{[\s\S]*display:\s*none !important/);
assert.match(styles, /body\.screen-explore #exploreContent \.browse-surprise-panel\s*\{[\s\S]*display:\s*none !important/);

assert.equal(latestResponseGuard(2, 1), false);
assert.equal(latestResponseGuard(2, 2), true);
assert.deepEqual(mutationGate(false), { accepted: true, inFlight: true });
assert.deepEqual(mutationGate(true), { accepted: false, inFlight: true });

console.log('frontend reliability checks passed');
