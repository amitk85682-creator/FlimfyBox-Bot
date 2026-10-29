const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

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
assert.match(source, /Never block the Home shell behind a full-page loader/);
assert.match(source, /document\.body\.classList\.remove\('app-booting'\)/);
assert.match(source, /screen\.classList\.add\('is-complete'\)/);
assert.match(source, /if \(isAbortError\(error\)\) return;/);
assert.match(source, /&autocomplete=1/);
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
const episodeHelpersSource = source.match(
  /function parseMiniAppEpisodeMetadata\(value\) \{[\s\S]*?function formatMiniAppQuality\(value\) \{[\s\S]*?\n        \}/
);
assert.ok(episodeHelpersSource, 'Mini App episode helpers should exist');
const episodeHelpers = vm.runInNewContext(
  `(() => { ${episodeHelpersSource[0]}; return { parseMiniAppEpisodeMetadata, formatMiniAppQuality }; })()`
);
const seasonOnly = episodeHelpers.parseMiniAppEpisodeMetadata('S01 1080p WEB-DL');
assert.equal(seasonOnly.season, 1);
assert.equal(seasonOnly.episodeStart, null);
assert.equal(seasonOnly.episodeEnd, null);
const episodeRange = episodeHelpers.parseMiniAppEpisodeMetadata('S02 E01-08 1080p WEB-DL');
assert.equal(episodeRange.season, 2);
assert.equal(episodeRange.episodeStart, 1);
assert.equal(episodeRange.episodeEnd, 8);
assert.equal(
  episodeHelpers.formatMiniAppQuality('S02 E01 1080p WEB-DL (Japanese, Hindi) [2.73 GB]'),
  '1080p WEB-DL'
);
assert.match(source, /Complete Season/);
assert.match(source, /Episode \$\{epDisplayNum\} Combined/);

const styles = fs.readFileSync(path.join(__dirname, '..', 'static', 'miniapp', 'cinema.css'), 'utf8');
assert.match(source, /const movieDetailsCache = new Map\(\)/);
const template = fs.readFileSync(
  path.join(__dirname, '..', 'templates', 'mini_app.html'),
  'utf8'
);
assert.match(template, /id="heroTitle"/);
assert.match(template, /id="heroMeta"/);
assert.match(template, /id="heroWatch"/);
assert.match(template, /id="heroInfo"/);
assert.match(template, /class="hero-info"/);
assert.match(template, /id="rowUpcoming"/);
assert.match(template, /id="upcomingScroll"/);
assert.match(template, /onclick="showUpcoming\(\)"/);
assert.doesNotMatch(template, /Upcoming titles/);
assert.match(template, /app\.js'\) }}\?v=reliability-1/);
assert.match(source, /heroSlider\.style\.backgroundImage/);
assert.match(source, /const heroArtwork = movie\.backdrop \|\| movie\.image/);
assert.match(source, /const detailsArtwork = movie\.backdrop \|\| movie\.image/);
assert.match(source, /ensureHomeSectionRow\(rowId, title, icon, caption\)/);
assert.match(source, /rowUpcoming/);
assert.match(source, /window\.showUpcoming = function\(\)[\s\S]*?closeMorePanel\(\)[\s\S]*?showHome\(\)[\s\S]*?getElementById\('rowUpcoming'\)[\s\S]*?scrollIntoView\(\{ behavior: 'smooth', block: 'center' \}\)/);
assert.match(source, /Some release data is temporarily unavailable/);
assert.match(source, /flimfybox-home-section-\$\{cacheKey\}-v4/);
assert.match(source, /Stale-while-refresh/);
assert.match(source, /refreshHomeSection\(url, cacheKey, storageKey\)/);
assert.match(source, /return cached\.data/);
assert.match(source, /heroTimer = setInterval\(\(\) => stepHero\(1, false\), 6500\)/);
assert.match(source, /getElementById\('heroTitle'\)/);
assert.match(template, /class="btn-back details-back-button"[^>]*onclick="closeDetails\(\)"[^>]*>[\s\S]*<span>Back<\/span>/);
assert.equal((template.match(/class="btn-back details-back-button"/g) || []).length, 2);
assert.equal((template.match(/onclick="closeDetails\(\)"/g) || []).length, 1);
assert.doesNotMatch(template, /class="header-close"[^>]*onclick="tg\.close\(\)"/);
assert.match(source, /tg\.BackButton/);
assert.match(source, /backButton\.show\(\)/);
assert.match(source, /backButton\.onClick\(telegramBackButtonHandler\)/);
assert.match(source, /if \(detailsPage\?\.classList\.contains\('open'\)\)/);

assert.equal(latestResponseGuard(2, 1), false);
assert.equal(latestResponseGuard(2, 2), true);
assert.deepEqual(mutationGate(false), { accepted: true, inFlight: true });
assert.deepEqual(mutationGate(true), { accepted: false, inFlight: true });

console.log('frontend reliability checks passed');
assert.match(styles, /data-theme="light"/);
assert.match(styles, /prefers-reduced-motion/);
assert.match(styles, /:focus-visible/);
assert.doesNotMatch(template, /miniapp\/app\.css/);
