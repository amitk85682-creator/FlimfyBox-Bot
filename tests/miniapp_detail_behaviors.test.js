const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, '../static/miniapp/app.js'), 'utf8');

function sourceSection(startMarker, endMarker) {
    const start = source.indexOf(startMarker);
    const end = source.indexOf(endMarker, start + startMarker.length);
    assert.ok(start >= 0 && end > start, `Missing executable detail behavior: ${startMarker}`);
    return source.slice(start, end);
}

function runtime(script, globals = {}) {
    const context = { ...globals };
    context.window = context.window || context;
    vm.runInNewContext(script, context);
    return context;
}

test('late rating-source response cannot overwrite the movie that is currently open', async () => {
    const meterContainer = {};
    const reviewContainer = {};
    let finishFetch;
    let rendered = 0;
    const meter = {
        renderLoading() {},
        endpointFor: (_movie, isTMDB) => isTMDB
            ? '/api/tmdb/movie/77/rating-sources'
            : '/api/movie/77/rating-sources',
        render() { rendered += 1; },
        renderUnavailable() { rendered += 10; }
    };
    const api = runtime(sourceSection('function loadMovieRatingSources', 'window.scrollToDetailDownloads'), {
        document: { getElementById: id => id === 'moctaleMeter' ? meterContainer : reviewContainer },
        MoctaleMeter: meter,
        fetchWithTimeout: () => new Promise(resolve => { finishFetch = resolve; }),
        detailsRequestId: 1,
        activeMovie: { id: 77 },
        isAbortError: error => error?.name === 'AbortError'
    });

    api.loadMovieRatingSources({ id: 77, source: 'tmdb' }, true, 1, {});
    api.detailsRequestId = 2;
    api.activeMovie = { id: 88 };
    finishFetch({ ok: true, json: async () => ({ status: 'success', imdb: { score: 8 }, tmdb: { score: 7 } }) });
    await new Promise(resolve => setImmediate(resolve));

    assert.equal(rendered, 0);
});

test('download quality ordering preserves the exact movie and file IDs on every action', () => {
    const api = runtime(sourceSection('function resolutionRank(value)', 'function setMovieDetailsMeta'), {
        formatMiniAppQuality: value => value,
        escapeHtml: value => String(value).replace(/[&<>"']/g, character => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[character]))
    });
    const html = api.renderMovieDownloadOptions(61, [
        { id: 8, quality: '720p', size: '800 MB' },
        { id: 9, quality: '1080p WEB-DL', size: '1.2 GB' }
    ]);

    assert.ok(html.indexOf('1080p WEB-DL') < html.indexOf('720p'));
    assert.match(html, /BEST AVAILABLE/);
    assert.match(html, /downloadMovie\(61, 9\)/);
    assert.match(html, /downloadMovie\(61, 8\)/);
    assert.doesNotMatch(html, /downloadMovie\(61, NaN\)/);
});

test('episode qualities are sorted highest first without changing the selected file IDs', () => {
    const links = { innerHTML: '' };
    const pill = {
        getAttribute: name => name === 'data-season' ? '1' : null,
        setAttribute() {},
        classList: { add() {}, remove() {} },
        scrollIntoView() {}
    };
    const code = sourceSection('function resolutionRank(value)', 'function setMovieDetailsMeta')
        + sourceSection('window.selectSeason = function(movieId, seasonNum)', 'window.closeDetails = function()');
    const api = runtime(code, {
        document: {
            querySelectorAll: () => [pill],
            getElementById: id => id === 'dpLinks' ? links : null
        },
        currentMovieSeasons: {
            1: { episodes: {
                1: { isCompleteSeason: false, episodeEnd: null, episodeStart: 1, qualities: [
                    { id: 72, quality: '720p', size: '700 MB' },
                    { id: 108, quality: '1080p WEB-DL', size: '1.4 GB' }
                ] }
            } }
        },
        formatMiniAppQuality: value => value,
        escapeHtml: value => String(value)
    });

    api.selectSeason(55, 1);

    assert.ok(links.innerHTML.indexOf('1080p WEB-DL') < links.innerHTML.indexOf('720p'));
    assert.match(links.innerHTML, /downloadMovie\(55, 108\)/);
    assert.match(links.innerHTML, /downloadMovie\(55, 72\)/);
});

test('Find a version scrolls to the download panel without relying on a stale node', () => {
    let scrollOptions = null;
    const target = { scrollIntoView: options => { scrollOptions = options; } };
    const api = runtime(sourceSection('window.scrollToDetailDownloads = function()', 'window.openDetails = function'), {
        document: { getElementById: id => id === 'dpLinks' ? target : null }
    });

    api.scrollToDetailDownloads();
    assert.equal(scrollOptions.behavior, 'smooth');
    assert.equal(scrollOptions.block, 'start');

    scrollOptions = null;
    api.document = { getElementById: () => null };
    assert.doesNotThrow(() => api.scrollToDetailDownloads());
    assert.equal(scrollOptions, null);
});
