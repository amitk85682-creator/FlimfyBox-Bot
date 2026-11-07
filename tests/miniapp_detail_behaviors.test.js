const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, '../static/miniapp/app.js'), 'utf8');
const templateSource = fs.readFileSync(path.join(__dirname, '../templates/mini_app.html'), 'utf8');

test('changed detail assets share a cache-busting template version', () => {
    const version = templateSource.match(/\{% set miniapp_asset_version = '([^']+)' %\}/);
    assert.ok(version, 'the detail assets have a shared cache-busting version');
    assert.match(version[1], /^\d{8}-detail-[a-z0-9-]+$/);
    for (const asset of ['miniapp/cinema.css', 'miniapp/rating-meter.js', 'miniapp/app.js']) {
        assert.ok(templateSource.includes(`filename='${asset}') }}?v={{ miniapp_asset_version }}`));
    }
    assert.doesNotMatch(templateSource, /20261107-live-cache-1/);
});

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
        summarize: () => ({ score: 7.6 }),
        render() { rendered += 1; },
        renderUnavailable() { rendered += 10; }
    };
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'), {
        document: { getElementById: id => id === 'flimfyVerdict' ? meterContainer : reviewContainer },
        FlimfyVerdict: meter,
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

test('Rate this scrolls smoothly to the FlimfyBox rating controls', () => {
    let scrollOptions = null;
    const target = { scrollIntoView: options => { scrollOptions = options; } };
    const api = runtime(sourceSection('window.scrollToDetailDownloads = function()', 'window.openDetails = function'), {
        document: { getElementById: id => id === 'communityRating' ? target : null }
    });

    api.scrollToCommunityRating();
    assert.equal(scrollOptions.behavior, 'smooth');
    assert.equal(scrollOptions.block, 'start');
});

test('member rating shows only the personal score and can be updated in place', async () => {
    const buttons = Array.from({ length: 5 }, (_, index) => ({
        dataset: { rating: String(index + 1) }, disabled: false, textContent: '☆', attributes: {},
        classList: { toggle() {} },
        setAttribute(name, value) { this.attributes[name] = value; }
    }));
    const submit = { disabled: false, innerHTML: '' };
    const summary = { textContent: '' };
    const container = {
        dataset: { movieId: '12', selectedRating: '', canRate: 'true' },
        querySelector: selector => selector === '.community-rating-submit' ? submit : summary,
        querySelectorAll: () => buttons
    };
    const meterContainer = {};
    const requests = [];
    const events = [];
    const api = runtime(sourceSection('function previewCommunityStars', 'function normalizeTrailerKey'), {
        document: { getElementById: id => id === 'flimfyVerdict' ? meterContainer : null },
        FlimfyVerdict: { setCommunityRating: () => {} },
        activeMovie: { id: 12 },
        fetchWithTimeout: async (...args) => {
            requests.push(args);
            return { ok: true, json: async () => ({ status: 'success', average: 4.2, count: 11, user_rating: 2, can_rate: true }) };
        },
        telegramAuthHeaders: () => ({ 'X-Telegram-Init-Data': 'signed' }),
        trackRecommendationEvent: (...event) => events.push(event)
    });

    api.updateCommunityRating(container, { average: 4.4, count: 10, user_rating: 4, can_rate: true });
    assert.equal(summary.textContent, '10 member ratings · Your rating: 4/5');
    assert.doesNotMatch(summary.textContent, /4\.4/);
    assert.equal(submit.disabled, false);
    assert.equal(buttons[3].attributes['aria-pressed'], 'true');

    api.selectCommunityRating(2, container);
    assert.equal(buttons[1].attributes['aria-pressed'], 'true');
    assert.equal(buttons[3].attributes['aria-pressed'], 'false');
    assert.match(submit.innerHTML, /Update my rating/);
    await api.submitCommunityRating(12, container);

    assert.equal(requests[0][0], '/api/movie/12/rating');
    assert.equal(requests[0][1].method, 'POST');
    assert.deepEqual(JSON.parse(requests[0][1].body), { rating: 2 });
    assert.equal(summary.textContent, '11 member ratings · Your rating: 2/5');
    assert.equal(events[0][0], 'rating_submitted');
    assert.equal(events[0][1], 12);
    assert.equal(events[0][2].rating, 2);
});

test('a warm persistent rating cache renders synchronously and skips the network', async () => {
    const now = Date.now();
    const values = new Map([['flimfybox:detail:v2:rating:77:900::movie', JSON.stringify({
        savedAt: now, freshForMs: 300000,
        value: { status: 'success', tmdb: { score: 7.8, votes: 600 }, imdb: { score: null } }
    })]]);
    const renders = [];
    const meter = {
        summarize: payload => ({ score: payload.tmdb?.score ?? null }),
        endpointFor: () => '/api/movie/77/rating-sources',
        renderLoading() {}, renderUnavailable() {},
        render: (_container, _reviews, payload) => renders.push(payload)
    };
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'), {
        document: { getElementById: () => ({}) },
        FlimfyVerdict: meter,
        localStorage: {
            getItem: key => values.get(key) || null,
            setItem: (key, value) => values.set(key, value),
            removeItem: key => values.delete(key)
        },
        fetchWithTimeout: async () => { throw new Error('fresh cache must avoid the network'); },
        detailsRequestId: 3,
        activeMovie: { id: 77 },
        isAbortError: error => error?.name === 'AbortError'
    });

    await api.loadMovieRatingSources({ id: 77, tmdb_id: 900 }, false, 3, {});

    assert.equal(renders.length, 1);
    assert.equal(renders[0].tmdb.score, 7.8);
    assert.equal(renders[0].cache_status, 'fresh');
});

test('an expired rating cache remains visible when the refresh request fails', async () => {
    const storedValue = JSON.stringify({
        savedAt: Date.now() - 6 * 60 * 1000,
        freshForMs: 5 * 60 * 1000,
        value: { status: 'success', tmdb: { score: 7.6, votes: 900 }, imdb: { score: null } }
    });
    const values = new Map([['flimfybox:detail:v2:rating:77:900::movie', storedValue]]);
    const renders = [];
    let unavailableRenders = 0;
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'), {
        document: { getElementById: () => ({}) },
        FlimfyVerdict: {
            summarize: payload => ({ score: payload.tmdb?.score ?? null }),
            endpointFor: () => '/api/movie/77/rating-sources',
            renderLoading() {},
            renderUnavailable() { unavailableRenders += 1; },
            render: (_container, _reviews, payload) => renders.push(payload)
        },
        localStorage: {
            getItem: key => values.get(key) || null,
            setItem: (key, value) => values.set(key, value),
            removeItem: key => values.delete(key)
        },
        fetchWithTimeout: async () => { throw new Error('simulated provider outage'); },
        detailsRequestId: 4,
        activeMovie: { id: 77 },
        isAbortError: error => error?.name === 'AbortError'
    });

    await api.loadMovieRatingSources({ id: 77, tmdb_id: 900 }, false, 4, {});

    assert.equal(renders.at(-1).tmdb.score, 7.6);
    assert.equal(renders.at(-1).cache_status, 'stale_cached');
    assert.equal(unavailableRenders, 0);
    assert.equal(values.get('flimfybox:detail:v2:rating:77:900::movie'), storedValue);
});

test('each review provider updates the feed as soon as its own request finishes', async () => {
    const container = { querySelector: () => null };
    const renders = [];
    let finishTmdb;
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'), {
        document: { getElementById: () => container },
        FlimfyVerdict: { renderReviews: (_container, payload) => renders.push(payload) },
        fetchWithTimeout: endpoint => endpoint.endsWith('/audience-reviews')
            ? new Promise(resolve => { finishTmdb = resolve; })
            : Promise.resolve({ ok: true, json: async () => ({
                status: 'success', reviews: [{ id: 'local', author: 'Flimfy member' }],
                review_count: 1, can_review: false
            }) }),
        telegramAuthHeaders: () => ({}),
        activeMovie: { id: 77 },
        detailsRequestId: 5,
        isAbortError: error => error?.name === 'AbortError'
    });

    api.loadFlimfyCommunityReviews({ id: 77, tmdb_id: 900, source: 'local' }, 5, { aborted: false });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(renders.at(-1).reviews[0].author, 'Flimfy member');
    assert.equal(renders.at(-1).tmdb_loading, true);

    finishTmdb({ ok: true, json: async () => ({
        status: 'success', reviews: [{ id: 'tmdb', author: 'TMDb member' }],
        review_count: 1, linked: true, cache_status: 'fresh'
    }) });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(renders.at(-1).reviews[0].author, 'Flimfy member');
    assert.equal(renders.at(-1).tmdb_reviews[0].author, 'TMDb member');
    assert.equal(renders.at(-1).tmdb_loading, false);
});

test('warm persistent review caches render before the member endpoint responds', () => {
    const now = Date.now();
    const values = new Map([
        ['flimfybox:detail:v2:reviews:77:900:movie:flimfybox', JSON.stringify({
            savedAt: now, freshForMs: 60000,
            value: { status: 'success', reviews: [{ id: 'local', author: 'Saved Flimfy member' }], review_count: 1 }
        })],
        ['flimfybox:detail:v2:reviews:77:900:movie:tmdb', JSON.stringify({
            savedAt: now, freshForMs: 60000,
            value: { status: 'success', linked: true, reviews: [{ id: 'tmdb', author: 'Saved TMDb member' }], review_count: 1, cache_status: 'fresh' }
        })]
    ]);
    const renders = [];
    const endpoints = [];
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'), {
        document: { getElementById: () => ({ querySelector: () => null }) },
        FlimfyVerdict: { renderReviews: (_container, payload) => renders.push(payload) },
        localStorage: {
            getItem: key => values.get(key) || null,
            setItem: (key, value) => values.set(key, value),
            removeItem: key => values.delete(key)
        },
        fetchWithTimeout: endpoint => {
            endpoints.push(endpoint);
            return new Promise(() => {});
        },
        telegramAuthHeaders: () => ({}),
        activeMovie: { id: 77 },
        detailsRequestId: 6
    });

    api.loadFlimfyCommunityReviews({ id: 77, tmdb_id: 900, source: 'local' }, 6, { aborted: false });

    assert.equal(renders.length, 1);
    assert.equal(renders[0].reviews[0].author, 'Saved Flimfy member');
    assert.equal(renders[0].tmdb_reviews[0].author, 'Saved TMDb member');
    assert.equal(renders[0].tmdb_loading, false);
    assert.equal(renders[0].flimfy_loading, true);
    assert.deepEqual(endpoints, ['/api/movie/77/reviews']);
});

test('detail caches separate movie and series that share a TMDb number', () => {
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'));
    const movie = { id: 'tmdb_900', tmdb_id: 900, media_type: 'movie' };
    const series = { id: 'tmdb_900', tmdb_id: 900, media_type: 'tv' };

    assert.notEqual(api.ratingDetailCacheKey(movie), api.ratingDetailCacheKey(series));
    assert.notEqual(api.reviewDetailCacheKey(movie, 'tmdb'), api.reviewDetailCacheKey(series, 'tmdb'));
});

test('a rating cache with a zero freshness window is immediately stale', () => {
    const now = Date.now();
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'), {
        localStorage: {
            getItem: () => JSON.stringify({ savedAt: now, freshForMs: 0, value: { tmdb: { score: 7.5 } } }),
            removeItem() {}
        }
    });

    assert.equal(api.readDetailCache('rating:zero-window', 300000).isFresh, false);
});

test('member review form saves through the authenticated FlimfyBox endpoint and reloads', async () => {
    const listeners = {};
    const input = { value: 'A warm, funny story with a great ending.' };
    const status = { textContent: '' };
    const submit = { disabled: false };
    const form = {
        addEventListener: (type, callback) => { listeners[type] = callback; },
        querySelector: selector => ({
            '.flimfy-review-input': input,
            '.flimfy-review-status': status,
            '.flimfy-review-submit': submit
        })[selector]
    };
    const container = { querySelector: selector => selector === '.flimfy-review-form' ? form : null };
    let fetchArgs = null;
    let reloadArgs = null;
    const toasts = [];
    const api = runtime(sourceSection('function bindFlimfyReviewForm', 'function loadFlimfyCommunityReviews'), {
        activeMovie: { id: 12 },
        detailsRequestId: 4,
        fetchWithTimeout: async (...args) => {
            fetchArgs = args;
            return { ok: true, json: async () => ({ status: 'success' }) };
        },
        telegramAuthHeaders: () => ({ 'X-Telegram-Init-Data': 'signed' }),
        showToast: message => toasts.push(message),
        loadFlimfyCommunityReviews: (...args) => { reloadArgs = args; }
    });
    const movie = { id: 12, title: 'Sample film' };
    api.bindFlimfyReviewForm(container, '12', movie, 4, {});
    let prevented = false;
    await listeners.submit({ preventDefault: () => { prevented = true; } });

    assert.equal(prevented, true);
    assert.equal(fetchArgs[0], '/api/movie/12/reviews');
    assert.equal(fetchArgs[1].method, 'POST');
    assert.equal(fetchArgs[1].headers['X-Telegram-Init-Data'], 'signed');
    assert.deepEqual(JSON.parse(fetchArgs[1].body), { review: input.value });
    assert.deepEqual(reloadArgs, [movie, 4, {}, 0, null, true]);
    assert.deepEqual(toasts, ['✓ FlimfyBox review saved']);
});

test('one FlimfyBox review panel combines local and TMDb reviews using the correct title IDs', async () => {
    const container = { querySelector: () => null };
    const endpoints = [];
    const renders = [];
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'), {
        document: { getElementById: id => id === 'flimfyReviews' ? container : null },
        FlimfyVerdict: {
            renderReviews: (_container, data) => renders.push(data)
        },
        fetchWithTimeout: async endpoint => {
            endpoints.push(endpoint);
            const isTmdb = endpoint.endsWith('/audience-reviews');
            return { ok: true, json: async () => isTmdb
                ? { status: 'success', source: 'TMDb', reviews: [{ author: 'TMDb member' }], review_count: 9, linked: true }
                : { status: 'success', reviews: [{ author: 'FlimfyBox member' }], review_count: 1, can_review: false } };
        },
        telegramAuthHeaders: () => ({ 'X-Telegram-Init-Data': 'test' }),
        activeMovie: { id: 77 },
        detailsRequestId: 1,
        isAbortError: error => error?.name === 'AbortError'
    });

    const localSignal = { aborted: false };
    api.loadFlimfyCommunityReviews({ id: 77, tmdb_id: 900, content_type: 'Web Series', source: 'local' }, 1, localSignal);
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(endpoints, ['/api/movie/77/reviews', '/api/movie/77/audience-reviews']);
    assert.equal(renders.at(-1).reviews[0].author, 'FlimfyBox member');
    assert.equal(renders.at(-1).tmdb_reviews[0].author, 'TMDb member');
    assert.equal(renders.at(-1).tmdb_review_count, 9);

    endpoints.length = 0;
    api.activeMovie = { id: 'tmdb_901' };
    api.loadFlimfyCommunityReviews({ id: 'tmdb_901', source: 'tmdb', media_type: 'tv' }, 1, { aborted: false });
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(endpoints, ['/api/tmdb/tv/901/audience-reviews']);
    assert.equal(renders.at(-1).reviews.length, 0);
    assert.equal(renders.at(-1).tmdb_reviews[0].author, 'TMDb member');
    assert.equal(renders.at(-1).is_catalogued, false);
});

test('one unavailable review provider does not hide the reviews returned by the other', async () => {
    const container = {};
    const endpoints = [];
    const renders = [];
    const api = runtime(sourceSection('const DETAIL_CACHE_PREFIX', 'window.scrollToDetailDownloads'), {
        document: { getElementById: id => id === 'flimfyReviews' ? container : null },
        FlimfyVerdict: { renderReviews: (_container, data) => renders.push(data) },
        fetchWithTimeout: async endpoint => {
            endpoints.push(endpoint);
            if (endpoint.endsWith('/reviews')) {
                return { ok: false, json: async () => ({ status: 'error', message: 'member store unavailable' }) };
            }
            return { ok: true, json: async () => ({
                status: 'success', reviews: [{ id: 'tmdb-3', author: 'Audience reviewer' }],
                review_count: 1, linked: true
            }) };
        },
        telegramAuthHeaders: () => ({}),
        activeMovie: { id: 77 },
        detailsRequestId: 5,
        isAbortError: error => error?.name === 'AbortError'
    });

    api.loadFlimfyCommunityReviews({ id: 77, tmdb_id: 900, source: 'local', content_type: 'Movie' }, 5, { aborted: false });
    await new Promise(resolve => setImmediate(resolve));

    assert.deepEqual(endpoints, ['/api/movie/77/reviews', '/api/movie/77/audience-reviews']);
    assert.equal(renders.at(-1).status, 'success');
    assert.equal(renders.at(-1).tmdb_reviews[0].author, 'Audience reviewer');
    assert.equal(renders.at(-1).flimfy_unavailable, true);
    assert.equal(renders.at(-1).tmdb_unavailable, false);
});
