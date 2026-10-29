const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../static/miniapp/app.js'), 'utf8');
const template = fs.readFileSync(path.join(__dirname, '../templates/mini_app.html'), 'utf8');

function section(start, end) {
    const from = source.indexOf(start);
    const to = source.indexOf(end, from + start.length);
    assert.ok(from >= 0 && to > from, `Missing source section ${start}`);
    return source.slice(from, to);
}
function runtime(script, globals = {}) {
    const context = { ...globals };
    context.window = context;
    vm.runInNewContext(script, context);
    return context;
}
const timeoutSource = section('async function fetchWithTimeout', 'function artworkUrl');

test('a stalled network request times out and aborts the underlying fetch', async () => {
    let aborted = false;
    const api = runtime(timeoutSource, {
        AbortController, setTimeout, clearTimeout,
        fetch: (_url, { signal }) => new Promise((_resolve, reject) => {
            signal.addEventListener('abort', () => { aborted = true; reject(new Error('aborted')); });
        })
    });
    await assert.rejects(api.fetchWithTimeout('/slow', {}, 10), /taking too long/);
    assert.equal(aborted, true);
});

test('search cancellation preserves AbortError and cancels its fetch', async () => {
    const api = runtime(timeoutSource, {
        AbortController, setTimeout, clearTimeout,
        fetch: (_url, { signal }) => new Promise((_resolve, reject) => {
            signal.addEventListener('abort', () => reject(new DOMException('cancelled', 'AbortError')));
        })
    });
    const controller = new AbortController();
    const pending = api.fetchWithTimeout('/search', { signal: controller.signal });
    controller.abort();
    await assert.rejects(pending, { name: 'AbortError' });
});

test('cards preserve quoted titles safely and use keyboard-operable native buttons', () => {
    const api = runtime(
        section('function artworkUrl', 'function syncSafeArea') +
        section('function escapeHtml', 'window.showSettings') +
        section('function renderCards', '// Hybrid Search'),
        { POSTER_PLACEHOLDER: '/placeholder.svg', FlimfyUI: require('../static/miniapp/ui-core.js') }
    );
    const title = `A "quote" & <img onerror=alert(1)> 'title'`;
    const html = api.renderCards([{ id: 1, title, image: 'https://image.tmdb.org/t/p/original/poster.jpg' }]);
    assert.match(html, /^<button type="button"/);
    assert.doesNotMatch(html, /onkeydown=/);
    assert.doesNotMatch(html, /<img onerror=alert/);
    assert.match(html, /\/w342\/poster.jpg/);
    assert.equal(JSON.parse(decodeURIComponent(html.match(/data-movie="([^"]+)"/)[1])).title, title);
    assert.equal(api.artworkUrl('javascript:alert(1)'), '/placeholder.svg');
});

test('mini app detail controls are real, named, and keyboard operable', () => {
    assert.match(template, /id="genreSearchInput"[^>]*aria-label=/);
    assert.match(template, /class="btn-more"[^>]*aria-label="Back to genres"[^>]*onclick="closeGenreDetail\(\)"/);
    assert.doesNotMatch(template, /Browse more genres from the Explore hub/);
    assert.match(template, /class="close-trailer-btn"[^>]*type="button"[^>]*aria-label="Close trailer"/);
    assert.match(template, /class="wp-close"[^>]*type="button"[^>]*aria-label="Close web player"/);
    assert.match(source, /<button type="button" class="season-pill"[^>]*aria-pressed="false"/);
    assert.match(source, /el\.setAttribute\('aria-pressed', String\(selected\)\)/);
});

test('genre poster failures replace broken artwork with the local placeholder', () => {
    const genreSection = section('function renderExploreScreen()', 'window.openGenreDetail = async function');
    assert.match(genreSection, /onerror="this\.onerror=null;\s*this\.src='\$\{POSTER_PLACEHOLDER\}'/);
});

test('a stale upcoming response cannot overwrite a newer home render', () => {
    const upcoming = section('const upcomingRow = ensureHomeSectionRow', 'const catalogueRows = [');
    assert.match(upcoming, /const requestId = \+\+upcomingRequestId/);
    assert.match(upcoming, /if \(requestId !== upcomingRequestId\) return;/);
});

test('both request actions authenticate, deduplicate concurrent clicks, and allow retry', async () => {
    let finish;
    const calls = [];
    const api = runtime(
        section('const pendingRequests = new Set();', '// 🛡️ NAYA:'), {
        telegramAuthHeaders: () => ({ 'X-Telegram-Init-Data': 'signed-fixture' }),
        tg: { HapticFeedback: { notificationOccurred() {} } }, showToast() {},
        fetchWithTimeout: (url, options) => {
            calls.push({ url, options });
            return new Promise(resolve => { finish = resolve; });
        }
    });
    const pending = api.requestMovie('A title');
    await api.requestSilent('A title');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].options.headers['X-Telegram-Init-Data'], 'signed-fixture');
    assert.deepEqual(JSON.parse(calls[0].options.body), { title: 'A title' });
    finish({ ok: false, json: async () => ({ status: 'error' }) });
    await pending;
    const retry = api.requestSilent('A title');
    assert.equal(calls.length, 2);
    finish({ ok: true, json: async () => ({ status: 'success' }) });
    await retry;
});

test('a failed next catalogue page retains loaded cards and retries the same page', async () => {
    const grid = { innerHTML: 'existing-cards', insertAdjacentHTML(_position, html) { this.innerHTML += html; } };
    let fail = true;
    const pages = [];
    const api = runtime(section('// Pagination State', '// 🔥 NAYA: Infinite Scroll Listener') +
        '\nwindow.state = () => ({currentPage, isFetching, catalogueRetryPending});', {
        document: { getElementById: id => id === 'moreGrid' ? grid : null },
        allMovies: [{ id: 1 }], console: { error() {} }, isAbortError: () => false,
        readHomeCatalogueCache: () => ({ movies: [{ id: 1 }], hasMore: true, fresh: true }),
        completeInitialHomeLoadingStep() {}, renderHome() {}, renderCards: () => 'next-cards',
        apiRequest: async url => { pages.push(url); if (fail) throw new Error('offline'); return { status: 'success', movies: [{ id: 2 }], has_more: false }; }
    });
    await api.loadMovies();
    assert.equal(api.state().currentPage, 2);
    await api.loadMovies(2);
    assert.match(grid.innerHTML, /^existing-cards/);
    assert.match(grid.innerHTML, /Try again/);
    assert.equal(api.state().currentPage, 2);
    fail = false;
    api.retryCatalogue();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(api.state().currentPage, 3);
    assert.equal(pages.length, 2);
    assert.equal(pages[0], pages[1]);
});

test('a stale cache at the end of the catalogue can retry refreshing page one', async () => {
    let fail = true;
    const pages = [];
    const grid = { insertAdjacentHTML() {} };
    const api = runtime(section('// Pagination State', '// 🔥 NAYA: Infinite Scroll Listener'), {
        document: { getElementById: id => id === 'moreGrid' ? grid : null },
        allMovies: [], console: { error() {} }, isAbortError: () => false,
        readHomeCatalogueCache: () => ({ movies: [{ id: 1 }], hasMore: false, fresh: false }),
        completeInitialHomeLoadingStep() {}, renderHome() {}, writeHomeCatalogueCache() {},
        apiRequest: async url => { pages.push(url); if (fail) throw new Error('offline'); return { status: 'success', movies: [{ id: 2 }], has_more: false }; }
    });
    await api.loadMovies();
    fail = false;
    api.retryCatalogue();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(pages.length, 2);
    assert.equal(pages[0], '/api/movies?page=1&limit=40');
    assert.equal(pages[1], pages[0]);
});
