const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../static/miniapp/app.js'), 'utf8');
const template = fs.readFileSync(path.join(__dirname, '../templates/mini_app.html'), 'utf8');
const webRoutes = fs.readFileSync(path.join(__dirname, '../webapp_routes.py'), 'utf8');
const previewHarness = fs.readFileSync(path.join(__dirname, 'miniapp_preview.py'), 'utf8');
const samplePreview = require('./miniapp_preview_node.js');

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
    assert.match(html, /Checking/);
    assert.match(api.renderCards([
        { id: 2, title: 'Ready', is_available: true },
        { id: 3, title: 'Request', is_available: false }
    ]), /Available[\s\S]*Request/);
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

test('Mood Reader offers its choices and passes filters and Telegram identity to the recommendation API', async () => {
    const moodState = section("let browseType = 'all';", 'let catalogueAvailability =');
    const moodFlow = section('window.toggleMoodReader = function()', '// Utility');
    const selections = ['fun', 'mind', 'action', 'heart', 'anime', 'any'].map(value => ({
        dataset: { moodValue: value }, setAttribute(name, selected) { this[name] = selected; },
        classList: { toggle() {} }
    }));
    const group = { querySelectorAll: () => selections };
    const moodButton = { closest: () => group };
    const panel = { hidden: true, scrollIntoView() {} };
    const toggle = { setAttribute(name, value) { this[name] = value; } };
    const submit = { disabled: false, setAttribute(name, value) { this[name] = value; }, removeAttribute(name) { delete this[name]; } };
    const recommendation = { hidden: true, setAttribute() {} };
    const feedbackButtons = [{ disabled: false }, { disabled: false }];
    const likedButton = { innerHTML: '' };
    const feedback = { hidden: true, querySelectorAll: () => feedbackButtons };
    const explanation = { textContent: '' };
    const result = { innerHTML: '', querySelector: () => ({ addEventListener() {} }) };
    const elements = { moodReader: panel, moodReaderToggle: toggle, moodReaderSubmit: submit,
        surpriseRecommendation: recommendation, surpriseResult: result, moodPickExplanation: explanation,
        moodPickFeedback: feedback };
    const calls = [];
    const events = [];
    const api = runtime(moodState + moodFlow, {
        document: {
            getElementById: id => elements[id] || null,
            querySelectorAll: () => feedbackButtons,
            querySelector: selector => selector === '.mood-pick-liked' ? likedButton : null
        },
        URLSearchParams, AbortController,
        telegramAuthHeaders: () => ({ 'X-Telegram-Init-Data': 'signed-fixture' }),
        fetchWithTimeout: async (url, options, timeout) => {
            calls.push({ url, options, timeout });
            return { ok: true, json: async () => ({
                status: 'success', mood_match: true, matched_genres: ['Action'], personalized: false,
                movie: { id: 52, title: 'Action title', image: '/poster.jpg', is_available: true }
            }) };
        },
        trackRecommendationEvent: (...event) => events.push(event),
        allMovies: [], renderCards: () => '<button class="grid-card">Action title</button>',
        escapeHtml: value => String(value), showToast() {}
    });

    api.window.toggleMoodReader();
    assert.equal(panel.hidden, false);
    assert.equal(toggle['aria-expanded'], 'true');
    api.window.setMoodReaderChoice('mood', 'action', moodButton);
    await api.window.surpriseMe();

    const params = new URLSearchParams(calls[0].url.split('?')[1]);
    assert.equal(params.get('mood'), 'action');
    assert.equal(params.get('language'), 'any');
    assert.equal(params.get('availability'), 'available');
    assert.equal(calls[0].options.headers['X-Telegram-Init-Data'], 'signed-fixture');
    assert.equal(calls[0].timeout, 16000);
    assert.equal(recommendation.hidden, false);
    assert.equal(feedback.hidden, false);
    assert.match(explanation.textContent, /action wali movies se match ki/);
    assert.equal(events[0][0], 'surprise_impression');
    assert.equal(submit.disabled, false);

    api.window.handleMoodReaderFeedback('liked');
    assert.equal(events.at(-1)[0], 'mood_pick_liked');
    assert.deepEqual(feedbackButtons.map(button => button.disabled), [true, true]);
});

test('Mood Reader backend enforces catalogue filters and accepts its feedback signals', () => {
    const routeStart = webRoutes.indexOf("@flask_app.route('/api/browse/surprise'");
    const routeEnd = webRoutes.indexOf("@flask_app.route('/api/movies'", routeStart + 1);
    assert.ok(routeStart >= 0 && routeEnd > routeStart, 'Mood Reader API route exists');
    const route = webRoutes.slice(routeStart, routeEnd);
    assert.match(template, /id="moodReader"[\s\S]*data-mood-value="anime"/);
    assert.match(template, /data-mood-value="hindi"[\s\S]*data-mood-value="english"/);
    assert.match(route, /AND NOT COALESCE\(m\.is_unreleased, FALSE\)/);
    assert.match(route, /where_availability/);
    assert.match(route, /matched_genres/);
    assert.match(webRoutes, /'mood_pick_liked',[\s\S]*'mood_pick_skipped'/);
    assert.match(template, /daily-picks-1/);
});

test('changing a Mood Reader filter aborts the stale network request and unlocks the picker', async () => {
    const moodState = section("let browseType = 'all';", 'let catalogueAvailability =');
    const moodFlow = section('window.toggleMoodReader = function()', '// Utility');
    let aborted = false;
    const submit = { disabled: false, setAttribute() {}, removeAttribute() {} };
    const recommendation = {
        hidden: false,
        setAttribute(name) { if (name === 'hidden') this.hidden = true; }
    };
    const option = { setAttribute() {}, classList: { toggle() {} } };
    const group = { querySelectorAll: () => [option] };
    const button = { closest: () => group };
    const elements = {
        moodReaderSubmit: submit,
        surpriseRecommendation: recommendation,
        surpriseResult: { innerHTML: '', querySelector: () => null },
        moodPickExplanation: { textContent: '' },
        moodPickFeedback: { hidden: true, querySelectorAll: () => [] }
    };
    const api = runtime(moodState + moodFlow, {
        AbortController, URLSearchParams,
        document: { getElementById: id => elements[id] || null },
        telegramAuthHeaders: () => ({}),
        fetchWithTimeout: (_url, options) => new Promise((_resolve, reject) => {
            options.signal.addEventListener('abort', () => {
                aborted = true;
                reject(new DOMException('cancelled', 'AbortError'));
            }, { once: true });
        }),
        trackRecommendationEvent() {}, allMovies: [], renderCards: () => '',
        escapeHtml: value => String(value), showToast() {}
    });

    const pending = api.window.surpriseMe();
    api.window.setMoodReaderChoice('mood', 'fun', button);
    await pending;
    assert.equal(aborted, true);
    assert.equal(recommendation.hidden, true);
    assert.equal(submit.disabled, false);
});

test('Daily Picks show three available released titles and remember card opens', async () => {
    const loader = section('async function loadDailyPicks()', 'window.retryDailyPicks');
    const row = { style: { display: 'none' } };
    const scroll = { innerHTML: '', querySelectorAll: () => cards };
    const dateLabel = { textContent: '' };
    const cards = [{ addEventListener(_name, handler) { this.handler = handler; } },
        { addEventListener(_name, handler) { this.handler = handler; } },
        { addEventListener(_name, handler) { this.handler = handler; } }];
    const elements = { rowDailyPicks: row, dailyPicksScroll: scroll, dailyPicksDate: dateLabel };
    const events = [];
    let rendered;
    let requested;
    const api = runtime(`let dailyPickRequestId = 0; let dailyPickController = null; ${loader}`, {
        AbortController,
        document: { getElementById: id => elements[id] || null },
        telegramAuthHeaders: () => ({ 'X-Telegram-Init-Data': 'signed-daily-fixture' }),
        fetchWithTimeout: async (url, options) => {
            requested = { url, options };
            return { ok: true, json: async () => ({ status: 'success', date: '2026-10-31', date_label: 'Sat, 31 Oct', personalized: false, movies: [
                { id: 1, title: 'Ready one', is_available: true },
                { id: 2, title: 'Ready two', is_available: true },
                { id: 3, title: 'Future', is_available: true, is_unreleased: true },
                { id: 4, title: 'Unavailable', is_available: false },
                { id: 5, title: 'Ready three', is_available: true }
            ] }) };
        },
        renderCards: moviesToRender => { rendered = moviesToRender; return 'three-cards'; },
        allMovies: [], trackRecommendationEvent: (...event) => events.push(event),
        escapeHtml: value => String(value)
    });

    await api.loadDailyPicks();
    assert.equal(requested.url, '/api/home/daily-picks');
    assert.ok(requested.options.signal);
    assert.equal(requested.options.headers['X-Telegram-Init-Data'], 'signed-daily-fixture');
    assert.deepEqual(rendered.map(movie => movie.id), [1, 2, 5]);
    assert.equal(row.style.display, '');
    assert.equal(scroll.innerHTML, 'three-cards');
    assert.equal(dateLabel.textContent, 'Aaj · Sat, 31 Oct');
    cards[0].handler();
    assert.equal(events[0][0], 'daily_pick_click');
    assert.equal(events[0][1], 1);
    assert.equal(JSON.parse(JSON.stringify(events[0][2])).pick_date, '2026-10-31');
});

test('Daily Picks backend rotates by India date, filters to available titles, and accepts click feedback', () => {
    const routeStart = webRoutes.indexOf("@flask_app.route('/api/home/daily-picks'");
    const routeEnd = webRoutes.indexOf("@flask_app.route('/api/recommendation-events'", routeStart + 1);
    assert.ok(routeStart >= 0 && routeEnd > routeStart, 'Daily Picks API route exists');
    const route = webRoutes.slice(routeStart, routeEnd);
    assert.match(template, /id="rowDailyPicks"[\s\S]*id="dailyPicksScroll"/);
    assert.match(route, /timezone\(timedelta\(hours=5, minutes=30\)\)/);
    assert.match(route, /AND NOT COALESCE\(m\.is_unreleased, FALSE\)/);
    assert.match(route, /random\.Random\(seed\)\.shuffle/);
    assert.match(route, /top_pool\[:3\]/);
    assert.match(webRoutes, /'daily_pick_click'/);
    assert.match(previewHarness, /'home\/daily-picks': set\(\)/);
});

test('local preview fixtures return a matching mood pick and a stable available daily three', () => {
    const params = new URLSearchParams('type=movies&mood=action&language=hindi&availability=available');
    const moodPick = samplePreview.pick(params);
    assert.equal(moodPick.statusCode, 200);
    assert.equal(moodPick.body.mood_match, true);
    assert.equal(moodPick.body.movie.is_available, true);
    assert.match(moodPick.body.movie.language, /Hindi/);
    assert.match(moodPick.body.movie.genre, /Action/);

    const today = samplePreview.dailyPicksFor('2026-10-31');
    const sameDay = samplePreview.dailyPicksFor('2026-10-31');
    assert.equal(today.length, 3);
    assert.deepEqual(today.map(movie => movie.id), sameDay.map(movie => movie.id));
    assert.ok(today.every(movie => movie.is_available === true && !movie.is_unreleased));

    const checkin = samplePreview.dailyCheckinFor('2099-03-12');
    assert.equal(checkin.current_streak, 4);
    assert.equal(checkin.week.length, 7);
    assert.equal(checkin.week.at(-1).is_today, true);
    assert.equal(checkin.new_checkin, true);
    assert.equal(samplePreview.dailyCheckinFor('2099-03-12').new_checkin, false);
});

test('Watch Streak posts an authenticated daily check-in and renders the seven-day progress', async () => {
    const loader = section('async function loadWatchStreak()', 'function renderHome(movies)');
    const elements = {
        watchStreakCard: { hidden: true },
        watchStreakHeading: { textContent: '' },
        watchStreakMessage: { textContent: '' },
        watchStreakCount: { textContent: '' },
        watchStreakWeek: { innerHTML: '' }
    };
    const week = Array.from({ length: 7 }, (_unused, index) => ({
        label: ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'][index],
        done: index >= 4, is_today: index === 6
    }));
    const calls = [];
    const api = runtime(`let watchStreakLoaded = false; let watchStreakRequestId = 0; let watchStreakController = null; ${loader}`, {
        AbortController,
        document: { getElementById: id => elements[id] || null },
        telegramAuthHeaders: () => ({ 'X-Telegram-Init-Data': 'signed-streak-fixture' }),
        fetchWithTimeout: async (url, options) => {
            calls.push({ url, options });
            return { ok: true, status: 200, json: async () => ({
                status: 'success', current_streak: 4, new_checkin: true, week
            }) };
        },
        escapeHtml: value => String(value), console: { warn() {} }
    });

    await api.loadWatchStreak();
    await api.loadWatchStreak();
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, '/api/home/daily-checkin');
    assert.equal(calls[0].options.method, 'POST');
    assert.equal(calls[0].options.headers['X-Telegram-Init-Data'], 'signed-streak-fixture');
    assert.deepEqual(JSON.parse(calls[0].options.body), {});
    assert.equal(elements.watchStreakCard.hidden, false);
    assert.equal(elements.watchStreakCount.textContent, '4');
    assert.match(elements.watchStreakMessage.textContent, /4 din lagataar/);
    assert.match(elements.watchStreakWeek.innerHTML, /Su/);
    assert.match(elements.watchStreakWeek.innerHTML, /is-today/);
});

test('Watch Streak check-ins are authenticated, daily-deduplicated, and backed by stored event dates', () => {
    const routeStart = webRoutes.indexOf("@flask_app.route('/api/home/daily-checkin'");
    const routeEnd = webRoutes.indexOf("@flask_app.route('/api/recommendation-events'", routeStart + 1);
    assert.ok(routeStart >= 0 && routeEnd > routeStart, 'Daily check-in API route exists');
    const route = webRoutes.slice(routeStart, routeEnd);
    assert.match(template, /id="watchStreakCard"[\s\S]*id="watchStreakWeek"/);
    assert.match(route, /require_telegram_user\(\)/);
    assert.match(route, /upsert_miniapp_user\(cur, user\)/);
    assert.match(route, /metadata->>'date' = %s/);
    assert.match(route, /current_streak/);
    assert.match(route, /checked_in_today/);
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
        AbortController, catalogueRequestId: 0, catalogueAvailability: 'available', catalogueController: null,
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

test('switching availability cancels stale results and requests the selected partition', async () => {
    const buttons = ['available', 'request'].map(value => ({
        dataset: { catalogueAvailability: value },
        classList: { toggle(_name, active) { this.active = active; } },
        setAttribute(_name, pressed) { this.pressed = pressed; }
    }));
    const grid = { innerHTML: '' };
    const caption = { textContent: '' };
    const requests = [];
    const api = runtime(section('// Pagination State', '// 🔥 NAYA: Infinite Scroll Listener') +
        '\nwindow.state = () => ({catalogueAvailability, currentPage, isFetching});', {
        AbortController, catalogueRequestId: 0, catalogueAvailability: 'available', catalogueController: null,
        document: {
            querySelectorAll: () => buttons,
            getElementById: id => id === 'moreGrid' ? grid : id === 'catalogueAvailabilityCaption' ? caption : null
        },
        addEventListener() {}, allMovies: [], console: { error() {} }, isAbortError: error => error?.name === 'AbortError',
        readHomeCatalogueCache: () => null, completeInitialHomeLoadingStep() {}, renderHome() {},
        writeHomeCatalogueCache() {}, renderCards: () => '',
        apiRequest: (url, { signal }) => new Promise((resolve, reject) => {
            requests.push({ url, signal, resolve });
            signal.addEventListener('abort', () => reject(new DOMException('cancelled', 'AbortError')));
        })
    });
    const oldRequest = api.loadMovies(1);
    await Promise.resolve();
    const selectedRequest = api.setCatalogueAvailability('request');
    assert.equal(requests[0].signal.aborted, true);
    assert.match(requests[1].url, /availability=request/);
    assert.equal(buttons[0].pressed, 'false');
    assert.equal(buttons[1].pressed, 'true');
    requests[1].resolve({ status: 'success', movies: [{ id: 22, is_available: false }], has_more: false });
    await Promise.all([oldRequest, selectedRequest]);
    assert.equal(api.state().catalogueAvailability, 'request');
    assert.equal(api.state().currentPage, 2);
    assert.equal(api.state().isFetching, false);
});

test('a stale cache at the end of the catalogue can retry refreshing page one', async () => {
    let fail = true;
    const pages = [];
    const grid = { insertAdjacentHTML() {} };
    const api = runtime(section('// Pagination State', '// 🔥 NAYA: Infinite Scroll Listener'), {
        document: { getElementById: id => id === 'moreGrid' ? grid : null },
        allMovies: [], console: { error() {} }, isAbortError: () => false,
        AbortController, catalogueRequestId: 0, catalogueAvailability: 'available', catalogueController: null,
        readHomeCatalogueCache: () => ({ movies: [{ id: 1 }], hasMore: false, fresh: false }),
        completeInitialHomeLoadingStep() {}, renderHome() {}, writeHomeCatalogueCache() {},
        apiRequest: async url => { pages.push(url); if (fail) throw new Error('offline'); return { status: 'success', movies: [{ id: 2 }], has_more: false }; }
    });
    await api.loadMovies();
    fail = false;
    api.retryCatalogue();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(pages.length, 2);
    assert.equal(pages[0], '/api/movies?page=1&limit=40&availability=available');
    assert.equal(pages[1], pages[0]);
});
