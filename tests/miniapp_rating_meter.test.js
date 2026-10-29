const { test } = require('node:test');
const assert = require('node:assert/strict');
const meter = require('../static/miniapp/rating-meter.js');

test('Moctale Meter is the one-decimal mean of available IMDb and TMDb scores', () => {
    assert.deepEqual(
        meter.summarize({ imdb: { score: 8.2 }, tmdb: { score: 7.6 } }),
        { score: 7.9, sourceCount: 2, providers: ['IMDb', 'TMDb'] }
    );
});

test('a single available provider stays attributable and is not treated as a blend', () => {
    assert.deepEqual(
        meter.summarize({ imdb: { score: 8.2 }, tmdb: { score: null } }),
        { score: 8.2, sourceCount: 1, providers: ['IMDb'] }
    );
});

test('missing, malformed, and out-of-range provider scores do not create a zero rating', () => {
    assert.deepEqual(
        meter.summarize({ imdb: { score: 'N/A' }, tmdb: { score: 11 } }),
        { score: null, sourceCount: 0, providers: [] }
    );
});

test('a TMDb zero-vote score is treated as unavailable rather than a real zero', () => {
    assert.deepEqual(
        meter.summarize({ imdb: { score: null }, tmdb: { score: 0, votes: 0 } }),
        { score: null, sourceCount: 0, providers: [] }
    );
});

test('TMDb-only result routing never confuses a TMDb ID with a local catalogue ID', () => {
    assert.equal(
        meter.endpointFor({ id: 77, tmdb_id: 77, source: 'tmdb', media_type: 'tv' }, true),
        '/api/tmdb/tv/77/rating-sources'
    );
    assert.equal(
        meter.endpointFor({ id: 77, tmdb_id: 900, source: 'local' }, false),
        '/api/movie/77/rating-sources'
    );
});

test('provider review links are restricted to TMDb review pages', () => {
    assert.equal(meter.isTrustedReviewUrl('https://www.themoviedb.org/review/abc'), true);
    assert.equal(meter.isTrustedReviewUrl('javascript:alert(1)'), false);
    assert.equal(meter.isTrustedReviewUrl('https://evil.example/review/abc'), false);
});

test('an unknown TMDb review count is not presented as zero reviews', () => {
    const previousDocument = global.document;
    class Element {
        constructor(tag) { this.tag = tag; this.className = ''; this.textContent = ''; this.children = []; }
        append(...children) { this.children.push(...children); }
        replaceChildren(...children) { this.children = children; }
        setAttribute() {}
        querySelector(selector) {
            const className = selector.startsWith('.') ? selector.slice(1) : '';
            const visit = item => item.className.split(/\s+/).includes(className)
                ? item
                : item.children.map(visit).find(Boolean);
            return this.children.map(visit).find(Boolean) || null;
        }
    }
    global.document = { createElement: tag => new Element(tag) };
    try {
        const reviews = new Element('section');
        meter.render(null, reviews, { reviews: [], review_count: null });
        assert.equal(reviews.querySelector('.review-count').textContent, 'TMDb');
    } finally {
        global.document = previousDocument;
    }
});

