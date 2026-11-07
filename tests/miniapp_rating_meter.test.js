const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const meter = require('../static/miniapp/rating-meter.js');

class Element {
    constructor(tag) {
        this.tag = tag;
        this.className = '';
        this.textContent = '';
        this.children = [];
        this.style = {};
        this.attributes = {};
        this.hidden = false;
    }
    append(...children) { this.children.push(...children); }
    replaceChildren(...children) { this.children = children; }
    setAttribute(name, value) { this.attributes[name] = value; }
    querySelector(selector) {
        const className = selector.startsWith('.') ? selector.slice(1) : '';
        const visit = item => item.className.split(/\s+/).includes(className)
            ? item
            : item.children.map(visit).find(Boolean);
        return this.children.map(visit).find(Boolean) || null;
    }
}

function withDocument(callback) {
    const previousDocument = global.document;
    global.document = { createElement: tag => new Element(tag) };
    try { callback(); } finally { global.document = previousDocument; }
}

function all(root, className) {
    const found = [];
    const visit = item => {
        if (item.className.split(/\s+/).includes(className)) found.push(item);
        item.children.forEach(visit);
    };
    visit(root);
    return found;
}

test('all three available sources produce the arithmetic mean on a ten-point scale', () => {
    assert.deepEqual(meter.summarize({
        imdb: { score: 8.0, votes: '10,000' },
        tmdb: { score: 7.5, votes: 2500 },
        flimfy: { score: 4.0, votes: 12 }
    }), { score: 7.8, sourceCount: 3, providers: ['IMDb', 'TMDb', 'FlimfyBox'] });
});

test('one source is enough and its score is preserved', () => {
    assert.deepEqual(meter.summarize({
        imdb: { score: 7.4, votes: 100 }, tmdb: { score: null }, flimfy: { score: null }
    }), { score: 7.4, sourceCount: 1, providers: ['IMDb'] });
});

test('two available sources average without counting absent ratings as zero', () => {
    assert.deepEqual(meter.summarize({
        imdb: { score: 8.2, votes: 300 }, tmdb: { score: 7.6, votes: 100 }, flimfy: null
    }), { score: 7.9, sourceCount: 2, providers: ['IMDb', 'TMDb'] });
});

test('FlimfyBox five-star average is normalized to the ten-point scale', () => {
    assert.deepEqual(meter.summarize({ flimfy: { score: 5, votes: 4 } }), {
        score: 10, sourceCount: 1, providers: ['FlimfyBox']
    });
});

test('the unrounded member average is used before the final result is rounded', () => {
    assert.equal(meter.summarize({ flimfy: { score: 4.24, votes: 3 } }).score, 8.5);
});

test('missing, invalid, out-of-range, zero-vote, and malformed-vote values are excluded', () => {
    assert.deepEqual(meter.summarize({
        imdb: { score: 'N/A', votes: 100 },
        tmdb: { score: 8.4, votes: 0 },
        flimfy: { score: 6, votes: 'not a number' }
    }), { score: null, sourceCount: 0, providers: [] });
    assert.deepEqual(meter.summarize({ imdb: { score: 11 }, tmdb: { score: -1 } }), {
        score: null, sourceCount: 0, providers: []
    });
    assert.equal(meter.verdictForSummary({ score: null }).label, 'Not enough ratings yet');
    assert.equal(meter.audienceRead({}).toLowerCase(), 'not enough ratings yet.');
});

test('final score is rounded to one decimal only after averaging', () => {
    const scores = { imdb: { score: 7.9 }, tmdb: { score: 8 }, flimfy: { score: 4, votes: 1 } };
    assert.equal(meter.summarize(scores).score, 8);
    assert.deepEqual(meter.summarize(scores), meter.summarize(scores));
});

test('rating meter renders one FlimfyBox aggregate without provider score cards', () => {
    withDocument(() => {
        const container = new Element('section');
        meter.render(container, null, {
            imdb: { score: 8, votes: 100 },
            tmdb: { score: 7.5, votes: 50 },
            flimfy: { score: 4, votes: 2 }
        });
        assert.equal(all(container, 'verdict-brand').length, 1);
        assert.equal(all(container, 'verdict-score').length, 1);
        assert.equal(all(container, 'verdict-score')[0].textContent, '7.8');
        assert.equal(all(container, 'verdict-source-card').length, 0);
        assert.equal(all(container, 'verdict-brand')[0].children[1].textContent, 'FLIMFYBOX RATING');
        assert.equal(all(container, 'verdict-audience-read-copy')[0].textContent,
            'Based on available audience ratings.');

        const empty = new Element('section');
        meter.render(empty, null, { imdb: { score: null }, tmdb: { score: null } });
        assert.equal(all(empty, 'verdict-score')[0].textContent, '—');
        assert.equal(all(empty, 'verdict-scale')[0].hidden, true);
        assert.equal(all(empty, 'verdict-audience-read-copy')[0].textContent, 'Not enough ratings yet.');
    });
});

test('rating and review status distinguish a background refresh from saved outage data', () => {
    withDocument(() => {
        const updatingRating = new Element('section');
        meter.render(updatingRating, null, {
            tmdb: { score: 7.8, votes: 100 }, cache_status: 'stale'
        });
        assert.equal(all(updatingRating, 'verdict-audience-read-copy')[0].textContent,
            'Showing saved ratings · Refreshing in the background.');

        const savedRating = new Element('section');
        meter.render(savedRating, null, {
            tmdb: { score: 7.8, votes: 100 }, cache_status: 'stale_cached'
        });
        assert.equal(all(savedRating, 'verdict-audience-read-copy')[0].textContent,
            'Showing saved ratings · We’ll check again soon.');

        const savedReviews = new Element('section');
        meter.renderReviews(savedReviews, {
            status: 'success', tmdb_cache_status: 'stale_cached',
            tmdb_reviews: [{ id: 'kept', author: 'Saved author', content: 'Saved review.' }]
        });
        assert.equal(all(savedReviews, 'review-refresh-state')[0].textContent, 'Saved');
        assert.equal(all(savedReviews, 'flimfy-review-author')[0].textContent, 'Saved author');
    });
});

test('review counts stay unconfirmed when a provider failed or is not linked', () => {
    withDocument(() => {
        const unlinked = new Element('section');
        meter.renderReviews(unlinked, {
            status: 'success', review_count: 0, reviews: [],
            tmdb_review_count: 0, tmdb_reviews: [], tmdb_linked: false
        });
        assert.equal(unlinked.querySelector('.review-count').textContent, 'Count unavailable');
        assert.match(all(unlinked, 'review-empty')[0].textContent, /Audience reviews are not linked/);

        const failed = new Element('section');
        meter.renderReviews(failed, {
            status: 'success', review_count: 2,
            reviews: [{ id: 1, author: 'Member', content: 'Known member review.' }],
            tmdb_unavailable: true, tmdb_linked: true
        });
        assert.equal(failed.querySelector('.review-count').textContent, 'At least 2 reviews');
        assert.equal(all(failed, 'flimfy-review-copy').length, 1);

        const confirmedEmpty = new Element('section');
        meter.renderReviews(confirmedEmpty, {
            status: 'success', review_count: 0, reviews: [],
            tmdb_review_count: 0, tmdb_reviews: [], tmdb_linked: true
        });
        assert.equal(confirmedEmpty.querySelector('.review-count').textContent, '0 reviews');
        assert.match(all(confirmedEmpty, 'review-empty')[0].textContent, /No reviews yet/);
    });
});

test('rating meter distinguishes an unlinked title from a provider outage', () => {
    withDocument(() => {
        const unlinked = new Element('section');
        meter.render(unlinked, null, {
            imdb: { score: null }, tmdb: { score: null }, cache_status: 'unlinked'
        });
        assert.equal(all(unlinked, 'verdict-audience-read-copy')[0].textContent,
            'No rating source is linked to this title yet.');

        const unavailable = new Element('section');
        meter.render(unavailable, null, {
            imdb: { score: null }, tmdb: { score: null }, cache_status: 'unavailable'
        });
        assert.equal(all(unavailable, 'verdict-audience-read-copy')[0].textContent,
            'Ratings are temporarily unavailable. Try again soon.');
    });
});

test('detail template places one aggregate and personal stars in the same rating card', () => {
    const template = fs.readFileSync(path.join(__dirname, '../templates/mini_app.html'), 'utf8');
    assert.equal((template.match(/id="flimfyVerdict"/g) || []).length, 1);
    assert.equal((template.match(/id="communityRating"/g) || []).length, 1);
    assert.match(template, /class="verdict-meter-panel"[\s\S]*?id="flimfyVerdict"[\s\S]*?id="communityRating"[\s\S]*?<\/section>/);
    assert.doesNotMatch(template, /class="community-rating"[^>]*id="communityRating"[\s\S]*?<\/main>/);
});

test('TMDb-only result routing preserves media type and does not confuse local IDs', () => {
    assert.equal(meter.endpointFor({ id: 77, tmdb_id: 77, source: 'tmdb', media_type: 'tv' }, true),
        '/api/tmdb/tv/77/rating-sources');
    assert.equal(meter.endpointFor({ id: 77, tmdb_id: 900, source: 'local' }),
        '/api/movie/77/rating-sources');
});

test('merged reviews sort newest first and deduplicate by canonical URL, never review text', () => {
    const merged = meter.mergeReviews({
        review_count: 2,
        tmdb_review_count: 3,
        reviews: [
            { id: 1, author: 'Member A', content: 'Same words.', created_at: '2026-10-01T00:00:00Z' },
            { id: 2, author: 'Member B', content: 'Same words.', created_at: '2026-10-03T00:00:00Z' }
        ],
        tmdb_reviews: [
            { id: 'linked', author: 'Original author', content: 'Imported copy.', created_at: '2026-10-02T00:00:00Z', url: 'https://www.themoviedb.org/review/shared?utm_source=app' },
            { id: 'new', author: 'New reviewer', content: 'Latest.', created_at: '2026-10-04T00:00:00Z', url: 'https://www.themoviedb.org/review/new' },
            { id: 'same-words', author: 'Another reviewer', content: 'Same words.', created_at: '2026-10-01T12:00:00Z' }
        ]
    });

    assert.equal(merged.total, 5);
    assert.equal(merged.reviews.length, 5);
    assert.deepEqual(merged.reviews.map(review => review.author), [
        'New reviewer', 'Member B', 'Original author', 'Another reviewer', 'Member A'
    ]);

    const linked = meter.mergeReviews({
        review_count: 1,
        tmdb_review_count: 1,
        reviews: [{ id: 1, author: 'Member', content: 'Original', url: 'https://www.themoviedb.org/review/linked/' }],
        tmdb_reviews: [{ id: 'linked', author: 'Original author', content: 'Same review', url: 'https://www.themoviedb.org/review/linked?ref=feed' }]
    });
    assert.equal(linked.reviews.length, 1);
    assert.equal(linked.total, 1);
    assert.equal(linked.reviews[0].author, 'Original author');
    assert.equal(linked.reviews[0].review_source, 'tmdb');
});

test('review renderer keeps authorship, provenance, text safety, and one total', () => {
    withDocument(() => {
        const reviews = new Element('section');
        meter.renderReviews(reviews, {
            status: 'success', review_count: 1, tmdb_review_count: 1,
            reviews: [{ id: 1, author: 'Mahi', content: '<img src=x onerror=alert(1)>', rating: 5 }],
            tmdb_reviews: [{ id: 'tmdb-1', author: 'Audience reviewer', content: 'A terrific film.', rating: 8.4,
                url: 'https://www.themoviedb.org/review/review-1' }]
        });
        assert.equal(reviews.querySelector('.section-kicker').textContent, 'FLIMFYBOX REVIEWS');
        assert.equal(reviews.querySelector('.review-count').textContent, '2 reviews');
        assert.equal(all(reviews, 'flimfy-review-provenance').length, 2);
        assert.deepEqual(all(reviews, 'flimfy-review-provenance').map(item => item.textContent), [
            'Member review', 'Imported audience review'
        ]);
        assert.deepEqual(all(reviews, 'flimfy-review-author').map(item => item.textContent), [
            'Mahi', 'Audience reviewer'
        ]);
        assert.equal(reviews.querySelector('.flimfy-review-copy').textContent,
            '<img src=x onerror=alert(1)>');
        assert.equal(all(reviews, 'flimfy-review-brand-pill').length, 0);
        const links = all(reviews, 'review-source-link');
        assert.equal(links.length, 1);
        assert.equal(links[0].href, 'https://www.themoviedb.org/review/review-1');
        assert.equal(links[0].rel, 'noopener noreferrer');
    });
});

test('review renderer omits an unsafe imported link and keeps member review editing', () => {
    withDocument(() => {
        const reviews = new Element('section');
        meter.renderReviews(reviews, {
            status: 'success', is_catalogued: true, can_review: true,
            user_review: 'An earlier review with enough characters.', review_count: 1,
            tmdb_reviews: [{ author: 'Audience member', content: 'Imported.', url: 'https://evil.example/review/fake' }]
        });
        assert.equal(all(reviews, 'review-source-link').length, 0);
        assert.equal(reviews.querySelector('.flimfy-review-input').value,
            'An earlier review with enough characters.');
        assert.equal(reviews.querySelector('.flimfy-review-submit').textContent, 'Update review');
    });
});
