const { test } = require('node:test');
const assert = require('node:assert/strict');
const meter = require('../static/miniapp/rating-meter.js');

test('Flimfy Verdict is the one-decimal mean of available IMDb and TMDb scores', () => {
    assert.deepEqual(
        meter.summarize({ imdb: { score: 8.2 }, tmdb: { score: 7.6 } }),
        { score: 7.9, sourceCount: 2, providers: ['IMDb', 'TMDb'] }
    );
});

test('Flimfy community stars are scaled to ten and join the two external sources', () => {
    assert.deepEqual(meter.summarize({
        imdb: { score: 7.1, votes: 84000 },
        tmdb: { score: 7.4, votes: 2100 },
        flimfy: { score: 4.5, votes: 30 }
    }), { score: 7.8, sourceCount: 3, providers: ['IMDb', 'TMDb', 'FlimfyBox'] });
    assert.equal(meter.audienceRead({
        imdb: { score: 7.1 }, tmdb: { score: 7.4 }, flimfy: { score: 4.5, votes: 30 }
    }), 'A split verdict · sources span 1.9 points.');
});

test('Flimfy Verdict gives a memorable watch cue without hiding its numeric score', () => {
    assert.deepEqual(meter.verdictForSummary(meter.summarize({
        imdb: { score: 7.1 }, tmdb: { score: 7.4 }
    })), { label: 'Worth your night', tone: 'positive' });
    assert.deepEqual(meter.verdictForSummary(meter.summarize({
        imdb: { score: 5.8 }, tmdb: { score: 5.9 }
    })), { label: 'For a specific mood', tone: 'niche' });
});

test('source comparison rewards real agreement and calls out real disagreement', () => {
    assert.equal(meter.audienceRead({ imdb: { score: 7.1 }, tmdb: { score: 7.4 } }),
        'IMDb + TMDb are in sync · 0.3 apart.');
    assert.equal(meter.audienceRead({ imdb: { score: 6.1 }, tmdb: { score: 8.0 } }),
        'A split verdict · scores differ by 1.9.');
    assert.equal(meter.audienceRead({ imdb: { score: 7.1 }, tmdb: { score: null } }),
        'IMDb is the only rating available so far.');
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

test('Flimfy community review text stays plain text and never receives a TMDb attribution', () => {
    const previousDocument = global.document;
    class Element {
        constructor(tag) { this.tag = tag; this.className = ''; this.textContent = ''; this.children = []; this.style = {}; }
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
        meter.renderReviews(reviews, {
            status: 'success',
            reviews: [{ author: 'Mahi', content: '<img src=x onerror=alert(1)>', rating: 5 }],
            review_count: 1,
            can_review: false
        });
        assert.equal(reviews.querySelector('.section-kicker').textContent, 'FROM THE FLIMFYBOX COMMUNITY');
        assert.equal(reviews.querySelector('.review-count').textContent, '1 review');
        assert.equal(reviews.querySelector('.flimfy-review-copy').textContent, '<img src=x onerror=alert(1)>');
        const hasLink = item => item.tag === 'a' || item.children.some(hasLink);
        assert.equal(reviews.children.some(hasLink), false);
    } finally {
        global.document = previousDocument;
    }
});

test('FlimfyBox source explains an empty community score without implying an outage', () => {
    const previousDocument = global.document;
    class Element {
        constructor(tag) { this.tag = tag; this.className = ''; this.textContent = ''; this.children = []; this.style = {}; }
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
        const meterContainer = new Element('section');
        meter.render(meterContainer, null, {
            imdb: { score: 7.1, votes: 84000 },
            tmdb: { score: 7.4, votes: 2100 },
            flimfy: { score: null, votes: 0 }
        });
        const flimfyCard = meterContainer.children[4].children.find(card => card.className.includes('flimfy'));
        const votes = flimfyCard.children.find(child => child.className === 'verdict-source-votes');
        assert.equal(votes.textContent, 'No member ratings yet');
    } finally {
        global.document = previousDocument;
    }
});

test('public TMDb reviews stay labeled as TMDb and only link to real TMDb review pages', () => {
    const previousDocument = global.document;
    class Element {
        constructor(tag) { this.tag = tag; this.className = ''; this.textContent = ''; this.children = []; this.style = {}; }
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
        meter.renderAudienceReviews(reviews, {
            status: 'success', source: 'TMDb', linked: true, review_count: 9,
            reviews: [{
                author: 'TMDb reviewer', content: 'A terrific film.', rating: 8.4,
                url: 'https://www.themoviedb.org/review/review-1'
            }, {
                author: 'Another reviewer', content: 'An untrusted link is omitted.',
                url: 'https://evil.example/review/fake'
            }]
        });
        const heading = reviews.children[0].children.find(child => child.tag === 'h2');
        assert.equal(heading.textContent, 'TMDb member reviews');
        assert.equal(reviews.querySelector('.tmdb-source-pill').textContent, 'TMDb');
        assert.equal(reviews.querySelector('.review-count').textContent, '9 on TMDb');
        const links = [];
        const collectLinks = item => {
            if (item.tag === 'a') links.push(item);
            item.children.forEach(collectLinks);
        };
        reviews.children.forEach(collectLinks);
        assert.equal(links.length, 1);
        assert.equal(links[0].href, 'https://www.themoviedb.org/review/review-1');
        assert.equal(links[0].rel, 'noopener noreferrer');
        assert.equal(reviews.querySelector('.flimfy-review-author').textContent, 'TMDb reviewer');

        const unlinked = new Element('section');
        meter.renderAudienceReviews(unlinked, {
            status: 'success', linked: false, review_count: 0, reviews: []
        });
        assert.equal(unlinked.querySelector('.review-count').textContent, 'TMDb source');
        assert.match(unlinked.querySelector('.review-empty').textContent, /not linked to a TMDb entry/);
    } finally {
        global.document = previousDocument;
    }
});

