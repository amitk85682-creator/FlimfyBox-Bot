(function attachMoctaleMeter(root) {
    'use strict';

    const providerDefinitions = [
        { key: 'imdb', label: 'IMDb' },
        { key: 'tmdb', label: 'TMDb' }
    ];

    function normalizedScore(value) {
        if (typeof value === 'boolean' || value === null || value === undefined || value === '') return null;
        const score = Number(value);
        if (!Number.isFinite(score) || score < 0 || score > 10) return null;
        return Math.round(score * 10) / 10;
    }

    function providerScore(payload, key) {
        const provider = payload?.[key];
        const score = normalizedScore(provider?.score);
        const hasVotes = provider?.votes !== null && provider?.votes !== undefined && provider?.votes !== '';
        if (score === null || (hasVotes && Number(provider.votes) === 0)) return null;
        return score;
    }

    function summarize(payload = {}) {
        const providers = providerDefinitions
            .filter(provider => providerScore(payload, provider.key) !== null)
            .map(provider => provider.label);
        const scores = providerDefinitions
            .map(provider => providerScore(payload, provider.key))
            .filter(score => score !== null);
        const score = scores.length
            ? Math.round((scores.reduce((total, value) => total + value, 0) / scores.length) * 10) / 10
            : null;
        return { score, sourceCount: scores.length, providers };
    }

    function endpointFor(movie, isTMDB = false) {
        if (!movie || typeof movie !== 'object') return null;
        const rawId = String(movie.id ?? '').trim();
        const isTMDBTitle = isTMDB || movie.source === 'tmdb' || /^tmdb_\d+$/.test(rawId);
        if (isTMDBTitle) {
            const tmdbId = String(movie.tmdb_id || rawId).replace(/^tmdb_/, '').trim();
            if (!/^\d+$/.test(tmdbId)) return null;
            const typeHint = [movie.media_type, movie.content_type, movie.category]
                .filter(Boolean).join(' ').toLowerCase();
            const mediaType = /\b(tv|series|web\s*series)\b/.test(typeHint) ? 'tv' : 'movie';
            return `/api/tmdb/${mediaType}/${encodeURIComponent(tmdbId)}/rating-sources`;
        }
        if (!/^\d+$/.test(rawId)) return null;
        return `/api/movie/${encodeURIComponent(rawId)}/rating-sources`;
    }

    function isTrustedReviewUrl(value) {
        try {
            const url = new URL(String(value || ''));
            return url.protocol === 'https:'
                && url.hostname === 'www.themoviedb.org'
                && url.pathname.startsWith('/review/');
        } catch (_error) {
            return false;
        }
    }

    function node(tag, className, text) {
        const element = document.createElement(tag);
        if (className) element.className = className;
        if (text !== undefined) element.textContent = String(text);
        return element;
    }

    function formatVotes(votes) {
        if (votes === null || votes === undefined || votes === '') return 'Vote count unavailable';
        const raw = String(votes).trim();
        const numeric = Number(raw.replace(/,/g, ''));
        const formatted = Number.isFinite(numeric) && numeric >= 0
            ? Math.round(numeric).toLocaleString()
            : raw;
        return `${formatted} votes`;
    }

    function formatReviewDate(value) {
        if (!value) return '';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return '';
        return new Intl.DateTimeFormat(undefined, { month: 'short', year: 'numeric' }).format(date);
    }

    function renderSources(container, payload, summary) {
        const sourceGrid = node('div', 'moctale-source-grid');
        providerDefinitions.forEach(provider => {
            const rating = payload?.[provider.key] || {};
            const score = providerScore(payload, provider.key);
            const card = node('div', `moctale-source-card ${provider.key}`);
            const name = node('span', 'moctale-source-name', provider.label);
            const value = node('strong', 'moctale-source-score', score === null ? '—' : score.toFixed(1));
            const votes = node('span', 'moctale-source-votes', score === null
                ? 'Not available'
                : formatVotes(rating.votes));
            card.append(name, value, votes);
            sourceGrid.append(card);
        });
        container.append(sourceGrid);
        const explanation = summary.sourceCount === 2
            ? 'Equal blend of IMDb + TMDb'
            : summary.sourceCount === 1
                ? `Based on ${summary.providers[0]} only`
                : 'No verified provider scores yet';
        container.append(node('p', 'moctale-method', explanation));
    }

    function renderMeter(container, payload) {
        const summary = summarize(payload?.scores || payload || {});
        const heading = node('div', 'moctale-heading');
        heading.append(node('span', 'section-kicker', 'MOCTALE METER'));
        heading.append(node('span', 'moctale-live-tag', 'SOURCE BLEND'));
        container.append(heading);

        const body = node('div', 'moctale-meter-body');
        const ring = node('div', 'moctale-score-ring');
        ring.setAttribute('role', 'img');
        ring.setAttribute('aria-label', summary.score === null
            ? 'Moctale Meter: not enough data'
            : `Moctale Meter: ${summary.score.toFixed(1)} out of 10`);
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('viewBox', '0 0 120 120');
        svg.setAttribute('aria-hidden', 'true');
        const track = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        track.setAttribute('class', 'moctale-ring-track');
        track.setAttribute('cx', '60'); track.setAttribute('cy', '60'); track.setAttribute('r', '52');
        const progress = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        progress.setAttribute('class', 'moctale-ring-progress');
        progress.setAttribute('cx', '60'); progress.setAttribute('cy', '60'); progress.setAttribute('r', '52');
        progress.setAttribute('transform', 'rotate(-90 60 60)');
        const circumference = 2 * Math.PI * 52;
        progress.setAttribute('stroke-dasharray', `${circumference.toFixed(2)} ${circumference.toFixed(2)}`);
        progress.setAttribute('stroke-dashoffset', summary.score === null
            ? String(circumference)
            : String(circumference * (1 - summary.score / 10)));
        svg.append(track, progress);
        const scoreCopy = node('span', 'moctale-score-copy');
        scoreCopy.append(
            node('strong', 'moctale-score-value', summary.score === null ? '—' : summary.score.toFixed(1)),
            node('span', 'moctale-score-scale', '/ 10')
        );
        ring.append(svg, scoreCopy);

        const context = node('div', 'moctale-context');
        context.append(
            node('strong', 'moctale-context-title', summary.score === null ? 'Waiting for ratings' : 'One clear signal'),
            node('span', 'moctale-context-copy', summary.sourceCount === 2
                ? 'A transparent blend across two independent movie databases.'
                : summary.sourceCount === 1
                    ? 'One provider score is available. The Meter updates when another source is found.'
                    : 'IMDb and TMDb scores will appear here when available.')
        );
        body.append(ring, context);
        container.append(body);
        renderSources(container, payload?.scores || payload || {}, summary);
    }

    function renderReviews(container, payload) {
        const reviews = Array.isArray(payload?.reviews) ? payload.reviews.slice(0, 3) : [];
        const heading = node('div', 'detail-section-heading');
        heading.append(
            node('span', 'section-kicker', 'FROM THE COMMUNITY'),
            node('h2', '', 'TMDb community reviews')
        );
        const count = payload?.review_count;
        const hasReviewCount = count !== null && count !== undefined && count !== ''
            && Number.isFinite(Number(count));
        heading.append(node('span', 'review-count', hasReviewCount
            ? `${Number(count).toLocaleString()} reviews`
            : 'TMDb'));
        container.append(heading);

        if (!reviews.length) {
            container.append(node('p', 'review-empty', 'No written TMDb reviews are available for this title yet.'));
            return;
        }

        const list = node('div', 'tmdb-review-list');
        reviews.forEach(review => {
            const card = node('article', 'tmdb-review-card');
            const meta = node('div', 'tmdb-review-meta');
            meta.append(node('strong', 'tmdb-review-author', review.author || 'TMDb member'));
            const date = formatReviewDate(review.created_at);
            if (date) meta.append(node('span', 'tmdb-review-date', date));
            const reviewRating = normalizedScore(review.rating);
            if (reviewRating !== null) meta.append(node('span', 'tmdb-review-rating', `★ ${reviewRating.toFixed(1)}`));
            card.append(meta, node('p', 'tmdb-review-copy', review.content || ''));
            if (isTrustedReviewUrl(review.url)) {
                const link = node('a', 'tmdb-review-link', 'Read on TMDb');
                link.href = review.url;
                link.target = '_blank';
                link.rel = 'noopener noreferrer';
                card.append(link);
            }
            list.append(card);
        });
        container.append(list);
    }

    const api = {
        summarize,
        endpointFor,
        isTrustedReviewUrl,
        render(meterContainer, reviewsContainer, payload) {
            if (meterContainer) {
                meterContainer.replaceChildren();
                renderMeter(meterContainer, payload || {});
            }
            if (reviewsContainer) {
                reviewsContainer.replaceChildren();
                renderReviews(reviewsContainer, payload || {});
            }
        },
        renderLoading(meterContainer, reviewsContainer) {
            if (meterContainer) {
                meterContainer.replaceChildren();
                meterContainer.append(node('span', 'section-kicker', 'MOCTALE METER'));
                meterContainer.append(node('p', 'moctale-loading', 'Gathering IMDb + TMDb scores…'));
            }
            if (reviewsContainer) {
                reviewsContainer.replaceChildren();
                reviewsContainer.append(node('span', 'section-kicker', 'FROM THE COMMUNITY'));
                reviewsContainer.append(node('p', 'review-empty', 'Loading community reviews…'));
            }
        },
        renderUnavailable(meterContainer, reviewsContainer) {
            this.render(meterContainer, reviewsContainer, {
                imdb: { score: null, votes: null },
                tmdb: { score: null, votes: null },
                reviews: [],
                review_count: null
            });
            const copy = meterContainer?.querySelector('.moctale-context-copy');
            if (copy) copy.textContent = 'Ratings are temporarily unavailable. Your movie details and downloads are still ready.';
        }
    };

    root.MoctaleMeter = api;
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
