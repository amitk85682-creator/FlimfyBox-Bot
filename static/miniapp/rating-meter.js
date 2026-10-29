(function attachFlimfyVerdict(root) {
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

    function verdictForSummary(summary) {
        const score = summary?.score;
        if (!Number.isFinite(score)) return { label: 'Too early to call', tone: 'quiet' };
        if (score >= 8) return { label: 'A standout pick', tone: 'excellent' };
        if (score >= 7) return { label: 'Worth your night', tone: 'positive' };
        if (score >= 6) return { label: 'See the trailer first', tone: 'curious' };
        return { label: 'For a specific mood', tone: 'niche' };
    }

    function audienceRead(payload = {}) {
        const imdb = providerScore(payload, 'imdb');
        const tmdb = providerScore(payload, 'tmdb');
        if (imdb === null && tmdb === null) return 'Verified IMDb and TMDb scores will appear here when available.';
        if (imdb === null) return 'IMDb score is unavailable · showing TMDb alone.';
        if (tmdb === null) return 'TMDb score is unavailable · showing IMDb alone.';
        const gap = Math.round(Math.abs(imdb - tmdb) * 10) / 10;
        if (gap <= 0.5) return `IMDb + TMDb are in sync · ${gap.toFixed(1)} apart.`;
        if (gap >= 1.5) return `A split verdict · scores differ by ${gap.toFixed(1)}.`;
        return `Close scores · ${gap.toFixed(1)} apart.`;
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

    function renderSources(container, payload) {
        const sourceGrid = node('div', 'verdict-source-grid');
        providerDefinitions.forEach(provider => {
            const rating = payload?.[provider.key] || {};
            const score = providerScore(payload, provider.key);
            const card = node('div', `verdict-source-card ${provider.key}`);
            const meta = node('div', 'verdict-source-meta');
            meta.append(node('span', 'verdict-source-mark', provider.key === 'imdb' ? 'i' : 'T'));
            meta.append(node('span', 'verdict-source-name', provider.label));
            const value = node('strong', 'verdict-source-score', score === null ? '—' : score.toFixed(1));
            const votes = node('span', 'verdict-source-votes', score === null
                ? 'Not available'
                : formatVotes(rating.votes));
            const rail = node('span', 'verdict-source-rail');
            const fill = node('span', 'verdict-source-fill');
            fill.style.width = score === null ? '0%' : `${score * 10}%`;
            rail.append(fill);
            card.append(meta, value, votes, rail);
            sourceGrid.append(card);
        });
        container.append(sourceGrid);
        container.append(node('p', 'verdict-method', 'Simple mean of available scores · vote totals stay separate.'));
    }

    function renderMeter(container, payload) {
        const summary = summarize(payload?.scores || payload || {});
        const scores = payload?.scores || payload || {};
        const verdict = verdictForSummary(summary);
        const heading = node('div', 'verdict-heading');
        const brand = node('span', 'verdict-brand');
        brand.append(node('span', 'verdict-brand-icon', 'F'), node('span', '', 'FLIMFY VERDICT'));
        heading.append(brand);
        heading.append(node('span', 'verdict-source-count', `${summary.sourceCount} ${summary.sourceCount === 1 ? 'SOURCE' : 'SOURCES'}`));
        container.append(heading);

        const hero = node('div', 'verdict-hero');
        const scoreLockup = node('div', 'verdict-score-lockup');
        scoreLockup.setAttribute('role', 'img');
        scoreLockup.setAttribute('aria-label', summary.score === null
            ? 'Flimfy Verdict: not enough data'
            : `Flimfy Verdict: ${summary.score.toFixed(1)} out of 10`);
        scoreLockup.append(
            node('strong', 'verdict-score', summary.score === null ? '—' : summary.score.toFixed(1)),
            node('span', 'verdict-score-scale', '/10')
        );
        const answer = node('div', `verdict-answer tone-${verdict.tone}`);
        answer.append(
            node('span', 'verdict-question', 'Worth your night?'),
            node('strong', 'verdict-answer-label', verdict.label),
            node('span', 'verdict-identity', summary.score === null ? 'Waiting for verified ratings' : 'A quick read of the ratings')
        );
        hero.append(scoreLockup, answer);
        container.append(hero);

        const read = node('div', 'verdict-audience-read');
        read.append(node('span', 'verdict-audience-dot'), node('span', 'verdict-audience-read-copy', audienceRead(scores)));
        container.append(read);

        const scale = node('div', 'verdict-scale');
        const track = node('div', 'verdict-scale-track');
        const fill = node('span', 'verdict-scale-fill');
        const marker = node('span', 'verdict-scale-marker');
        const percentage = summary.score === null ? 0 : summary.score * 10;
        fill.style.width = `${percentage}%`;
        marker.style.left = `${percentage}%`;
        track.append(fill, marker);
        scale.append(track);
        const labels = node('div', 'verdict-scale-labels');
        labels.append(node('span', '', 'LOW'), node('span', '', 'GOOD'), node('span', '', 'GREAT'));
        scale.append(labels);
        container.append(scale);
        renderSources(container, scores);
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
        verdictForSummary,
        audienceRead,
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
                meterContainer.append(node('span', 'section-kicker', 'FLIMFY VERDICT'));
                meterContainer.append(node('p', 'verdict-loading', 'Reading IMDb + TMDb scores…'));
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
            const copy = meterContainer?.querySelector('.verdict-audience-read-copy');
            if (copy) copy.textContent = 'Ratings are temporarily unavailable. Your movie details and downloads are still ready.';
        }
    };

    root.FlimfyVerdict = api;
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
