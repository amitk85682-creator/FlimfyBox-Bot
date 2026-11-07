(function attachFlimfyVerdict(root) {
    'use strict';

    const providerDefinitions = [
        { key: 'imdb', label: 'IMDb', scale: 10 },
        { key: 'tmdb', label: 'TMDb', scale: 10 },
        { key: 'flimfy', label: 'FlimfyBox', scale: 5 }
    ];
    const ratingsByContainer = new WeakMap();

    function validScore(value, maximum = 10) {
        if (typeof value === 'boolean' || value === null || value === undefined
            || value === '' || (typeof value === 'string' && value.trim() === '')) return null;
        const score = Number(value);
        if (!Number.isFinite(score) || score < 0 || score > maximum) return null;
        return score;
    }

    function providerScore(payload, key) {
        const provider = payload?.[key];
        const definition = providerDefinitions.find(item => item.key === key);
        const score = validScore(provider?.score, definition?.scale || 10);
        const hasVotes = provider?.votes !== null && provider?.votes !== undefined && provider?.votes !== '';
        const votes = hasVotes ? Number(String(provider.votes).replace(/,/g, '').trim()) : null;
        if (score === null || !definition || (hasVotes && (!Number.isFinite(votes) || votes <= 0))) return null;
        return score * (10 / definition.scale);
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
        if (!Number.isFinite(score)) return { label: 'Not enough ratings yet', tone: 'quiet' };
        if (score >= 8) return { label: 'A standout pick', tone: 'excellent' };
        if (score >= 7) return { label: 'Worth your night', tone: 'positive' };
        if (score >= 6) return { label: 'See the trailer first', tone: 'curious' };
        return { label: 'For a specific mood', tone: 'niche' };
    }

    function audienceRead(payload = {}) {
        return summarize(payload).score === null
            ? 'Not enough ratings yet.'
            : 'Based on available audience ratings.';
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

    function node(tag, className, text) {
        const element = document.createElement(tag);
        if (className) element.className = className;
        if (text !== undefined) element.textContent = String(text);
        return element;
    }

    function formatReviewDate(value) {
        if (!value) return '';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return '';
        return new Intl.DateTimeFormat(undefined, { month: 'short', year: 'numeric' }).format(date);
    }

    function trustedTmdbReviewUrl(value) {
        try {
            const url = new URL(String(value || ''));
            return url.protocol === 'https:'
                && url.hostname === 'www.themoviedb.org'
                && (!url.port || url.port === '443')
                && !url.username && !url.password
                && /^\/review\/[A-Za-z0-9_-]+\/?$/.test(url.pathname)
                ? url.href
                : '';
        } catch (_error) {
            return '';
        }
    }

    function renderMeter(container, payload) {
        const summary = summarize(payload?.scores || payload || {});
        const scores = payload?.scores || payload || {};
        const verdict = verdictForSummary(summary);
        const heading = node('div', 'verdict-heading');
        const brand = node('span', 'verdict-brand');
        brand.append(node('span', 'verdict-brand-icon', 'F'), node('span', '', 'FLIMFYBOX RATING'));
        heading.append(brand);
        container.append(heading);

        const hero = node('div', 'verdict-hero');
        const scoreLockup = node('div', 'verdict-score-lockup');
        scoreLockup.setAttribute('role', 'img');
        scoreLockup.setAttribute('aria-label', summary.score === null
            ? 'FlimfyBox rating: not enough ratings yet'
            : `FlimfyBox rating: ${summary.score.toFixed(1)} out of 10`);
        scoreLockup.append(
            node('strong', 'verdict-score', summary.score === null ? '—' : summary.score.toFixed(1)),
            node('span', 'verdict-score-scale', '/10')
        );
        const answer = node('div', `verdict-answer tone-${verdict.tone}`);
        answer.append(
            node('span', 'verdict-question', 'Worth your night?'),
            node('strong', 'verdict-answer-label', verdict.label),
            node('span', 'verdict-identity', summary.score === null ? 'Waiting for audience ratings' : 'One score from available audience ratings')
        );
        hero.append(scoreLockup, answer);
        container.append(hero);

        const read = node('div', 'verdict-audience-read');
        let audienceCopy = audienceRead(scores);
        if (payload?.cache_status === 'stale') {
            audienceCopy = summary.score === null
                ? 'Checking for fresh ratings…'
                : 'Showing saved ratings · Refreshing in the background.';
        } else if (payload?.cache_status === 'stale_cached') {
            audienceCopy = summary.score === null
                ? 'Showing saved ratings while providers recover.'
                : 'Showing saved ratings · We’ll check again soon.';
        } else if (payload?.cache_status === 'refreshing') {
            audienceCopy = summary.score === null
                ? 'Loading audience ratings…'
                : 'Checking for fresh ratings…';
        } else if (payload?.cache_status === 'partial') {
            audienceCopy = summary.score === null
                ? 'Some rating sources are unavailable. Try again soon.'
                : 'Based on available audience ratings. Some sources are unavailable.';
        } else if (payload?.cache_status === 'unlinked' && summary.score === null) {
            audienceCopy = 'No rating source is linked to this title yet.';
        } else if (payload?.cache_status === 'not_configured' && summary.score === null) {
            audienceCopy = 'Rating sources are not available right now.';
        } else if (payload?.cache_status === 'unavailable' && summary.score === null) {
            audienceCopy = 'Ratings are temporarily unavailable. Try again soon.';
        }
        read.append(node('span', 'verdict-audience-dot'), node('span', 'verdict-audience-read-copy', audienceCopy));
        container.append(read);

        const scale = node('div', 'verdict-scale');
        scale.hidden = summary.score === null;
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
    }

    function canonicalReviewUrl(value) {
        const safeUrl = trustedTmdbReviewUrl(value);
        if (!safeUrl) return '';
        try {
            const url = new URL(safeUrl);
            url.search = '';
            url.hash = '';
            url.pathname = url.pathname.replace(/\/$/, '');
            return url.href;
        } catch (_error) {
            return '';
        }
    }

    function reviewSource(review) {
        return String(review?.review_source || review?.source || '').toLowerCase().includes('tmdb')
            ? 'tmdb' : 'flimfybox';
    }

    function mergeReviews(payload = {}) {
        const localReviews = Array.isArray(payload?.reviews)
            ? payload.reviews.slice(0, 10).map(review => ({ ...review, review_source: 'flimfybox' }))
            : [];
        const importedReviews = Array.isArray(payload?.tmdb_reviews)
            ? payload.tmdb_reviews.slice(0, 5).map(review => ({ ...review, review_source: 'tmdb' }))
            : [];
        const merged = [];
        const seen = new Map();
        const crossSourceDuplicateIds = new Set();
        [...localReviews, ...importedReviews].forEach((review, index) => {
            const source = reviewSource(review);
            const canonicalUrl = canonicalReviewUrl(review.url);
            const sourceId = String(review.id ?? review.review_id ?? '').trim();
            const identity = canonicalUrl
                ? `url:${canonicalUrl}`
                : sourceId ? `id:${source}:${sourceId}` : '';
            if (identity && seen.has(identity)) {
                const previous = seen.get(identity);
                if (previous.source !== source && identity.startsWith('url:')) {
                    crossSourceDuplicateIds.add(identity);
                }
                // Keep the original provider's author and source when both feeds contain the same linked review.
                if (previous.source !== 'tmdb' && source === 'tmdb') {
                    merged[previous.index] = {
                        ...review,
                        review_source: 'tmdb',
                        _mergeIndex: merged[previous.index]?._mergeIndex ?? index
                    };
                    seen.set(identity, { source, index: previous.index });
                }
                return;
            }
            const item = { ...review, review_source: source, _mergeIndex: index };
            if (identity) seen.set(identity, { source, index: merged.length });
            merged.push(item);
        });
        merged.sort((left, right) => {
            const leftTime = reviewTimestamp(left);
            const rightTime = reviewTimestamp(right);
            if (leftTime === null && rightTime === null) return left._mergeIndex - right._mergeIndex;
            if (leftTime === null) return 1;
            if (rightTime === null) return -1;
            return rightTime - leftTime || left._mergeIndex - right._mergeIndex;
        });
        merged.forEach(review => { delete review._mergeIndex; });
        const localCount = safeReviewCount(payload?.review_count, localReviews.length);
        const importedCount = safeReviewCount(payload?.tmdb_review_count, importedReviews.length);
        const crossSourceDuplicates = crossSourceDuplicateIds.size;
        const total = Math.max(0, localCount + importedCount - crossSourceDuplicates);
        const loading = Boolean(payload?.tmdb_loading || payload?.flimfy_loading);
        const incomplete = Boolean(loading || payload?.tmdb_unavailable || payload?.flimfy_unavailable
            || payload?.tmdb_linked === false);
        return { reviews: merged, total, crossSourceDuplicates, incomplete, loading };
    }

    function reviewTimestamp(review) {
        for (const value of [review?.updated_at, review?.created_at]) {
            if (!value) continue;
            const timestamp = new Date(value).getTime();
            if (Number.isFinite(timestamp)) return timestamp;
        }
        return null;
    }

    function safeReviewCount(value, fallback) {
        if (value === null || value === undefined || value === '') return fallback;
        const count = Number(value);
        return Number.isFinite(count) && count >= 0 ? Math.max(Math.floor(count), fallback) : fallback;
    }

    function renderReviews(container, payload = {}) {
        const { reviews, total, incomplete, loading } = mergeReviews(payload);
        const heading = node('div', 'detail-section-heading');
        heading.append(
            node('span', 'section-kicker', 'FLIMFYBOX REVIEWS'),
            node('h2', '', 'Reviews')
        );
        const countLabel = payload?.status === 'loading' ? 'Loading…'
            : payload?.status !== undefined && payload?.status !== 'success' ? 'Unavailable'
                : incomplete
                    ? total > 0 ? `At least ${total.toLocaleString()} ${total === 1 ? 'review' : 'reviews'}` : loading ? 'Updating…' : 'Count unavailable'
                    : `${total.toLocaleString()} ${total === 1 ? 'review' : 'reviews'}`;
        heading.append(node('span', 'review-count', countLabel));
        if (loading || payload?.tmdb_cache_status === 'stale' || payload?.tmdb_cache_status === 'refreshing') {
            heading.append(node('span', 'review-refresh-state', 'Updating'));
        } else if (payload?.tmdb_cache_status === 'stale_cached') {
            heading.append(node('span', 'review-refresh-state', 'Saved'));
        }
        container.append(heading);

        if (payload?.status === 'loading') {
            container.append(node('p', 'review-empty', 'Loading reviews…'));
            return;
        }
        if (payload?.status !== 'success' && payload?.status !== undefined) {
            container.append(node('p', 'review-empty', 'Reviews are temporarily unavailable.'));
            return;
        }

        if (!reviews.length) {
            const emptyCopy = payload?.is_upcoming
                ? 'Reviews open after this title is released.'
                : payload?.tmdb_unavailable || payload?.flimfy_unavailable
                    ? 'Some reviews could not be loaded. Please try again later.'
                    : loading
                        ? 'Checking available review sources…'
                    : payload?.tmdb_linked === false
                        ? 'No FlimfyBox member reviews yet. Audience reviews are not linked for this title.'
                        : 'No reviews yet. Be the first to share what you thought.';
            container.append(node('p', 'review-empty', emptyCopy));
        } else {
            const list = node('div', 'flimfy-review-list');
            reviews.forEach(review => {
                const card = node('article', 'flimfy-review-card');
                const meta = node('div', 'flimfy-review-meta');
                const isImportedReview = review.review_source === 'tmdb';
                meta.append(node('strong', 'flimfy-review-author', review.author || (isImportedReview ? 'Audience member' : 'FlimfyBox member')));
                meta.append(node('span', 'flimfy-review-provenance', isImportedReview ? 'Imported audience review' : 'Member review'));
                const date = formatReviewDate(review.updated_at || review.created_at);
                if (date) meta.append(node('span', 'flimfy-review-date', date));
                const reviewRating = validScore(review.rating, isImportedReview ? 10 : 5);
                if (reviewRating !== null) meta.append(node('span', 'flimfy-review-rating', `★ ${(Math.round(reviewRating * 10) / 10).toFixed(1)} / ${isImportedReview ? 10 : 5}`));
                card.append(meta, node('p', 'flimfy-review-copy', review.content || ''));
                if (isImportedReview) {
                    const url = trustedTmdbReviewUrl(review.url);
                    if (url) {
                        const sourceLink = node('a', 'review-source-link', 'View original review');
                        sourceLink.href = url;
                        sourceLink.target = '_blank';
                        sourceLink.rel = 'noopener noreferrer';
                        card.append(sourceLink);
                    }
                }
                list.append(card);
            });
            container.append(list);
        }
        if (payload?.tmdb_linked === false) {
            container.append(node('p', 'flimfy-review-readonly', 'More audience reviews will appear when this title is linked.'));
        } else if ((payload?.tmdb_unavailable || payload?.flimfy_unavailable) && reviews.length) {
            container.append(node('p', 'flimfy-review-readonly', 'Some audience reviews could not be loaded.'));
        }

        if (payload?.can_review) {
            const form = node('form', 'flimfy-review-form');
            const label = node('label', '', payload.user_review ? 'Update your review' : 'Write a review');
            label.htmlFor = 'flimfyReviewInput';
            const input = node('textarea', 'flimfy-review-input');
            input.id = 'flimfyReviewInput';
            input.name = 'review';
            input.rows = 4;
            input.maxLength = 800;
            input.minLength = 20;
            input.required = true;
            input.placeholder = 'What stayed with you after watching?';
            input.value = payload.user_review || '';
            input.setAttribute('aria-describedby', 'flimfyReviewHelp flimfyReviewStatus');
            const help = node('p', 'flimfy-review-help', '20–800 characters. Your first name is shown with your review.');
            help.id = 'flimfyReviewHelp';
            const status = node('p', 'flimfy-review-status');
            status.id = 'flimfyReviewStatus';
            status.setAttribute('aria-live', 'polite');
            const submit = node('button', 'flimfy-review-submit', payload.user_review ? 'Update review' : 'Post review');
            submit.type = 'submit';
            form.append(label, input, help, status, submit);
            container.append(form);
        } else if (payload?.is_catalogued && !payload?.is_upcoming) {
            container.append(node('p', 'flimfy-review-readonly', 'Open FlimfyBox in Telegram to post or update your review.'));
        }
    }

    const api = {
        summarize,
        verdictForSummary,
        audienceRead,
        mergeReviews,
        endpointFor,
        render(meterContainer, reviewsContainer, payload) {
            if (meterContainer) {
                const scores = { ...(payload?.scores || payload || {}) };
                const existing = ratingsByContainer.get(meterContainer);
                if (!scores.flimfy && existing?.flimfy) scores.flimfy = existing.flimfy;
                ratingsByContainer.set(meterContainer, scores);
                meterContainer.replaceChildren();
                renderMeter(meterContainer, scores);
            }
            if (reviewsContainer) {
                reviewsContainer.replaceChildren();
                renderReviews(reviewsContainer, payload || {});
            }
        },
        renderReviews(reviewContainer, payload) {
            if (!reviewContainer) return;
            reviewContainer.replaceChildren();
            renderReviews(reviewContainer, payload || {});
        },
        setCommunityRating(meterContainer, data) {
            if (!meterContainer) return;
            const scores = { ...(ratingsByContainer.get(meterContainer) || {}) };
            const count = Number(data?.count);
            const average = validScore(data?.average, 5);
            scores.flimfy = {
                score: Number.isFinite(count) && count > 0 ? average : null,
                votes: Number.isFinite(count) && count > 0 ? count : 0
            };
            ratingsByContainer.set(meterContainer, scores);
            meterContainer.replaceChildren();
            renderMeter(meterContainer, scores);
        },
        renderLoading(meterContainer, reviewsContainer) {
            if (meterContainer) {
                ratingsByContainer.delete(meterContainer);
                meterContainer.replaceChildren();
                meterContainer.append(node('span', 'section-kicker', 'FLIMFYBOX RATING'));
                meterContainer.append(node('p', 'verdict-loading', 'Loading audience ratings…'));
            }
            if (reviewsContainer) {
                reviewsContainer.replaceChildren();
                renderReviews(reviewsContainer, { status: 'loading' });
            }
        },
        renderUnavailable(meterContainer, reviewsContainer) {
            this.render(meterContainer, null, {
                imdb: { score: null, votes: null },
                tmdb: { score: null, votes: null }
            });
            const copy = meterContainer?.querySelector('.verdict-audience-read-copy');
            if (copy && summarize(ratingsByContainer.get(meterContainer) || {}).sourceCount === 0) {
                copy.textContent = 'Ratings are temporarily unavailable. Your movie details and downloads are still ready.';
            }
            if (reviewsContainer) this.renderReviews(reviewsContainer, { status: 'unavailable' });
        }
    };

    root.FlimfyVerdict = api;
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
