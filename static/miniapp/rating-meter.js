(function attachFlimfyVerdict(root) {
    'use strict';

    const providerDefinitions = [
        { key: 'imdb', label: 'IMDb', scale: 10, mark: 'i', quantity: 'votes' },
        { key: 'tmdb', label: 'TMDb', scale: 10, mark: 'T', quantity: 'votes' },
        { key: 'flimfy', label: 'FlimfyBox', scale: 5, mark: 'F', quantity: 'ratings' }
    ];
    const ratingsByContainer = new WeakMap();

    function normalizedScore(value, maximum = 10) {
        if (typeof value === 'boolean' || value === null || value === undefined || value === '') return null;
        const score = Number(value);
        if (!Number.isFinite(score) || score < 0 || score > maximum) return null;
        return Math.round(score * 10) / 10;
    }

    function providerDisplayScore(payload, key) {
        const provider = payload?.[key];
        const definition = providerDefinitions.find(item => item.key === key);
        const score = normalizedScore(provider?.score, definition?.scale || 10);
        const hasVotes = provider?.votes !== null && provider?.votes !== undefined && provider?.votes !== '';
        if (score === null || (hasVotes && Number(provider.votes) === 0)) return null;
        return score;
    }

    function providerScore(payload, key) {
        const definition = providerDefinitions.find(item => item.key === key);
        const score = providerDisplayScore(payload, key);
        return score === null || !definition ? null : Math.round(score * (10 / definition.scale) * 10) / 10;
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
        const available = providerDefinitions
            .map(provider => ({ ...provider, score: providerScore(payload, provider.key) }))
            .filter(provider => provider.score !== null);
        if (!available.length) return 'IMDb, TMDb and FlimfyBox ratings will appear here when available.';
        if (available.length === 1) return `${available[0].label} is the only rating available so far.`;
        const values = available.map(provider => provider.score);
        const gap = Math.round((Math.max(...values) - Math.min(...values)) * 10) / 10;
        const names = available.length === 2
            ? `${available[0].label} + ${available[1].label}`
            : `${available[0].label}, ${available[1].label} + ${available[2].label}`;
        if (available.length === 2 && available[0].key === 'imdb' && available[1].key === 'tmdb') {
            if (gap <= 0.5) return `IMDb + TMDb are in sync · ${gap.toFixed(1)} apart.`;
            if (gap >= 1.5) return `A split verdict · scores differ by ${gap.toFixed(1)}.`;
            return `Close scores · ${gap.toFixed(1)} apart.`;
        }
        if (gap <= 0.5) return `${names} are in sync · ${gap.toFixed(1)} apart.`;
        if (gap >= 1.5) return `A split verdict · sources span ${gap.toFixed(1)} points.`;
        return `${names} are close · ${gap.toFixed(1)} apart.`;
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

    function formatVotes(votes, quantity = 'votes') {
        if (votes === null || votes === undefined || votes === '') return `${quantity} unavailable`;
        const raw = String(votes).trim();
        const numeric = Number(raw.replace(/,/g, ''));
        const formatted = Number.isFinite(numeric) && numeric >= 0
            ? Math.round(numeric).toLocaleString()
            : raw;
        return `${formatted} ${quantity}`;
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

    function renderSources(container, payload) {
        const sourceGrid = node('div', 'verdict-source-grid');
        providerDefinitions.forEach(provider => {
            const rating = payload?.[provider.key] || {};
            const score = providerDisplayScore(payload, provider.key);
            const card = node('div', `verdict-source-card ${provider.key}`);
            const meta = node('div', 'verdict-source-meta');
            meta.append(node('span', 'verdict-source-mark', provider.mark));
            meta.append(node('span', 'verdict-source-name', provider.label));
            const value = node('strong', 'verdict-source-score');
            value.append(node('span', '', score === null ? '—' : score.toFixed(1)));
            if (score !== null) value.append(node('span', 'verdict-source-unit', `/${provider.scale}`));
            const hasRatingCount = rating.votes !== null && rating.votes !== undefined && rating.votes !== '';
            const noRatingsYet = provider.key === 'flimfy'
                && hasRatingCount && Number(rating.votes) === 0;
            const votes = node('span', 'verdict-source-votes', noRatingsYet
                ? 'No member ratings yet'
                : score === null
                    ? 'Not available'
                    : formatVotes(rating.votes, provider.quantity));
            const rail = node('span', 'verdict-source-rail');
            const fill = node('span', 'verdict-source-fill');
            fill.style.width = score === null ? '0%' : `${score * (100 / provider.scale)}%`;
            rail.append(fill);
            card.append(meta, value, votes, rail);
            sourceGrid.append(card);
        });
        container.append(sourceGrid);
        container.append(node('p', 'verdict-method', 'Equal mean · FlimfyBox /5 is scaled to /10 · each source count stays separate.'));
    }

    function renderMeter(container, payload) {
        const summary = summarize(payload?.scores || payload || {});
        const scores = payload?.scores || payload || {};
        const verdict = verdictForSummary(summary);
        const heading = node('div', 'verdict-heading');
        const brand = node('span', 'verdict-brand');
        brand.append(node('span', 'verdict-brand-icon', 'F'), node('span', '', 'FLIMFY VERDICT'));
        heading.append(brand);
        heading.append(node('span', 'verdict-source-count', `${summary.sourceCount} ${summary.sourceCount === 1 ? 'SIGNAL' : 'SIGNALS'}`));
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

    function renderReviews(container, payload = {}) {
        const reviews = Array.isArray(payload?.reviews) ? payload.reviews.slice(0, 10) : [];
        const heading = node('div', 'detail-section-heading');
        heading.append(
            node('span', 'section-kicker', 'FROM THE FLIMFYBOX COMMUNITY'),
            node('h2', '', 'Member reviews')
        );
        const count = payload?.review_count;
        const hasReviewCount = count !== null && count !== undefined && count !== ''
            && Number.isFinite(Number(count));
        heading.append(node('span', 'review-count', hasReviewCount
            ? `${Number(count).toLocaleString()} ${Number(count) === 1 ? 'review' : 'reviews'}`
            : 'FlimfyBox'));
        container.append(heading);

        if (payload?.status === 'loading') {
            container.append(node('p', 'review-empty', 'Loading member reviews…'));
            return;
        }
        if (payload?.status !== 'success' && payload?.status !== undefined) {
            container.append(node('p', 'review-empty', 'Member reviews are temporarily unavailable.'));
            return;
        }

        if (!reviews.length) {
            container.append(node('p', 'review-empty', payload?.is_upcoming
                ? 'Reviews open after this title is released.'
                : 'No member reviews yet. Be the first to share what you thought.'));
        } else {
            const list = node('div', 'flimfy-review-list');
            reviews.forEach(review => {
                const card = node('article', 'flimfy-review-card');
                const meta = node('div', 'flimfy-review-meta');
                meta.append(node('strong', 'flimfy-review-author', review.author || 'FlimfyBox member'));
                const date = formatReviewDate(review.created_at);
                if (date) meta.append(node('span', 'flimfy-review-date', date));
                const reviewRating = normalizedScore(review.rating, 5);
                if (reviewRating !== null) meta.append(node('span', 'flimfy-review-rating', `★ ${reviewRating.toFixed(1)} / 5`));
                card.append(meta, node('p', 'flimfy-review-copy', review.content || ''));
                list.append(card);
            });
            container.append(list);
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
        } else if (!payload?.is_upcoming) {
            container.append(node('p', 'flimfy-review-readonly', 'Open FlimfyBox in Telegram to post or update a review.'));
        }
    }

    function renderAudienceReviews(container, payload = {}) {
        const reviews = Array.isArray(payload?.reviews) ? payload.reviews.slice(0, 5) : [];
        const heading = node('div', 'detail-section-heading');
        heading.append(
            node('span', 'section-kicker', 'PUBLIC AUDIENCE VOICES'),
            node('h2', '', 'TMDb member reviews')
        );
        const count = Number(payload?.review_count);
        heading.append(node('span', 'review-count', payload?.linked === false
            ? 'TMDb source'
            : Number.isFinite(count) && count >= 0
            ? `${count.toLocaleString()} on TMDb`
            : 'TMDb source'));
        container.append(heading);

        if (payload?.status === 'loading') {
            container.append(node('p', 'review-empty', 'Loading public reviews from TMDb…'));
            return;
        }
        if (payload?.status !== 'success' && payload?.status !== undefined) {
            container.append(node('p', 'review-empty', 'TMDb reviews are temporarily unavailable.'));
            return;
        }
        if (payload?.linked === false) {
            container.append(node('p', 'review-empty', 'This catalogue title is not linked to a TMDb entry yet.'));
            return;
        }
        if (!reviews.length) {
            container.append(node('p', 'review-empty', 'No public TMDb review excerpts are available for this title yet.'));
            return;
        }

        const list = node('div', 'flimfy-review-list tmdb-audience-review-list');
        reviews.forEach(review => {
            const card = node('article', 'flimfy-review-card tmdb-audience-review-card');
            const meta = node('div', 'flimfy-review-meta');
            meta.append(node('strong', 'flimfy-review-author', review.author || 'TMDb member'));
            meta.append(node('span', 'tmdb-source-pill', 'TMDb'));
            const date = formatReviewDate(review.created_at);
            if (date) meta.append(node('span', 'flimfy-review-date', date));
            const reviewRating = normalizedScore(review.rating);
            if (reviewRating !== null) meta.append(node('span', 'flimfy-review-rating', `★ ${reviewRating.toFixed(1)} / 10`));
            const copy = node('p', 'flimfy-review-copy', review.content || '');
            card.append(meta, copy);
            const url = trustedTmdbReviewUrl(review.url);
            if (url) {
                const sourceLink = node('a', 'tmdb-review-source-link', 'Read full review on TMDb');
                sourceLink.href = url;
                sourceLink.target = '_blank';
                sourceLink.rel = 'noopener noreferrer';
                card.append(sourceLink);
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
        renderAudienceReviews(reviewContainer, payload) {
            if (!reviewContainer) return;
            reviewContainer.replaceChildren();
            renderAudienceReviews(reviewContainer, payload || {});
        },
        setCommunityRating(meterContainer, data) {
            if (!meterContainer) return;
            const scores = { ...(ratingsByContainer.get(meterContainer) || {}) };
            const count = Number(data?.count);
            const average = normalizedScore(data?.average, 5);
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
                meterContainer.append(node('span', 'section-kicker', 'FLIMFY VERDICT'));
                meterContainer.append(node('p', 'verdict-loading', 'Reading IMDb, TMDb + FlimfyBox ratings…'));
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
