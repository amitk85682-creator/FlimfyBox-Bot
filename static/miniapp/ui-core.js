/* Shared, deterministic UI rules. Also loaded before paint to restore appearance. */
(function (root) {
    const normalize = value => String(value || '').normalize('NFKC').trim().toLocaleLowerCase();
    const searchKey = value => normalize(value).replace(/[’']s\b/g, '').replace(/[^a-z0-9]/g, '');
    function prefixMatches(movies, query, limit = 20) {
        const prefix = searchKey(query);
        if (!prefix) return [];
        const seen = new Set();
        return movies.filter(movie => {
            const id = String(movie.id);
            if (seen.has(id) || !searchKey(movie.title).startsWith(prefix)) return false;
            seen.add(id);
            return true;
        }).sort((a, b) => normalize(a.title).localeCompare(normalize(b.title))).slice(0, limit);
    }
    function futureRelease(movie, now = new Date()) {
        if (movie.is_available === true || movie.availability_state === 'available' || movie.availability_state === 'released') return false;
        const date = String(movie.media_type === 'tv' ? (movie.first_air_date || movie.release_date || '') : (movie.release_date || ''));
        if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return false;
        const parsed = new Date(date + 'T00:00:00Z');
        if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== date) return false;
        // Calendar dates, not midnight timestamps: a title releasing today isn't upcoming.
        const today = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
        return date > today;
    }
    function themePreference(value) { return ['dark', 'light', 'system'].includes(value) ? value : 'system'; }
    function resolveTheme(value, systemDark) { return themePreference(value) === 'system' ? (systemDark ? 'dark' : 'light') : value; }
    function availabilityLabel(movie) {
        return futureRelease(movie) ? 'Coming soon' : movie.is_available === false || movie.source === 'tmdb' ? 'Request' : 'In your catalogue';
    }
    function contentLabel(movie) {
        const type = normalize(movie.content_type || movie.media_type || movie.category);
        if (['tv', 'series', 'web series', 'tv series'].includes(type)) return 'Series';
        if (type === 'anime') return 'Anime';
        return 'Movie';
    }
    function identityLabel(movie) {
        return [movie.year || 'Year unknown', contentLabel(movie)].join(' · ');
    }
    const api = { normalize, searchKey, prefixMatches, futureRelease, themePreference, resolveTheme, availabilityLabel, contentLabel, identityLabel };
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
    root.FlimfyUI = api;
    if (!root.document) return;
    const media = root.matchMedia('(prefers-color-scheme: dark)');
    let preference = 'system';
    try { preference = themePreference(root.localStorage.getItem('flimfybox-theme')); } catch (_) {}
    api.applyTheme = function (value, persist = true) {
        preference = themePreference(value);
        const theme = resolveTheme(preference, media.matches);
        const html = root.document.documentElement;
        html.dataset.theme = theme;
        html.dataset.themePreference = preference;
        html.style.colorScheme = theme;
        root.document.querySelector('meta[name="theme-color"]')?.setAttribute('content', theme === 'dark' ? '#0c0e17' : '#f6f7fb');
        if (persist) { try { root.localStorage.setItem('flimfybox-theme', preference); } catch (_) {} }
        root.document.querySelectorAll('[data-theme-choice]').forEach(button => {
            const selected = button.dataset.themeChoice === preference;
            button.classList.toggle('selected', selected);
            button.setAttribute('aria-pressed', String(selected));
        });
        return theme;
    };
    api.applyTheme(preference, false);
    media.addEventListener?.('change', () => api.applyTheme(preference, false));
})(typeof window !== 'undefined' ? window : globalThis);
