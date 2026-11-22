const test = require('node:test');
const assert = require('node:assert/strict');
const cinema = require('../static/miniapp/cinematic-helpers.js');

test('isSeriesItem reliably distinguishes movies from TV/web series', () => {
    assert.equal(cinema.isSeriesItem({ title: 'Dune', category: 'Hollywood', content_type: 'movie', media_type: 'movie' }), false);
    assert.equal(cinema.isSeriesItem({ title: 'Dark', media_type: 'tv' }), true);
    assert.equal(cinema.isSeriesItem({ title: 'Mirzapur', category: 'Web Series' }), true);
    assert.equal(cinema.isSeriesItem({ title: 'Severance', seasons_data: { '1': {} } }), true);
    assert.equal(cinema.isSeriesItem({
        title: 'Untitled',
        files: [{ extra_info: 'S01E01 1080p', quality: '1080p' }],
    }), true);
    assert.equal(cinema.isSeriesItem(null), false);
});

test('parseCredits builds deduplicated Cast & Crew cards with roles and initials', () => {
    const credits = cinema.parseCredits(
        'Director: Denis Villeneuve, Timothée Chalamet, Rebecca Ferguson (Lady Jessica), Timothée Chalamet, N/A',
        'Denis Villeneuve'
    );
    assert.equal(credits.length, 3);
    assert.equal(credits[0].name, 'Denis Villeneuve');
    assert.equal(credits[0].role, 'Director');
    assert.equal(credits[0].initials, 'DV');
    assert.equal(credits[1].name, 'Timothée Chalamet');
    assert.equal(credits[1].role, 'Cast');
    assert.equal(credits[2].name, 'Rebecca Ferguson');
    assert.equal(credits[2].role, 'Lady Jessica');
    assert.deepEqual(cinema.parseCredits('', ''), []);
});

test('selectSimilarTitles isolates movies from series and ranks by genre/language', () => {
    const currentMovie = {
        id: 1,
        title: 'Interstellar',
        genre: 'Sci-Fi, Drama, Adventure',
        language: 'English',
        category: 'Hollywood',
        media_type: 'movie',
        year: 2014,
    };
    const catalog = [
        currentMovie,
        { id: 2, title: 'Dark', genre: 'Sci-Fi, Drama', language: 'German', category: 'Web Series', media_type: 'tv', year: 2017, is_available: true, rating: '8.7' },
        { id: 3, title: 'Arrival', genre: 'Sci-Fi, Drama', language: 'English', category: 'Hollywood', media_type: 'movie', year: 2016, is_available: true, rating: '7.9' },
        { id: 4, title: 'Superbad', genre: 'Comedy', language: 'English', category: 'Hollywood', media_type: 'movie', year: 2007, is_available: true, rating: '7.6' },
        { id: 5, title: 'The Martian', genre: 'Sci-Fi, Adventure', language: 'English', category: 'Hollywood', media_type: 'movie', year: 2015, is_available: true, rating: '8.0' },
        { id: 6, title: 'Severance', genre: 'Sci-Fi, Thriller', language: 'English', category: 'TV Series', media_type: 'tv', year: 2022, is_available: true, rating: '8.7' },
        { id: 7, title: 'Late Night Drama', genre: 'Drama, 18+', language: 'Hindi', category: '18+ Adult', media_type: 'movie', year: 2025, is_available: true, rating: '9.5' },
    ];

    const similarMovies = cinema.selectSimilarTitles(currentMovie, catalog, 5);
    assert.equal(similarMovies.every((item) => !cinema.isSeriesItem(item)), true);
    assert.equal(similarMovies.some((item) => item.id === 1), false);
    assert.equal(similarMovies.some((item) => item.id === 7), false);
    assert.equal(similarMovies[0].title, 'The Martian');
    assert.equal(similarMovies[1].title, 'Arrival');

    const similarSeries = cinema.selectSimilarTitles(catalog[1], catalog, 5);
    assert.equal(similarSeries.length, 1);
    assert.equal(similarSeries[0].title, 'Severance');
});

test('buildDetailMetadataItems separates Movie and TV Series copy cleanly', () => {
    const movieMeta = cinema.buildDetailMetadataItems({
        year: 2024,
        runtime: 166,
        certification: 'PG-13',
        genre: 'Sci-Fi, Adventure',
        language: 'English · Hindi',
        rating: '8.6',
    }, false, 0);
    assert.equal(movieMeta.kicker, 'FEATURE FILM');
    assert.equal(movieMeta.durationOrSeasons, '2h 46m');
    assert.equal(movieMeta.certification, 'PG-13');
    assert.equal(movieMeta.primaryCtaLabel, 'Watch & Download');
    assert.equal(movieMeta.shareAriaLabel, 'Share movie');

    const seriesMeta = cinema.buildDetailMetadataItems({
        year: 2023,
        runtime: 55,
        genre: 'Drama, Thriller',
        language: 'Hindi',
        rating: '8.4',
    }, true, 3);
    assert.equal(seriesMeta.kicker, 'TV SERIES');
    assert.equal(seriesMeta.durationOrSeasons, '3 Seasons');
    assert.equal(seriesMeta.primaryCtaLabel, 'Browse episodes');
    assert.equal(seriesMeta.shareAriaLabel, 'Share series');
});

test('buildEpisodeDisplayMeta formats episode code, clean title, and fallback thumbnail', () => {
    const ep = {
        episodeStart: 2,
        episodeEnd: null,
        isCompleteSeason: false,
        qualities: [
            { id: 10, quality: '1080p', size: '1.2 GB', extra_info: 'S01E02 - The Awakening | Dual Audio' },
            { id: 11, quality: '720p', size: '650 MB', extra_info: 'S01E02' },
        ],
    };
    const meta = cinema.buildEpisodeDisplayMeta(ep, 1, '02', 'https://example.com/backdrop.jpg');
    assert.equal(meta.badgeCode, 'S01 · E02');
    assert.equal(meta.title, 'The Awakening');
    assert.equal(meta.fileCount, 2);
    assert.equal(meta.thumbUrl, 'https://example.com/backdrop.jpg');
    assert.match(meta.subtitle, /2 versions ready/);

    const completeMeta = cinema.buildEpisodeDisplayMeta({
        isCompleteSeason: true,
        qualities: [{ id: 12, quality: '1080p', size: '8.4 GB' }],
    }, 2, '—', '');
    assert.equal(completeMeta.badgeCode, 'S02 · FULL');
    assert.equal(completeMeta.title, 'Complete Season Pack');
    assert.equal(completeMeta.thumbUrl, '/static/miniapp/poster-placeholder.svg');
});

test('getSynopsisState handles missing and long synopses gracefully', () => {
    assert.equal(cinema.getSynopsisState('').canToggle, false);
    assert.equal(cinema.getSynopsisState('Short plot').canToggle, false);
    assert.equal(cinema.getSynopsisState('A'.repeat(320)).canToggle, true);
});
