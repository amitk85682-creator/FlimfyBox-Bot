/*
 * Read-only UI preview for machines without the project's Flask runtime.
 * Run: node tests/miniapp_preview_node.js
 * Uses a small sample catalogue and never writes to the production database.
 */
'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { URL } = require('node:url');

const ROOT = path.resolve(__dirname, '..');
const PORT = Number(process.env.FLIMFY_PREVIEW_PORT || 8787);
const movies = [
    { id: 101, title: '3 Idiots', year: 2009, image: 'https://image.tmdb.org/t/p/w500/66A9MqXOyVFCssoloscw79z8Tew.jpg', rating: '8.4', genre: 'Comedy, Drama', category: 'Bollywood', language: 'Hindi', content_type: 'Movie', is_available: true },
    { id: 102, title: 'Andhadhun', year: 2018, image: 'https://image.tmdb.org/t/p/w500/dy3K6hNvwE05siGgiLJcEiwgpdO.jpg', rating: '8.2', genre: 'Thriller, Mystery, Crime', category: 'Bollywood', language: 'Hindi', content_type: 'Movie', is_available: true },
    { id: 103, title: 'Dangal', year: 2016, image: 'https://image.tmdb.org/t/p/w500/cJRPOLEey7i1viA5Y7F5IO2f9L.jpg', rating: '8.3', genre: 'Drama, Action', category: 'Bollywood', language: 'Hindi', content_type: 'Movie', is_available: true },
    { id: 104, title: 'Dil Chahta Hai', year: 2001, image: 'https://image.tmdb.org/t/p/w500/1RUrQYwlX8Yb4o1w5sS6S9n9k6Q.jpg', rating: '8.1', genre: 'Drama, Romance, Comedy', category: 'Bollywood', language: 'Hindi', content_type: 'Movie', is_available: true },
    { id: 105, title: 'Demon Slayer: Mugen Train', year: 2020, image: 'https://image.tmdb.org/t/p/w500/h8Rb9gBr48ODIwYUttZNYeMWeUU.jpg', rating: '8.2', genre: 'Anime, Animation, Action', category: 'Anime', language: 'Japanese, Hindi', content_type: 'Movie', is_available: true },
    { id: 106, title: 'Your Name', year: 2016, image: 'https://image.tmdb.org/t/p/w500/q719jXXEzOoYaps6babgknONONX.jpg', rating: '8.5', genre: 'Anime, Animation, Romance', category: 'Anime', language: 'Japanese, English', content_type: 'Movie', is_available: true },
    { id: 107, title: 'The Dark Knight', year: 2008, image: 'https://image.tmdb.org/t/p/w500/qJ2tW6WMUDux911r6m7haRef0WH.jpg', rating: '9.0', genre: 'Action, Crime, Drama', category: 'Hollywood', language: 'English', content_type: 'Movie', is_available: true },
    { id: 108, title: 'Knives Out', year: 2019, image: 'https://image.tmdb.org/t/p/w500/pThyQovXQrw2m0s9x82twj48Jq4.jpg', rating: '7.9', genre: 'Mystery, Thriller, Comedy', category: 'Hollywood', language: 'English', content_type: 'Movie', is_available: true },
    { id: 109, title: 'Panchayat', year: 2020, image: 'https://image.tmdb.org/t/p/w500/3y4s6z9Y4UjgXcZ9bK9No0zQYFz.jpg', rating: '8.9', genre: 'Comedy, Drama', category: 'Web Series', language: 'Hindi', content_type: 'Web Series', is_available: true },
    { id: 110, title: 'Sherlock', year: 2010, image: 'https://image.tmdb.org/t/p/w500/7WTsnHkbA0FaG6R9twfFde0I9hl.jpg', rating: '9.0', genre: 'Crime, Mystery, Drama', category: 'Web Series', language: 'English', content_type: 'Web Series', is_available: true },
    { id: 111, title: 'Spider-Man: Into the Spider-Verse', year: 2018, image: 'https://image.tmdb.org/t/p/w500/iiZZdoQBEYBv6id8su7ImL0oCbD.jpg', rating: '8.4', genre: 'Animation, Action, Adventure', category: 'Hollywood', language: 'English, Hindi', content_type: 'Movie', is_available: false, availability_state: 'unavailable' },
];
const moodGenres = {
    any: [], fun: ['comedy', 'animation', 'animated', 'family', 'musical'],
    mind: ['thriller', 'mystery', 'crime', 'sci-fi', 'sci fi', 'science fiction', 'detective'],
    action: ['action', 'adventure', 'war', 'fantasy', 'superhero'],
    heart: ['romance', 'romantic', 'drama', 'family'],
    anime: ['anime', 'animation', 'animated', 'isekai', 'shonen', 'shojo', 'seinen'],
};
const previewCheckinDays = new Set();

function isTv(movie) {
    return movie.content_type === 'Web Series' || movie.category === 'Web Series';
}

function pick(query) {
    const type = query.get('type') || 'all';
    const mood = query.get('mood') || 'any';
    const language = query.get('language') || 'any';
    const availability = query.get('availability') || 'available';
    if (!['all', 'movies', 'tv'].includes(type) || !Object.hasOwn(moodGenres, mood)
        || !['any', 'hindi', 'english'].includes(language) || !['all', 'available'].includes(availability)) {
        return { statusCode: 400, body: { status: 'error', message: 'Invalid Mood Reader filters.' } };
    }
    const excluded = new Set((query.get('exclude') || '').split(',').filter(Boolean).slice(0, 8));
    const candidates = movies.filter(movie => {
        if (movie.is_unreleased || excluded.has(String(movie.id))) return false;
        if (type === 'movies' && isTv(movie) || type === 'tv' && !isTv(movie)) return false;
        if (availability === 'available' && movie.is_available !== true) return false;
        return language === 'any' || new RegExp(`\\b${language}\\b`, 'i').test(movie.language || '');
    }).map(movie => {
        const tags = `${movie.genre},${movie.category},${movie.content_type}`.toLowerCase()
            .split(/[,/&|]/).map(part => part.trim()).filter(Boolean);
        const matched = [...new Set(tags.filter(tag => moodGenres[mood].includes(tag)))];
        return { movie, matched };
    });
    if (!candidates.length) {
        return { statusCode: 200, body: { status: 'success', movie: null, message: 'Is filter mein sample catalogue ka koi title nahi mila.' } };
    }
    const exact = mood === 'any' ? [] : candidates.filter(item => item.matched.length);
    const list = exact.length ? exact : candidates;
    const selected = list[Math.floor(Math.random() * list.length)];
    return { statusCode: 200, body: {
        status: 'success', type, movie: selected.movie,
        mood_match: Boolean(selected.matched.length), matched_genres: selected.matched, personalized: false,
    } };
}

function catalogueType(query) {
    const type = query.get('type') || 'all';
    return movies.filter(movie => type === 'all' || (type === 'tv' ? isTv(movie) : !isTv(movie)));
}

function dailyPicksFor(day, catalogue = movies) {
    return catalogue.filter(movie => movie.is_available === true && !movie.is_unreleased)
        .slice()
        .sort((a, b) => {
            const hashA = crypto.createHash('sha256').update(`${day}:${a.id}`).digest('hex');
            const hashB = crypto.createHash('sha256').update(`${day}:${b.id}`).digest('hex');
            return hashA.localeCompare(hashB);
        })
        .slice(0, 3);
}

function dailyCheckinFor(day, performCheckin = false) {
    const newCheckin = performCheckin && !previewCheckinDays.has(day);
    if (performCheckin) previewCheckinDays.add(day);
    const checkedInToday = previewCheckinDays.has(day);
    const week = [];
    for (let daysAgo = 6; daysAgo >= 0; daysAgo -= 1) {
        const date = new Date(new Date(`${day}T00:00:00Z`).getTime() - daysAgo * 86400000).toISOString().slice(0, 10);
        const dateForLabel = new Date(`${date}T12:00:00Z`);
        week.push({
            date,
            label: dateForLabel.toLocaleDateString('en-IN', { weekday: 'short', timeZone: 'UTC' }).slice(0, 2),
            done: (daysAgo > 0 && daysAgo <= 3) || (daysAgo === 0 && checkedInToday),
            is_today: daysAgo === 0,
        });
    }
    return {
        status: 'success', current_streak: checkedInToday ? 4 : 3,
        best_streak: 6, total_checkins: 12, checked_in_today: checkedInToday,
        new_checkin: newCheckin, ticket_earned: false, tickets_earned_count: 0, priority_tickets: 0,
        next_milestone: { days: 7, name: 'Weekly Regular' },
        badges: [
            { days: 3, name: 'Fresh Start', unlocked: true, newly_unlocked: false },
            { days: 7, name: 'Weekly Regular', unlocked: false, newly_unlocked: false },
            { days: 14, name: 'Film Fan', unlocked: false, newly_unlocked: false },
            { days: 30, name: 'Flimfy Legend', unlocked: false, newly_unlocked: false },
        ],
        week,
    };
}

function json(res, statusCode, body) {
    res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(body));
}

function serveStatic(reqPath, res) {
    const requested = path.resolve(ROOT, `.${decodeURIComponent(reqPath)}`);
    const staticRoot = path.resolve(ROOT, 'static');
    if (!requested.startsWith(staticRoot + path.sep)) return false;
    let file;
    try { file = fs.readFileSync(requested); } catch (_error) { return false; }
    const ext = path.extname(requested).toLowerCase();
    const types = { '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg', '.webp': 'image/webp' };
    res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream', 'Cache-Control': 'no-store' });
    res.end(file);
    return true;
}

const server = http.createServer((req, res) => {
    const requestUrl = new URL(req.url, `http://127.0.0.1:${PORT}`);
    if ((req.method === 'GET' || req.method === 'POST') && requestUrl.pathname === '/api/home/daily-checkin') {
        if (req.method === 'POST') req.resume();
        const parts = new Intl.DateTimeFormat('en-CA', {
            timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit'
        }).formatToParts(new Date());
        const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
        return json(res, 200, dailyCheckinFor(`${values.year}-${values.month}-${values.day}`, req.method === 'POST'));
    }
    if (req.method !== 'GET') return json(res, 403, { status: 'error', message: 'This preview is read-only.' });
    if (requestUrl.pathname === '/' || requestUrl.pathname === '/preview') {
        const width = Math.max(320, Math.min(1600, Number(requestUrl.searchParams.get('width')) || 1280));
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
        return res.end(`<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1"><title>FlimfyBox · Local preview</title></head><body style="margin:0;background:#171820;color:#ddd;font:12px system-ui"><header style="height:48px;display:flex;align-items:center;justify-content:center;gap:16px"><span>READ-ONLY PREVIEW · SAMPLE CATALOGUE</span><a style="color:#c2b5ff" href="/preview?width=1280">Desktop</a><a style="color:#c2b5ff" href="/preview?width=390">Mobile</a><a style="color:#c2b5ff" href="/preview?width=320">Small phone</a><a style="color:#c2b5ff" href="/webapp">Open app</a></header><iframe title="FlimfyBox UI preview" src="/webapp" style="display:block;width:${width}px;max-width:100%;height:calc(100dvh - 48px);border:0;margin:auto"></iframe></body></html>`);
    }
    if (requestUrl.pathname === '/webapp') {
        let html = fs.readFileSync(path.join(ROOT, 'templates', 'mini_app.html'), 'utf8');
        html = html.replace(/\{\{\s*url_for\('static',\s*filename='([^']+)'\)\s*\}\}/g, '/static/$1');
        html = html.replace(/<body([^>]*)>/, '<body$1><div style="position:fixed;z-index:2000;top:0;left:0;padding:3px 8px;border-radius:0 0 6px;background:#b5a1ff;color:#201439;font:700 8px system-ui;letter-spacing:.08em">LOCAL SAMPLE PREVIEW</div>');
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
        return res.end(html);
    }
    if (requestUrl.pathname.startsWith('/static/')) {
        if (!serveStatic(requestUrl.pathname, res)) return res.writeHead(404).end('Not found');
        return;
    }
    if (requestUrl.pathname === '/api/movies') {
        const availability = requestUrl.searchParams.get('availability') || 'all';
        const filtered = movies.filter(movie => availability !== 'available' || movie.is_available === true);
        const page = Math.max(1, Number(requestUrl.searchParams.get('page')) || 1);
        const limit = Math.max(1, Math.min(100, Number(requestUrl.searchParams.get('limit')) || 40));
        const offset = (page - 1) * limit;
        return json(res, 200, { status: 'success', movies: filtered.slice(offset, offset + limit), has_more: offset + limit < filtered.length });
    }
    if (requestUrl.pathname === '/api/home/daily-picks') {
        const parts = new Intl.DateTimeFormat('en-CA', {
            timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit'
        }).formatToParts(new Date());
        const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
        const day = `${values.year}-${values.month}-${values.day}`;
        const picks = dailyPicksFor(day);
        return json(res, 200, {
            status: 'success', date: day,
            date_label: new Date(`${day}T12:00:00+05:30`).toLocaleDateString('en-IN', { day: 'numeric', month: 'short' }),
            movies: picks, personalized: false
        });
    }
    if (requestUrl.pathname === '/api/browse/surprise') {
        const result = pick(requestUrl.searchParams);
        return json(res, result.statusCode, result.body);
    }
    if (requestUrl.pathname === '/api/genres') {
        const list = catalogueType(requestUrl.searchParams);
        const names = [...new Set(list.flatMap(movie => movie.genre.split(/[,/&|]/).map(item => item.trim().toLowerCase()).filter(Boolean)))];
        return json(res, 200, { status: 'success', genres: names.map(name => ({ id: name.replace(/\s+/g, '-'), label: name.replace(/\b\w/g, char => char.toUpperCase()), count: list.filter(movie => movie.genre.toLowerCase().includes(name)).length })) });
    }
    if (requestUrl.pathname === '/api/browse') {
        return json(res, 200, { status: 'success', collections: [{ id: 'sample', label: 'Sample titles', movies: catalogueType(requestUrl.searchParams) }] });
    }
    if (requestUrl.pathname.startsWith('/api/genre/')) {
        const genre = decodeURIComponent(requestUrl.pathname.slice('/api/genre/'.length)).replace(/-/g, ' ');
        const list = catalogueType(requestUrl.searchParams).filter(movie => movie.genre.toLowerCase().includes(genre));
        return json(res, 200, { status: 'success', genre: { id: genre, label: genre, count: list.length }, movies: list });
    }
    if (requestUrl.pathname === '/api/upcoming') return json(res, 200, { status: 'success', movies: [] });
    if (requestUrl.pathname === '/api/home/catalogue-rows') return json(res, 200, { status: 'success', collections: { hollywood: movies.filter(movie => movie.category === 'Hollywood'), bollywood: movies.filter(movie => movie.category === 'Bollywood'), anime: movies.filter(movie => movie.category === 'Anime') } });
    if (requestUrl.pathname === '/api/home/new-releases') return json(res, 200, { status: 'success', movies: movies.filter(movie => movie.year >= 2020), window_days: 60 });
    if (requestUrl.pathname === '/api/home/trending') return json(res, 200, { status: 'success', results: movies.slice(0, 7), hero_limit: 5 });
    if (requestUrl.pathname.startsWith('/api/movie/')) {
        const movie = movies.find(item => String(item.id) === requestUrl.pathname.split('/').pop());
        return json(res, movie ? 200 : 404, movie ? { status: 'success', movie } : { status: 'error', message: 'Not included in the sample catalogue.' });
    }
    if (requestUrl.pathname === '/favicon.ico') return res.writeHead(204).end();
    return json(res, 404, { status: 'error', message: 'This sample preview does not include that API.' });
});

if (require.main === module) {
    server.listen(PORT, '127.0.0.1', () => {
        process.stdout.write(`FlimfyBox read-only sample preview: http://127.0.0.1:${PORT}/preview\n`);
    });
}

module.exports = { dailyCheckinFor, dailyPicksFor, movies, pick };
