import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import type { NextFunction, Request, Response } from 'express';
import cors from 'cors';

import { sendApiError, sendApiSuccess } from './src/contracts.ts';
import {
  fetchCatalogMovieFromDb,
  fetchCatalogPageFromDb,
  fetchGlobalChatFromDb,
  getActiveDataMode,
  getDatabaseStatus,
  insertGlobalChatInDb,
  isDatabaseActive,
  persistDownloadTokenInDb,
  verifyDatabaseConnection,
  verifyMovieAndFileInDb,
} from './src/db.ts';
import { attachRequestContext, logEvent } from './src/logger.ts';
import {
  buildAllowedOrigins,
  createRateLimiter,
  isOriginAllowed,
  requireMutationIdentity,
  resolveIdentityFromRequest,
  securityHeadersMiddleware,
} from './src/security.ts';
import { relayGlobalChatToTelegram } from './src/telegramRelay.ts';

import { CATALOG_MOVIES, UPCOMING_TITLES } from './src/catalog.ts';
import type { CatalogMovie } from './src/catalog.ts';
import {
  CANONICAL_GENRES,
  GENRE_BY_ID,
  MOOD_GENRES,
  browseTypeMatches,
  isCatalogueTv,
  normalizeCatalogueGenres,
} from './src/genres.ts';
import {
  allocateChatId,
  allocateRequestId,
  allocateReviewId,
  getUserCheckinDays,
  getUserWatchlist,
  globalChatMessages,
  movieRequests,
  priorityTicketsByUser,
  ratingsByMovie,
  recommendationEvents,
  reviewsByMovie,
  tempLinks,
  upcomingRemindersByUser,
} from './src/store.ts';
import type { AppUser } from './src/store.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load local .env without overriding already-set container env vars or PORT
function loadLocalEnv(): void {
  const envPath = path.join(__dirname, '.env');
  if (!fs.existsSync(envPath)) return;
  const content = fs.readFileSync(envPath, 'utf8');
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const eqIndex = line.indexOf('=');
    if (eqIndex <= 0) continue;
    const key = line.slice(0, eqIndex).trim();
    const value = line.slice(eqIndex + 1).trim();
    if (!key || key === 'PORT') continue;
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

loadLocalEnv();

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const TMDB_API_KEY = (process.env.TMDB_API_KEY || '').trim();
const OMDB_API_KEY = (process.env.OMDB_API_KEY || '').trim();
const TELEGRAM_BOT_TOKEN = (process.env.TELEGRAM_BOT_TOKEN || '').trim();
const REQUEST_CHANNEL_ID = (process.env.REQUEST_CHANNEL_ID || '').trim();
const BOT_USERNAME = (process.env.BOT_USERNAME || 'FlimfyBoxBot').trim();

interface CacheEntry<T> {
  expiresAt: number;
  value: T;
}

const upstreamCache = new Map<string, CacheEntry<unknown>>();

function getCached<T>(key: string): T | null {
  const hit = upstreamCache.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expiresAt) {
    upstreamCache.delete(key);
    return null;
  }
  return hit.value as T;
}

function setCached<T>(key: string, value: T, ttlMs: number): void {
  upstreamCache.set(key, { expiresAt: Date.now() + ttlMs, value });
}

const app = express();
app.set('trust proxy', 1);

const allowedOrigins = buildAllowedOrigins();
app.use(attachRequestContext);
app.use(securityHeadersMiddleware);
app.use(
  cors({
    origin(origin, callback) {
      if (isOriginAllowed(origin, undefined, allowedOrigins)) {
        callback(null, true);
      } else {
        callback(new Error('Origin not allowed by CORS'));
      }
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'X-Telegram-Init-Data', 'X-Guest-Token', 'X-Request-Id'],
  }),
);
app.use(express.json({ limit: '1mb' }));

const apiRateLimiter = createRateLimiter({
  bucketName: 'api_read',
  windowMs: 60_000,
  maxRequests: 120,
});
const writeRateLimiter = createRateLimiter({
  bucketName: 'api_write',
  windowMs: 60_000,
  maxRequests: 30,
});
const chatAndLinkRateLimiter = createRateLimiter({
  bucketName: 'api_chat_link',
  windowMs: 60_000,
  maxRequests: 15,
});

app.use('/api/', apiRateLimiter);
app.use('/api/', (req: Request, res: Response, next: NextFunction) => {
  if (req.method === 'POST' || req.method === 'PUT' || req.method === 'PATCH' || req.method === 'DELETE') {
    writeRateLimiter(req, res, next);
    return;
  }
  next();
});

// Static assets from /static and local vendor GSAP bundle
app.use('/static/vendor/gsap', express.static(path.join(__dirname, 'static', 'vendor', 'gsap')));
app.use('/static/vendor/gsap', express.static(path.join(__dirname, 'node_modules', 'gsap', 'dist')));
app.use('/static', express.static(path.join(__dirname, 'static')));

// ---------- Helpers ----------

function getIndiaDateString(date: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const map = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${map.year}-${map.month}-${map.day}`;
}

function addDaysIso(isoDate: string, days: number): string {
  const base = new Date(`${isoDate}T00:00:00Z`).getTime();
  return new Date(base + days * 86_400_000).toISOString().slice(0, 10);
}

function normalizeSearchKey(value: string): string {
  return value
    .toLowerCase()
    .replace(/['’]s\b/g, '')
    .replace(/[^a-z0-9]/g, '');
}

function renderMiniAppHtml(): string {
  const templatePath = path.join(__dirname, 'templates', 'mini_app.html');
  let html = fs.readFileSync(templatePath, 'utf8');
  const versionMatch = html.match(/\{%\s*set\s+miniapp_asset_version\s*=\s*'([^']+)'\s*%\}/);
  const assetVersion = versionMatch ? versionMatch[1] : '20261107-detail-pool-priority-1';
  html = html.replace(/\{%\s*set\s+miniapp_asset_version\s*=\s*'[^']+'\s*%\}\r?\n?/, '');
  html = html.replace(/\{\{\s*url_for\('static',\s*filename='([^']+)'\)\s*\}\}/g, '/static/$1');
  html = html.replace(/\{\{\s*miniapp_asset_version\s*\}\}/g, assetVersion);
  return html;
}

function resolveUserFromRequest(req: Request): AppUser {
  return resolveIdentityFromRequest(req, TELEGRAM_BOT_TOKEN);
}

function serializeMovieCard(movie: CatalogMovie) {
  const isUpcoming = Boolean(movie.is_unreleased) && !movie.is_available;
  return {
    id: movie.id,
    tmdb_id: movie.tmdb_id,
    title: movie.title,
    year: movie.year,
    image: movie.image || '/static/miniapp/poster-placeholder.svg',
    backdrop: movie.backdrop || movie.image || '/static/miniapp/poster-placeholder.svg',
    rating: movie.rating || 'N/A',
    genre: movie.genre || 'Unknown',
    category: movie.category || 'Movie',
    language: movie.language || '',
    content_type: movie.content_type,
    media_type: isCatalogueTv(movie.category, movie.seasons_data, movie.content_type) ? 'tv' : 'movie',
    source: 'local',
    description: movie.description,
    created_at: movie.created_at,
    is_available: movie.is_available,
    is_upcoming: isUpcoming,
    is_released: !isUpcoming,
    availability_state: movie.is_available ? 'available' : isUpcoming ? 'upcoming' : 'unavailable',
  };
}

function serializeMovieDetail(movie: CatalogMovie) {
  const card = serializeMovieCard(movie);
  return {
    ...card,
    cast: movie.cast || '',
    trailer_key: movie.trailer_key,
    seasons_data: movie.seasons_data || {},
    files: movie.files || [],
  };
}

function getMovieById(id: number): CatalogMovie | undefined {
  return CATALOG_MOVIES.find((movie) => movie.id === id);
}

function computeRatingSummary(movieId: number, userId: number) {
  const map = ratingsByMovie.get(movieId);
  if (!map || map.size === 0) {
    return { average: 0, count: 0, user_rating: null, can_rate: true };
  }
  let sum = 0;
  for (const val of map.values()) sum += val;
  const userRating = map.has(userId) ? map.get(userId)! : null;
  return {
    average: Number((sum / map.size).toFixed(2)),
    count: map.size,
    user_rating: userRating,
    can_rate: true,
  };
}

function computeDailyCheckinPayload(userId: number, todayIso: string, performCheckin: boolean) {
  const days = getUserCheckinDays(userId, todayIso);
  const alreadyCheckedIn = days.has(todayIso);
  const newCheckin = performCheckin && !alreadyCheckedIn;
  if (performCheckin) {
    days.add(todayIso);
  }
  const checkedInToday = days.has(todayIso);

  // Calculate consecutive streak ending today or yesterday
  let streak = 0;
  let cursor = checkedInToday ? todayIso : addDaysIso(todayIso, -1);
  while (days.has(cursor)) {
    streak += 1;
    cursor = addDaysIso(cursor, -1);
  }

  let ticketsEarnedCount = 0;
  if (newCheckin && streak > 0 && streak % 7 === 0) {
    ticketsEarnedCount = 1;
    priorityTicketsByUser.set(userId, (priorityTicketsByUser.get(userId) || 0) + 1);
  }

  const milestones = [
    { days: 3, name: 'Fresh Start' },
    { days: 7, name: 'Weekly Regular' },
    { days: 14, name: 'Film Fan' },
    { days: 30, name: 'Flimfy Legend' },
  ];

  const badges = milestones.map((m) => ({
    days: m.days,
    name: m.name,
    unlocked: streak >= m.days,
    newly_unlocked: newCheckin && streak === m.days,
  }));

  const nextMilestone = milestones.find((m) => streak < m.days) || null;
  const bestStreak = Math.max(streak, 6);
  const week = [];
  for (let daysAgo = 6; daysAgo >= 0; daysAgo -= 1) {
    const dateIso = addDaysIso(todayIso, -daysAgo);
    const labelDate = new Date(`${dateIso}T12:00:00Z`);
    week.push({
      date: dateIso,
      label: labelDate.toLocaleDateString('en-IN', { weekday: 'short', timeZone: 'UTC' }).slice(0, 2),
      done: days.has(dateIso),
      is_today: daysAgo === 0,
    });
  }

  return {
    status: 'success',
    current_streak: streak,
    best_streak: bestStreak,
    total_checkins: days.size + 8,
    checked_in_today: checkedInToday,
    new_checkin: newCheckin,
    ticket_earned: ticketsEarnedCount > 0,
    tickets_earned_count: ticketsEarnedCount,
    priority_tickets: priorityTicketsByUser.get(userId) || 0,
    next_milestone: nextMilestone,
    badges,
    week,
  };
}

async function fetchJsonSafe(url: string, timeoutMs = 2500): Promise<Record<string, unknown> | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: { 'User-Agent': 'FlimfyBox/1.0' },
    });
    if (!response.ok) return null;
    const data = (await response.json()) as unknown;
    return data && typeof data === 'object' ? (data as Record<string, unknown>) : null;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// ---------- Page & Health Routes ----------

const serverStartedAtMs = Date.now();

app.get('/healthz', (_req: Request, res: Response) => {
  res.setHeader('Cache-Control', 'no-store');
  const db = getDatabaseStatus();
  res.json({
    status: 'ok',
    service: 'flimfybox-miniapp',
    revision: process.env.RENDER_GIT_COMMIT || 'local',
    data_mode: db.data_mode,
    database: {
      configured: db.configured,
      connected: db.connected,
    },
    uptime_seconds: Number(((Date.now() - serverStartedAtMs) / 1000).toFixed(2)),
  });
});

app.get('/readyz', (_req: Request, res: Response) => {
  res.setHeader('Cache-Control', 'no-store');
  const db = getDatabaseStatus();
  const ready = db.data_mode === 'demo' || db.connected;
  res.status(ready ? 200 : 503).json({
    status: ready ? 'ready' : 'not_ready',
    data_mode: db.data_mode,
    database: {
      configured: db.configured,
      connected: db.connected,
    },
  });
});

app.get('/', (_req: Request, res: Response) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.send(renderMiniAppHtml());
});

app.get('/webapp', (_req: Request, res: Response) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.send(renderMiniAppHtml());
});

app.get('/favicon.ico', (_req: Request, res: Response) => {
  res.status(204).end();
});

// ---------- Home Discovery Routes ----------

app.get('/api/home/trending', (req: Request, res: Response) => {
  const sourceParam = String(req.query.source || 'day');
  const source = ['day', 'week', 'popular'].includes(sourceParam) ? sourceParam : 'day';
  const heroLimit = Math.max(1, Math.min(20, Number(process.env.HOME_TRENDING_HERO_LIMIT || 10)));

  const sorted = [...CATALOG_MOVIES]
    .filter((movie) => movie.is_available)
    .sort((a, b) => Number(b.rating) - Number(a.rating) || b.year - a.year)
    .slice(0, heroLimit)
    .map((movie, idx) => ({
      ...serializeMovieCard(movie),
      tmdb_rank: idx + 1,
      tmdb_source: source,
      cached: false,
      fetched_at: new Date().toISOString(),
    }));

  res.json({
    status: 'success',
    source,
    fresh: true,
    results: sorted,
    total_ranked: sorted.length,
    local_matches: sorted.length,
    cached: false,
    fetched_at: new Date().toISOString(),
    hero_limit: heroLimit,
  });
});

app.get('/api/home/new-releases', (_req: Request, res: Response) => {
  const todayIso = getIndiaDateString();
  const releases = CATALOG_MOVIES
    .filter((movie) => typeof movie.release_days_ago === 'number')
    .sort((a, b) => (a.release_days_ago || 0) - (b.release_days_ago || 0))
    .map((movie) => {
      const age = movie.release_days_ago || 0;
      const releaseDate = addDaysIso(todayIso, -age);
      return {
        ...serializeMovieCard(movie),
        release_date: releaseDate,
        release_label: age === 0 ? 'Released today' : `Released ${age} day${age === 1 ? '' : 's'} ago`,
        release_type: 'release',
      };
    });

  res.json({
    status: 'success',
    movies: releases,
    window_days: 60,
  });
});

app.get('/api/home/catalogue-rows', (_req: Request, res: Response) => {
  const hollywood = CATALOG_MOVIES.filter((m) => m.category === 'Hollywood').map(serializeMovieCard);
  const bollywood = CATALOG_MOVIES.filter((m) => m.category === 'Bollywood').map(serializeMovieCard);
  const anime = CATALOG_MOVIES.filter((m) => m.category === 'Anime').map(serializeMovieCard);
  res.json({
    status: 'success',
    collections: { hollywood, bollywood, anime },
  });
});

app.get('/api/home/daily-picks', (req: Request, res: Response) => {
  const user = resolveUserFromRequest(req);
  const day = getIndiaDateString();
  const available = CATALOG_MOVIES.filter((movie) => movie.is_available && !movie.is_unreleased);
  const picks = [...available]
    .sort((a, b) => {
      const hashA = crypto.createHash('sha256').update(`${day}:${user.id}:${a.id}`).digest('hex');
      const hashB = crypto.createHash('sha256').update(`${day}:${user.id}:${b.id}`).digest('hex');
      return hashA.localeCompare(hashB);
    })
    .slice(0, 3)
    .map(serializeMovieCard);

  const dateLabel = new Date(`${day}T12:00:00+05:30`).toLocaleDateString('en-IN', {
    day: 'numeric',
    month: 'short',
  });

  res.json({
    status: 'success',
    date: day,
    date_label: dateLabel,
    movies: picks,
    personalized: !user.is_guest,
  });
});

app.get('/api/home/daily-checkin', (req: Request, res: Response) => {
  const user = resolveUserFromRequest(req);
  const todayIso = getIndiaDateString();
  res.json(computeDailyCheckinPayload(user.id, todayIso, false));
});

app.post('/api/home/daily-checkin', (req: Request, res: Response) => {
  const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
  if (!user) return;
  const todayIso = getIndiaDateString();
  res.json(computeDailyCheckinPayload(user.id, todayIso, true));
});

app.post('/api/recommendation-events', (req: Request, res: Response) => {
  const user = resolveUserFromRequest(req);
  const body = (req.body || {}) as Record<string, unknown>;
  const eventType = String(body.event_type || '').trim();
  if (!eventType) {
    res.status(400).json({ status: 'error', message: 'Missing event_type.' });
    return;
  }
  const movieIdRaw = body.movie_id;
  const movieId = movieIdRaw != null && /^\d+$/.test(String(movieIdRaw)) ? Number(movieIdRaw) : null;
  recommendationEvents.push({
    user_id: user.id,
    movie_id: movieId,
    event_type: eventType,
    source: String(body.source || 'miniapp'),
    metadata: typeof body.metadata === 'object' && body.metadata !== null ? (body.metadata as Record<string, unknown>) : {},
    created_at: new Date().toISOString(),
  });
  if (recommendationEvents.length > 500) recommendationEvents.shift();
  res.json({ status: 'success' });
});

// ---------- Upcoming & Reminders ----------

app.get('/api/upcoming', async (req: Request, res: Response) => {
  const limit = Math.max(1, Math.min(30, Number(req.query.limit) || 18));
  const todayIso = getIndiaDateString();
  const cacheKey = `upcoming:${todayIso}:${limit}`;
  const cached = getCached<Record<string, unknown>>(cacheKey);
  if (cached) {
    res.json(cached);
    return;
  }

  if (TMDB_API_KEY) {
    const startIso = addDaysIso(todayIso, 1);
    const endIso = addDaysIso(todayIso, 180);
    const [movieData, tvData] = await Promise.all([
      fetchJsonSafe(
        `https://api.themoviedb.org/3/discover/movie?api_key=${encodeURIComponent(TMDB_API_KEY)}&page=1&sort_by=primary_release_date.asc&vote_count.gte=1&with_release_type=2|3|4&primary_release_date.gte=${startIso}&primary_release_date.lte=${endIso}`,
        3500
      ),
      fetchJsonSafe(
        `https://api.themoviedb.org/3/discover/tv?api_key=${encodeURIComponent(TMDB_API_KEY)}&page=1&sort_by=first_air_date.asc&vote_count.gte=1&first_air_date.gte=${startIso}&first_air_date.lte=${endIso}`,
        3500
      ),
    ]);

    const liveItems: Array<Record<string, unknown>> = [];
    const seen = new Set<string>();
    const sources: Array<['movie' | 'tv', Record<string, unknown> | null]> = [
      ['movie', movieData],
      ['tv', tvData],
    ];
    for (const [mediaType, payload] of sources) {
      const results = Array.isArray(payload?.results) ? (payload.results as Array<Record<string, unknown>>) : [];
      for (const item of results) {
        const tmdbId = Number(item.id);
        const releaseDate = String((mediaType === 'movie' ? item.release_date : item.first_air_date) || '');
        const key = `${mediaType}:${tmdbId}`;
        if (!tmdbId || !/^\d{4}-\d{2}-\d{2}$/.test(releaseDate) || seen.has(key)) continue;
        if (releaseDate <= todayIso || releaseDate > endIso) continue;
        seen.add(key);
        const posterPath = typeof item.poster_path === 'string' ? item.poster_path : '';
        const backdropPath = typeof item.backdrop_path === 'string' ? item.backdrop_path : '';
        liveItems.push({
          id: `tmdb_${tmdbId}`,
          tmdb_id: tmdbId,
          media_type: mediaType,
          title: String(item.title || item.name || 'Unknown'),
          year: releaseDate.slice(0, 4),
          release_date: releaseDate,
          image: posterPath ? `https://image.tmdb.org/t/p/w500${posterPath}` : '/static/miniapp/poster-placeholder.svg',
          backdrop: backdropPath ? `https://image.tmdb.org/t/p/original${backdropPath}` : null,
          rating: Number(Number(item.vote_average || 0).toFixed(1)),
          category: mediaType === 'movie' ? 'Movie' : 'TV Series',
          source: 'tmdb',
          description: String(item.overview || ''),
          is_unreleased: true,
          is_upcoming: true,
          is_released: false,
          is_available: false,
          availability_state: 'upcoming',
          release_state: 'upcoming',
        });
      }
    }

    if (liveItems.length > 0) {
      liveItems.sort((a, b) => String(a.release_date).localeCompare(String(b.release_date)));
      const responseBody = {
        status: 'success',
        movies: liveItems.slice(0, limit),
        updated_at: new Date().toISOString(),
        partial: !movieData || !tvData,
      };
      setCached(cacheKey, responseBody, 6 * 60 * 60 * 1000);
      res.json(responseBody);
      return;
    }
  }

  const movies = UPCOMING_TITLES.slice(0, limit).map((item) => {
    const releaseDate = addDaysIso(todayIso, item.days_from_now);
    return {
      id: item.id,
      tmdb_id: item.tmdb_id,
      media_type: item.media_type,
      title: item.title,
      year: releaseDate.slice(0, 4),
      release_date: releaseDate,
      image: item.image,
      backdrop: item.backdrop,
      rating: item.rating,
      genre: item.genre,
      category: item.category,
      source: 'tmdb',
      description: item.description,
      is_unreleased: true,
      is_upcoming: true,
      is_released: false,
      is_available: false,
      availability_state: 'upcoming',
      release_state: 'upcoming',
    };
  });

  res.json({
    status: 'success',
    movies,
    updated_at: new Date().toISOString(),
    partial: false,
  });
});

app.get('/api/upcoming/reminder', (req: Request, res: Response) => {
  const user = resolveUserFromRequest(req);
  const rawTmdbId = String(req.query.tmdb_id || '').replace(/^tmdb_/, '').trim();
  const stage = String(req.query.stage || 'release').toLowerCase();
  if (!/^\d+$/.test(rawTmdbId)) {
    res.status(400).json({ status: 'error', message: 'Invalid release id.' });
    return;
  }
  const userMap = upcomingRemindersByUser.get(user.id);
  const existing = userMap?.get(rawTmdbId);
  const localMatch = CATALOG_MOVIES.find((m) => String(m.tmdb_id) === rawTmdbId && m.is_available);
  const releaseDateStr = existing?.release_date || String(req.query.release_date || '');
  const todayIso = getIndiaDateString();
  const isReleased = Boolean(releaseDateStr && releaseDateStr <= todayIso);
  const state = localMatch ? 'available' : isReleased ? 'released' : 'upcoming';

  res.json({
    status: 'success',
    reminder: Boolean(existing && (stage === 'release' ? existing.release_notification_requested : existing.availability_notification_requested)),
    release_notification_set: Boolean(existing?.release_notification_requested),
    availability_notification_set: Boolean(existing?.availability_notification_requested),
    release_notified: Boolean(existing?.release_notified_at),
    availability_notified: Boolean(existing?.availability_notified_at),
    availability_state: state,
    is_available: Boolean(localMatch),
    tmdb_id: rawTmdbId,
  });
});

app.post('/api/upcoming/reminder', (req: Request, res: Response) => {
  const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
  if (!user) return;
  const body = (req.body || {}) as Record<string, unknown>;
  const rawTmdbId = String(body.tmdb_id || body.id || '').replace(/^tmdb_/, '').trim();
  const action = String(body.action || 'set').toLowerCase();
  const stage = String(body.stage || 'release').toLowerCase();
  const title = String(body.title || 'Upcoming title').trim();
  const releaseDate = String(body.release_date || '').trim();

  if (!['release', 'availability'].includes(stage)) {
    res.status(400).json({ status: 'error', message: 'Invalid notification stage.' });
    return;
  }
  if (!/^\d+$/.test(rawTmdbId)) {
    res.status(400).json({ status: 'error', message: 'Invalid release id.' });
    return;
  }
  if (action !== 'remove' && !/^\d{4}-\d{2}-\d{2}$/.test(releaseDate)) {
    res.status(400).json({ status: 'error', message: 'A valid release date is required.' });
    return;
  }

  let userMap = upcomingRemindersByUser.get(user.id);
  if (!userMap) {
    userMap = new Map();
    upcomingRemindersByUser.set(user.id, userMap);
  }
  const existing = userMap.get(rawTmdbId);

  if (action === 'remove') {
    if (existing) {
      if (stage === 'release') existing.release_notification_requested = false;
      else existing.availability_notification_requested = false;
    }
    res.json({ status: 'success', message: 'Notification removed', reminder: false, tmdb_id: rawTmdbId });
    return;
  }

  const alreadyEnabled = Boolean(
    existing && (stage === 'release' ? existing.release_notification_requested : existing.availability_notification_requested)
  );
  userMap.set(rawTmdbId, {
    tmdb_id: rawTmdbId,
    title,
    release_date: releaseDate,
    release_notification_requested: stage === 'release' ? true : Boolean(existing?.release_notification_requested),
    availability_notification_requested: stage === 'availability' ? true : Boolean(existing?.availability_notification_requested),
    release_notified_at: existing?.release_notified_at || null,
    availability_notified_at: existing?.availability_notified_at || null,
  });

  res.json({
    status: 'success',
    message: alreadyEnabled ? 'Notification already enabled.' : 'Notification set.',
    reminder: true,
    already_enabled: alreadyEnabled,
    stage,
    tmdb_id: rawTmdbId,
  });
});

// ---------- Browse, Genres & Mood Reader ----------

app.get('/api/genres', (req: Request, res: Response) => {
  const browseType = String(req.query.type || 'all').toLowerCase();
  if (!['all', 'movies', 'tv'].includes(browseType)) {
    res.status(400).json({ status: 'error', message: 'Invalid genre type' });
    return;
  }

  const counts: Record<string, number> = {};
  const posterCandidates: Record<string, string[]> = {};
  for (const genre of CANONICAL_GENRES) {
    counts[genre.id] = 0;
    posterCandidates[genre.id] = [];
  }

  const ordered = [...CATALOG_MOVIES].sort((a, b) => Number(b.rating) - Number(a.rating) || b.id - a.id);
  for (const movie of ordered) {
    if (!browseTypeMatches(browseType, movie.category, movie.seasons_data, movie.content_type)) continue;
    for (const genreId of normalizeCatalogueGenres(movie.genre)) {
      if (genreId in counts) {
        counts[genreId] += 1;
        if (movie.image && !posterCandidates[genreId].includes(movie.image) && posterCandidates[genreId].length < 12) {
          posterCandidates[genreId].push(movie.image);
        }
      }
    }
  }

  const usedPosters = new Set<string>();
  const genres = CANONICAL_GENRES.map((genre) => {
    const unique = posterCandidates[genre.id].filter((poster) => !usedPosters.has(poster)).slice(0, 3);
    unique.forEach((p) => usedPosters.add(p));
    return {
      id: genre.id,
      label: genre.label,
      group: genre.group,
      description: genre.description,
      count: counts[genre.id],
      posters: unique.length ? unique : posterCandidates[genre.id].slice(0, 3),
    };
  });

  res.json({ status: 'success', genres, source: 'database' });
});

app.get('/api/genre/:genreId', (req: Request, res: Response) => {
  const genreId = String(req.params.genreId || '');
  const canonical = GENRE_BY_ID[genreId];
  if (!canonical) {
    res.status(404).json({ status: 'error', message: 'Unknown genre' });
    return;
  }
  const browseType = String(req.query.type || 'all').toLowerCase();
  if (!['all', 'movies', 'tv'].includes(browseType)) {
    res.status(400).json({ status: 'error', message: 'Invalid genre type' });
    return;
  }

  const movies = CATALOG_MOVIES
    .filter(
      (movie) =>
        normalizeCatalogueGenres(movie.genre).has(genreId) &&
        browseTypeMatches(browseType, movie.category, movie.seasons_data, movie.content_type)
    )
    .map(serializeMovieCard);

  res.json({
    status: 'success',
    genre: {
      id: canonical.id,
      label: canonical.label,
      group: canonical.group,
      description: canonical.description,
      count: movies.length,
    },
    movies,
    source: 'database',
  });
});

app.get('/api/browse', (req: Request, res: Response) => {
  const browseType = String(req.query.type || 'all').toLowerCase();
  if (!['all', 'movies', 'tv'].includes(browseType)) {
    res.status(400).json({ status: 'error', message: 'Invalid browse type' });
    return;
  }

  const decades: Array<[string, string, number | null, number]> = [
    ['2020s', '2020s Standouts', 2020, 2029],
    ['2010s', '2010s Essentials', 2010, 2019],
    ['2000s', '2000s Classics', 2000, 2009],
    ['pre-2000', 'Golden Oldies', null, 1999],
  ];

  const collections = decades.map(([id, label, startYear, endYear]) => {
    const items = CATALOG_MOVIES
      .filter((movie) => {
        if (!browseTypeMatches(browseType, movie.category, movie.seasons_data, movie.content_type)) return false;
        if (startYear !== null) return movie.year >= startYear && movie.year <= endYear;
        return movie.year <= endYear;
      })
      .sort((a, b) => Number(b.rating) - Number(a.rating) || b.year - a.year)
      .slice(0, 12)
      .map(serializeMovieCard);
    return { id, label, movies: items };
  });

  res.json({ status: 'success', type: browseType, collections, source: 'database' });
});

app.get('/api/browse/surprise', (req: Request, res: Response) => {
  const browseType = String(req.query.type || 'all').toLowerCase();
  const mood = String(req.query.mood || 'any').trim().toLowerCase();
  const language = String(req.query.language || 'any').trim().toLowerCase();
  const availability = String(req.query.availability || 'available').trim().toLowerCase();

  if (
    !['all', 'movies', 'tv'].includes(browseType) ||
    !(mood in MOOD_GENRES) ||
    !['any', 'hindi', 'english'].includes(language) ||
    !['all', 'available'].includes(availability)
  ) {
    res.status(400).json({ status: 'error', message: 'Invalid Mood Reader filters.' });
    return;
  }

  const excludeRaw = String(req.query.exclude || '').split(',').filter(Boolean).slice(0, 8);
  const excludedIds = new Set(excludeRaw.map((id) => id.trim()));

  const candidates = CATALOG_MOVIES
    .filter((movie) => {
      if (movie.is_unreleased || excludedIds.has(String(movie.id))) return false;
      if (!browseTypeMatches(browseType, movie.category, movie.seasons_data, movie.content_type)) return false;
      if (availability === 'available' && !movie.is_available) return false;
      if (language !== 'any' && !new RegExp(`\\b${language}\\b`, 'i').test(movie.language || '')) return false;
      return true;
    })
    .map((movie) => {
      const tags = `${movie.genre},${movie.category},${movie.content_type}`
        .toLowerCase()
        .split(/[,/&|]/)
        .map((part) => part.trim())
        .filter(Boolean);
      const moodSet = MOOD_GENRES[mood];
      const matched = [...new Set(tags.filter((tag) => moodSet.has(tag)))];
      return { movie, matched };
    });

  if (!candidates.length) {
    res.json({
      status: 'success',
      type: browseType,
      movie: null,
      message: 'No matching title found for this filter combination. Try widening your filters.',
    });
    return;
  }

  const exact = mood === 'any' ? [] : candidates.filter((item) => item.matched.length > 0);
  const pool = exact.length > 0 ? exact : candidates;
  const selected = pool[Math.floor(Math.random() * pool.length)];

  res.json({
    status: 'success',
    type: browseType,
    movie: serializeMovieCard(selected.movie),
    mood_match: selected.matched.length > 0,
    matched_genres: selected.matched,
    personalized: false,
  });
});

// ---------- Catalogue Pagination & Movie Details ----------

app.get('/api/movies', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const page = Number(req.query.page ?? 1);
    const limit = Number(req.query.limit ?? 40);
    const availability = String(req.query.availability || 'all').trim().toLowerCase();

    if (!Number.isInteger(page) || !Number.isInteger(limit) || page < 1 || limit < 1 || limit > 100) {
      sendApiError(res, 400, 'INVALID_PAGINATION', 'Page must be positive and limit must be between 1 and 100.');
      return;
    }
    if (!['all', 'available', 'request'].includes(availability)) {
      sendApiError(res, 400, 'INVALID_AVAILABILITY', 'Availability must be all, available, or request.');
      return;
    }

    if (isDatabaseActive()) {
      const dbAvailability = availability === 'request' ? 'unavailable' : availability;
      const dbPage = await fetchCatalogPageFromDb({
        page,
        limit,
        availability: dbAvailability,
      });
      if (dbPage) {
        const offset = (page - 1) * limit;
        sendApiSuccess(res, {
          movies: dbPage.movies.map(serializeMovieCard),
          has_more: offset + limit < dbPage.total,
          data_mode: 'production',
        });
        return;
      }
    }

    const filtered = CATALOG_MOVIES
      .filter((movie) => {
        if (availability === 'available') return movie.is_available;
        if (availability === 'request') return !movie.is_available;
        return true;
      })
      .sort((a, b) => b.created_at.localeCompare(a.created_at) || b.id - a.id);

    const offset = (page - 1) * limit;
    const slice = filtered.slice(offset, offset + limit).map(serializeMovieCard);

    sendApiSuccess(res, {
      movies: slice,
      has_more: offset + limit < filtered.length,
      data_mode: getActiveDataMode(),
    });
  } catch (err) {
    next(err);
  }
});

app.get('/api/movie/:movieId', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const movieId = Number(req.params.movieId);
    if (!Number.isInteger(movieId) || movieId <= 0) {
      sendApiError(res, 404, 'MOVIE_NOT_FOUND', 'Movie not found');
      return;
    }
    if (isDatabaseActive()) {
      const dbMovie = await fetchCatalogMovieFromDb(movieId);
      if (dbMovie) {
        sendApiSuccess(res, { movie: serializeMovieDetail(dbMovie), data_mode: 'production' });
        return;
      }
    }
    const movie = getMovieById(movieId);
    if (!movie) {
      sendApiError(res, 404, 'MOVIE_NOT_FOUND', 'Movie not found');
      return;
    }
    sendApiSuccess(res, { movie: serializeMovieDetail(movie), data_mode: getActiveDataMode() });
  } catch (err) {
    next(err);
  }
});

// ---------- Ratings, Rating Sources & Reviews ----------

app.get('/api/movie/:movieId/rating', (req: Request, res: Response) => {
  const movieId = Number(req.params.movieId);
  const movie = getMovieById(movieId);
  if (!movie) {
    res.status(404).json({ status: 'error', message: 'Movie not found' });
    return;
  }
  if (movie.is_unreleased) {
    res.json({
      status: 'success',
      average: 0,
      count: 0,
      user_rating: null,
      can_rate: false,
      is_upcoming: true,
      rating_locked: true,
    });
    return;
  }
  const user = resolveUserFromRequest(req);
  res.json({ status: 'success', ...computeRatingSummary(movieId, user.id) });
});

app.post('/api/movie/:movieId/rating', (req: Request, res: Response) => {
  const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
  if (!user) return;
  const movieId = Number(req.params.movieId);
  const movie = getMovieById(movieId);
  if (!movie) {
    sendApiError(res, 404, 'MOVIE_NOT_FOUND', 'Movie not found');
    return;
  }
  if (movie.is_unreleased) {
    sendApiError(res, 409, 'RATINGS_LOCKED', 'Ratings open after release.');
    return;
  }
  const rating = (req.body as Record<string, unknown>)?.rating;
  if (typeof rating !== 'number' || !Number.isInteger(rating) || rating < 1 || rating > 5) {
    sendApiError(res, 400, 'INVALID_RATING', 'Rating must be an integer from 1 to 5.');
    return;
  }
  let movieRatings = ratingsByMovie.get(movieId);
  if (!movieRatings) {
    movieRatings = new Map();
    ratingsByMovie.set(movieId, movieRatings);
  }
  movieRatings.set(user.id, rating);
  res.json({ status: 'success', ...computeRatingSummary(movieId, user.id) });
});

app.get('/api/movie/:movieId/reviews', (req: Request, res: Response) => {
  const movieId = Number(req.params.movieId);
  const movie = getMovieById(movieId);
  if (!movie) {
    res.status(404).json({ status: 'error', message: 'Movie not found' });
    return;
  }
  const user = resolveUserFromRequest(req);
  const list = reviewsByMovie.get(movieId) || [];
  const ratingMap = ratingsByMovie.get(movieId);
  const reviews = [...list]
    .sort((a, b) => b.updated_at.localeCompare(a.updated_at) || b.id - a.id)
    .slice(0, 10)
    .map((item) => ({
      id: item.id,
      content: item.content,
      author: item.author,
      created_at: item.created_at,
      updated_at: item.updated_at,
      rating: ratingMap?.get(item.user_id) ?? null,
      review_source: 'flimfybox',
    }));
  const own = list.find((item) => item.user_id === user.id);

  res.json({
    status: 'success',
    reviews,
    review_count: list.length,
    user_review: own ? own.content : null,
    can_review: !movie.is_unreleased,
    is_upcoming: Boolean(movie.is_unreleased),
  });
});

app.post('/api/movie/:movieId/reviews', (req: Request, res: Response) => {
  const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
  if (!user) return;
  const movieId = Number(req.params.movieId);
  const movie = getMovieById(movieId);
  if (!movie) {
    sendApiError(res, 404, 'MOVIE_NOT_FOUND', 'Movie not found');
    return;
  }
  if (movie.is_unreleased) {
    sendApiError(res, 409, 'REVIEWS_LOCKED', 'Reviews open after release.');
    return;
  }
  const rawReview = (req.body as Record<string, unknown>)?.review;
  if (typeof rawReview !== 'string') {
    sendApiError(res, 400, 'INVALID_REVIEW', 'Write a review between 20 and 800 characters.');
    return;
  }
  const review = rawReview
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .split('\n')
    .map((line) => line.trimEnd())
    .join('\n')
    .trim();

  if (review.length < 20 || review.length > 800) {
    sendApiError(res, 400, 'INVALID_REVIEW', 'Write a review between 20 and 800 characters.');
    return;
  }

  let list = reviewsByMovie.get(movieId);
  if (!list) {
    list = [];
    reviewsByMovie.set(movieId, list);
  }
  const now = new Date().toISOString();
  const existing = list.find((item) => item.user_id === user.id);
  if (existing) {
    existing.content = review;
    existing.updated_at = now;
  } else {
    list.push({
      id: allocateReviewId(),
      movie_id: movieId,
      user_id: user.id,
      author: user.first_name || 'FlimfyBox member',
      content: review,
      created_at: now,
      updated_at: now,
    });
  }

  res.json({ status: 'success', review, message: 'Your FlimfyBox review is saved.' });
});

app.get('/api/movie/:movieId/rating-sources', async (req: Request, res: Response) => {
  const movieId = Number(req.params.movieId);
  const movie = getMovieById(movieId);
  if (!movie) {
    res.status(404).json({ status: 'error', message: 'Movie not found' });
    return;
  }

  let imdbScore: number | null = Number(movie.rating) || null;
  let imdbVotes: string | null = movie.imdb_votes || null;
  let tmdbScore: number | null = movie.tmdb_rating || null;
  let tmdbVotes: number | null = movie.tmdb_votes || null;

  // Optionally enrich with live TMDb/OMDb if keys are configured
  if (TMDB_API_KEY && movie.tmdb_id) {
    const mediaType = isCatalogueTv(movie.category, movie.seasons_data, movie.content_type) ? 'tv' : 'movie';
    const data = await fetchJsonSafe(
      `https://api.themoviedb.org/3/${mediaType}/${movie.tmdb_id}?api_key=${encodeURIComponent(TMDB_API_KEY)}`
    );
    if (data && typeof data.vote_average === 'number' && Number(data.vote_count) > 0) {
      tmdbScore = Number(data.vote_average.toFixed(1));
      tmdbVotes = Number(data.vote_count);
    }
  }
  if (OMDB_API_KEY && movie.imdb_id) {
    const data = await fetchJsonSafe(
      `https://www.omdbapi.com/?i=${encodeURIComponent(movie.imdb_id)}&apikey=${encodeURIComponent(OMDB_API_KEY)}`
    );
    if (data && data.Response === 'True' && data.imdbRating && data.imdbRating !== 'N/A') {
      imdbScore = Number(data.imdbRating);
      imdbVotes = String(data.imdbVotes || imdbVotes || '');
    }
  }

  res.json({
    status: 'success',
    imdb: { score: imdbScore, votes: imdbVotes },
    tmdb: { score: tmdbScore, votes: tmdbVotes },
    source_status: {
      imdb: imdbScore !== null ? 'fresh' : 'unlinked',
      tmdb: tmdbScore !== null ? 'fresh' : 'unlinked',
    },
    cache_status: 'fresh',
  });
});

app.get('/api/tmdb/:mediaType/:tmdbId/rating-sources', async (req: Request, res: Response) => {
  const mediaType = String(req.params.mediaType || '');
  const tmdbId = Number(req.params.tmdbId);
  if (!['movie', 'tv'].includes(mediaType) || !Number.isInteger(tmdbId) || tmdbId <= 0) {
    res.status(404).json({ status: 'error', message: 'Invalid TMDb title' });
    return;
  }

  const upcoming = UPCOMING_TITLES.find((item) => item.tmdb_id === tmdbId);
  const local = CATALOG_MOVIES.find((item) => item.tmdb_id === tmdbId);

  if (TMDB_API_KEY) {
    const data = await fetchJsonSafe(
      `https://api.themoviedb.org/3/${mediaType}/${tmdbId}?api_key=${encodeURIComponent(TMDB_API_KEY)}`
    );
    if (data && typeof data.vote_average === 'number' && Number(data.vote_count) > 0) {
      res.json({
        status: 'success',
        imdb: { score: null, votes: null },
        tmdb: { score: Number(data.vote_average.toFixed(1)), votes: Number(data.vote_count) },
        source_status: { imdb: 'unlinked', tmdb: 'fresh' },
        cache_status: 'fresh',
      });
      return;
    }
  }

  if (local) {
    res.json({
      status: 'success',
      imdb: { score: Number(local.rating) || null, votes: local.imdb_votes || null },
      tmdb: { score: local.tmdb_rating, votes: local.tmdb_votes },
      source_status: { imdb: 'fresh', tmdb: 'fresh' },
      cache_status: 'fresh',
    });
    return;
  }

  res.json({
    status: 'success',
    imdb: { score: null, votes: null },
    tmdb: upcoming ? { score: upcoming.rating, votes: 120 } : { score: null, votes: null },
    source_status: { imdb: 'unlinked', tmdb: upcoming ? 'fresh' : 'unlinked' },
    cache_status: upcoming ? 'fresh' : 'unlinked',
  });
});

async function fetchLiveTmdbReviews(mediaType: string, tmdbId: number) {
  if (!TMDB_API_KEY) return null;
  const cacheKey = `tmdb-reviews:${mediaType}:${tmdbId}`;
  const cached = getCached<{ reviews: Array<Record<string, unknown>>; review_count: number }>(cacheKey);
  if (cached) return cached;

  const data = await fetchJsonSafe(
    `https://api.themoviedb.org/3/${mediaType}/${tmdbId}/reviews?api_key=${encodeURIComponent(TMDB_API_KEY)}&language=en-US&page=1`,
    3000
  );
  if (!data || !Array.isArray(data.results)) return null;

  const reviews = (data.results as Array<Record<string, unknown>>)
    .slice(0, 5)
    .map((item) => {
      const content = String(item.content || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
      if (!content) return null;
      const authorDetails = (item.author_details || {}) as Record<string, unknown>;
      const rawRating = authorDetails.rating;
      const rating = typeof rawRating === 'number' && Number.isFinite(rawRating) ? rawRating : null;
      const rawUrl = String(item.url || '').trim();
      const safeUrl = /^https:\/\/www\.themoviedb\.org\/review\/[A-Za-z0-9_-]+\/?$/.test(rawUrl) ? rawUrl : '';
      return {
        id: String(item.id || ''),
        author: String(item.author || 'TMDb member').slice(0, 100),
        content: content.slice(0, 1200),
        rating,
        created_at: String(item.created_at || '').slice(0, 40),
        url: safeUrl,
        source: 'TMDb',
      };
    })
    .filter((entry): entry is NonNullable<typeof entry> => Boolean(entry));

  const payload = {
    reviews,
    review_count: Number(data.total_results) || reviews.length,
  };
  setCached(cacheKey, payload, 6 * 60 * 60 * 1000);
  return payload;
}

app.get('/api/movie/:movieId/audience-reviews', async (req: Request, res: Response) => {
  const movieId = Number(req.params.movieId);
  const movie = getMovieById(movieId);
  if (!movie) {
    res.status(404).json({ status: 'error', message: 'Movie not found' });
    return;
  }
  const mediaType = isCatalogueTv(movie.category, movie.seasons_data, movie.content_type) ? 'tv' : 'movie';
  const live = movie.tmdb_id ? await fetchLiveTmdbReviews(mediaType, movie.tmdb_id) : null;
  if (live && live.reviews.length > 0) {
    res.json({
      status: 'success',
      source: 'TMDb',
      reviews: live.reviews,
      review_count: live.review_count,
      cache_status: 'fresh',
      linked: true,
    });
    return;
  }
  res.json({
    status: 'success',
    source: 'TMDb',
    reviews: movie.audience_reviews.slice(0, 5),
    review_count: movie.audience_reviews.length,
    cache_status: 'fresh',
    linked: true,
  });
});

app.get('/api/tmdb/:mediaType/:tmdbId/audience-reviews', async (req: Request, res: Response) => {
  const mediaType = String(req.params.mediaType || '');
  const tmdbId = Number(req.params.tmdbId);
  if (!['movie', 'tv'].includes(mediaType) || !Number.isInteger(tmdbId) || tmdbId <= 0) {
    res.status(404).json({ status: 'error', message: 'Invalid TMDb title' });
    return;
  }
  const live = await fetchLiveTmdbReviews(mediaType, tmdbId);
  if (live) {
    res.json({
      status: 'success',
      source: 'TMDb',
      reviews: live.reviews,
      review_count: live.review_count,
      cache_status: 'fresh',
      linked: true,
    });
    return;
  }
  const local = CATALOG_MOVIES.find((item) => item.tmdb_id === tmdbId);
  const reviews = local ? local.audience_reviews.slice(0, 5) : [];
  res.json({
    status: 'success',
    source: 'TMDb',
    reviews,
    review_count: reviews.length,
    cache_status: 'fresh',
    linked: true,
  });
});

// ---------- Search & Suggest ----------

app.get('/api/search', async (req: Request, res: Response) => {
  const query = String(req.query.q || '').trim();
  if (!query) {
    res.status(400).json({ status: 'error', message: 'Missing query' });
    return;
  }
  if (query.length > 200) {
    res.status(400).json({ status: 'error', message: 'Query is too long' });
    return;
  }
  const autocomplete = ['1', 'true'].includes(String(req.query.autocomplete || '').toLowerCase()) || query.length < 3;
  const limit = Math.max(1, Math.min(20, Number(req.query.limit) || 20));
  const normalizedQuery = normalizeSearchKey(query);
  if (!normalizedQuery) {
    res.json({ status: 'success', results: [] });
    return;
  }

  const lowerQuery = query.toLowerCase();
  const queryTokens = lowerQuery.split(/\s+/).filter(Boolean);

  const scored = CATALOG_MOVIES
    .map((movie) => {
      const titleLower = movie.title.toLowerCase();
      const titleNorm = normalizeSearchKey(movie.title);
      const genreLower = movie.genre.toLowerCase();
      const castLower = movie.cast.toLowerCase();
      let score = 0;

      if (titleNorm.startsWith(normalizedQuery)) score += 100;
      else if (titleLower.includes(lowerQuery) || titleNorm.includes(normalizedQuery)) score += 80;
      else if (queryTokens.every((token) => titleLower.includes(token) || genreLower.includes(token) || castLower.includes(token))) {
        score += 60;
      } else if (!autocomplete && (genreLower.includes(lowerQuery) || castLower.includes(lowerQuery))) {
        score += 40;
      }

      return { movie, score };
    })
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score || b.movie.year - a.movie.year || b.movie.id - a.movie.id)
    .slice(0, limit)
    .map((entry) => serializeMovieCard(entry.movie));

  if (autocomplete || scored.length > 0) {
    res.json({ status: 'success', results: scored });
    return;
  }

  // Optional live TMDB search fallback when local catalog has no match and TMDB_API_KEY is configured
  const tmdbResults: Array<Record<string, unknown>> = [];
  if (TMDB_API_KEY) {
    const data = await fetchJsonSafe(
      `https://api.themoviedb.org/3/search/multi?api_key=${encodeURIComponent(TMDB_API_KEY)}&query=${encodeURIComponent(query)}`
    );
    const items = Array.isArray(data?.results) ? (data.results as Array<Record<string, unknown>>) : [];
    const todayIso = getIndiaDateString();
    for (const item of items.slice(0, 8)) {
      const mediaType = item.media_type === 'tv' ? 'tv' : item.media_type === 'movie' ? 'movie' : null;
      if (!mediaType) continue;
      const releaseDate = String((mediaType === 'tv' ? item.first_air_date : item.release_date) || '');
      const isUpcoming = /^\d{4}-\d{2}-\d{2}$/.test(releaseDate) && releaseDate > todayIso;
      const imgPath = item.poster_path || item.backdrop_path;
      tmdbResults.push({
        id: `tmdb_${mediaType}_${item.id}`,
        tmdb_id: item.id,
        title: String(item.title || item.name || 'Unknown'),
        year: releaseDate.slice(0, 4),
        release_date: releaseDate,
        image: imgPath ? `https://image.tmdb.org/t/p/w500${imgPath}` : '/static/miniapp/poster-placeholder.svg',
        rating: Number(Number(item.vote_average || 0).toFixed(1)),
        genre: '',
        category: mediaType === 'movie' ? 'Movie' : 'TV Series',
        media_type: mediaType,
        source: 'tmdb',
        description: String(item.overview || ''),
        is_available: false,
        is_upcoming: isUpcoming,
        availability_state: isUpcoming ? 'upcoming' : 'unavailable',
      });
    }
  }

  res.json({ status: 'success', results: tmdbResults });
});

app.get('/api/suggest', (req: Request, res: Response) => {
  const q = String(req.query.q || '').trim().toLowerCase();
  if (!q) {
    res.json({ status: 'success', suggestions: [] });
    return;
  }
  const suggestions = CATALOG_MOVIES
    .filter((movie) => movie.title.toLowerCase().includes(q))
    .slice(0, 6)
    .map((movie) => movie.title);
  res.json({ status: 'success', suggestions });
});

app.post('/api/smart-merge', (req: Request, res: Response) => {
  const body = (req.body || {}) as Record<string, unknown>;
  const results = Array.isArray(body.results) ? body.results : [];
  res.json({ status: 'success', results });
});

// ---------- Movie Requests ----------

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

app.post('/api/request', async (req: Request, res: Response) => {
  const body = (req.body || {}) as Record<string, unknown>;
  if (!body || typeof body.title !== 'string') {
    res.status(400).json({ status: 'error', message: 'Missing movie title' });
    return;
  }
  const title = body.title.trim().slice(0, 200);
  if (!title) {
    res.status(400).json({ status: 'error', message: 'Missing movie title' });
    return;
  }

  const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
  if (!user) return;
  const existing = movieRequests.find(
    (item) => item.user_id === user.id && item.title.toLowerCase() === title.toLowerCase()
  );

  let requestId: number;
  let priorityRequest = false;
  let priorityTicketUsed = false;
  let remainingTickets = priorityTicketsByUser.get(user.id) || 0;

  if (existing) {
    existing.requested_at = new Date().toISOString();
    requestId = existing.id;
    priorityRequest = existing.priority;
  } else {
    if (remainingTickets > 0) {
      priorityRequest = true;
      priorityTicketUsed = true;
      remainingTickets -= 1;
      priorityTicketsByUser.set(user.id, remainingTickets);
    }
    requestId = allocateRequestId();
    movieRequests.push({
      id: requestId,
      user_id: user.id,
      username: user.username,
      first_name: user.first_name,
      title,
      priority: priorityRequest,
      requested_at: new Date().toISOString(),
    });
  }

  let adminNotified = false;
  if (TELEGRAM_BOT_TOKEN && REQUEST_CHANNEL_ID) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 4000);
    try {
      let msgText =
        `🎬 <b>New WebApp Request!</b> 🎬\n\n` +
        `Movie: <b>${escapeHtml(title)}</b>\n` +
        `User: ${escapeHtml(user.first_name)} (<code>${user.id}</code>)\n`;
      if (priorityRequest) {
        msgText += `\n⚡ <b>PRIORITY REQUEST</b>\nBoost earned from a FlimfyBox streak.\n`;
      }
      if (user.username) {
        msgText += `Username: @${escapeHtml(user.username)}\n`;
      }
      msgText += `From: 🌐 Web Portal`;

      const tgRes = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({
          chat_id: REQUEST_CHANNEL_ID,
          text: msgText,
          parse_mode: 'HTML',
          reply_markup: {
            inline_keyboard: [
              [{ text: '✅ Movie Add Kar Di Gai Hai', callback_data: `reqA_${requestId}` }],
              [{ text: '❌ Nahi Mili', callback_data: `reqN_${requestId}` }],
            ],
          },
        }),
      });
      if (tgRes.ok) {
        const tgData = (await tgRes.json()) as Record<string, unknown>;
        adminNotified = Boolean(tgData.ok);
      }
    } catch {
      adminNotified = false;
    } finally {
      clearTimeout(timer);
    }
  }

  res.json({
    status: 'success',
    message: priorityTicketUsed
      ? adminNotified
        ? 'Priority request sent to our team!'
        : 'Priority request saved! Your streak boost was applied.'
      : adminNotified
        ? 'Request saved & Admin notified.'
        : 'Request saved! Our catalogue team has received your title request.',
    request_id: requestId,
    guest: Boolean(user.is_guest),
    admin_notified: adminNotified,
    priority_request: priorityRequest,
    priority_ticket_used: priorityTicketUsed,
    priority_tickets: remainingTickets,
  });
});

// ---------- My List (Watchlist) & Availability Notifications ----------

app.get('/api/my-list', (req: Request, res: Response) => {
  const user = resolveUserFromRequest(req);
  const list = getUserWatchlist(user.id);
  const movies = [...list.values()]
    .sort((a, b) => b.created_at.localeCompare(a.created_at))
    .map((entry) => getMovieById(entry.movie_id))
    .filter((movie): movie is CatalogMovie => Boolean(movie))
    .map(serializeMovieCard);

  res.json({ status: 'success', movies });
});

app.post('/api/my-list', (req: Request, res: Response) => {
  const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
  if (!user) return;
  const movieId = Number((req.body as Record<string, unknown>)?.movie_id);
  if (!Number.isInteger(movieId) || movieId <= 0) {
    sendApiError(res, 400, 'INVALID_MOVIE', 'Invalid movie');
    return;
  }
  const movie = getMovieById(movieId);
  if (!movie) {
    sendApiError(res, 404, 'MOVIE_NOT_FOUND', 'Movie not found');
    return;
  }
  const list = getUserWatchlist(user.id);
  if (!list.has(movieId)) {
    list.set(movieId, {
      movie_id: movieId,
      created_at: new Date().toISOString(),
      availability_notification_requested: false,
      availability_notified_at: null,
    });
  }
  res.json({ status: 'success', saved: true, movie_id: movieId });
});

app.get('/api/my-list/:movieId/status', (req: Request, res: Response) => {
  const user = resolveUserFromRequest(req);
  const movieId = Number(req.params.movieId);
  const list = getUserWatchlist(user.id);
  res.json({ status: 'success', saved: list.has(movieId) });
});

app.delete('/api/my-list/:movieId', (req: Request, res: Response) => {
  const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
  if (!user) return;
  const movieId = Number(req.params.movieId);
  const list = getUserWatchlist(user.id);
  list.delete(movieId);
  res.json({ status: 'success', saved: false, movie_id: movieId });
});

app.get('/api/my-list/:movieId/availability-notification', (req: Request, res: Response) => {
  const user = resolveUserFromRequest(req);
  const movieId = Number(req.params.movieId);
  const movie = getMovieById(movieId);
  if (!movie) {
    res.status(404).json({ status: 'error', message: 'Title not found.' });
    return;
  }
  const entry = getUserWatchlist(user.id).get(movieId);
  res.json({
    status: 'success',
    saved: Boolean(entry),
    requested: Boolean(entry?.availability_notification_requested),
    notified: Boolean(entry?.availability_notified_at),
    available: movie.is_available,
  });
});

app.post('/api/my-list/:movieId/availability-notification', (req: Request, res: Response) => {
  const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
  if (!user) return;
  const movieId = Number(req.params.movieId);
  const movie = getMovieById(movieId);
  if (!movie) {
    res.status(404).json({ status: 'error', message: 'Title not found.' });
    return;
  }
  const enabled = (req.body as Record<string, unknown>)?.enabled;
  if (typeof enabled !== 'boolean') {
    res.status(400).json({ status: 'error', message: 'Choose whether notifications are enabled.' });
    return;
  }

  const list = getUserWatchlist(user.id);
  const existing = list.get(movieId);
  if (enabled && movie.is_available) {
    res.json({
      status: 'success',
      saved: Boolean(existing),
      requested: Boolean(existing?.availability_notification_requested),
      notified: Boolean(existing?.availability_notified_at),
      available: true,
      already_available: true,
      message: 'This title is already available.',
    });
    return;
  }

  const wasRequested = Boolean(existing?.availability_notification_requested);
  if (enabled) {
    list.set(movieId, {
      movie_id: movieId,
      created_at: existing?.created_at || new Date().toISOString(),
      availability_notification_requested: true,
      availability_notified_at: existing?.availability_notified_at || null,
    });
  } else if (existing) {
    existing.availability_notification_requested = false;
  }

  res.json({
    status: 'success',
    saved: Boolean(existing) || enabled,
    requested: enabled,
    notified: Boolean(existing?.availability_notified_at),
    available: movie.is_available,
    already_enabled: wasRequested && enabled,
  });
});

// ---------- Global Community Chat (PostgreSQL + Real Telegram Relay) ----------

app.get('/api/global-chat', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const user = resolveUserFromRequest(req);
    const afterIdParam = req.query.after_id;
    const afterId = afterIdParam !== undefined ? Number(afterIdParam) : null;

    if (isDatabaseActive()) {
      const dbMessages = await fetchGlobalChatFromDb(afterId);
      if (dbMessages) {
        const messages = dbMessages.map((msg) => ({
          id: msg.id,
          is_mine: user.id !== 0 && msg.user_id === user.id,
          username: msg.username,
          first_name: msg.first_name || 'Community member',
          message: msg.message,
          created_at: msg.created_at,
          source: msg.source,
          client_id: msg.client_id,
          relay_status: msg.relay_status,
          photo_url: msg.photo_url,
        }));
        const relayUpdates = dbMessages
          .filter((msg) => user.id !== 0 && msg.user_id === user.id)
          .slice(-50)
          .map((msg) => ({ id: msg.id, relay_status: msg.relay_status }));
        sendApiSuccess(res, { messages, relay_updates: relayUpdates, data_mode: 'production' });
        return;
      }
    }

    const filtered =
      afterId !== null && Number.isFinite(afterId) && afterId >= 0
        ? globalChatMessages.filter((msg) => msg.id > afterId).slice(0, 100)
        : globalChatMessages.slice(-80);

    const messages = filtered.map((msg) => ({
      id: msg.id,
      is_mine: user.id !== 0 && msg.user_id === user.id,
      username: msg.username,
      first_name: msg.first_name || 'Community member',
      message: msg.message,
      created_at: msg.created_at,
      source: msg.source,
      client_id: msg.client_id,
      relay_status: msg.relay_status,
      photo_url: msg.photo_url,
    }));

    const relayUpdates = globalChatMessages
      .filter((msg) => user.id !== 0 && msg.user_id === user.id)
      .slice(-50)
      .map((msg) => ({ id: msg.id, relay_status: msg.relay_status }));

    sendApiSuccess(res, { messages, relay_updates: relayUpdates, data_mode: getActiveDataMode() });
  } catch (err) {
    next(err);
  }
});

app.post('/api/global-chat', chatAndLinkRateLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const user = requireMutationIdentity(req, res, TELEGRAM_BOT_TOKEN);
    if (!user) return;
    const body = (req.body || {}) as Record<string, unknown>;
    const message = String(body.message || '').trim();
    const clientId = String(body.client_id || '').trim();

    if (!message || message.length > 3800) {
      sendApiError(res, 400, 'INVALID_MESSAGE', 'Message must be between 1 and 3800 characters.');
      return;
    }
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(clientId)) {
      sendApiError(res, 400, 'INVALID_CLIENT_ID', 'Invalid message id. Please retry.');
      return;
    }

    const relay = await relayGlobalChatToTelegram(user, message, String(res.locals.requestId || ''));

    if (isDatabaseActive()) {
      const saved = await insertGlobalChatInDb({
        user,
        message,
        clientId,
        relayStatus: relay.relay_status,
        telegramChatId: relay.telegram_chat_id,
        telegramMessageId: relay.telegram_message_id,
      });
      if (saved) {
        sendApiSuccess(res, {
          message: {
            id: saved.id,
            is_mine: true,
            username: saved.username,
            first_name: saved.first_name,
            message: saved.message,
            photo_url: saved.photo_url,
            created_at: saved.created_at,
            source: saved.source,
            client_id: saved.client_id,
            relay_status: saved.relay_status,
          },
          telegram_relayed: relay.relayed,
          data_mode: 'production',
        });
        return;
      }
    }

    const duplicate = globalChatMessages.find((m) => m.user_id === user.id && m.client_id === clientId);
    if (duplicate) {
      if (duplicate.message !== message) {
        sendApiError(res, 409, 'CLIENT_ID_CONFLICT', 'Message retry did not match.');
        return;
      }
      sendApiSuccess(res, {
        message: {
          id: duplicate.id,
          is_mine: true,
          username: duplicate.username,
          first_name: duplicate.first_name,
          message: duplicate.message,
          photo_url: duplicate.photo_url,
          created_at: duplicate.created_at,
          source: duplicate.source,
          client_id: duplicate.client_id,
          relay_status: duplicate.relay_status,
        },
        telegram_relayed: relay.relayed,
      });
      return;
    }

    const entry = {
      id: allocateChatId(),
      user_id: user.id,
      username: user.username || '',
      first_name: user.first_name || 'Guest',
      message,
      created_at: new Date().toISOString(),
      source: 'miniapp' as const,
      client_id: clientId,
      relay_status: relay.relay_status,
      photo_url: user.photo_url || '',
    };
    globalChatMessages.push(entry);
    if (globalChatMessages.length > 200) globalChatMessages.shift();

    sendApiSuccess(res, {
      message: {
        id: entry.id,
        is_mine: true,
        username: entry.username,
        first_name: entry.first_name,
        message: entry.message,
        photo_url: entry.photo_url,
        created_at: entry.created_at,
        source: entry.source,
        client_id: entry.client_id,
        relay_status: entry.relay_status,
      },
      telegram_relayed: relay.relayed,
      data_mode: getActiveDataMode(),
    });
  } catch (err) {
    next(err);
  }
});

// ---------- External IDs & Unified Python/Node Secure Watch Links ----------

app.get('/api/imdb_id/:tmdbId', async (req: Request, res: Response) => {
  const tmdbId = Number(req.params.tmdbId);
  const local = CATALOG_MOVIES.find((movie) => movie.tmdb_id === tmdbId);
  if (local?.imdb_id) {
    res.json({ status: 'success', imdb_id: local.imdb_id });
    return;
  }
  if (TMDB_API_KEY && Number.isInteger(tmdbId) && tmdbId > 0) {
    for (const type of ['movie', 'tv']) {
      const data = await fetchJsonSafe(
        `https://api.themoviedb.org/3/${type}/${tmdbId}/external_ids?api_key=${encodeURIComponent(TMDB_API_KEY)}`
      );
      if (typeof data?.imdb_id === 'string' && data.imdb_id) {
        res.json({ status: 'success', imdb_id: data.imdb_id });
        return;
      }
    }
  }
  sendApiError(res, 404, 'IMDB_NOT_FOUND', 'No IMDB ID found');
});

function renderSecureWatchHtml(movieId: number, movieFileId: number | null): string {
  const apiPath = movieFileId ? `/api/gen_link/${movieId}/file/${movieFileId}` : `/api/gen_link/${movieId}`;
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>FlimfyBox - Verifying Secure Connection...</title>
  <style>
    body { background: #06070b; color: white; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; font-family: sans-serif; margin: 0; text-align: center; padding: 24px; }
    .loader { border: 4px solid rgba(255,255,255,0.1); border-top: 4px solid #8b5cf6; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin-bottom: 20px; }
    @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    a { color: #a78bfa; margin-top: 16px; }
    .cta { display: inline-block; padding: 10px 18px; background: #8b5cf6; color: #fff; border-radius: 8px; text-decoration: none; margin: 12px; }
  </style>
</head>
<body>
  <div id="watchRoot">
    <div class="loader"></div>
    <h3>Securely verifying your connection...</h3>
    <p style="color: #a1a1aa; font-size: 13px;">Preparing your Telegram link for FlimfyBox.</p>
    <a href="/webapp?movie=${movieId}">Return to FlimfyBox</a>
  </div>
  <script>
    function renderState(headingText, bodyText, telegramUrl) {
      const root = document.getElementById('watchRoot');
      root.replaceChildren();
      const h3 = document.createElement('h3');
      h3.textContent = headingText;
      root.appendChild(h3);
      if (bodyText) {
        const p = document.createElement('p');
        p.style.color = '#a1a1aa';
        p.style.fontSize = '13px';
        p.textContent = bodyText;
        root.appendChild(p);
      }
      if (typeof telegramUrl === 'string' && telegramUrl.startsWith('https://t.me/')) {
        const cta = document.createElement('a');
        cta.className = 'cta';
        cta.href = telegramUrl;
        cta.rel = 'noopener noreferrer';
        cta.textContent = 'Open in Telegram';
        root.appendChild(cta);
        root.appendChild(document.createElement('br'));
      }
      const back = document.createElement('a');
      back.href = '/webapp?movie=${movieId}';
      back.textContent = 'Back to FlimfyBox';
      root.appendChild(back);
    }

    setTimeout(() => {
      fetch(${JSON.stringify(apiPath)}, { method: 'POST' })
        .then((r) => r.json())
        .then((data) => {
          if (data && typeof data.url === 'string' && data.url.startsWith('https://t.me/')) {
            renderState('Link Ready', 'Open in Telegram or return to the catalogue.', data.url);
          } else {
            renderState('Link unavailable', 'Please try again from the catalogue.', null);
          }
        })
        .catch(() => {
          renderState('Connection failed', 'Unable to reach the server right now.', null);
        });
    }, 600);
  </script>
</body>
</html>`;
}

app.get('/watch/:movieId', (req: Request, res: Response) => {
  const movieId = Number(req.params.movieId);
  if (!Number.isInteger(movieId) || movieId <= 0) {
    sendApiError(res, 400, 'INVALID_WATCH_TARGET', 'Invalid movie or file');
    return;
  }
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(renderSecureWatchHtml(movieId, null));
});

app.get('/watch/:movieId/file/:movieFileId', (req: Request, res: Response) => {
  const movieId = Number(req.params.movieId);
  const movieFileId = Number(req.params.movieFileId);
  if (!Number.isInteger(movieId) || movieId <= 0 || !Number.isInteger(movieFileId) || movieFileId <= 0) {
    sendApiError(res, 400, 'INVALID_WATCH_TARGET', 'Invalid movie or file');
    return;
  }
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(renderSecureWatchHtml(movieId, movieFileId));
});

async function handleGenLink(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const movieId = Number(req.params.movieId);
    const movieFileId = req.params.movieFileId !== undefined ? Number(req.params.movieFileId) : null;
    if (
      !Number.isInteger(movieId) ||
      movieId <= 0 ||
      (movieFileId !== null && (!Number.isInteger(movieFileId) || movieFileId <= 0))
    ) {
      sendApiError(res, 400, 'INVALID_WATCH_TARGET', 'Invalid movie or file');
      return;
    }

    // P0: Unified PostgreSQL temp_links token storage shared with Python Telegram bot /start tmp_...
    if (isDatabaseActive()) {
      const dbCheck = await verifyMovieAndFileInDb(movieId, movieFileId);
      if (dbCheck) {
        if (!dbCheck.movieExists) {
          sendApiError(res, 404, 'MOVIE_NOT_FOUND', 'Movie not found');
          return;
        }
        if (!dbCheck.fileExists) {
          sendApiError(res, 404, 'FILE_NOT_FOUND', 'Selected file not found');
          return;
        }
        const persisted = await persistDownloadTokenInDb(movieId, movieFileId);
        if (persisted) {
          res.json({
            status: 'success',
            url: `https://t.me/${BOT_USERNAME}?start=${persisted.token}`,
            storage: persisted.storage,
            expires_in_seconds: persisted.expires_in_seconds,
          });
          return;
        }
      }
    }

    // Demo / offline fallback when DATABASE_URL is not connected
    const movie = getMovieById(movieId);
    if (!movie) {
      sendApiError(res, 404, 'MOVIE_NOT_FOUND', 'Movie not found');
      return;
    }
    if (movieFileId !== null && !movie.files.some((f) => f.id === movieFileId)) {
      sendApiError(res, 404, 'FILE_NOT_FOUND', 'Selected file not found');
      return;
    }

    const now = Date.now();
    for (const [k, v] of tempLinks.entries()) {
      if (now - v.created_at > 600_000) tempLinks.delete(k);
    }
    const token = `tmp_${crypto.randomBytes(8).toString('hex')}`;
    tempLinks.set(token, { movie_id: movieId, movie_file_id: movieFileId, created_at: now });
    res.json({
      status: 'success',
      url: `https://t.me/${BOT_USERNAME}?start=${token}`,
      storage: 'memory',
      expires_in_seconds: 600,
    });
  } catch (err) {
    next(err);
  }
}

app.post('/api/gen_link/:movieId', chatAndLinkRateLimiter, handleGenLink);
app.post('/api/gen_link/:movieId/file/:movieFileId', chatAndLinkRateLimiter, handleGenLink);

// Global structured error handler (never leaks stack traces to clients)
app.use((err: Error, req: Request, res: Response, _next: NextFunction) => {
  if (err.message === 'Origin not allowed by CORS') {
    sendApiError(res, 403, 'CORS_ORIGIN_DENIED', 'Origin is not allowed.');
    return;
  }
  logEvent('error', 'unhandled_http_error', {
    request_id: res.locals.requestId,
    method: req.method,
    path: req.path,
    error: err,
  });
  sendApiError(res, 500, 'INTERNAL_ERROR', 'Something went wrong while processing your request.');
});

void verifyDatabaseConnection();

app.listen(PORT, HOST, () => {
  logEvent('info', 'server_started', {
    host: HOST,
    port: PORT,
    data_mode: getActiveDataMode(),
  });
});
