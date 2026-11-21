import crypto from 'node:crypto';
import { Pool } from 'pg';
import type { CatalogMovie, MovieFile } from './catalog.ts';
import { resolveDataMode } from './contracts.ts';
import type { DataMode } from './contracts.ts';
import { logEvent } from './logger.ts';
import type { AppUser, ChatMessage } from './store.ts';

let pool: Pool | null = null;
let dbConnected = false;
let lastDbError: string | null = null;

function getDatabaseUrl(): string {
  if (String(process.env.APP_DATA_MODE || '').trim().toLowerCase() === 'demo') {
    return '';
  }
  const usePooler = String(process.env.USE_POOLER || 'true').toLowerCase() !== 'false';
  const poolerUrl = (process.env.DATABASE_POOLER_URL || '').trim();
  const directUrl = (process.env.DATABASE_URL || '').trim();
  return (usePooler ? poolerUrl || directUrl : directUrl || poolerUrl);
}

export function initDatabasePool(): Pool | null {
  const connectionString = getDatabaseUrl();
  if (!connectionString) {
    dbConnected = false;
    lastDbError = null;
    logEvent('info', 'db_pool_skipped', { mode: 'demo' });
    return null;
  }
  if (pool) return pool;

  const requiresSsl =
    connectionString.includes('supabase.com') ||
    connectionString.includes('sslmode=require') ||
    process.env.DB_SSL === 'true';

  pool = new Pool({
    connectionString,
    max: Number(process.env.NODE_DB_POOL_MAX || '5'),
    idleTimeoutMillis: 20_000,
    connectionTimeoutMillis: 6_000,
    ssl: requiresSsl ? { rejectUnauthorized: false } : undefined,
  });

  pool.on('error', (err) => {
    dbConnected = false;
    lastDbError = err.message;
    logEvent('error', 'db_pool_error', { error: err });
  });

  return pool;
}

export async function verifyDatabaseConnection(): Promise<boolean> {
  const activePool = initDatabasePool();
  if (!activePool) return false;
  try {
    await activePool.query('SELECT 1');
    dbConnected = true;
    lastDbError = null;
    logEvent('info', 'db_connected', { mode: getActiveDataMode() });
    return true;
  } catch (err) {
    dbConnected = false;
    lastDbError = err instanceof Error ? err.message : String(err);
    logEvent('warn', 'db_connect_failed', { error: err, fallback: 'in_memory' });
    return false;
  }
}

export function isDatabaseActive(): boolean {
  return Boolean(pool && dbConnected);
}

export function getActiveDataMode(): DataMode {
  return resolveDataMode(isDatabaseActive());
}

export function getDatabaseStatus(): {
  connected: boolean;
  configured: boolean;
  data_mode: DataMode;
  last_error: string | null;
} {
  return {
    connected: dbConnected,
    configured: Boolean(getDatabaseUrl()),
    data_mode: getActiveDataMode(),
    last_error: lastDbError,
  };
}

// ---------- Shared Download Token Storage (Python <-> Node temp_links) ----------

export interface DownloadTokenResult {
  token: string;
  storage: 'postgres' | 'memory';
  expires_in_seconds: number;
}

export async function verifyMovieAndFileInDb(
  movieId: number,
  movieFileId: number | null,
): Promise<{ movieExists: boolean; fileExists: boolean } | null> {
  if (!isDatabaseActive() || !pool) return null;
  const movieRes = await pool.query('SELECT id FROM movies WHERE id = $1 LIMIT 1', [movieId]);
  if (movieRes.rowCount === 0) {
    return { movieExists: false, fileExists: false };
  }
  if (movieFileId === null) {
    return { movieExists: true, fileExists: true };
  }
  const fileRes = await pool.query(
    'SELECT id FROM movie_files WHERE id = $1 AND movie_id = $2 LIMIT 1',
    [movieFileId, movieId],
  );
  return { movieExists: true, fileExists: (fileRes.rowCount || 0) > 0 };
}

/**
 * Inserts a short-lived `tmp_<hex>` download token into PostgreSQL `temp_links`
 * so the Python Telegram bot `/start tmp_...` handler resolves and delivers
 * the exact movie/file seamlessly.
 */
export async function persistDownloadTokenInDb(
  movieId: number,
  movieFileId: number | null,
): Promise<DownloadTokenResult | null> {
  if (!isDatabaseActive() || !pool) return null;
  const token = `tmp_${crypto.randomBytes(8).toString('hex')}`;
  await pool.query("DELETE FROM temp_links WHERE created_at < NOW() - INTERVAL '10 minutes'");
  await pool.query(
    'INSERT INTO temp_links (token, movie_id, movie_file_id, created_at) VALUES ($1, $2, $3, NOW())',
    [token, movieId, movieFileId],
  );
  return {
    token,
    storage: 'postgres',
    expires_in_seconds: 600,
  };
}

// ---------- PostgreSQL Catalog Read Helpers ----------

function normalizeSeasonsData(
  raw: unknown,
): Record<string, Array<{ ep: number; title: string }>> {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const result: Record<string, Array<{ ep: number; title: string }>> = {};
  for (const [seasonKey, episodes] of Object.entries(raw as Record<string, unknown>)) {
    if (!Array.isArray(episodes)) continue;
    result[seasonKey] = episodes.map((item, idx) => {
      if (item && typeof item === 'object') {
        const rec = item as Record<string, unknown>;
        return {
          ep: Number(rec.ep ?? rec.episode_number ?? idx + 1) || idx + 1,
          title: String(rec.title ?? rec.name ?? `Episode ${idx + 1}`),
        };
      }
      return { ep: idx + 1, title: String(item ?? `Episode ${idx + 1}`) };
    });
  }
  return result;
}

function mapMovieRowToCatalogMovie(
  row: Record<string, unknown>,
  files: MovieFile[],
  isAvailable: boolean,
): CatalogMovie {
  const poster = String(row.poster_url || '');
  const backdrop = String(row.backdrop_poster_url || poster || '');
  const rawType = String(row.content_type || 'Movie').toLowerCase();
  const contentType: 'Movie' | 'Web Series' =
    rawType.includes('series') || rawType.includes('tv') || rawType.includes('show')
      ? 'Web Series'
      : 'Movie';
  const numericRating = Number.parseFloat(String(row.rating || '0'));

  return {
    id: Number(row.id),
    tmdb_id: row.tmdb_id ? Number(row.tmdb_id) : 0,
    imdb_id: row.imdb_id ? String(row.imdb_id) : '',
    title: String(row.title || 'Untitled'),
    year: Number(row.year || 0),
    image: poster && poster !== 'N/A' ? poster : '/static/miniapp/poster-placeholder.svg',
    backdrop: backdrop && backdrop !== 'N/A' ? backdrop : '/static/miniapp/poster-placeholder.svg',
    rating: String(row.rating || 'N/A'),
    tmdb_rating: Number.isFinite(numericRating) ? numericRating : 0,
    tmdb_votes: 0,
    imdb_votes: '',
    genre: String(row.genre || 'Drama'),
    category: String(row.category || 'Movies'),
    language: String(row.language || 'Hindi'),
    content_type: contentType,
    description: String(row.description || ''),
    cast: String(row.cast_list || ''),
    trailer_key: row.trailer_key ? String(row.trailer_key) : null,
    seasons_data: normalizeSeasonsData(row.seasons_data),
    files,
    created_at: row.created_at ? new Date(String(row.created_at)).toISOString() : new Date().toISOString(),
    is_available: isAvailable,
    is_unreleased: Boolean(row.is_unreleased),
    audience_reviews: [],
  };
}

export async function fetchCatalogMovieFromDb(movieId: number): Promise<CatalogMovie | null> {
  if (!isDatabaseActive() || !pool) return null;
  const res = await pool.query(
    `SELECT id, tmdb_id, imdb_id, title, year, poster_url, backdrop_poster_url,
            rating, genre, category, language, content_type, description,
            "cast" AS cast_list, trailer_key, seasons_data, created_at,
            is_unreleased
     FROM movies WHERE id = $1 LIMIT 1`,
    [movieId],
  );
  if (res.rowCount === 0) return null;
  const row = res.rows[0] as Record<string, unknown>;
  const filesRes = await pool.query(
    `SELECT id, quality, file_size, extra_info
     FROM movie_files WHERE movie_id = $1 ORDER BY id ASC`,
    [movieId],
  );
  const files: MovieFile[] = filesRes.rows.map((f: Record<string, unknown>) => ({
    id: Number(f.id),
    quality: String(f.quality || 'HD'),
    size: String(f.file_size || ''),
    extra_info: String(f.extra_info || ''),
  }));

  return mapMovieRowToCatalogMovie(row, files, files.length > 0);
}

export async function fetchCatalogPageFromDb(options: {
  page: number;
  limit: number;
  search?: string;
  category?: string;
  availability?: string;
}): Promise<{ movies: CatalogMovie[]; total: number } | null> {
  if (!isDatabaseActive() || !pool) return null;
  const conditions: string[] = [];
  const params: unknown[] = [];

  if (options.search) {
    params.push(`%${options.search}%`);
    conditions.push(`m.title ILIKE $${params.length}`);
  }
  if (options.category && options.category.toLowerCase() !== 'all') {
    params.push(options.category);
    conditions.push(`LOWER(COALESCE(m.category, '')) = LOWER($${params.length})`);
  }
  if (options.availability === 'available') {
    conditions.push('EXISTS (SELECT 1 FROM movie_files mf WHERE mf.movie_id = m.id)');
  } else if (options.availability === 'unavailable') {
    conditions.push('NOT EXISTS (SELECT 1 FROM movie_files mf WHERE mf.movie_id = m.id)');
  }

  const whereSql = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const countRes = await pool.query(`SELECT COUNT(*)::int AS total FROM movies m ${whereSql}`, params);
  const total = Number(countRes.rows[0]?.total || 0);

  const offset = (options.page - 1) * options.limit;
  params.push(options.limit, offset);
  const listRes = await pool.query(
    `SELECT m.id, m.tmdb_id, m.imdb_id, m.title, m.year, m.poster_url, m.backdrop_poster_url,
            m.rating, m.genre, m.category, m.language, m.content_type, m.description,
            m."cast" AS cast_list, m.trailer_key, m.seasons_data, m.created_at,
            m.is_unreleased,
            EXISTS (SELECT 1 FROM movie_files mf WHERE mf.movie_id = m.id) AS has_files
     FROM movies m
     ${whereSql}
     ORDER BY m.id DESC
     LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params,
  );

  const movies: CatalogMovie[] = listRes.rows.map((row: Record<string, unknown>) =>
    mapMovieRowToCatalogMovie(row, [], Boolean(row.has_files)),
  );

  return { movies, total };
}

// ---------- PostgreSQL Global Chat Storage ----------

export async function ensureMiniappUserInDb(user: AppUser): Promise<void> {
  if (!isDatabaseActive() || !pool || user.id === 0) return;
  await pool.query(
    `INSERT INTO miniapp_users (user_id, username, first_name, photo_url, last_seen)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (user_id) DO UPDATE
       SET username = COALESCE(NULLIF(EXCLUDED.username, ''), miniapp_users.username),
           first_name = COALESCE(NULLIF(EXCLUDED.first_name, ''), miniapp_users.first_name),
           photo_url = COALESCE(NULLIF(EXCLUDED.photo_url, ''), miniapp_users.photo_url),
           last_seen = NOW()`,
    [user.id, user.username || '', user.first_name || 'Member', user.photo_url || ''],
  );
}

export async function fetchGlobalChatFromDb(afterId: number | null): Promise<ChatMessage[] | null> {
  if (!isDatabaseActive() || !pool) return null;
  const query =
    afterId !== null && Number.isFinite(afterId) && afterId >= 0
      ? {
          text: `SELECT g.id, g.user_id, g.username, g.first_name, g.message, g.created_at,
                        COALESCE(g.source, 'miniapp') AS source,
                        COALESCE(g.client_message_id::text, '') AS client_id,
                        CASE
                          WHEN COALESCE(g.source, 'miniapp') = 'telegram' OR g.telegram_message_id IS NOT NULL THEN 'sent'
                          ELSE 'pending'
                        END AS relay_status,
                        COALESCE(mu.photo_url, '') AS photo_url
                 FROM global_chat_messages g
                 LEFT JOIN miniapp_users mu ON mu.user_id = g.user_id
                 WHERE g.id > $1
                 ORDER BY g.id ASC
                 LIMIT 100`,
          values: [afterId],
        }
      : {
          text: `SELECT recent.id, recent.user_id, recent.username, recent.first_name,
                        recent.message, recent.created_at,
                        COALESCE(recent.source, 'miniapp') AS source,
                        COALESCE(recent.client_message_id::text, '') AS client_id,
                        CASE
                          WHEN COALESCE(recent.source, 'miniapp') = 'telegram' OR recent.telegram_message_id IS NOT NULL THEN 'sent'
                          ELSE 'pending'
                        END AS relay_status,
                        COALESCE(mu.photo_url, '') AS photo_url
                 FROM (
                   SELECT * FROM global_chat_messages ORDER BY id DESC LIMIT 80
                 ) recent
                 LEFT JOIN miniapp_users mu ON mu.user_id = recent.user_id
                 ORDER BY recent.id ASC`,
          values: [],
        };

  const res = await pool.query(query.text, query.values);
  return res.rows.map((r: Record<string, unknown>) => ({
    id: Number(r.id),
    user_id: Number(r.user_id),
    username: String(r.username || ''),
    first_name: String(r.first_name || 'Community member'),
    message: String(r.message || ''),
    created_at: r.created_at ? new Date(String(r.created_at)).toISOString() : new Date().toISOString(),
    source: r.source === 'telegram' ? 'telegram' : 'miniapp',
    client_id: String(r.client_id || ''),
    relay_status: r.relay_status === 'pending' ? 'pending' : 'sent',
    photo_url: String(r.photo_url || ''),
  }));
}

export async function insertGlobalChatInDb(params: {
  user: AppUser;
  message: string;
  clientId: string;
  relayStatus: 'sent' | 'pending';
  telegramChatId: string | null;
  telegramMessageId: number | null;
}): Promise<ChatMessage | null> {
  if (!isDatabaseActive() || !pool) return null;
  await ensureMiniappUserInDb(params.user);

  const existing = await pool.query(
    `SELECT id, user_id, username, first_name, message, created_at,
            COALESCE(source, 'miniapp') AS source,
            COALESCE(client_message_id::text, '') AS client_id,
            CASE WHEN telegram_message_id IS NOT NULL THEN 'sent' ELSE 'pending' END AS relay_status
     FROM global_chat_messages
     WHERE user_id = $1 AND client_message_id = $2::uuid
     LIMIT 1`,
    [params.user.id, params.clientId],
  );
  if ((existing.rowCount || 0) > 0) {
    const r = existing.rows[0] as Record<string, unknown>;
    return {
      id: Number(r.id),
      user_id: Number(r.user_id),
      username: String(r.username || ''),
      first_name: String(r.first_name || 'Member'),
      message: String(r.message || ''),
      created_at: new Date(String(r.created_at)).toISOString(),
      source: r.source === 'telegram' ? 'telegram' : 'miniapp',
      client_id: String(r.client_id || ''),
      relay_status: r.relay_status === 'pending' ? 'pending' : 'sent',
      photo_url: params.user.photo_url || '',
    };
  }

  const inserted = await pool.query(
    `INSERT INTO global_chat_messages
       (user_id, username, first_name, message, source, client_message_id, telegram_chat_id, telegram_message_id)
     VALUES ($1, $2, $3, $4, 'miniapp', $5::uuid, $6, $7)
     RETURNING id, created_at`,
    [
      params.user.id,
      params.user.username || '',
      params.user.first_name || 'Member',
      params.message.slice(0, 3800),
      params.clientId,
      params.telegramChatId ? Number(params.telegramChatId) : null,
      params.telegramMessageId,
    ],
  );

  const row = inserted.rows[0] as { id: string | number; created_at: string };
  return {
    id: Number(row.id),
    user_id: params.user.id,
    username: params.user.username || '',
    first_name: params.user.first_name || 'Member',
    message: params.message,
    created_at: new Date(row.created_at).toISOString(),
    source: 'miniapp',
    client_id: params.clientId,
    relay_status: params.relayStatus,
    photo_url: params.user.photo_url || '',
  };
}
