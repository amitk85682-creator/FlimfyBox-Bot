"""Versioned PostgreSQL schema migrations for FlimfyBox."""

import logging
import os
from typing import Callable, Iterable, Tuple

import psycopg2

logger = logging.getLogger(__name__)

Migration = Tuple[int, Callable]


def _migration_1(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL DEFAULT '',
                file_id TEXT,
                is_unreleased BOOLEAN DEFAULT FALSE,
                imdb_id TEXT,
                poster_url TEXT,
                year INTEGER DEFAULT 0,
                genre TEXT,
                rating TEXT,
                description TEXT,
                category TEXT,
                seasons_data JSONB DEFAULT '{}'::jsonb
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS temp_links (
                token VARCHAR(50) PRIMARY KEY,
                movie_id INTEGER,
                movie_file_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS miniapp_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_watchlist (
                user_id BIGINT NOT NULL REFERENCES miniapp_users(user_id) ON DELETE CASCADE,
                movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, movie_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movie_ratings (
                id BIGSERIAL PRIMARY KEY,
                movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES miniapp_users(user_id) ON DELETE CASCADE,
                rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (movie_id, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_recommendation_events (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES miniapp_users(user_id) ON DELETE CASCADE,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                event_type TEXT NOT NULL,
                source TEXT NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS global_chat_messages (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES miniapp_users(user_id) ON DELETE CASCADE,
                username TEXT,
                first_name TEXT,
                message TEXT NOT NULL CHECK (char_length(message) BETWEEN 1 AND 500),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS auto_delete_queue (
                id SERIAL PRIMARY KEY,
                bot_username TEXT NOT NULL,
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                delete_at TIMESTAMP NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movie_files (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                quality TEXT NOT NULL,
                url TEXT,
                file_id TEXT,
                file_size TEXT,
                backup_map JSONB DEFAULT '{}'::jsonb,
                UNIQUE(movie_id, quality)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_info (
                id SERIAL PRIMARY KEY,
                last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                movie_title TEXT NOT NULL,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notified BOOLEAN DEFAULT FALSE,
                group_id BIGINT,
                message_id BIGINT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movie_aliases (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                alias TEXT NOT NULL,
                UNIQUE(movie_id, alias)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channel_posts (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                bot_username TEXT,
                posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel_id, message_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_activity (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                chat_id BIGINT,
                chat_type TEXT,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_history (
                tmdb_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                media_type TEXT DEFAULT 'movie',
                popularity REAL DEFAULT 0,
                vote_average REAL DEFAULT 0,
                alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                posted_by TEXT DEFAULT NULL,
                status TEXT DEFAULT 'alerted',
                imdb_id TEXT DEFAULT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_meta (
                id INTEGER PRIMARY KEY,
                last_check TIMESTAMP,
                locked_by TEXT DEFAULT NULL,
                lock_time TIMESTAMP DEFAULT NULL
            )
        """)


def _migration_2(conn):
    statements = (
        "ALTER TABLE temp_links ADD COLUMN IF NOT EXISTS movie_file_id INTEGER",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS imdb_id TEXT",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS poster_url TEXT",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS year INTEGER DEFAULT 0",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS genre TEXT",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS rating TEXT",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS description TEXT",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS category TEXT",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS language TEXT",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS extra_info TEXT",
        'ALTER TABLE movies ADD COLUMN IF NOT EXISTS "cast" TEXT',
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS trailer_key TEXT",
        "ALTER TABLE movies ADD COLUMN IF NOT EXISTS seasons_data JSONB DEFAULT '{}'::jsonb",
        "ALTER TABLE user_activity ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE user_activity ADD COLUMN IF NOT EXISTS chat_id BIGINT",
        "ALTER TABLE user_activity ADD COLUMN IF NOT EXISTS chat_type TEXT",
        "ALTER TABLE movie_files ADD COLUMN IF NOT EXISTS languages TEXT DEFAULT ''",
        "ALTER TABLE movie_files ADD COLUMN IF NOT EXISTS extra_info TEXT DEFAULT ''",
        "ALTER TABLE movie_files ADD COLUMN IF NOT EXISTS file_unique_id TEXT",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS caption TEXT",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS media_file_id TEXT",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS media_type TEXT DEFAULT 'photo'",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS keyboard_data TEXT",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS topic_id INTEGER",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS content_type TEXT DEFAULT 'movies'",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS is_restored BOOLEAN DEFAULT FALSE",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS restored_at TIMESTAMP",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS movie_name TEXT",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS imdb_id TEXT",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS tmdb_id TEXT",
        "ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS channel_name TEXT",
    )
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)


def _migration_3(conn):
    statements = (
        "CREATE INDEX IF NOT EXISTS idx_movie_ratings_movie_id ON movie_ratings(movie_id)",
        "CREATE INDEX IF NOT EXISTS idx_movie_ratings_user_id ON movie_ratings(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_global_chat_created_at ON global_chat_messages(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_delete_at ON auto_delete_queue (bot_username, delete_at)",
        "CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title)",
        "CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_movies_title_norm_trgm ON movies USING gin (regexp_replace(LOWER(title), '[^a-z0-9]', '', 'g') gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_movies_imdb_id ON movies (imdb_id)",
        "CREATE INDEX IF NOT EXISTS idx_movies_year ON movies (year)",
        "CREATE INDEX IF NOT EXISTS idx_user_requests_movie_title ON user_requests (movie_title)",
        "CREATE INDEX IF NOT EXISTS idx_user_requests_user_id ON user_requests (user_id)",
        "CREATE INDEX IF NOT EXISTS idx_movie_aliases_alias ON movie_aliases (alias)",
        "CREATE INDEX IF NOT EXISTS idx_movie_aliases_alias_norm_trgm ON movie_aliases USING gin (regexp_replace(LOWER(alias), '[^a-z0-9]', '', 'g') gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_movie_files_movie_id ON movie_files (movie_id)",
        "CREATE INDEX IF NOT EXISTS idx_channel_posts_movie_id ON channel_posts (movie_id)",
        "CREATE INDEX IF NOT EXISTS idx_recommendation_events_user_created ON user_recommendation_events(user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_recommendation_events_movie_type ON user_recommendation_events(movie_id, event_type)",
        "CREATE INDEX IF NOT EXISTS idx_recommendation_events_type_created ON user_recommendation_events(event_type, created_at DESC)",
    )
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'user_requests_unique_constraint') THEN
                    ALTER TABLE user_requests
                    ADD CONSTRAINT user_requests_unique_constraint UNIQUE (user_id, movie_title);
                END IF;
            END $$;
        """)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'channel_posts_unique_idx') THEN
                    ALTER TABLE channel_posts ADD CONSTRAINT channel_posts_unique_idx UNIQUE (channel_id, message_id);
                END IF;
            END $$;
        """)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'movies_title_unique') THEN
                    ALTER TABLE movies ADD CONSTRAINT movies_title_unique UNIQUE (title);
                END IF;
            END $$;
        """)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'movies_title_key') THEN
                    ALTER TABLE movies ADD CONSTRAINT movies_title_key UNIQUE (title);
                END IF;
            END $$;
        """)


def _migration_4(conn):
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE movie_files DROP CONSTRAINT IF EXISTS movie_files_movie_id_quality_key")
        cur.execute("ALTER TABLE movie_files DROP CONSTRAINT IF EXISTS movie_files_unique_size")
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'movie_files_file_unique_id_key'
                ) THEN
                    ALTER TABLE movie_files
                    ADD CONSTRAINT movie_files_file_unique_id_key UNIQUE (file_unique_id);
                END IF;
            END $$;
        """)
        cur.execute("INSERT INTO trending_meta (id, last_check) VALUES (1, '2000-01-01') ON CONFLICT (id) DO NOTHING")


MIGRATIONS: Tuple[Migration, ...] = (
    (1, _migration_1),
    (2, _migration_2),
    (3, _migration_3),
    (4, _migration_4),
)


def run_migrations(database_url: str) -> None:
    if not database_url:
        raise ValueError("DATABASE_URL is not set.")
    versions = [version for version, _ in MIGRATIONS]
    if len(versions) != len(set(versions)):
        raise ValueError("Duplicate migration version detected.")
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT version FROM schema_migrations")
            applied = {row[0] for row in cur.fetchall()}
        for version, migration in sorted(MIGRATIONS, key=lambda item: item[0]):
            if version in applied:
                continue
            try:
                migration(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)",
                        (version,),
                    )
                conn.commit()
                logger.info("Applied database migration %s", version)
            except Exception:
                conn.rollback()
                raise
    finally:
        conn.close()
