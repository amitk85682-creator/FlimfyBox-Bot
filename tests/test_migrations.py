import sqlite3
import inspect
import textwrap

import pytest

import db_migrations


FILE_ID_UNIQUE_INDEX_SQL = """
            CREATE UNIQUE INDEX IF NOT EXISTS movie_files_file_id_unique_idx
            ON movie_files (file_id)
            WHERE file_id IS NOT NULL
        """


class MigrationCursor:
    def __init__(self, connection):
        self.connection = connection
        self.query = ''

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        self.query = query
        self.connection.queries.append((query, params))

    def fetchall(self):
        return [(1,)] if 'SELECT version' in self.query else []


class MigrationConnection:
    def __init__(self):
        self.queries = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return MigrationCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_migration_versions_are_unique_and_ordered():
    versions = [version for version, _ in db_migrations.MIGRATIONS]
    assert versions == sorted(versions)
    assert len(versions) == len(set(versions))


def test_migration_11_is_defined_and_creates_partial_file_id_index():
    migrations = dict(db_migrations.MIGRATIONS)
    assert 11 in migrations

    connection = MigrationConnection()
    migrations[11](connection)

    statements = [query for query, _ in connection.queries]
    assert len(statements) == 1
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS movie_files_file_id_unique_idx' in statements[0]
    assert 'ON movie_files (file_id)' in statements[0]
    assert 'WHERE file_id IS NOT NULL' in statements[0]


def test_migration_11_index_rejects_duplicate_non_null_file_ids_but_allows_nulls():
    migrations = dict(db_migrations.MIGRATIONS)
    connection = MigrationConnection()
    migrations[11](connection)
    index_sql = connection.queries[0][0]

    sqlite_conn = sqlite3.connect(':memory:')
    try:
        sqlite_conn.execute('CREATE TABLE movie_files (file_id TEXT)')
        sqlite_conn.execute(index_sql)
        sqlite_conn.execute('INSERT INTO movie_files (file_id) VALUES (?)', ('telegram-file-1',))
        with pytest.raises(sqlite3.IntegrityError):
            sqlite_conn.execute('INSERT INTO movie_files (file_id) VALUES (?)', ('telegram-file-1',))

        sqlite_conn.execute('INSERT INTO movie_files (file_id) VALUES (NULL)')
        sqlite_conn.execute('INSERT INTO movie_files (file_id) VALUES (NULL)')
        assert sqlite_conn.execute(
            'SELECT COUNT(*) FROM movie_files WHERE file_id IS NULL'
        ).fetchone()[0] == 2
    finally:
        sqlite_conn.close()


def test_migration_10_definition_remains_unchanged():
    migrations = dict(db_migrations.MIGRATIONS)
    connection = MigrationConnection()
    migrations[10](connection)

    statements = [query for query, _ in connection.queries]
    assert len(statements) == 1
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS movie_files_file_id_unique_idx' in statements[0]
    assert 'ON movie_files (file_id)' in statements[0]
    assert 'WHERE file_id IS NOT NULL' in statements[0]
    expected_source = '''\
def _migration_10(conn):
    """Make the Telegram file ID a database-enforced fallback identity."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS movie_files_file_id_unique_idx
            ON movie_files (file_id)
            WHERE file_id IS NOT NULL
        """)
'''
    assert textwrap.dedent(inspect.getsource(db_migrations._migration_10)) == expected_source


def test_migration_12_defines_durable_batch_and_item_recovery_schema():
    migrations = dict(db_migrations.MIGRATIONS)
    assert 12 in migrations

    connection = MigrationConnection()
    migrations[12](connection)
    statements = [query for query, _ in connection.queries]
    joined = "\n".join(statements)

    assert "CREATE TABLE IF NOT EXISTS ingestion_batches" in joined
    assert "CREATE TABLE IF NOT EXISTS ingestion_batch_items" in joined
    assert "bot_id BIGINT" in joined
    assert "update_id BIGINT NOT NULL" in joined
    assert "source_chat_id BIGINT NOT NULL" in joined
    assert "source_message_id BIGINT NOT NULL" in joined
    assert "storage_map JSONB NOT NULL" in joined
    assert "storage_pending_chat BIGINT" in joined
    assert "retry_after_at TIMESTAMPTZ" in joined
    assert "UNIQUE (batch_id, update_id)" in joined
    assert "idx_ingestion_batch_items_recovery" in joined


def test_already_applied_migrations_are_skipped(monkeypatch):
    connection = MigrationConnection()
    called = []
    monkeypatch.setattr(db_migrations.psycopg2, 'connect', lambda _url: connection)
    monkeypatch.setattr(
        db_migrations,
        'MIGRATIONS',
        ((1, lambda _conn: called.append(1)),),
    )

    db_migrations.run_migrations('synthetic://database')

    assert called == []
    assert connection.closed


def test_successful_migration_is_recorded_and_connection_closes(monkeypatch):
    connection = MigrationConnection()
    monkeypatch.setattr(db_migrations.psycopg2, 'connect', lambda _url: connection)
    monkeypatch.setattr(
        db_migrations,
        'MIGRATIONS',
        ((2, lambda _conn: None),),
    )

    db_migrations.run_migrations('synthetic://database')

    assert connection.commits == 2
    assert any('INSERT INTO schema_migrations' in query for query, _ in connection.queries)
    assert connection.closed


def test_failed_migration_rolls_back_and_is_not_recorded(monkeypatch):
    connection = MigrationConnection()
    monkeypatch.setattr(db_migrations.psycopg2, 'connect', lambda _url: connection)
    monkeypatch.setattr(
        db_migrations,
        'MIGRATIONS',
        ((2, lambda _conn: (_ for _ in ()).throw(RuntimeError('synthetic failure'))),),
    )

    with pytest.raises(RuntimeError, match='synthetic failure'):
        db_migrations.run_migrations('synthetic://database')

    assert connection.rollbacks == 1
    assert not any(
        'INSERT INTO schema_migrations' in query for query, _ in connection.queries
    )
    assert connection.closed
