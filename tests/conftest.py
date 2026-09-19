import hashlib
import hmac
import json
import sys
import time
from pathlib import Path

import pytest
from flask import Flask


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.last_query = ''
        self.last_params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        self.last_query = query
        self.last_params = params
        self.connection.queries.append((query, params))

    def fetchone(self):
        return self.connection.fetchone_result

    def fetchall(self):
        return self.connection.fetchall_result

    def close(self):
        return None


class FakeConnection:
    def __init__(self, *, fetchone_result=None, fetchall_result=None):
        self.fetchone_result = fetchone_result
        self.fetchall_result = fetchall_result or []
        self.queries = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def webapp_factory(monkeypatch):
    from webapp_routes import register_webapp_routes

    def factory(*, connection=None, store_user_request=None):
        app = Flask(__name__)
        captured = []
        connection = connection or FakeConnection()

        def get_connection():
            return connection

        def close_connection(_connection):
            return None

        def store_request(*args):
            captured.append(args)
            return True if store_user_request is None else store_user_request(*args)

        monkeypatch.setenv('TELEGRAM_BOT_TOKEN', 'synthetic-test-token')
        monkeypatch.delenv('REQUEST_CHANNEL_ID', raising=False)
        register_webapp_routes(
            app,
            api_movies_cache=type('Cache', (), {'get': lambda *_: None, 'set': lambda *_: None})(),
            search_cache=type('Cache', (), {'get': lambda *_: None, 'set': lambda *_: None})(),
            get_db_connection=get_connection,
            close_db_connection=close_connection,
            store_user_request=store_request,
            TMDB_API_KEY='synthetic-tmdb-key',
            logger=app.logger,
        )
        return app, captured, connection

    return factory


def signed_init_data(*, user=None, auth_date=None, token='synthetic-test-token'):
    values = {
        'auth_date': str(int(time.time()) if auth_date is None else auth_date),
        'query_id': 'synthetic-query',
        'user': json.dumps(
            user or {'id': 101, 'username': 'verified', 'first_name': 'Verified'},
            separators=(',', ':'),
        ),
    }
    data_check_string = '\n'.join(
        f'{key}={value}' for key, value in sorted(values.items())
    )
    secret_key = hmac.new(
        b'WebAppData', token.encode(), hashlib.sha256
    ).digest()
    values['hash'] = hmac.new(
        secret_key, data_check_string.encode(), hashlib.sha256
    ).hexdigest()
    return '&'.join(f'{key}={value}' for key, value in values.items())
