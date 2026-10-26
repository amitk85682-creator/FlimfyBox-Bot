"""Route behavior for fast keystroke search and trustworthy release dates."""
from datetime import datetime, timedelta, timezone

import pytest
from flask import Flask

from webapp_routes import register_webapp_routes, requests as route_requests


class Cache:
    def __init__(self):
        self.items = {}

    def get(self, key):
        return self.items.get(key)

    def set(self, key, value):
        self.items[key] = value


def row(identifier, title):
    return (identifier, title, 2025, None, '8.0', 'Action', 'TV Series',
            identifier + 1000, 'TV Series', False, True)


class Connection:
    def __init__(self, rows=(), fuzzy_rows=(), broken=False):
        self.rows = list(rows)
        self.fuzzy_rows = list(fuzzy_rows)
        self.queries = []
        self.broken = broken

    def cursor(self):
        if self.broken:
            raise RuntimeError('database unavailable')
        return self

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchall(self):
        query, params = self.queries[-1]
        if 'WHERE LOWER(title) LIKE %s' in query:
            prefix = params[0][:-1].replace('\\%', '%').replace('\\_', '_').replace('\\\\', '\\')
            return sorted((r for r in self.rows if r[1].lower().startswith(prefix)), key=lambda r: r[1].lower())[:params[1]]
        if 'WHERE title %% %s' in query:
            return self.fuzzy_rows
        return self.rows

    def close(self):
        pass


class Response:
    status_code = 200

    def __init__(self, results):
        self.results = results

    def raise_for_status(self):
        pass

    def json(self):
        return {'results': self.results}


def make_app(connection=None):
    app = Flask(__name__)
    connection = connection or Connection()
    cache = Cache()
    closed = []
    register_webapp_routes(
        app, api_movies_cache=cache, search_cache=Cache(),
        get_db_connection=lambda: connection,
        close_db_connection=closed.append, store_user_request=lambda *_: None,
        TMDB_API_KEY='test-key', logger=app.logger,
    )
    return app.test_client(), connection, cache, closed


unexpected_external_calls = []


@pytest.fixture(autouse=True)
def enforce_external_guard():
    unexpected_external_calls.clear()
    yield
    assert not unexpected_external_calls, 'Local search invoked an external provider'


def no_external(*args, **kwargs):
    unexpected_external_calls.append(args)
    raise AssertionError('Local search must not make an external request')


@pytest.mark.parametrize('query_string', ['q=re&autocomplete=1', 'q=RE', 'q=r&autocomplete=true'])
def test_prefix_search_only_returns_matching_titles_without_external_work(monkeypatch, query_string):
    monkeypatch.setattr(route_requests, 'get', no_external)
    connection = Connection([row(1, 'Reacher'), row(2, 'Red Notice'), row(3, 'The Red Queen')])
    client, _, _, closed = make_app(connection)

    response = client.get('/api/search?' + query_string)

    assert response.status_code == 200
    assert [m['title'] for m in response.json['results']] == ['Reacher', 'Red Notice']
    assert response.json['results'][0]['is_available'] is True
    assert len(connection.queries) == 1
    assert 'WHERE LOWER(title) LIKE %s' in connection.queries[0][0]
    assert connection.queries[0][1][-1] == 20
    assert closed == [connection]


def test_autocomplete_long_prefix_is_not_reused_as_full_search_cache(monkeypatch):
    monkeypatch.setattr(route_requests, 'get', no_external)
    client, conn, _, _ = make_app(Connection([row(1, 'Reacher')]))
    assert client.get('/api/search?q=reach&autocomplete=1').json['results'][0]['title'] == 'Reacher'
    assert client.get('/api/search?q=reach').status_code == 200
    assert len(conn.queries) == 2
    assert 'WHERE LOWER(title) LIKE %s' in conn.queries[0][0]
    assert 'WHERE title ILIKE %s' in conn.queries[1][0]


def test_prefix_limit_and_literal_wildcards(monkeypatch):
    monkeypatch.setattr(route_requests, 'get', no_external)
    client, conn, _, _ = make_app(Connection([row(1, '100%_Real'), row(2, '100other')]))
    response = client.get('/api/search', query_string={'q': '100%_', 'autocomplete': 1, 'limit': 999})
    assert [m['title'] for m in response.json['results']] == ['100%_Real']
    assert conn.queries[0][1] == ('100\\%\\_%', 20)
    assert len(client.get('/api/search?q=100&autocomplete=1&limit=1').json['results']) == 1


def test_empty_prefix_match_skips_fuzzy_scan_and_network(monkeypatch):
    monkeypatch.setattr(route_requests, 'get', no_external)
    client, conn, _, _ = make_app()
    response = client.get('/api/search?q=zz&autocomplete=1')
    assert response.json == {'status': 'success', 'results': []}
    assert len(conn.queries) == 1


def test_prefix_database_failure_is_an_error_not_empty_success(monkeypatch):
    monkeypatch.setattr(route_requests, 'get', no_external)
    client, _, _, closed = make_app(Connection(broken=True))
    response = client.get('/api/search?q=re&autocomplete=1')
    assert response.status_code == 503
    assert response.json['status'] == 'error'
    assert len(closed) == 1


def test_full_local_result_returns_without_any_provider_lookup(monkeypatch):
    monkeypatch.setattr(route_requests, 'get', no_external)
    client, conn, _, _ = make_app(Connection([row(1, 'Reacher')]))
    response = client.get('/api/search?q=Reacher')
    assert response.status_code == 200
    assert response.json['results'][0]['source'] == 'local'
    assert len(conn.queries) == 1


def test_typo_candidates_are_bounded_and_duplicate_titles_survive(monkeypatch):
    monkeypatch.setattr(route_requests, 'get', no_external)
    client, conn, _, _ = make_app(Connection(fuzzy_rows=[row(1, 'Reacher'), row(2, 'Reacher')]))
    response = client.get('/api/search?q=rechar')
    assert response.status_code == 200
    assert {m['id'] for m in response.json['results']} == {1, 2}
    assert len(conn.queries) == 2
    assert 'WHERE title %% %s' in conn.queries[1][0]
    assert 'LIMIT 80' in conn.queries[1][0]


def test_missing_full_query_uses_one_bounded_tmdb_call(monkeypatch):
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response([{'id': 123, 'media_type': 'movie', 'title': 'Future Test', 'release_date': '2099-01-01'}])

    monkeypatch.setattr(route_requests, 'get', get)
    client, _, _, _ = make_app()
    response = client.get('/api/search?q=FutureTest')
    assert response.status_code == 200
    assert response.json['results'][0]['title'] == 'Future Test'
    assert len(calls) == 1
    assert '/search/multi?' in calls[0][0]
    assert calls[0][1]['timeout'] == (1, 2)
    assert response.json['results'][0]['genre'] == ''  # No fabricated genre.


def test_provider_failure_is_retryable_not_cached_as_no_matches(monkeypatch):
    calls = []

    def get(*args, **kwargs):
        calls.append(args)
        raise route_requests.Timeout('timeout')

    monkeypatch.setattr(route_requests, 'get', get)
    client, _, _, _ = make_app()
    assert client.get('/api/search?q=missing').status_code == 503
    assert client.get('/api/search?q=missing').status_code == 503
    assert len(calls) == 2


def test_upcoming_filters_past_today_invalid_and_wrong_media_dates(monkeypatch):
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    tomorrow = (today + timedelta(days=1)).isoformat()
    later = (today + timedelta(days=2)).isoformat()
    movies = [
        {'id': 1, 'title': 'Released', 'release_date': (today - timedelta(days=10)).isoformat()},
        {'id': 2, 'title': 'Today', 'release_date': today.isoformat()},
        {'id': 3, 'title': 'Unknown', 'release_date': ''},
        {'id': 4, 'title': 'Invalid', 'release_date': '2026-02-30'},
        {'id': 5, 'title': 'Movie', 'release_date': later, 'first_air_date': today.isoformat()},
        {'id': 6, 'title': 'Wrong media field', 'first_air_date': tomorrow},
        {'id': 7, 'title': 'Far future', 'release_date': (today + timedelta(days=181)).isoformat()},
        {'id': 8, 'title': 'Non canonical', 'release_date': '2099-1-1'},
    ]
    television = [
        {'id': 9, 'name': 'Show', 'first_air_date': tomorrow, 'release_date': today.isoformat()},
        {'id': 10, 'name': 'Old show', 'first_air_date': today.isoformat(), 'release_date': tomorrow},
        {'id': 9, 'name': 'Duplicate show', 'first_air_date': tomorrow},
    ]
    monkeypatch.setattr(route_requests, 'get', lambda url, **kwargs: Response(television if url.endswith('/tv') else movies))
    client, _, _, _ = make_app()
    response = client.get('/api/upcoming')
    assert response.status_code == 200
    assert [m['title'] for m in response.json['movies']] == ['Show', 'Movie']
    assert [m['release_date'] for m in response.json['movies']] == [tomorrow, later]
    assert all(m['is_upcoming'] and not m['is_released'] for m in response.json['movies'])


def test_upcoming_cache_cannot_leak_another_requests_limit(monkeypatch):
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    movies = [{'id': i, 'title': f'Movie {i}', 'release_date': (today + timedelta(days=i)).isoformat()} for i in range(1, 5)]
    monkeypatch.setattr(route_requests, 'get', lambda url, **kwargs: Response([] if url.endswith('/tv') else movies))
    client, _, cache, _ = make_app()
    assert len(client.get('/api/upcoming?limit=1').json['movies']) == 1
    assert len(client.get('/api/upcoming?limit=3').json['movies']) == 3
    assert len(client.get('/api/upcoming?limit=1').json['movies']) == 1
    assert len(cache.items) == 2
