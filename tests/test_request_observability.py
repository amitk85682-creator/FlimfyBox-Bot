import ast
import logging
import re
from pathlib import Path
from types import SimpleNamespace

import requests
from observability import bind_request_id, current_request_id, log_search_timing, reset_request_id


def test_search_returns_correlation_id_and_logs_route_without_query(webapp_factory, caplog, monkeypatch):
    import webapp_routes

    class EmptyTmdbResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {'results': []}

    monkeypatch.setattr(webapp_routes.requests, 'get', lambda *_args, **_kwargs: EmptyTmdbResponse())
    app, _, _ = webapp_factory()
    caplog.set_level(logging.INFO)

    response = app.test_client().get('/api/search?q=PrivateSearchTerm')

    request_id = response.headers.get('X-Request-ID', '')
    assert re.fullmatch(r'[0-9a-f]{16}', request_id)
    assert (
        f'miniapp_api request_id={request_id} route=/api/search '
        'method=GET status=200'
    ) in caplog.text
    assert 'PrivateSearchTerm' not in caplog.text


def test_health_endpoint_is_not_added_to_request_metrics(webapp_factory, caplog):
    app, _, connection = webapp_factory()
    caplog.set_level(logging.INFO)

    response = app.test_client().get('/healthz')

    assert response.status_code == 200
    assert 'X-Request-ID' not in response.headers
    assert not connection.queries
    assert 'miniapp_api' not in caplog.text


def test_request_id_context_is_restored_after_request_work():
    token = bind_request_id('0123456789abcdef')
    try:
        assert current_request_id() == '0123456789abcdef'
    finally:
        reset_request_id(token)
    assert current_request_id() == '-'


def test_search_timing_logs_update_id_without_title(caplog):
    caplog.set_level(logging.INFO, logger='test.search_timing')
    logger = logging.getLogger('test.search_timing')

    log_search_timing(logger, 7342, {'total_ms': 17, 'first_db_ms': 4})

    assert 'search_timing update_id=7342' in caplog.text
    assert 'PrivateSearchTerm' not in caplog.text


def test_search_provider_failure_log_does_not_expose_query_or_api_key(
    webapp_factory, caplog, monkeypatch
):
    import webapp_routes

    def fail_provider(*_args, **_kwargs):
        raise requests.RequestException(
            'https://provider.invalid/search?api_key=synthetic-provider-key&query=PrivateSearchTerm'
        )

    monkeypatch.setattr(webapp_routes.requests, 'get', fail_provider)
    app, _, _ = webapp_factory(tmdb_api_key='synthetic-provider-key')
    caplog.set_level(logging.ERROR)

    response = app.test_client().get('/api/search?q=PrivateSearchTerm')

    assert response.status_code == 503
    assert 'TMDB search failed type=RequestException' in caplog.text
    assert 'PrivateSearchTerm' not in caplog.text
    assert 'synthetic-provider-key' not in caplog.text


def test_telegram_catalogue_search_logs_only_error_type(caplog):
    root = Path(__file__).resolve().parents[1]
    source = (root / 'main.py').read_text(encoding='utf-8')
    tree = ast.parse(source)
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == '_get_movies_from_db_nocache'
    )

    class Cursor:
        def execute(self, *_args, **_kwargs):
            raise ValueError('PrivateSearchTerm and synthetic-db-secret')

        def close(self):
            return None

    connection = SimpleNamespace(cursor=lambda: Cursor())
    namespace = {
        'logger': logging.getLogger('test.catalogue_search'),
        're': re,
        'get_db_connection': lambda: connection,
        'close_db_connection': lambda _connection: None,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), 'main.py', 'exec'), namespace)
    caplog.set_level(logging.INFO, logger='test.catalogue_search')

    namespace['_get_movies_from_db_nocache']('PrivateSearchTerm')

    assert 'catalogue_search_failed type=ValueError' in caplog.text
    assert 'PrivateSearchTerm' not in caplog.text
    assert 'synthetic-db-secret' not in caplog.text
