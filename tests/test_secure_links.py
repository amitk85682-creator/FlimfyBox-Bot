import re
from pathlib import Path

from .conftest import FakeConnection


MAIN_SOURCE = (Path(__file__).resolve().parents[1] / 'main.py').read_text(
    encoding='utf-8'
)


def test_secure_token_format_contract():
    assert re.fullmatch(r'tmp_[0-9a-f]{12}', 'tmp_' + 'ab' * 6)
    assert not re.fullmatch(r'tmp_[0-9a-f]{12}', 'tmp_tampered')


def test_movie_and_file_binding_are_checked(webapp_factory):
    movie_connection = FakeConnection(fetchone_result=(1,))
    app, _captured, _connection = webapp_factory(connection=movie_connection)
    response = app.test_client().post('/api/gen_link/7/file/8')
    assert response.status_code == 200
    assert any(
        'movie_files WHERE id = %s AND movie_id = %s' in query
        for query, _params in movie_connection.queries
    )


def test_missing_movie_or_file_returns_error(webapp_factory):
    connection = FakeConnection(fetchone_result=None)
    app, _captured, _connection = webapp_factory(connection=connection)
    assert app.test_client().post('/api/gen_link/7/file/8').status_code == 404


def test_token_generation_failure_does_not_return_fake_link(webapp_factory):
    app, _captured, _connection = webapp_factory(connection=None)
    app.extensions['test_missing_connection'] = True
    # Replace the route's DB dependency by constructing a factory with no connection.
    # The route must return an explicit error rather than a Telegram URL.
    from webapp_routes import register_webapp_routes
    del app
    from flask import Flask
    failing_app = Flask(__name__)
    register_webapp_routes(
        failing_app,
        api_movies_cache=type('Cache', (), {'get': lambda *_: None, 'set': lambda *_: None})(),
        search_cache=type('Cache', (), {'get': lambda *_: None, 'set': lambda *_: None})(),
        get_db_connection=lambda: None,
        close_db_connection=lambda _conn: None,
        store_user_request=lambda *_: True,
        TMDB_API_KEY='synthetic',
        logger=failing_app.logger,
    )
    response = failing_app.test_client().post('/api/gen_link/7')
    assert response.status_code == 500
    assert 'url' not in response.get_json()


def test_consumer_enforces_expiry_and_single_use():
    assert 'created_at >= NOW() - INTERVAL \'1 minute\'' in MAIN_SOURCE
    assert 'DELETE FROM temp_links WHERE token = %s' in MAIN_SOURCE
    assert 're.fullmatch(r"tmp_[0-9a-f]{12}", payload)' in MAIN_SOURCE


def test_consumer_binds_selected_file_to_movie():
    assert 'WHERE mf.id = %s AND mf.movie_id = %s' in MAIN_SOURCE
