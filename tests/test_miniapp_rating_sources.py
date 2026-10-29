"""IMDb and TMDb ratings used by the mini-app movie detail page."""

import pytest

from .conftest import FakeConnection
from webapp_routes import register_webapp_routes, requests as route_requests


class ProviderResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def tmdb_detail(identifier, *, score=7.6, votes=2100, imdb_id='tt1234567'):
    return {
        'id': identifier,
        'vote_average': score,
        'vote_count': votes,
        'external_ids': {'imdb_id': imdb_id},
    }


def test_local_movie_audience_reviews_keep_tmdb_attribution_and_safe_source_links(webapp_factory, monkeypatch):
    connection = FakeConnection(fetchone_result=(42, 'Movie', 'Movie'))
    app, _, _ = webapp_factory(connection=connection)
    calls = []
    reviews = {
        'results': [{
            'author': 'A TMDb reviewer',
            'content': '<p>Sharp, tense &amp; beautifully acted.</p>',
            'author_details': {'rating': 8.5},
            'created_at': '2026-09-20T11:00:00.000Z',
            'url': 'https://www.themoviedb.org/review/rev-123',
        }, {
            'author': 'Unsafe link reviewer',
            'content': '<b>Still just plain text.</b>',
            'url': 'https://evil.example/review/fake',
        }, {
            'author': 'Nonstandard port reviewer',
            'content': 'Source URL must stay on the official web origin.',
            'url': 'https://www.themoviedb.org:444/review/rev-456',
        }],
        'total_results': 23,
    }

    def provider_get(url, **kwargs):
        calls.append((url, kwargs))
        assert '/movie/42/reviews' in url
        return ProviderResponse(reviews)

    monkeypatch.setattr(route_requests, 'get', provider_get)
    response = app.test_client().get('/api/movie/12/audience-reviews')

    assert response.status_code == 200
    assert response.json['source'] == 'TMDb'
    assert response.json['review_count'] == 23
    assert response.json['linked'] is True
    assert response.json['reviews'][0] == {
        'author': 'A TMDb reviewer',
        'content': 'Sharp, tense & beautifully acted.',
        'rating': 8.5,
        'created_at': '2026-09-20T11:00:00.000Z',
        'url': 'https://www.themoviedb.org/review/rev-123',
        'source': 'TMDb',
    }
    assert response.json['reviews'][1]['source'] == 'TMDb'
    assert response.json['reviews'][1]['url'] == ''
    assert response.json['reviews'][2]['url'] == ''
    assert len(calls) == 1


def test_tmdb_series_audience_reviews_use_the_tv_review_endpoint(webapp_factory, monkeypatch):
    app, _, connection = webapp_factory()
    calls = []

    def provider_get(url, **kwargs):
        calls.append(url)
        return ProviderResponse({'results': [], 'total_results': 0})

    monkeypatch.setattr(route_requests, 'get', provider_get)
    response = app.test_client().get('/api/tmdb/tv/77/audience-reviews')

    assert response.status_code == 200
    assert response.json == {
        'status': 'success', 'source': 'TMDb', 'reviews': [],
        'review_count': 0, 'linked': True,
    }
    assert calls and '/tv/77/reviews' in calls[0]
    assert connection.queries == []


def test_local_movie_scores_are_cached_without_fetching_tmdb_reviews(webapp_factory, monkeypatch):
    monkeypatch.setenv('OMDB_API_KEY', 'synthetic-omdb-key')
    connection = FakeConnection(fetchone_result=('tt1234567', 42, 'Movie', 'Movie'))
    app, _, _ = webapp_factory(connection=connection)
    calls = []

    def provider_get(url, **kwargs):
        calls.append((url, kwargs))
        if 'omdbapi.com' in url:
            return ProviderResponse({
                'Response': 'True', 'imdbRating': '8.2', 'imdbVotes': '125,500'
            })
        if '/movie/42' in url:
            return ProviderResponse(tmdb_detail(42))
        raise AssertionError(f'unexpected provider request: {url}')

    monkeypatch.setattr(route_requests, 'get', provider_get)
    client = app.test_client()

    first = client.get('/api/movie/12/rating-sources')
    second = client.get('/api/movie/12/rating-sources')

    assert first.status_code == 200
    assert first.json['imdb'] == {'score': 8.2, 'votes': '125,500'}
    assert first.json['tmdb'] == {'score': 7.6, 'votes': 2100}
    assert 'reviews' not in first.json
    assert second.json == first.json
    assert len(calls) == 2
    assert not any('/reviews' in url for url, _kwargs in calls)


def test_tmdb_only_detail_uses_explicit_media_type_and_external_imdb_id(webapp_factory, monkeypatch):
    monkeypatch.setenv('OMDB_API_KEY', 'synthetic-omdb-key')
    app, _, connection = webapp_factory()
    calls = []

    def provider_get(url, **kwargs):
        calls.append((url, kwargs))
        if 'omdbapi.com' in url:
            return ProviderResponse({
                'Response': 'True', 'imdbRating': '7.1', 'imdbVotes': '4,321'
            })
        if '/tv/77' in url:
            return ProviderResponse(tmdb_detail(77, score=7.4, votes=990, imdb_id='tt7654321'))
        raise AssertionError(f'unexpected provider request: {url}')

    monkeypatch.setattr(route_requests, 'get', provider_get)

    response = app.test_client().get('/api/tmdb/tv/77/rating-sources')

    assert response.status_code == 200
    assert response.json['imdb']['score'] == 7.1
    assert response.json['tmdb'] == {'score': 7.4, 'votes': 990}
    assert 'reviews' not in response.json
    assert connection.queries == []
    assert any('/tv/77' in url for url, _kwargs in calls)
    assert any(kwargs.get('params', {}).get('i') == 'tt7654321'
               for url, kwargs in calls if 'omdbapi.com' in url)


def test_one_provider_timeout_returns_partial_ratings(webapp_factory, monkeypatch):
    monkeypatch.setenv('OMDB_API_KEY', 'synthetic-omdb-key')
    connection = FakeConnection(fetchone_result=('tt1234567', 42, 'Movie', 'Movie'))
    app, _, _ = webapp_factory(connection=connection)

    def provider_get(url, **kwargs):
        if 'omdbapi.com' in url:
            raise route_requests.Timeout('OMDb is unavailable')
        if '/movie/42' in url:
            return ProviderResponse(tmdb_detail(42, score=7.4, votes=88))
        raise AssertionError(f'unexpected provider request: {url}')

    monkeypatch.setattr(route_requests, 'get', provider_get)

    response = app.test_client().get('/api/movie/12/rating-sources')

    assert response.status_code == 200
    assert response.json['imdb']['score'] is None
    assert response.json['tmdb'] == {'score': 7.4, 'votes': 88}
    assert 'reviews' not in response.json


def test_tmdb_zero_votes_does_not_create_a_zero_star_score(webapp_factory, monkeypatch):
    connection = FakeConnection(fetchone_result=('tt1234567', 42, 'Movie', 'Movie'))
    app, _, _ = webapp_factory(connection=connection)

    def provider_get(url, **kwargs):
        return ProviderResponse(tmdb_detail(42, score=0, votes=0))

    monkeypatch.setattr(route_requests, 'get', provider_get)

    response = app.test_client().get('/api/movie/12/rating-sources')

    assert response.status_code == 200
    assert response.json['tmdb'] == {'score': None, 'votes': None}


def test_provider_request_errors_do_not_log_api_keys(webapp_factory, monkeypatch, caplog):
    monkeypatch.setenv('OMDB_API_KEY', 'synthetic-omdb-secret')
    connection = FakeConnection(fetchone_result=('tt1234567', 42, 'Movie', 'Movie'))
    app, _, _ = webapp_factory(connection=connection)

    def provider_get(url, **_kwargs):
        if 'omdbapi.com' in url:
            raise route_requests.ConnectionError(
                'request failed at https://www.omdbapi.com/?apikey=synthetic-omdb-secret'
            )
        raise route_requests.ConnectionError(
            'request failed at https://api.themoviedb.org/3/movie/42?api_key=synthetic-tmdb-secret'
        )

    monkeypatch.setattr(route_requests, 'get', provider_get)
    app.test_client().get('/api/movie/12/rating-sources')

    assert 'synthetic-omdb-secret' not in caplog.text
    assert 'synthetic-tmdb-secret' not in caplog.text


def test_missing_local_movie_and_invalid_tmdb_type_do_not_call_providers(webapp_factory, monkeypatch):
    app, _, _ = webapp_factory(connection=FakeConnection(fetchone_result=None))

    def unexpected_provider_call(*_args, **_kwargs):
        pytest.fail('provider should not be queried for invalid movie identity')

    monkeypatch.setattr(route_requests, 'get', unexpected_provider_call)
    client = app.test_client()

    assert client.get('/api/movie/12/rating-sources').status_code == 404
    assert client.get('/api/tmdb/person/77/rating-sources').status_code == 404


def test_local_db_connection_is_released_before_provider_lookups(monkeypatch):
    monkeypatch.delenv('OMDB_API_KEY', raising=False)
    connection = FakeConnection(fetchone_result=('tt1234567', 42, 'Movie', 'Movie'))
    released_connections = []
    provider_observations = []
    app = __import__('flask').Flask(__name__)

    register_webapp_routes(
        app,
        api_movies_cache=type('Cache', (), {'get': lambda *_: None, 'set': lambda *_: None})(),
        search_cache=type('Cache', (), {'get': lambda *_: None, 'set': lambda *_: None})(),
        get_db_connection=lambda: connection,
        close_db_connection=released_connections.append,
        store_user_request=lambda *_: None,
        TMDB_API_KEY='synthetic-tmdb-key',
        logger=app.logger,
    )

    def provider_get(url, **_kwargs):
        provider_observations.append(bool(released_connections))
        return ProviderResponse(tmdb_detail(42))

    monkeypatch.setattr(route_requests, 'get', provider_get)
    response = app.test_client().get('/api/movie/12/rating-sources')

    assert response.status_code == 200
    assert provider_observations == [True]

