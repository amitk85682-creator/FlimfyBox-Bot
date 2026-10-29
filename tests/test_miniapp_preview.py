from datetime import datetime, timedelta, timezone

import pytest
import requests

from tests import miniapp_preview as preview


def test_default_proxy_forwards_only_public_query_fields(monkeypatch):
    calls = []
    payload = {'status': 'success', 'results': [{'id': 1, 'title': 'Reacher'}]}
    monkeypatch.setattr(preview, 'deployed_json', lambda path, params: calls.append((path, params)) or payload)
    client = preview.create_preview().test_client()
    response = client.get('/api/search?q=re&autocomplete=1&initData=secret&token=secret&url=http://other',
                          headers={'Authorization': 'Bearer secret', 'X-Telegram-Init-Data': 'secret', 'Cookie': 'session=secret'})
    assert response.json == payload
    assert calls == [('search', {'q': 're', 'autocomplete': '1'})]
    assert response.headers['X-Preview-Mode'] == 'deployed-read-only'
    assert response.headers['Cache-Control'] == 'no-store'
    assert 'Deployed catalogue API' in client.get('/preview').text


@pytest.mark.parametrize('path', [
    '/api/movie/12/rating',
    '/api/movie/12/rating-sources',
    '/api/movie/12/reviews',
    '/api/movie/12/audience-reviews',
    '/api/tmdb/movie/77/rating-sources',
    '/api/tmdb/tv/77/rating-sources',
    '/api/tmdb/movie/77/audience-reviews',
    '/api/tmdb/tv/77/audience-reviews',
])
def test_rating_source_endpoints_are_available_in_read_only_proxy(monkeypatch, path):
    calls = []
    payload = {'status': 'success', 'imdb': {'score': None}, 'tmdb': {'score': 7.8}}
    monkeypatch.setattr(preview, 'deployed_json', lambda upstream, params: calls.append((upstream, params)) or payload)
    response = preview.create_preview().test_client().get(path + '?token=must-not-forward')
    assert response.status_code == 200
    assert response.json == payload
    assert calls == [(path.removeprefix('/api/'), {})]


@pytest.mark.parametrize('method,path', [
    ('post', '/api/request'), ('post', '/api/movie/1/rating'),
    ('delete', '/api/my-list/1'), ('get', '/api/my-list'),
    ('get', '/api/my-list/1/status'), ('get', '/api/upcoming/reminder'),
    ('get', '/api/global-chat'), ('post', '/api/movies'),
    ('put', '/api/movies'), ('get', '/api/unknown'),
    ('get', '/api/https://example.org'), ('get', '/watch/1/file/2'),
])
def test_preview_never_calls_live_for_mutations_or_private_routes(monkeypatch, method, path):
    def unexpected(*args):
        pytest.fail('Private or mutation request reached the upstream')
    monkeypatch.setattr(preview, 'deployed_json', unexpected)
    client = preview.create_preview().test_client()
    assert getattr(client, method)(path).status_code == 403


@pytest.mark.parametrize('failure,status', [(requests.Timeout(), 504), (requests.ConnectionError(), 502), (ValueError(), 502)])
def test_proxy_failure_is_an_error_not_a_fake_success(monkeypatch, failure, status):
    def fail(*args):
        raise failure
    monkeypatch.setattr(preview, 'deployed_json', fail)
    response = preview.create_preview().test_client().get('/api/movies')
    assert response.status_code == status
    assert response.json['status'] == 'error'


def test_offline_snapshot_preserves_metadata_and_filters_real_dates():
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    past = (today - timedelta(days=2)).isoformat()
    future = (today + timedelta(days=2)).isoformat()
    movies = [
        {'id': 1, 'title': 'Released', 'release_date': past},
        {'id': 2, 'title': 'Unknown date'},
        {'id': 3, 'title': 'Invalid date', 'release_date': '2027-02-30'},
        {'id': 4, 'title': 'Future film', 'release_date': future},
        {'id': 5, 'title': 'Available already', 'release_date': future, 'is_available': True},
        {'id': 6, 'title': 'Future show', 'media_type': 'tv', 'first_air_date': future},
        {'id': 7, 'title': 'Show already aired', 'media_type': 'tv', 'first_air_date': past, 'release_date': future},
    ]
    client = preview.create_preview({'movies': movies}).test_client()
    assert client.get('/api/movie/1').json['movie'] == movies[0]
    assert client.get('/api/movies').json['movies'] == movies
    assert {m['id'] for m in client.get('/api/upcoming').json['movies']} == {4, 6}
    assert {m['id'] for m in client.get('/api/home/new-releases').json['movies']} == {1, 7}
    assert client.post('/api/request', json={'title': 'X'}).status_code == 403
    assert client.get('/api/movie/1/rating').status_code == 404
    assert client.get('/api/movie/1/reviews').json['status'] == 'unavailable'
    assert client.get('/api/movie/1/audience-reviews').json['status'] == 'unavailable'


def test_snapshot_prefix_search_and_real_genre_membership():
    movies = [
        {'id': 1, 'title': 'Reacher', 'genre': 'Action, Drama', 'category': 'Web Series'},
        {'id': 2, 'title': 'The Red Queen', 'genre': 'Drama', 'category': 'Hollywood'},
        {'id': 3, 'title': 'Rebecca', 'genre': 'Romance', 'category': 'Hollywood'},
    ]
    client = preview.create_preview({'movies': movies}).test_client()
    assert [m['title'] for m in client.get('/api/search?q=re&autocomplete=1').json['results']] == ['Reacher', 'Rebecca']
    assert len(client.get('/api/search?q=re').json['results']) == 3
    assert [m['id'] for m in client.get('/api/genre/drama?type=tv').json['movies']] == [1]
    counts = {g['id']: g['count'] for g in client.get('/api/genres').json['genres']}
    assert counts == {'action': 1, 'drama': 2, 'romance': 1}
    assert client.get('/api/movies?page=2&limit=2').json == {'status': 'success', 'movies': [movies[2]], 'has_more': False}


def test_http_client_disables_credentials_redirects_and_bounds_reads(monkeypatch):
    captured = {}

    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def iter_content(self, chunk_size):
            captured['chunk_size'] = chunk_size
            yield b'{"status":"success","movies":[]}'

    class Session:
        trust_env = True
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, url, **kwargs):
            captured.update(url=url, trust_env=self.trust_env, **kwargs)
            return Response()

    monkeypatch.setattr(preview.requests, 'Session', Session)
    assert preview.deployed_json('movies', {'page': '1'})['status'] == 'success'
    assert captured['url'] == preview.DEPLOYED_ORIGIN + '/api/movies'
    assert captured['trust_env'] is False
    assert captured['allow_redirects'] is False
    assert captured['timeout'] == (3, 8)
    assert captured['headers'] == {'Accept': 'application/json'}
    assert 'cookies' not in captured and 'auth' not in captured


def test_upstream_redirects_are_not_followed(monkeypatch):
    class Response:
        status_code = 302
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, *args, **kwargs): return Response()

    monkeypatch.setattr(preview.requests, 'Session', Session)
    with pytest.raises(requests.RequestException):
        preview.deployed_json('movies', {})
