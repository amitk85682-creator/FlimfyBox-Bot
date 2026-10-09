from webapp_routes import requests as route_requests


class BrokenConnection:
    def cursor(self):
        raise RuntimeError("database unavailable")


class FakeTmdbResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


def test_search_uses_tmdb_when_local_catalogue_is_unavailable(webapp_factory, monkeypatch):
    def fake_get(url, **kwargs):
        if "suggestqueries.google.com" in url:
            return FakeTmdbResponse([None, []])
        return FakeTmdbResponse({
            "results": [{
                "id": 123,
                "media_type": "movie",
                "title": "This Is a Genocide",
                "release_date": "2026-09-24",
                "poster_path": "/poster.jpg",
                "vote_average": 6.7,
                "overview": "A test result",
            }]
        })

    monkeypatch.setattr(route_requests, "get", fake_get)
    app, _captured, _connection = webapp_factory(connection=BrokenConnection())

    response = app.test_client().get("/api/search?q=This%20is%20a%20Genocide")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "success"
    assert payload["results"][0]["source"] == "tmdb"
    assert payload["results"][0]["title"] == "This Is a Genocide"
    assert payload["results"][0]["is_upcoming"] is False
