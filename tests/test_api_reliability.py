def test_movies_rejects_malformed_and_pathological_pagination(webapp_factory):
    app, _captured, _connection = webapp_factory()
    client = app.test_client()
    assert client.get('/api/movies?page=abc').status_code == 400
    assert client.get('/api/movies?page=0').status_code == 400
    assert client.get('/api/movies?limit=101').status_code == 400


def test_search_rejects_unbounded_query(webapp_factory):
    app, _captured, _connection = webapp_factory()
    response = app.test_client().get('/api/search?q=' + ('x' * 201))
    assert response.status_code == 400


def test_invalid_positive_identifier_is_rejected(webapp_factory):
    app, _captured, _connection = webapp_factory()
    assert app.test_client().post('/api/gen_link/0').status_code == 400
    assert app.test_client().post('/api/gen_link/2/file/0').status_code == 400


def test_internal_route_errors_use_safe_messages():
    from pathlib import Path

    source = (Path(__file__).resolve().parents[1] / 'webapp_routes.py').read_text(
        encoding='utf-8'
    )
    assert "return jsonify({'status': 'error', 'message': str(e)}), 500" not in source
    assert "return jsonify({'status': 'error', 'message': str(error)}), 500" not in source
