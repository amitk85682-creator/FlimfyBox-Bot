from .conftest import signed_init_data


def test_frontend_identity_cannot_impersonate_user(webapp_factory):
    app, captured, _connection = webapp_factory()
    response = app.test_client().post(
        '/api/request',
        json={
            'title': 'Synthetic Movie',
            'user_id': 999999,
            'username': 'attacker',
            'first_name': 'Attacker',
        },
    )
    assert response.status_code == 200
    assert captured[0][:3] == (0, '', 'WebApp User')


def test_valid_telegram_identity_is_used(webapp_factory):
    app, captured, _connection = webapp_factory()
    response = app.test_client().post(
        '/api/request',
        json={
            'title': 'Synthetic Movie',
            'user_id': 999999,
            'username': 'attacker',
            'first_name': 'Attacker',
        },
        headers={'X-Telegram-Init-Data': signed_init_data()},
    )
    assert response.status_code == 200
    assert captured[0][:3] == (101, 'verified', 'Verified')


def test_anonymous_request_remains_supported(webapp_factory):
    app, captured, _connection = webapp_factory()
    response = app.test_client().post('/api/request', json={'title': 'Anonymous'})
    assert response.status_code == 200
    assert captured[0][0] == 0


def test_request_rejects_non_text_title(webapp_factory):
    app, _captured, _connection = webapp_factory()
    response = app.test_client().post('/api/request', json={'title': ['not', 'text']})
    assert response.status_code == 400
