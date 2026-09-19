import time

from .conftest import signed_init_data


def test_valid_signed_init_data_is_accepted(webapp_factory):
    app, _captured, _connection = webapp_factory()
    response = app.test_client().get(
        '/api/my-list',
        headers={'X-Telegram-Init-Data': signed_init_data()},
    )
    assert response.status_code == 200


def test_invalid_or_missing_hash_is_rejected(webapp_factory):
    app, _captured, _connection = webapp_factory()
    client = app.test_client()
    assert client.get('/api/my-list', headers={
        'X-Telegram-Init-Data': 'auth_date=1&user=%7B%22id%22%3A101%7D&hash=bad'
    }).status_code == 401
    assert client.get('/api/my-list', headers={
        'X-Telegram-Init-Data': 'auth_date=1&user=%7B%22id%22%3A101%7D'
    }).status_code == 401


def test_missing_stale_and_future_auth_dates_are_rejected(webapp_factory, monkeypatch):
    app, _captured, _connection = webapp_factory()
    client = app.test_client()
    monkeypatch.setenv('TELEGRAM_AUTH_MAX_AGE_SECONDS', '60')

    missing_date = signed_init_data().replace('auth_date=', 'missing_date=')
    assert client.get('/api/my-list', headers={
        'X-Telegram-Init-Data': missing_date
    }).status_code == 401
    assert client.get('/api/my-list', headers={
        'X-Telegram-Init-Data': signed_init_data(auth_date=int(time.time()) - 61)
    }).status_code == 401
    assert client.get('/api/my-list', headers={
        'X-Telegram-Init-Data': signed_init_data(auth_date=int(time.time()) + 61)
    }).status_code == 401


def test_configured_max_age_is_honored(webapp_factory, monkeypatch):
    app, _captured, _connection = webapp_factory()
    monkeypatch.setenv('TELEGRAM_AUTH_MAX_AGE_SECONDS', '120')
    response = app.test_client().get(
        '/api/my-list',
        headers={
            'X-Telegram-Init-Data': signed_init_data(
                auth_date=int(time.time()) - 119
            )
        },
    )
    assert response.status_code == 200
