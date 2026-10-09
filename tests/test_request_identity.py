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
    assert response.get_json()['guest'] is True
    assert response.get_json()['request_id'] == 17


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
    assert response.get_json()['guest'] is False
    assert response.get_json()['request_id'] == 17


def test_missing_init_data_is_saved_as_guest_not_json_identity(webapp_factory):
    app, captured, _connection = webapp_factory()
    response = app.test_client().post(
        '/api/request',
        json={
            'title': 'Guest Request',
            'user_id': 101,
            'username': 'untrusted',
            'first_name': 'Untrusted',
        },
    )

    assert response.status_code == 200
    assert response.get_json()['guest'] is True
    assert 'Open this page inside Telegram' in response.get_json()['message']
    assert captured[0][:3] == (0, '', 'WebApp User')


def test_anonymous_request_remains_supported(webapp_factory):
    app, captured, _connection = webapp_factory()
    response = app.test_client().post('/api/request', json={'title': 'Anonymous'})
    assert response.status_code == 200
    assert captured[0][0] == 0


def test_request_rejects_non_text_title(webapp_factory):
    app, _captured, _connection = webapp_factory()
    response = app.test_client().post('/api/request', json={'title': ['not', 'text']})
    assert response.status_code == 400


def test_request_admin_buttons_use_only_the_saved_request_id(webapp_factory, monkeypatch):
    import webapp_routes

    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {'ok': True}

    def fake_post(url, *, json, timeout):
        captured.update(json)
        return FakeResponse()

    monkeypatch.setattr(webapp_routes.requests, 'post', fake_post)
    app, _stored, _connection = webapp_factory()
    monkeypatch.setenv('REQUEST_CHANNEL_ID', '-100123')
    response = app.test_client().post(
        '/api/request',
        json={'title': 'A title with underscores_and_long_suffix'},
        headers={'X-Telegram-Init-Data': signed_init_data()},
    )

    assert response.status_code == 200
    keyboard = captured['reply_markup']['inline_keyboard']
    assert keyboard[0][0]['callback_data'] == 'reqA_17'
    assert keyboard[1][0]['callback_data'] == 'reqN_17'
    assert 'underscores' not in keyboard[0][0]['callback_data']


def test_guest_request_does_not_claim_a_telegram_dm_was_sent(webapp_factory):
    app, _captured, _connection = webapp_factory()
    response = app.test_client().post('/api/request', json={'title': 'Guest Movie'})

    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['guest'] is True
    assert 'Open this page inside Telegram' in payload['message']


def test_both_request_functions_send_signed_init_data_header():
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / 'static' / 'miniapp' / 'app.js'
    ).read_text(encoding='utf-8')
    for function_name in ('requestMovie', 'requestSilent'):
        start = source.index(f'window.{function_name} = function')
        end = source.index('\n        };', start)
        function_source = source[start:end]
        assert "'Content-Type': 'application/json'" in function_source
        assert '...telegramAuthHeaders()' in function_source
        assert 'user_id:' not in function_source
        assert 'username:' not in function_source
        assert 'first_name:' not in function_source
