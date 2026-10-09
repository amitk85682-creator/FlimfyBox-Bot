from pathlib import Path


MAIN_SOURCE = (Path(__file__).resolve().parents[1] / 'main.py').read_text(
    encoding='utf-8'
)


def test_cors_uses_explicit_environment_origins():
    assert 'CORS_ALLOWED_ORIGINS' in MAIN_SOURCE
    assert 'origins": "*"' not in MAIN_SOURCE
    assert "origins': '*'" not in MAIN_SOURCE


def test_cors_configuration_does_not_use_wildcard():
    cors_block = MAIN_SOURCE.split('CORS(flask_app', 1)[1].split(
        '# --- TMDB API Key', 1
    )[0]
    assert 'origins": "*"' not in cors_block
    assert "origins': '*'" not in cors_block
