from pathlib import Path


MAIN_SOURCE = Path(__file__).resolve().parents[1].joinpath("main.py").read_text(
    encoding="utf-8"
)


def test_tmdb_key_is_defined_before_startup_validation():
    definition = MAIN_SOURCE.index('TMDB_API_KEY = os.environ.get("TMDB_API_KEY")')
    validation = MAIN_SOURCE.index('if not TMDB_API_KEY:')
    assert definition < validation


def test_tmdb_key_is_not_hardcoded():
    assert 'TMDB_API_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"' not in MAIN_SOURCE
