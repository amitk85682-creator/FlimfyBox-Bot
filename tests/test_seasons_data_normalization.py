from pathlib import Path


MAIN = (Path(__file__).resolve().parents[1] / "main.py").read_text(
    encoding="utf-8"
)


def test_batch_metadata_normalizes_string_seasons_data_before_keys():
    assert "def normalize_seasons_data(value):" in MAIN
    assert "seasons_data = normalize_seasons_data(seasons_data)" in MAIN
    assert 'extra_info=" ".join(seasons_data.keys())' in MAIN
    assert "return parsed if isinstance(parsed, dict) else {}" in MAIN
