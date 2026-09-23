from pathlib import Path


MAIN = (Path(__file__).resolve().parents[1] / "main.py").read_text(
    encoding="utf-8"
)


def test_batch_id_resolves_title_collision_before_insert():
    section = MAIN[
        MAIN.index("async def batch_id_command"):
        MAIN.index("async def", MAIN.index("async def batch_id_command") + 10)
    ]
    assert "WHERE imdb_id = %s OR title = %s" in section
    assert "existing_movie = cur.fetchone()" in section
    assert "if existing_movie:" in section
    assert "INSERT INTO movies" in section
