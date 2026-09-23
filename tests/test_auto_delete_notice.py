from pathlib import Path


MAIN = (Path(__file__).resolve().parents[1] / "main.py").read_text(
    encoding="utf-8"
)


def test_auto_delete_notice_uses_file_delete_window():
    notice_start = MAIN.index("𝗔𝘂𝘁𝗼-𝗗𝗲𝗹𝗲𝘁𝗲 𝗡𝗼𝘁𝗶𝗰𝗲")
    notice_end = MAIN.index("except:", notice_start)
    notice_block = MAIN[notice_start:notice_end]
    assert "USER_FILE_DELETE_SECONDS" in notice_block
    assert "USER_TEXT_DELETE_SECONDS" not in notice_block
