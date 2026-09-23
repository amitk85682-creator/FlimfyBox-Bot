from pathlib import Path


MAIN = (Path(__file__).resolve().parents[1] / "main.py").read_text(
    encoding="utf-8"
)


def test_group_welcome_uses_group_title_and_clickable_display_name():
    assert "update.effective_chat.title" in MAIN
    assert "tg://user?id={member.id}" in MAIN
    assert 'f"<b>Hey ♥️ {identity}, Welcome to {group_name}...</b>"' in MAIN
    assert "member.username" not in MAIN[
        MAIN.index("async def group_member_welcome"):
        MAIN.index("async def web_app_data_handler")
    ]
