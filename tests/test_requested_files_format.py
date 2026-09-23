from pathlib import Path


MAIN_SOURCE = (Path(__file__).parents[1] / "main.py").read_text(encoding="utf-8")


def test_requested_files_header_has_branded_metadata():
    assert "def _format_requested_files_header(" in MAIN_SOURCE
    assert "ᴛɪᴛʟᴇ" in MAIN_SOURCE
    assert "𝙻𝚊𝚗𝚐𝚞𝚊" in MAIN_SOURCE
    assert "ʀᴇsᴜʟᴛ ɪɴ" not in MAIN_SOURCE
    assert "ʀᴇǫᴜᴇsᴛᴇᴅ ʙʏ" in MAIN_SOURCE
    assert "ᴘᴏᴡᴇʀᴇᴅ ʙʏ" in MAIN_SOURCE
    assert '"Dynamic Language"' in MAIN_SOURCE
    assert "await context.bot.get_me()" in MAIN_SOURCE
    assert "tg://user?id={requester_id}" in MAIN_SOURCE
    assert "tg://user?id={bot_id}" in MAIN_SOURCE
    assert ".strip().title()" in MAIN_SOURCE
    assert "re.split(r\"[,/|]+\"" in MAIN_SOURCE
    assert "<b>🧱 𝙻𝚊𝚗𝚐𝚞𝚊ɢᴇ </b><code>{language_label}</code>\\n\\n" in MAIN_SOURCE


def test_requested_files_header_preserves_file_delivery_links():
    assert "start=file_{movie_id}_{real_idx}" in MAIN_SOURCE
    assert "Your Requested Files Are Here" in MAIN_SOURCE
    assert "file_list_text += f" in MAIN_SOURCE


def test_unknown_suggestion_offers_request_instead_of_silent_return():
    assert "This title is not available yet" not in MAIN_SOURCE
    assert "अभी database में available नहीं है" in MAIN_SOURCE
    assert 'callback_data=f"request_prefill_{request_title}"' in MAIN_SOURCE
    assert "Open Request Portal" in MAIN_SOURCE
