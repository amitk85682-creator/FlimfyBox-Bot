from pathlib import Path


MAIN = (Path(__file__).resolve().parents[1] / "main.py").read_text(
    encoding="utf-8"
)


def test_auto_post_has_two_vertical_buttons():
    assert 'InlineKeyboardButton("Get Now", url=secure_url)' in MAIN
    assert 'InlineKeyboardButton("Join Channel", url=FILMFYBOX_CHANNEL_URL)' in MAIN
    assert 'InlineKeyboardButton("Download Now", url=secure_url)' not in MAIN
    assert 'InlineKeyboardButton("⚡ Download Now", url=secure_url)' not in MAIN
    assert 'InlineKeyboardButton("📥 Download Now", url=secure_url)' not in MAIN
    assert 'InlineKeyboardButton("Get Now", url=secure_url), InlineKeyboardButton' not in MAIN
