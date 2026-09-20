from pathlib import Path


MAIN_SOURCE = Path(__file__).resolve().parents[1].joinpath("main.py").read_text(
    encoding="utf-8"
)


def test_search_progress_copies_the_approved_loading_media():
    start = MAIN_SOURCE.index("async def send_search_progress")
    end = MAIN_SOURCE.index("async def remove_search_progress", start)
    progress_source = MAIN_SOURCE[start:end]

    assert "context.bot.copy_message" in progress_source
    assert "from_chat_id=START_GIF_CHANNEL_ID" in progress_source
    assert "message_id=START_GIF_MESSAGE_ID" in progress_source
    assert "random.choice(SEARCH_ERROR_GIFS)" not in progress_source


def test_send_all_has_pm_start_deep_link_for_group_users():
    assert "start=sendall_{movie_id}_" in MAIN_SOURCE
    assert "await update.callback_query.answer(url=start_url)" in MAIN_SOURCE
    assert "deliver_movie_page_on_start" in MAIN_SOURCE


def test_send_all_deep_link_delivers_only_requested_page():
    assert "page_files = qualities[start:start + 10]" in MAIN_SOURCE
    assert "payload.startswith(\"sendall_\")" in MAIN_SOURCE
