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


def test_approved_loading_media_points_to_the_configured_source_message():
    assert "START_GIF_CHANNEL_ID = -1003893346701" in MAIN_SOURCE
    assert "START_GIF_MESSAGE_ID = 62" in MAIN_SOURCE


def test_user_message_retention_policy_uses_five_minutes_for_text_and_two_for_files():
    assert "USER_TEXT_DELETE_SECONDS = 5 * 60" in MAIN_SOURCE
    assert "USER_FILE_DELETE_SECONDS = 2 * 60" in MAIN_SOURCE
    assert "delay = USER_FILE_DELETE_SECONDS if is_file else USER_TEXT_DELETE_SECONDS" in MAIN_SOURCE
    assert "Copyright Protection — 2 minutes" in MAIN_SOURCE
    assert "automatically❕️deletes after 2 minutes" in MAIN_SOURCE


def test_send_all_has_pm_start_deep_link_for_group_users():
    assert "start=sendall_{movie_id}_" in MAIN_SOURCE
    assert "await update.callback_query.answer(url=start_url)" in MAIN_SOURCE
    assert "deliver_movie_page_on_start" in MAIN_SOURCE


def test_send_all_deep_link_delivers_only_requested_page():
    assert "page_files = qualities[start:start + 10]" in MAIN_SOURCE
    assert "payload.startswith(\"sendall_\")" in MAIN_SOURCE


def test_search_loading_media_is_cleaned_up_on_every_search_exit():
    search_source = MAIN_SOURCE[
        MAIN_SOURCE.index("async def search_movies("):
        MAIN_SOURCE.index("async def request_movie(", MAIN_SOURCE.index("async def search_movies("))
    ]
    assert "finally:" in search_source
    assert "await remove_search_progress(progress_message)" in search_source


def test_file_warning_sticker_uses_file_retention_window():
    assert (
        "track_user_message_for_deletion(\n"
        "                        context, user_id, warning_msg, is_file=True"
        in MAIN_SOURCE
    )


def test_delete_queue_uses_database_clock_for_deadlines():
    assert "NOW() + (%s * INTERVAL '1 second')" in MAIN_SOURCE
    assert "datetime.now() + timedelta(seconds=delay)" not in MAIN_SOURCE


def test_auto_delete_worker_retries_failed_telegram_deletions():
    worker_source = MAIN_SOURCE[MAIN_SOURCE.index("async def auto_delete_worker("):]
    assert "Auto-delete retry needed" in worker_source
    assert "else:\n                        cur.execute(\"DELETE FROM auto_delete_queue" in worker_source


def test_temporary_search_and_file_status_media_are_also_tracked():
    assert (
        "track_user_message_for_deletion(\n"
        "                            context, chat_id, status_msg, is_file=True"
        in MAIN_SOURCE
    )
    search_source = MAIN_SOURCE[
        MAIN_SOURCE.index("async def send_search_progress("):
        MAIN_SOURCE.index("async def remove_search_progress(", MAIN_SOURCE.index("async def send_search_progress("))
    ]
    assert "track_user_message_for_deletion(" in search_source
