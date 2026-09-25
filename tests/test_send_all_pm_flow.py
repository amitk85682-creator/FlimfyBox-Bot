from pathlib import Path


MAIN_SOURCE = Path(__file__).resolve().parents[1].joinpath("main.py").read_text(
    encoding="utf-8"
)


def test_search_progress_sends_one_lightweight_status_message():
    start = MAIN_SOURCE.index("async def send_search_progress")
    end = MAIN_SOURCE.index("async def remove_search_progress", start)
    progress_source = MAIN_SOURCE[start:end]

    assert "return None" in progress_source
    assert "copy_message" not in progress_source
    assert "send_animation" not in progress_source
    assert "reply_text" in progress_source
    assert '🔎 Searching for "' in progress_source


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


def test_send_all_redirects_group_callbacks_before_sending_group_status():
    send_all_source = MAIN_SOURCE[
        MAIN_SOURCE.index('if query.data.startswith("sendall_"):'):
        MAIN_SOURCE.index("    # === NEW: SCAN INFO POPUP ===")
    ]
    assert 'update.effective_chat.type in ("group", "supergroup")' in send_all_source
    assert "await query.answer(url=start_url)" in send_all_source
    assert send_all_source.index("await query.answer(url=start_url)") < send_all_source.index(
        "status_msg = await query.message.reply_text"
    )


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


def test_tracking_schedules_direct_delete_and_durable_queue():
    tracking_source = MAIN_SOURCE[
        MAIN_SOURCE.index("def track_message_for_deletion("):
        MAIN_SOURCE.index("def track_user_message_for_deletion(", MAIN_SOURCE.index("def track_message_for_deletion("))
    ]
    assert "add_messages_to_db_queue" in tracking_source
    assert "delete_message_directly_after_delay" in tracking_source
    assert "asyncio.create_task" in tracking_source


def test_file_status_media_is_tracked_as_a_temporary_file_message():
    assert (
        "track_user_message_for_deletion(\n"
        "                            context, chat_id, status_msg, is_file=True"
        in MAIN_SOURCE
    )
