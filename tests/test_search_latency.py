from pathlib import Path
import ast
import asyncio
import functools
import logging
import re
import threading
import __future__
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fuzzywuzzy import fuzz, process


MAIN_SOURCE = Path(__file__).resolve().parents[1].joinpath("main.py").read_text(
    encoding="utf-8"
)


def test_fast_search_separates_indexed_lookup_from_similarity_fallback():
    search_source = MAIN_SOURCE[
        MAIN_SOURCE.index("def _get_movies_fast_sql_nocache"):
        MAIN_SOURCE.index("def get_movie_by_imdb_id", MAIN_SOURCE.index("def _get_movies_fast_sql_nocache"))
    ]
    assert "exact_sql = " in search_source
    assert "WHERE m.title ILIKE %s" in search_source
    assert "if not results:" in search_source
    assert "fuzzy_sql = " in search_source
    assert "OR SIMILARITY" not in search_source


def test_search_bounds_google_fallback_and_joins_delivery_data():
    assert "GOOGLE_SUGGESTION_TIMEOUT_SECONDS = 1.5" in MAIN_SOURCE
    assert "get_google_title_suggestions_with_timeout" in MAIN_SOURCE
    assert "def get_movie_delivery_data(movie_id)" in MAIN_SOURCE
    assert "LEFT JOIN movie_files AS mf" in MAIN_SOURCE


def load_functions(*names, **dependencies):
    nodes = [node for node in ast.parse(MAIN_SOURCE).body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    namespace = dict(asyncio=asyncio, functools=functools, re=re,
                     logger=logging.getLogger(__name__), **dependencies)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


def test_fuzzy_fallback_bounds_candidates_and_preserves_duplicate_titles():
    movies = [(1, 'Reacher', '', '', '', '', 2022, ''),
              (2, 'Reacher', '', '', '', '', 2025, '')]
    queries, released = [], []

    class Cursor:
        def execute(self, query, params=None):
            queries.append((query, params))

        def fetchall(self):
            return movies if 'SIMILARITY(' in queries[-1][0] else []

        def close(self):
            pass

    connection = SimpleNamespace(cursor=Cursor)
    ns = load_functions('_get_movies_from_db_nocache', fuzz=fuzz, process=process,
                        get_db_connection=lambda: connection,
                        close_db_connection=released.append)
    assert ns['_get_movies_from_db_nocache']('rechar') == movies
    assert released == [connection]
    selects = [(sql, params) for sql, params in queries if 'SELECT' in sql]
    assert all('WHERE' in sql and 'LIMIT %s' in sql for sql, _ in selects)
    assert selects[-1][1][-1] <= 100
    assert "%% %s" in selects[-1][0]


def test_fast_typo_lookup_uses_trigram_operator_and_returns_movie_rows():
    movie = (1, 'Reacher', '', '', '', '', 2022, '')
    queries = []

    class Cursor:
        def execute(self, query, params=None):
            queries.append((query, params))

        def fetchall(self):
            return [movie + (0.4,)] if 'WHERE m.title %% %s' in queries[-1][0] else []

        def close(self):
            pass

    ns = load_functions('_get_movies_fast_sql_nocache',
                        get_db_connection=lambda: SimpleNamespace(cursor=Cursor),
                        close_db_connection=lambda _: None)
    assert ns['_get_movies_fast_sql_nocache']('rechar') == [movie]
    assert queries[-1][1] == ('rechar', 'rechar', 5)


def test_delete_queue_write_does_not_block_other_users_or_fetch_bot_again():
    async def scenario():
        started, release = threading.Event(), threading.Event()
        calls = []

        def slow_save(*args):
            calls.append(args)
            started.set()
            assert release.wait(2)

        ns = load_functions('run_async', 'add_messages_to_db_queue', _save_delete_queue=slow_save)
        bot = SimpleNamespace(username='testbot', get_me=AsyncMock())
        task = asyncio.create_task(ns['add_messages_to_db_queue'](
            SimpleNamespace(bot=bot), 10, [20], 300))
        try:
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(0.001)
            assert started.is_set()
            assert not task.done()  # The event loop ran while the DB was blocked.
        finally:
            release.set()
            await task
        assert calls == [('testbot', 10, [20], 300)]
        bot.get_me.assert_not_awaited()

    asyncio.run(scenario())


@pytest.mark.parametrize('poster_result', ['hang', 'url', 'bytes'])
def test_search_delivers_files_when_poster_is_slow_or_unavailable(poster_result):
    async def scenario():
        async def poster(_):
            if poster_result == 'hang':
                await asyncio.Event().wait()
            return BytesIO(b'poster') if poster_result == 'bytes' else 'https://slow.example/poster'

        async def run_async(func, *args):
            return func(*args)

        message = SimpleNamespace(reply_text=AsyncMock(), reply_photo=AsyncMock())
        update = SimpleNamespace(message=message, effective_user=SimpleNamespace(id=1),
                                 effective_chat=SimpleNamespace(id=2))
        context = SimpleNamespace(user_data={}, bot=SimpleNamespace(username='testbot'))
        ns = load_functions(
            'process_movie_exact_match', SEARCH_TIMING_ENABLED=False,
            SEARCH_POSTER_TIMEOUT_SECONDS=0.01, run_async=run_async,
            get_movie_delivery_data=lambda _: ([('1080p', '', '', '1 GB')], ('Movie', 'https://slow.example/poster')),
            _format_requested_files_header=lambda *args: 'Files',
            _format_file_link_label=lambda *args: '1080p', html_escape=lambda value: value,
            create_quality_selection_keyboard=lambda **kwargs: None,
            make_landscape_poster=poster, USER_TEXT_DELETE_SECONDS=300,
            track_message_for_deletion=lambda *args: None,
        )
        await asyncio.wait_for(ns['process_movie_exact_match'](update, context, 7, 'Reacher'), 0.5)
        if poster_result == 'bytes':
            message.reply_photo.assert_awaited_once()
            message.reply_text.assert_not_awaited()
        else:
            message.reply_photo.assert_not_awaited()
            message.reply_text.assert_awaited_once()
            assert '?start=file_7_0' in message.reply_text.call_args.args[0]

    asyncio.run(scenario())


def test_delete_worker_only_removes_successful_or_already_deleted_messages():
    class BadRequest(Exception):
        pass

    async def scenario():
        removed, thread_ids = [], []

        def due(_):
            thread_ids.append(threading.get_ident())
            return [(1, 10, 11), (2, 10, 22), (3, 10, 33), (4, 10, 44)]

        def remove(ids):
            thread_ids.append(threading.get_ident())
            removed.extend(ids)

        async def stop_after_page(_):
            raise asyncio.CancelledError

        bot = SimpleNamespace(
            get_me=AsyncMock(return_value=SimpleNamespace(username='testbot')),
            delete_message=AsyncMock(side_effect=[None, BadRequest('message not found'),
                                                   BadRequest('permission denied'), RuntimeError('timeout')]),
        )
        ns = load_functions('run_async', 'auto_delete_worker',
                            _get_due_delete_messages=due, _remove_deleted_messages=remove,
                            telegram=SimpleNamespace(error=SimpleNamespace(BadRequest=BadRequest)))
        ns['asyncio'] = SimpleNamespace(get_running_loop=asyncio.get_running_loop, sleep=stop_after_page)
        with pytest.raises(asyncio.CancelledError):
            await ns['auto_delete_worker'](SimpleNamespace(bot=bot))
        assert removed == [1, 2]
        assert all(thread_id != threading.get_ident() for thread_id in thread_ids)

    asyncio.run(scenario())
