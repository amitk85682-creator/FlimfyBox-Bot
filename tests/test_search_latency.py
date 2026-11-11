from pathlib import Path
import ast
import asyncio
import functools
import logging
import re
import threading
import __future__
import concurrent.futures
from datetime import datetime, timezone
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fuzzywuzzy import fuzz, process


MAIN_SOURCE = Path(__file__).resolve().parents[1].joinpath("main.py").read_text(
    encoding="utf-8"
)


def test_fast_search_separates_indexed_lookup_from_similarity_fallback():
    movie = (1, 'Reacher', '', '', '', '', 2022, '', 'Series')
    queries = []

    class Cursor:
        def execute(self, sql, params=None):
            queries.append((sql, params))

        def fetchall(self):
            return [movie + (0.0,)] if 'WHERE m.title %% %s' not in queries[-1][0] else []

        def close(self):
            pass

    ns = load_functions('_normalize_search_text', '_get_movies_fast_sql_nocache',
                        get_db_connection=lambda: SimpleNamespace(cursor=Cursor),
                        close_db_connection=lambda _: None)

    assert ns['_get_movies_fast_sql_nocache']('Reacher') == [movie]
    assert len(queries) == 1
    assert 'LIKE %s' in queries[0][0]
    assert '%%' not in queries[0][0]


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
    movies = [(1, 'Reacher', '', '', '', '', 2022, '', 'Series'),
              (2, 'Reacher', '', '', '', '', 2025, '', 'Series')]
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
    movie = (1, 'Reacher', '', '', '', '', 2022, '', 'Series')
    queries = []

    class Cursor:
        def execute(self, query, params=None):
            queries.append((query, params))

        def fetchall(self):
            return [movie + (0.4,)] if 'WHERE m.title %% %s' in queries[-1][0] else []

        def close(self):
            pass

    ns = load_functions('_normalize_search_text', '_get_movies_fast_sql_nocache',
                        get_db_connection=lambda: SimpleNamespace(cursor=Cursor),
                        close_db_connection=lambda _: None)
    assert ns['_get_movies_fast_sql_nocache']('rechar') == [movie]
    assert queries[-1][1] == ('rechar', 'rechar', 5)


@pytest.mark.parametrize(('query', 'normalized_prefix'), [('r', 'r'), ('re', 're')])
def test_fast_short_prefix_lookup_uses_existing_prefix_index(query, normalized_prefix):
    movie = (1, 'Reacher', '', '', '', '', 2022, '', 'Series')
    queries = []

    class Cursor:
        def execute(self, sql, params=None):
            queries.append((sql, params))

        def fetchall(self):
            return [movie + (0.0,)]

        def close(self):
            pass

    ns = load_functions('_normalize_search_text', '_get_movies_fast_sql_nocache',
                        get_db_connection=lambda: SimpleNamespace(cursor=Cursor),
                        close_db_connection=lambda _: None)

    assert ns['_get_movies_fast_sql_nocache'](query) == [movie]
    assert len(queries) == 1
    assert "regexp_replace(LOWER(m.title), '[^a-z0-9]', '', 'g') LIKE %s" in queries[0][0]
    assert queries[0][1][0] == f'{normalized_prefix}%'
    assert 'AS content_type' in queries[0][0]
    assert 'AS has_files' not in queries[0][0]
    assert 'FROM movie_files mf' not in queries[0][0]


def test_short_prefix_miss_does_not_run_trigram_scan():
    query_rows = []

    class Cursor:
        def execute(self, sql, params=None):
            query_rows.append((sql, params))

        def fetchall(self):
            return []

        def close(self):
            pass

    ns = load_functions('_normalize_search_text', '_get_movies_fast_sql_nocache',
                        get_db_connection=lambda: SimpleNamespace(cursor=Cursor),
                        close_db_connection=lambda _: None)
    assert ns['_get_movies_fast_sql_nocache']('zz') == []
    assert len(query_rows) == 1
    assert "LIKE %s" in query_rows[0][0]
    assert "%%" not in query_rows[0][0]


def test_short_prefix_miss_skips_legacy_and_google_suggestions():
    fallback_calls = []
    suggestion_calls = []

    async def run_interactive(function, *args, **kwargs):
        return function(*args, **kwargs)

    async def get_suggestions(*args, **kwargs):
        suggestion_calls.append((args, kwargs))
        return []

    async def send_search_progress(*_args, **_kwargs):
        return None

    async def remove_search_progress(_message):
        return None

    async def reply_text(*_args, **_kwargs):
        return SimpleNamespace(message_id=22)

    message = SimpleNamespace(text='zz', reply_text=reply_text)
    update = SimpleNamespace(
        callback_query=None,
        message=message,
        effective_user=SimpleNamespace(id=7),
        effective_chat=SimpleNamespace(id=9),
    )
    context = SimpleNamespace(user_data={})
    handler = load_functions(
        'search_movies',
        SEARCH_TIMING_ENABLED=False,
        send_search_progress=send_search_progress,
        remove_search_progress=remove_search_progress,
        run_interactive=run_interactive,
        get_movies_fast_sql=lambda *_args, **_kwargs: [],
        get_movies_from_db=lambda *args, **kwargs: fallback_calls.append((args, kwargs)) or [],
        get_google_title_suggestions_with_timeout=get_suggestions,
        schedule_recommendation_event=lambda **_kwargs: None,
        _normalize_search_text=lambda value: re.sub(r'[^a-z0-9]', '', value.lower()),
        quote=lambda value: value,
        WEB_APP_URL='https://example.invalid/webapp',
        InlineKeyboardButton=lambda *args, **kwargs: (args, kwargs),
        InlineKeyboardMarkup=lambda rows: rows,
        WebAppInfo=lambda **kwargs: kwargs,
        UPDATE_CHANNEL_URL='https://example.invalid/channel',
        USER_TEXT_DELETE_SECONDS=300,
        track_user_message_for_deletion=lambda *_args: None,
    )
    coroutine = handler['search_movies'](update, context)
    try:
        coroutine.send(None)
    except StopIteration:
        pass
    else:
        pytest.fail('The short-prefix search handler suspended unexpectedly')

    assert fallback_calls == []
    assert suggestion_calls == []


def test_exact_title_with_related_movies_requires_a_user_choice():
    exact_spider_man = (1, 'Spider-Man', '', '', '', '', 2002, 'Action', 'Movie')
    related_movie = (2, 'Spider-Man 2', '', '', '', '', 2004, 'Action', 'Movie')
    ns = load_functions('_normalize_search_text', '_select_single_search_result', fuzz=fuzz)

    assert ns['_select_single_search_result']('Spider-Man', [exact_spider_man, related_movie]) is None
    assert ns['_select_single_search_result']('Spider-Man', [exact_spider_man]) == exact_spider_man
    assert ns['_select_single_search_result']('Reacher', [(3, 'The Matrix')]) is None


@pytest.mark.parametrize(
    ('candidate_count', 'expected_summary'),
    [(3, '<b>3</b> results'), (100, 'Showing the first <b>100</b> matches')],
)
def test_telegram_search_shows_all_bounded_matches_instead_of_auto_opening_exact_one(
    candidate_count, expected_summary
):
    movies = [
        (1, 'Spider-Man', '', '', '', '', 2002, 'Action', 'Movie'),
        (2, 'Spider-Man 2', '', '', '', '', 2004, 'Action', 'Movie'),
        (3, 'Spider-Man: Homecoming', '', '', '', '', 2017, 'Action', 'Movie'),
    ]
    movies.extend(
        (movie_id, f'Spider-Man related title {movie_id}', '', '', '', '', 2000 + movie_id % 25,
         'Action', 'Movie')
        for movie_id in range(4, candidate_count + 1)
    )
    fast_search_calls = []
    opened_movies = []
    sent = {}
    selection_keyboard = object()

    async def run_interactive(function, *args, **kwargs):
        return function(*args, **kwargs)

    def fast_search(*args, **kwargs):
        fast_search_calls.append((args, kwargs))
        return movies

    async def reply_text(text, **kwargs):
        sent['text'] = text
        sent.update(kwargs)
        return SimpleNamespace(message_id=44)

    async def search_progress(*_args, **_kwargs):
        return None

    async def process_movie_exact_match(*args, **_kwargs):
        opened_movies.append(args)

    message = SimpleNamespace(text='Spider-Man', reply_text=reply_text)
    update = SimpleNamespace(
        callback_query=None,
        message=message,
        effective_user=SimpleNamespace(id=7),
        effective_chat=SimpleNamespace(id=9),
    )
    context = SimpleNamespace(user_data={})
    ns = load_functions(
        'search_movies', '_normalize_search_text', '_select_single_search_result',
        SEARCH_TIMING_ENABLED=False,
        send_search_progress=search_progress,
        remove_search_progress=search_progress,
        run_interactive=run_interactive,
        get_movies_fast_sql=fast_search,
        get_movies_from_db=lambda *_args, **_kwargs: pytest.fail('Unexpected fallback search'),
        get_google_title_suggestions_with_timeout=lambda *_args, **_kwargs: pytest.fail('Unexpected suggestion request'),
        schedule_recommendation_event=lambda **_kwargs: None,
        process_movie_exact_match=process_movie_exact_match,
        create_movie_selection_keyboard=lambda _movies, page=0: selection_keyboard,
        html_escape=lambda value: value,
        track_message_for_deletion=lambda *_args: None,
        USER_TEXT_DELETE_SECONDS=300,
    )

    coroutine = ns['search_movies'](update, context)
    try:
        coroutine.send(None)
    except StopIteration:
        pass
    else:
        pytest.fail('The search handler suspended unexpectedly')

    assert fast_search_calls == [(('Spider-Man',), {'limit': 100})]
    assert opened_movies == []
    assert context.user_data['search_results'] == movies
    assert expected_summary in sent['text']
    assert sent['reply_markup'] is selection_keyboard


def test_start_link_uses_the_same_ambiguous_catalogue_search():
    movies = [
        (1, 'Spider-Man', '', '', '', '', 2002, 'Action', 'Movie'),
        (2, 'Spider-Man 2', '', '', '', '', 2004, 'Action', 'Movie'),
    ]
    fast_calls = []
    sent = {}
    selection_keyboard = object()

    async def run_async(function, *args, **kwargs):
        fast_calls.append((function.__name__, args, kwargs))
        return function(*args, **kwargs)

    def fast_search(*_args, **_kwargs):
        return movies

    async def send_message(**kwargs):
        sent.update(kwargs)
        return SimpleNamespace(message_id=88)

    async def delete_status():
        return None

    update = SimpleNamespace(effective_chat=SimpleNamespace(id=9))
    context = SimpleNamespace(
        user_data={}, bot=SimpleNamespace(send_message=send_message)
    )
    ns = load_functions(
        'background_search_and_send',
        run_async=run_async,
        get_movies_fast_sql=fast_search,
        get_movies_from_db=lambda *_args, **_kwargs: pytest.fail('Unexpected fallback search'),
        _normalize_search_text=lambda value: re.sub(r'[^a-z0-9]', '', value.lower()),
        get_google_title_suggestions=lambda *_args, **_kwargs: pytest.fail('Unexpected suggestion request'),
        quote=lambda value: value,
        WEB_APP_URL='https://example.invalid/webapp',
        InlineKeyboardButton=lambda *args, **kwargs: (args, kwargs),
        InlineKeyboardMarkup=lambda rows: rows,
        create_movie_selection_keyboard=lambda values, page=0: selection_keyboard,
        html_escape=lambda value: value,
    )

    coroutine = ns['background_search_and_send'](
        update, context, 'Spider-Man', SimpleNamespace(delete=delete_status)
    )
    try:
        coroutine.send(None)
    except StopIteration:
        pass
    else:
        pytest.fail('The start-link search handler suspended unexpectedly')

    assert fast_calls == [('fast_search', ('Spider-Man',), {'limit': 100})]
    assert context.user_data['search_results'] == movies
    assert sent['reply_markup'] is selection_keyboard
    assert 'Multiple results found' in sent['text']


def test_group_search_uses_the_same_paginated_catalogue_results():
    movies = [
        (1, 'Spider-Man', '', '', '', '', 2002, 'Action', 'Movie'),
        (2, 'Spider-Man 2', '', '', '', '', 2004, 'Action', 'Movie'),
    ]
    fast_calls = []
    sent = {}
    selection_keyboard = object()

    async def run_interactive(function, *args, **kwargs):
        fast_calls.append((function.__name__, args, kwargs))
        return function(*args, **kwargs)

    def fast_search(*_args, **_kwargs):
        return movies

    async def reply_text(text, **kwargs):
        sent['text'] = text
        sent.update(kwargs)
        return SimpleNamespace(message_id=99)

    def make_keyboard(values, page=0, requester_id=None):
        sent['requester_id'] = requester_id
        return selection_keyboard

    update = SimpleNamespace(
        message=SimpleNamespace(text='Spider-Man', reply_text=reply_text),
        effective_chat=SimpleNamespace(id=-12345),
        effective_user=SimpleNamespace(id=77),
    )
    context = SimpleNamespace(user_data={})
    ns = load_functions(
        'handle_group_message',
        GLOBAL_CHAT_GROUP_ID=-4327792876,
        run_interactive=run_interactive,
        get_movies_fast_sql=fast_search,
        _select_single_search_result=lambda _query, _values: None,
        schedule_recommendation_event=lambda **_kwargs: None,
        process_movie_exact_match=lambda *_args, **_kwargs: pytest.fail('Unexpected auto-open'),
        create_movie_selection_keyboard=make_keyboard,
        html_escape=lambda value: value,
        USER_TEXT_DELETE_SECONDS=300,
        track_message_for_deletion=lambda *_args: None,
    )

    coroutine = ns['handle_group_message'](update, context)
    try:
        coroutine.send(None)
    except StopIteration:
        pass
    else:
        pytest.fail('The group search handler suspended unexpectedly')

    assert fast_calls == [('fast_search', ('Spider-Man',), {'limit': 100})]
    assert context.user_data['search_results'] == movies
    assert sent['requester_id'] == 77
    assert sent['reply_markup'] is selection_keyboard


def test_movie_selection_buttons_show_year_and_type_without_availability_labels():
    rows = [
        # Ignore a legacy trailing availability value if an older cached row
        # is passed to the keyboard helper.
        (1, 'Spider-Man: No Way Home', '', '', '', '', 2021, 'Action', 'Movie', True),
        (2, 'Marvel\'s Ultimate Spider-Man', '', '', '', '', 2012, 'Animation', 'Web Series', False),
    ]
    rows.extend(
        (movie_id, 'A Very Long Movie Title That Needs To Be Safely Trimmed For Telegram Buttons',
         '', '', '', '', 2020, 'Drama', 'Movie', True)
        for movie_id in range(3, 13)
    )
    ns = load_functions(
        'create_movie_selection_keyboard',
        InlineKeyboardButton=lambda text, **kwargs: {'text': text, **kwargs},
        InlineKeyboardMarkup=lambda rows: rows,
        UPDATE_CHANNEL_URL='https://example.invalid/updates',
    )

    keyboard = ns['create_movie_selection_keyboard'](rows)
    buttons = [row[0]['text'] for row in keyboard[:2]]

    assert '2021' in buttons[0] and 'Movie' in buttons[0]
    assert '2012' in buttons[1] and 'Series' in buttons[1]
    button_texts = [row[0]['text'] for row in keyboard if row and 'text' in row[0]]
    assert all('Available' not in text and 'No files' not in text for text in button_texts)
    assert len(buttons[0]) <= 60
    assert len(keyboard[2][0]['text']) <= 60
    assert '2020' in keyboard[2][0]['text'] and 'Movie' in keyboard[2][0]['text']
    assert any(button.get('callback_data') == 'page_1' for row in keyboard for button in row)

    second_page = ns['create_movie_selection_keyboard'](rows, page=1)
    assert any(button.get('callback_data') == 'page_0' for row in second_page for button in row)


def test_fast_search_ignores_title_punctuation():
    movie = (6815, 'Spider-Man: Brand New Day', '', '', 'tt22084616', '', 2026, '', 'Movie')
    queries = []

    class Cursor:
        def execute(self, query, params=None):
            queries.append((query, params))

        def fetchall(self):
            return [movie + (0.0,)]

        def close(self):
            pass

    ns = load_functions('_normalize_search_text', '_get_movies_fast_sql_nocache',
                        get_db_connection=lambda: SimpleNamespace(cursor=Cursor),
                        close_db_connection=lambda _: None)
    assert ns['_get_movies_fast_sql_nocache']('Spider Man Brand New Day') == [movie]
    assert queries[0][1][0] == '%spidermanbrandnewday%'


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


def test_search_delivers_file_links_without_waiting_for_any_poster_io():
    async def scenario():
        poster = AsyncMock(side_effect=AssertionError('Search must not fetch posters'))

        async def run_async(func, *args):
            return func(*args)

        message = SimpleNamespace(reply_text=AsyncMock(), reply_photo=AsyncMock())
        update = SimpleNamespace(message=message, effective_user=SimpleNamespace(id=1),
                                 effective_chat=SimpleNamespace(id=2))
        context = SimpleNamespace(user_data={}, bot=SimpleNamespace(username='testbot'))
        ns = load_functions(
            'process_movie_exact_match', SEARCH_TIMING_ENABLED=False,
            run_interactive=run_async,
            get_movie_delivery_data=lambda _: ([('1080p', '', '', '1 GB')], ('Movie', 'https://slow.example/poster')),
            _format_requested_files_header=lambda *args: 'Files',
            _format_file_link_label=lambda *args: '1080p', html_escape=lambda value: value,
            create_quality_selection_keyboard=lambda **kwargs: None,
            make_landscape_poster=poster, USER_TEXT_DELETE_SECONDS=300,
            track_message_for_deletion=lambda *args: None,
        )
        await asyncio.wait_for(ns['process_movie_exact_match'](update, context, 7, 'Reacher'), 0.5)
        poster.assert_not_awaited()
        message.reply_photo.assert_not_awaited()
        message.reply_text.assert_awaited_once()
        assert '?start=file_7_0' in message.reply_text.call_args.args[0]

    asyncio.run(scenario())


def test_interactive_search_runs_even_when_background_executor_is_saturated():
    async def scenario():
        release = threading.Event()
        loop = asyncio.get_running_loop()
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=1))
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as interactive:
            ns = load_functions('run_async', 'run_interactive', _interactive_executor=interactive)
            background = asyncio.create_task(ns['run_async'](release.wait, 2))
            await asyncio.sleep(0.01)
            try:
                assert await asyncio.wait_for(ns['run_interactive'](lambda: 'Reacher'), 0.2) == 'Reacher'
                assert not background.done()
            finally:
                release.set()
                await background

    asyncio.run(scenario())


def test_upcoming_worker_does_not_block_event_loop_and_saves_only_sent_events():
    async def scenario():
        release = threading.Event()
        entered = threading.Event()
        thread_ids, saved = [], []

        def load():
            thread_ids.append(threading.get_ident())
            entered.set()
            assert release.wait(2)
            return [(7, 5, '123', 'Reacher', datetime.now(timezone.utc).date(), False, True, True)]

        def mark(*args):
            thread_ids.append(threading.get_ident())
            saved.append(args)

        async def send(coro):
            return await coro

        async def stop(_):
            raise asyncio.CancelledError

        bot = SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(username='testbot')),
                              send_message=AsyncMock())
        ns = load_functions('run_async', 'upcoming_reminder_worker',
                            _get_pending_upcoming_notifications=load,
                            _claim_pending_watchlist_availability_notifications=lambda: [],
                            _mark_upcoming_notification=mark, datetime=datetime,
                            html_escape=lambda value: value, safe_send=send)
        ns['asyncio'] = SimpleNamespace(get_running_loop=asyncio.get_running_loop,
                                       CancelledError=asyncio.CancelledError, sleep=stop)
        task = asyncio.create_task(ns['upcoming_reminder_worker'](SimpleNamespace(bot=bot)))
        try:
            for _ in range(100):
                if entered.is_set():
                    break
                await asyncio.sleep(0.001)
            assert entered.is_set()
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert saved == [(7, 'availability')]
        bot.send_message.assert_awaited_once()
        assert 'available for download' in bot.send_message.call_args.kwargs['text']
        assert all(item != threading.get_ident() for item in thread_ids)

    asyncio.run(scenario())


def test_notification_read_releases_connection_even_when_no_rows():
    released = []

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args): pass
        def fetchall(self): return []

    conn = SimpleNamespace(cursor=Cursor)
    ns = load_functions('_get_pending_upcoming_notifications',
                        get_db_connection=lambda: conn, close_db_connection=released.append)
    assert ns['_get_pending_upcoming_notifications']() == []
    assert released == [conn]


def test_title_notification_releases_connection_before_telegram_and_tracks_success():
    async def scenario():
        active, queries, threads, tracked = [], [], [], []

        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def execute(self, sql, params=None):
                queries.append((sql, params))
                threads.append(threading.get_ident())
            def fetchall(self): return [(42, 'tester', 'Test')]

        conn = SimpleNamespace(cursor=Cursor, commit=lambda: None)

        def acquire():
            active.append(conn)
            return conn

        async def send(**kwargs):
            assert active == [], 'Telegram I/O must not hold the shared database pool'
            return SimpleNamespace(message_id=1)

        async def safe(coro): return await coro
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=send), copy_message=AsyncMock(side_effect=send))
        ns = load_functions('run_async', '_load_requested_title_users', '_mark_title_requests_notified',
                            'notify_users_for_movie', get_db_connection=acquire,
                            close_db_connection=active.remove, safe_send=safe,
                            FILMFYBOX_CHANNEL_URL='https://t.me/test',
                            InlineKeyboardButton=lambda *a, **k: None, InlineKeyboardMarkup=lambda *a: None,
                            get_primary_dump_channel_id=lambda: -1001,
                            track_user_message_for_deletion=lambda *a, **k: tracked.append(k),
                            telegram=SimpleNamespace(error=SimpleNamespace(Forbidden=type('Forbidden',(Exception,),{}))))
        count = await ns['notify_users_for_movie'](SimpleNamespace(bot=bot), 'Reacher', 'https://example.com/file')
        assert count == 1
        assert active == []
        assert len(tracked) == 2
        assert tracked[0]['is_file'] is False
        assert any('UPDATE user_requests' in query for query, _ in queries)
        assert all(identifier != threading.get_ident() for identifier in threads)

    asyncio.run(scenario())


def test_membership_network_failure_is_not_cached_as_a_denial():
    class Forbidden(Exception): pass
    class BadRequest(Exception): pass

    async def scenario():
        verified = {}

        async def lookup(chat_id, user_id):
            if chat_id == 10:
                raise TimeoutError('Telegram unreachable')
            return SimpleNamespace(status='member')

        ns = load_functions('is_user_member', FORCE_JOIN_ENABLED=True,
                            ACTIVE_FSUB={'id': 10}, BACKUP_FSUB_LIST=[],
                            REQUIRED_GROUP_ID=20, VERIFICATION_CACHE_TIME=3600,
                            verified_users=verified, datetime=datetime,
                            telegram=SimpleNamespace(error=SimpleNamespace(Forbidden=Forbidden, BadRequest=BadRequest)))
        bot = SimpleNamespace(get_chat_member=AsyncMock(side_effect=lookup))
        result = await ns['is_user_member'](SimpleNamespace(bot=bot), 42)
        assert result['is_member'] is False
        assert result['error'] == 'membership_check_unavailable'
        assert verified == {}

        bot.get_chat_member.side_effect = None
        bot.get_chat_member.return_value = SimpleNamespace(status='member')
        assert (await ns['is_user_member'](SimpleNamespace(bot=bot), 42))['is_member']
        bot.get_chat_member.reset_mock()
        assert (await ns['is_user_member'](SimpleNamespace(bot=bot), 42))['is_member']
        bot.get_chat_member.assert_not_awaited()

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
