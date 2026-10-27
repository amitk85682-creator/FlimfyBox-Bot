import ast
import asyncio
import datetime
import json
import logging
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
MAIN_TREE = ast.parse((ROOT / "main.py").read_text(encoding="utf-8"))


class _AsyncioFacade:
    Lock = asyncio.Lock

    @staticmethod
    async def sleep(_seconds):
        return None

    @staticmethod
    async def wait_for(awaitable, timeout):
        return await asyncio.wait_for(awaitable, timeout=timeout)


class _Connection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def cursor(self):
        return self

    def execute(self, query, params=None):
        self.query = query

    def fetchone(self):
        return ('Movie', 2026, 'Movie')


class _RetryAfter(Exception):
    def __init__(self, retry_after):
        self.retry_after = retry_after


def _saver_harness(*, channels=(10,), existing=None, upsert=None):
    nodes = [
        item for item in MAIN_TREE.body
        if isinstance(item, (ast.AsyncFunctionDef, ast.FunctionDef))
        and item.name in {"_pm_save_file", "_pm_save_file_locked", "_precheck_batch_file", "_commit_batch_file"}
    ]
    item = [
        1, "batch", 1, 100, 200, "document", "telegram-file-id",
        "telegram-unique-id", "Movie.1080p.mkv", 1000, "", {},
        "processing", "accepted", 1, 55, None, {}, None, None,
    ]
    state = {"item": item, "updates": [], "copies": 0, "upserts": 0}

    async def run_async(function, *args, **kwargs):
        return function(*args, **kwargs)

    async def empty_extraction(_text):
        return {}

    def item_fetch(_item_id):
        return tuple(state["item"])

    def item_update(_item_id, **fields):
        state["updates"].append(dict(fields))
        row_fields = {
            "state": 12,
            "stage": 13,
            "movie_id": 15,
            "movie_file_id": 16,
            "storage_map": 17,
            "storage_pending_chat": 18,
            "retry_after_at": 19,
        }
        for key, index in row_fields.items():
            if key in fields:
                state["item"][index] = fields[key]

    def fake_upsert(_conn, *_args, **_kwargs):
        state["upserts"] += 1
        if upsert:
            return upsert(state, _conn, *_args, **_kwargs)
        return 901

    namespace = {
        "asyncio": _AsyncioFacade,
        "datetime": datetime.datetime,
        "timezone": datetime.timezone,
        "timedelta": datetime.timedelta,
        "json": json,
        "logger": logging.getLogger("test_batch_recovery"),
        "_FILE_SAVE_LOCKS": {},
        "_FILE_SAVE_LOCKS_GUARD": _AsyncioFacade.Lock(),
        "RetryAfter": _RetryAfter,
        "BATCH_SESSION": {
            "active": True,
            "movie_id": 55,
            "movie_title": "Movie",
            "file_count": 0,
            "language": "Hindi",
        },
        "run_async": run_async,
        "_batch_item_fetch": item_fetch,
        "_batch_item_update": item_update,
        "get_db_connection": _Connection,
        "_set_batch_db_limits": lambda _conn: None,
        "close_db_connection": lambda _conn: None,
        "_find_existing_movie_file": lambda *_args: existing,
        "is_downgrade": lambda *_args: (False, None),
        "get_storage_channels": lambda: list(channels),
        "fallback_extraction": empty_extraction,
        "strip_caption_junk": lambda text: text,
        "generate_quality_label": lambda *_args: "1080p WEB-DL",
        "_merge_quality_labels": lambda *values: next(
            (value for value in values if value), ""
        ),
        "_merge_csv_values": lambda *values: ", ".join(
            value for value in values if value
        ),
        "_merge_extra_info": lambda *values: ", ".join(
            value for value in values if value
        ),
        "get_readable_file_size": lambda _size: "1 MB",
        "upsert_movie_file": fake_upsert,
        "auto_upgrade_delete": lambda *_args: (0, []),
    }
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec"),
        namespace,
    )

    class Message:
        document = SimpleNamespace(
            file_name="Movie.1080p.mkv",
            file_size=1000,
            file_unique_id="telegram-unique-id",
            file_id="telegram-file-id",
            thumbnail=None,
            thumb=None,
        )
        video = None
        caption = ""
        text = ""

        async def copy(self, chat_id):
            state["copies"] += 1
            return SimpleNamespace(message_id=1000 + chat_id)

    return namespace["_pm_save_file"], state, Message(), namespace["BATCH_SESSION"]


def test_exact_telegram_file_reuses_existing_row_before_storage_copy():
    async def scenario():
        saver, state, message, _session = _saver_harness(
            existing=(77, 55, "1080p WEB-DL")
        )
        label = await saver(message, SimpleNamespace(), batch_item_id=1)
        assert label == "1080p WEB-DL"
        assert state["copies"] == 0
        assert state["upserts"] == 0
        assert state["item"][12] == "succeeded"
        assert state["item"][16] == 77

    asyncio.run(scenario())


def test_retry_after_storage_copy_failure_reuses_checkpointed_copy():
    async def scenario():
        def fail_first_upsert(state, conn, *_args, **_kwargs):
            if state["upserts"] == 1:
                raise RuntimeError("synthetic database failure")
            conn.commit()
            return 902

        saver, state, message, session = _saver_harness(upsert=fail_first_upsert)
        first_result = await saver(message, SimpleNamespace(), batch_item_id=1)
        assert first_result is None
        assert state["copies"] == 1
        assert state["item"][12] == "failed"
        assert state["item"][17] == {"10": 1010}

        second_result = await saver(message, SimpleNamespace(), batch_item_id=1)
        assert second_result == "1080p WEB-DL"
        assert state["copies"] == 1
        assert state["upserts"] == 2
        assert state["item"][12] == "succeeded"
        assert session["file_count"] == 1

    asyncio.run(scenario())


def test_retry_after_preserves_confirmed_copies_for_retry():
    async def scenario():
        saver, state, message, _session = _saver_harness(channels=(10, 20))
        original_copy = message.copy
        rate_limited = {"active": True}

        async def copy_with_rate_limit(chat_id):
            if chat_id == 20 and rate_limited["active"]:
                raise _RetryAfter(30)
            return await original_copy(chat_id)

        message.copy = copy_with_rate_limit
        first_result = await saver(message, SimpleNamespace(), batch_item_id=1)
        assert first_result is None
        assert state["copies"] == 1
        assert state["upserts"] == 0
        assert state["item"][12] == "failed"
        assert state["item"][17] == {"10": 1010}

        rate_limited["active"] = False
        state["item"][19] = None
        second_result = await saver(message, SimpleNamespace(), batch_item_id=1)
        assert second_result == "1080p WEB-DL"
        assert state["copies"] == 2
        assert state["upserts"] == 1
        assert state["item"][12] == "succeeded"
        assert state["item"][17] == {"10": 1010, "20": 1020}

    asyncio.run(scenario())


def test_concurrent_same_file_saves_share_one_storage_copy():
    async def scenario():
        saver, state, message, _session = _saver_harness()

        labels = await asyncio.gather(
            saver(message, SimpleNamespace(), batch_item_id=1),
            saver(message, SimpleNamespace(), batch_item_id=1),
        )

        assert labels == ["1080p WEB-DL", "1080p WEB-DL"]
        assert state["copies"] == 1
        assert state["upserts"] == 1
        assert state["item"][12] == "succeeded"

    asyncio.run(scenario())


def test_one_storage_channel_failure_does_not_block_other_channels():
    async def scenario():
        saver, state, message, _session = _saver_harness(channels=(10, 20))
        original_copy = message.copy

        async def copy_with_one_failure(chat_id):
            if chat_id == 10:
                raise PermissionError("synthetic inaccessible channel")
            return await original_copy(chat_id)

        message.copy = copy_with_one_failure
        result = await saver(message, SimpleNamespace(), batch_item_id=1)

        assert result == "1080p WEB-DL"
        assert state["copies"] == 1
        assert state["upserts"] == 1
        assert state["item"][12] == "succeeded"
        assert state["item"][17] == {"20": 1020}

    asyncio.run(scenario())


def test_interrupted_storage_copy_is_not_blindly_repeated():
    async def scenario():
        saver, state, message, _session = _saver_harness()
        state["item"][18] = 10

        result = await saver(message, SimpleNamespace(), batch_item_id=1)

        assert result is None
        assert state["copies"] == 0
        assert state["upserts"] == 0
        assert state["item"][12] == "unrecoverable"
        assert state["item"][13] == "storage_copying"

    asyncio.run(scenario())


def test_recovered_message_copies_from_original_telegram_reference():
    node = next(
        item for item in MAIN_TREE.body
        if isinstance(item, ast.ClassDef)
        and item.name == "_RecoveredTelegramMessage"
    )
    namespace = {}
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), "main.py", "exec"),
        namespace,
    )

    async def scenario():
        calls = []

        class Bot:
            async def copy_message(self, **kwargs):
                calls.append(kwargs)
                return SimpleNamespace(message_id=123)

        row = (
            1, "batch", 500, -1234, 99, "document",
            "file-id", "unique-id", "movie.mkv", 1000, "caption", {},
        )
        message = namespace["_RecoveredTelegramMessage"](Bot(), row)
        result = await message.copy(chat_id=-5000)

        assert result.message_id == 123
        assert calls == [{
            "chat_id": -5000,
            "from_chat_id": -1234,
            "message_id": 99,
        }]
        assert message.document.file_unique_id == "unique-id"

    asyncio.run(scenario())


def test_active_heartbeat_renews_only_this_process_leases():
    node = next(
        item for item in MAIN_TREE.body
        if isinstance(item, ast.FunctionDef)
        and item.name == "_batch_active_heartbeat"
    )
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            statements.append((sql, params))

    class Connection:
        committed = False

        def cursor(self):
            return Cursor()

        def commit(self):
            self.committed = True

        def rollback(self):
            raise AssertionError("heartbeat should not roll back")

    connection = Connection()
    namespace = {
        "get_db_connection": lambda: connection,
        "close_db_connection": lambda _conn: None,
        "BATCH_PROCESS_ID": "process-1",
        "RuntimeError": RuntimeError,
    }
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), "main.py", "exec"),
        namespace,
    )

    namespace["_batch_active_heartbeat"]()

    assert connection.committed is True
    assert len(statements) == 2
    assert all(params == ("process-1",) for _sql, params in statements)
    assert all("lease_owner = %s" in sql for sql, _params in statements)
    assert "status IN ('collecting', 'processing')" in statements[0][0]
    assert "state = 'processing'" in statements[1][0]


def test_superbatch_file_failure_isolated_from_later_files():
    node = next(
        item for item in MAIN_TREE.body
        if isinstance(item, ast.AsyncFunctionDef)
        and item.name == "_save_superbatch_files"
    )
    calls = []
    item_states = {
        1: [1, None, None, None, None, None, None, None, None, None, None, None,
            "processing", "storage_copying"],
        2: [2, None, None, None, None, None, None, None, None, None, None, None,
            "processing", "storage_copying"],
    }

    async def run_async(function, *args, **kwargs):
        return function(*args, **kwargs)

    async def save(message, _context, item_id):
        calls.append(item_id)
        if item_id == 1:
            raise RuntimeError("synthetic per-file failure")
        return "720p WEB-DL"

    def update(item_id, **fields):
        for key, value in fields.items():
            if key == "state":
                item_states[item_id][12] = value
            elif key == "stage":
                item_states[item_id][13] = value

    namespace = {
        "asyncio": _AsyncioFacade,
        "run_async": run_async,
        "_batch_item_claim": lambda _item_id: True,
        "_batch_item_fetch": lambda item_id: item_states[item_id],
        "_batch_item_update": update,
        "_pm_save_file": save,
        "logger": logging.getLogger("test_superbatch_item_isolation"),
        "SUPERBATCH_FILE_TIMEOUT_SECONDS": 1,
    }
    failure_node = next(item for item in MAIN_TREE.body
                        if isinstance(item, ast.AsyncFunctionDef) and item.name == "_mark_superbatch_item_failed")
    save_node = next(item for item in MAIN_TREE.body
                     if isinstance(item, ast.AsyncFunctionDef) and item.name == "_save_one_superbatch_file")
    exec(
        compile(ast.Module(body=[node, failure_node, save_node], type_ignores=[]), "main.py", "exec"),
        namespace,
    )

    async def scenario():
        labels = await namespace["_save_superbatch_files"](
            [
                {"batch_item_id": 1, "message_obj": object()},
                {"batch_item_id": 2, "message_obj": object()},
            ],
            SimpleNamespace(),
            55,
        )
        assert labels == ["720p WEB-DL"]
        assert calls == [1, 2]
        assert item_states[1][12] == "failed"

    asyncio.run(scenario())


def test_recovery_continues_to_later_items_and_restores_movie_session():
    node = next(
        item for item in MAIN_TREE.body
        if isinstance(item, ast.AsyncFunctionDef)
        and item.name == "_recover_interrupted_batches"
    )
    batch_id = "batch-1"
    job = (batch_id, 42, 7, "movie", "collecting", 100, None, 55, {})
    rows = [
        (1, batch_id, 101, 42, 1001, "document", "f1", "u1", "a.mkv",
         1000, "", {}, "pending", "accepted", 0, None, None, {}, None, None),
        (2, batch_id, 102, 42, 1002, "document", "f2", "u2", "b.mkv",
         1000, "", {}, "pending", "accepted", 0, None, None, {}, None, None),
    ]
    resumed = []
    restored_sessions = {}
    job_updates = []

    async def run_async(function, *args, **kwargs):
        return function(*args, **kwargs)

    def begin(admin_id, movie_id, start_update_id, **kwargs):
        session = {
            "batch_id": kwargs["batch_id"],
            "movie_id": movie_id,
            "start_update_id": start_update_id,
        }
        restored_sessions[admin_id] = session
        return session

    async def resume(_bot, _job, row):
        resumed.append(row[0])
        return row[0] == 2

    namespace = {
        "run_async": run_async,
        "BATCH_PROCESS_ID": "process-1",
        "_batch_active_heartbeat": lambda: None,
        "_batch_recovery_jobs_fetch": lambda: [job],
        "_RECOVERED_BATCH_IDS": set(),
        "batch_upload_coordinator": SimpleNamespace(
            _sessions=restored_sessions,
            begin=begin,
        ),
        "BATCH_SESSION": {},
        "SUPER_BATCH_SESSION": {},
        "_batch_job_claim": lambda _batch_id: True,
        "_batch_item_retry_failed": lambda *_args: 0,
        "_batch_items_fetch": lambda _batch_id, unfinished_only=False: rows,
        "_resume_batch_item": resume,
        "_movie_batch_metadata_fetch": lambda _movie_id: (
            "Movie", 2024, "Movies", "Hindi"
        ),
        "_batch_job_update": lambda *args, **kwargs: job_updates.append(
            (args, kwargs)
        ),
        "logger": logging.getLogger("test_batch_recovery_orchestration"),
    }
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), "main.py", "exec"),
        namespace,
    )

    class Bot:
        id = 7

    class Application:
        bot = Bot()

    asyncio.run(namespace["_recover_interrupted_batches"](Application()))

    assert resumed == [1, 2]
    assert namespace["BATCH_SESSION"]["active"] is True
    assert namespace["BATCH_SESSION"]["movie_id"] == 55
    assert namespace["_RECOVERED_BATCH_IDS"] == {batch_id}
    assert job_updates[-1][1]["status"] == "collecting"
    assert job_updates[-1][1]["lease_owner"] == "process-1"
