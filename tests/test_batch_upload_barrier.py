import ast
import asyncio
import logging
import re
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main.py"
MAIN_TREE = ast.parse(MAIN_PATH.read_text(encoding="utf-8"))


def _coordinator():
    node = next(
        item for item in MAIN_TREE.body
        if isinstance(item, ast.ClassDef) and item.name == "BatchUploadCoordinator"
    )
    namespace = {
        "asyncio": asyncio,
        "re": re,
        "BATCH_SESSION": {
            "active": True,
            "admin_id": 42,
            "movie_id": 7001,
        },
        "logger": logging.getLogger("test_batch_upload_barrier"),
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(MAIN_PATH), "exec"), namespace)
    coordinator = namespace["BatchUploadCoordinator"]()
    coordinator.begin(42, 7001)
    return coordinator, namespace["BATCH_SESSION"]


def _processor_type(coordinator, concurrency):
    nodes = [
        item for item in MAIN_TREE.body
        if isinstance(item, ast.ClassDef)
        and item.name in {"BatchAwareUpdateProcessor"}
    ]

    class FakeBaseUpdateProcessor:
        def __init__(self, max_concurrent_updates):
            self._semaphore = asyncio.Semaphore(max_concurrent_updates)

    namespace = {
        "asyncio": asyncio,
        "batch_upload_coordinator": coordinator,
        "BaseUpdateProcessor": FakeBaseUpdateProcessor,
    }
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), str(MAIN_PATH), "exec"),
        namespace,
    )
    return namespace["BatchAwareUpdateProcessor"](concurrency)


def _update(update_id, *, text=None, document=False, user_id=42):
    message = SimpleNamespace(
        text=text,
        document=object() if document else None,
        video=None,
        chat=SimpleNamespace(type="private"),
    )
    return SimpleNamespace(
        update_id=update_id,
        effective_user=SimpleNamespace(id=user_id),
        effective_message=message,
    )


def test_three_prior_uploads_are_drained_before_done_finalizes():
    async def scenario():
        coordinator, _batch = _coordinator()
        completed = []
        tickets = [
            coordinator.admit_update(_update(update_id, document=True))
            for update_id in (101, 102, 103)
        ]
        coordinator.admit_update(_update(104, text="/done"))

        async def save(index, delay):
            await asyncio.sleep(delay)
            completed.append(index)
            coordinator.complete_upload(tickets[index])

        finalizer = asyncio.create_task(coordinator.wait_for_prior_uploads(42))
        await asyncio.sleep(0)
        assert not finalizer.done()

        await asyncio.gather(save(0, 0.03), save(1, 0.01), save(2, 0.02))
        await finalizer
        assert sorted(completed) == [0, 1, 2]
        assert len(completed) == 3

    asyncio.run(scenario())


def test_done_during_active_upload_waits_for_that_upload():
    async def scenario():
        coordinator, _batch = _coordinator()
        ticket = coordinator.admit_update(_update(200, document=True))
        coordinator.admit_update(_update(201, text="/done"))

        drain = asyncio.create_task(coordinator.wait_for_prior_uploads(42))
        await asyncio.sleep(0)
        assert not drain.done()

        coordinator.complete_upload(ticket)
        await asyncio.wait_for(drain, timeout=0.1)

    asyncio.run(scenario())


def test_done_waits_until_first_automatic_upload_finishes_movie_resolution():
    async def scenario():
        coordinator, _batch = _coordinator()
        session = coordinator.begin(
            42, None, 350, kind="movie", batch_id="auto-batch",
            starting=True,
        )
        session["ready"].clear()
        assert coordinator.admit_update(
            _update(350, document=True)
        ) is None
        coordinator.admit_update(_update(351, text="/done"))

        ready_wait = asyncio.create_task(coordinator.wait_until_ready(42))
        await asyncio.sleep(0)
        assert not ready_wait.done()

        coordinator.begin(
            42, 7001, 350, kind="movie", batch_id="auto-batch"
        )
        coordinator.mark_ready(42)
        restored = await asyncio.wait_for(ready_wait, timeout=0.1)
        assert restored["movie_id"] == 7001
        await coordinator.wait_for_prior_uploads(42)

    asyncio.run(scenario())


def test_repeated_done_claims_finalize_only_once():
    coordinator, _batch = _coordinator()
    coordinator.admit_update(_update(301, text="/done"))
    coordinator.admit_update(_update(302, text="/done"))

    assert coordinator.claim_finalization(42) is True
    assert coordinator.claim_finalization(42) is False


def test_simultaneous_upload_completions_drain_without_race():
    async def scenario():
        coordinator, _batch = _coordinator()
        tickets = [
            coordinator.admit_update(_update(update_id, document=True))
            for update_id in range(401, 421)
        ]
        coordinator.admit_update(_update(421, text="/done"))
        finalized = []

        async def finish(ticket):
            await asyncio.sleep(0)
            coordinator.complete_upload(ticket)

        async def finalize():
            await coordinator.wait_for_prior_uploads(42)
            finalized.append("done")

        completion_tasks = [
            asyncio.create_task(finish(ticket)) for ticket in tickets
        ]
        finalizer = asyncio.create_task(finalize())
        await asyncio.gather(*completion_tasks)
        await asyncio.wait_for(finalizer, timeout=0.1)

        assert finalized == ["done"]
        assert all(ticket[2].done() for ticket in tickets)

    asyncio.run(scenario())


def test_upload_received_after_done_is_not_admitted_or_allowed():
    coordinator, _batch = _coordinator()
    coordinator.admit_update(_update(501, text="/done"))
    after_done = _update(502, document=True)

    assert coordinator.admit_update(after_done) is None
    assert coordinator.is_upload_allowed(after_done) is False


def test_queued_pre_done_uploads_are_included_even_before_handlers_start():
    async def scenario():
        coordinator, _batch = _coordinator()
        processor = _processor_type(coordinator, concurrency=5)
        first_five_started = asyncio.Event()
        release_first_five = asyncio.Event()
        later_five_started = asyncio.Event()
        release_later_five = asyncio.Event()
        done_handler_started = asyncio.Event()
        completed = []
        started = set()
        finalized = []

        async def upload_handler(index):
            started.add(index)
            if len(started.intersection(range(5))) == 5:
                first_five_started.set()
            if index < 5:
                await release_first_five.wait()
            else:
                if len(started.intersection(range(5, 10))) == 5:
                    later_five_started.set()
                await release_later_five.wait()
            completed.append(index)

        async def upload_update(update_id, index):
            await processor.process_update(
                _update(update_id, document=True),
                upload_handler(index),
            )

        upload_tasks = [
            asyncio.create_task(upload_update(600 + index, index))
            for index in range(10)
        ]

        async def done_handler():
            done_handler_started.set()
            assert coordinator.claim_finalization(42)
            await coordinator.wait_for_prior_uploads(42)
            finalized.append(True)

        done_task = asyncio.create_task(
            processor.process_update(_update(610, text="/done"), done_handler())
        )

        await asyncio.wait_for(first_five_started.wait(), timeout=0.1)
        await asyncio.wait_for(done_handler_started.wait(), timeout=0.1)

        # All ten update IDs have been registered, while the five later handlers
        # are still queued behind the per-processor concurrency semaphore.
        session = coordinator._sessions[42]
        assert set(session["pending"]) == set(range(600, 610))
        assert started == {0, 1, 2, 3, 4}
        assert finalized == []

        release_first_five.set()
        await asyncio.wait_for(later_five_started.wait(), timeout=0.1)
        await asyncio.sleep(0)
        assert sorted(completed) == [0, 1, 2, 3, 4]
        assert finalized == []

        release_later_five.set()
        await asyncio.gather(*upload_tasks)
        await asyncio.wait_for(done_task, timeout=0.1)

        assert sorted(completed) == list(range(10))
        assert finalized == [True]

        after_done_ticket = coordinator.admit_update(
            _update(611, document=True)
        )
        assert after_done_ticket is None
        assert coordinator.is_upload_allowed(_update(611, document=True)) is False

    asyncio.run(scenario())


def test_search_is_admitted_while_all_upload_slots_are_busy():
    async def scenario():
        coordinator, _batch = _coordinator()
        processor = _processor_type(coordinator, concurrency=1)
        upload_started, release_upload = asyncio.Event(), asyncio.Event()
        replied = []

        async def upload():
            upload_started.set()
            await release_upload.wait()

        async def search():
            replied.append('Reacher')

        task = asyncio.create_task(processor.process_update(_update(700, document=True), upload()))
        await upload_started.wait()
        try:
            await asyncio.wait_for(processor.process_update(_update(701, text='Reacher', user_id=99), search()), 0.1)
            assert replied == ['Reacher']
            assert not task.done()
        finally:
            release_upload.set()
            await task

    asyncio.run(scenario())


def test_second_superbatch_is_collected_while_first_finalizes_then_runs_in_order():
    async def scenario():
        class FakeBot:
            id = 77

        context = SimpleNamespace(bot=FakeBot())
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        started_batches = []
        created_batches = []
        finished_batches = []
        accepted_messages = []

        async def run_async(function, *args, **kwargs):
            return function(*args, **kwargs)

        def batch_create(batch_id, *_args):
            created_batches.append(batch_id)

        def batch_finish(batch_id, *_args):
            finished_batches.append(batch_id)

        async def safe_reply(_message, text, **_kwargs):
            accepted_messages.append(text)
            return SimpleNamespace(text=text)

        async def finalize(_update, _context, *, batch_id, files, status_msg):
            assert files == []
            assert status_msg is not None
            started_batches.append(batch_id)
            if batch_id == "batch-1":
                first_started.set()
                await release_first.wait()

        coordinator_node = next(
            item for item in MAIN_TREE.body
            if isinstance(item, ast.ClassDef) and item.name == "BatchUploadCoordinator"
        )
        handler_nodes = [
            item for item in MAIN_TREE.body
            if isinstance(item, ast.AsyncFunctionDef)
            and item.name in {"superbatch_start", "superbatch_done"}
        ]
        namespace = {
            "asyncio": asyncio,
            "re": re,
            "ADMIN_IDS": {42},
            "BATCH_SESSION": {"active": False},
            "SUPER_BATCH_SESSION": {
                "active": False, "admin_id": None, "files": [],
                "batch_id": None, "status": "idle", "closing_update_id": None,
            },
            "BATCH_PROCESS_ID": "test-worker",
            "_RECOVERED_BATCH_IDS": set(),
            "auto_batch_lock": asyncio.Lock(),
            "_SUPERBATCH_FINALIZATION_LOCK": asyncio.Lock(),
            "uuid": SimpleNamespace(uuid4=lambda: next(batch_ids)),
            "Update": object,
            "ContextTypes": SimpleNamespace(DEFAULT_TYPE=object),
            "run_async": run_async,
            "_batch_job_create": batch_create,
            "_batch_job_update": lambda *_args, **_kwargs: None,
            "_batch_job_finish": batch_finish,
            "_safe_batch_reply": safe_reply,
            "_safe_batch_status_edit": safe_reply,
            "_superbatch_done_impl": finalize,
            "logger": logging.getLogger("test_concurrent_superbatch"),
        }
        batch_ids = iter(("batch-1", "batch-2"))
        nodes = [coordinator_node, *handler_nodes]
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(MAIN_PATH), "exec"), namespace)
        coordinator = namespace["BatchUploadCoordinator"]()
        namespace["batch_upload_coordinator"] = coordinator

        def command_update(update_id, text):
            message = SimpleNamespace(
                text=text,
                chat=SimpleNamespace(type="private"),
            )
            async def reply_text(reply, **_kwargs):
                accepted_messages.append(reply)
                return SimpleNamespace(text=reply)
            message.reply_text = reply_text
            return SimpleNamespace(
                update_id=update_id,
                effective_user=SimpleNamespace(id=42),
                effective_message=message,
                message=message,
            )

        start_first = command_update(100, "/superbatch")
        assert coordinator.admit_update(start_first)[0] is not None
        await namespace["superbatch_start"](start_first, context)

        done_first = command_update(110, "/superdone")
        coordinator.admit_update(done_first)
        first_task = asyncio.create_task(namespace["superbatch_done"](done_first, context))
        await asyncio.wait_for(first_started.wait(), timeout=0.2)

        # A new command during finalization reserves a separate collecting session.
        start_second = command_update(120, "/superbatch")
        assert coordinator.admit_update(start_second)[0] is not None
        await namespace["superbatch_start"](start_second, context)
        assert namespace["SUPER_BATCH_SESSION"]["batch_id"] == "batch-2"
        assert coordinator.session_for_update(start_second)["batch_id"] == "batch-2"

        done_second = command_update(130, "/superdone")
        coordinator.admit_update(done_second)
        second_task = asyncio.create_task(namespace["superbatch_done"](done_second, context))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert started_batches == ["batch-1"]
        assert not second_task.done()

        release_first.set()
        await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=1)

        assert created_batches == ["batch-1", "batch-2"]
        assert started_batches == ["batch-1", "batch-2"]
        assert sorted(finished_batches) == ["batch-1", "batch-2"]
        assert coordinator._sessions == {}
        assert any("accepted with 0 files" in message for message in accepted_messages)

    asyncio.run(scenario())
