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
