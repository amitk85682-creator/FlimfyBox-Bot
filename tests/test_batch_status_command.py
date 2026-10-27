import ast
import asyncio
import __future__
import logging
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "main.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def _load_status_command(*, latest_id, snapshot):
    nodes = [
        node for node in TREE.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "batch_status_command"
    ]
    replies = []

    async def run_async(function, *args, **kwargs):
        if function is latest_lookup:
            return latest_id
        if function is snapshot_lookup:
            return snapshot
        return []

    def latest_lookup(_admin_id):
        raise AssertionError("run_async should intercept this call")

    def snapshot_lookup(_batch_id, _admin_id):
        raise AssertionError("run_async should intercept this call")

    async def reply(_message, text, **_kwargs):
        replies.append(text)

    namespace = {
        "is_admin": lambda _admin_id: True,
        "run_async": run_async,
        "_batch_latest_job_id": latest_lookup,
        "_batch_job_snapshot": snapshot_lookup,
        "_safe_batch_reply": reply,
        "get_db_connection": lambda: None,
        "close_db_connection": lambda _conn: None,
        "logger": logging.getLogger("test_batch_status"),
    }
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            "main.py",
            "exec",
            flags=__future__.annotations.compiler_flag,
        ),
        namespace,
    )
    return namespace["batch_status_command"], replies


def test_batchstatus_without_id_uses_latest_batch_and_shows_retry_command():
    snapshot = (
        "latest-batch", "super", "completed_with_errors", None, None,
        11, 5, 0, 6, 0, 0, 0,
    )
    command, replies = _load_status_command(
        latest_id="latest-batch", snapshot=snapshot
    )
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=42),
        effective_message=object(),
    )
    asyncio.run(command(update, SimpleNamespace(args=[])))

    assert "Batch: latest-batch" in replies[0]
    assert "Saved: 5" in replies[0]
    assert "Failed: 6" in replies[0]
    assert "/batchretry latest-batch" in replies[0]


def test_batchstatus_without_history_returns_clear_message():
    command, replies = _load_status_command(latest_id=None, snapshot=None)
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=42),
        effective_message=object(),
    )
    asyncio.run(command(update, SimpleNamespace(args=[])))

    assert replies == ["Abhi tak koi batch history nahi mili."]
