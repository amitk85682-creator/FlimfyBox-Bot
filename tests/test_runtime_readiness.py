from pathlib import Path


MAIN_SOURCE = (Path(__file__).resolve().parents[1] / 'main.py').read_text(
    encoding='utf-8'
)


def test_healthz_is_liveness_only():
    assert "def _miniapp_healthz()" in MAIN_SOURCE
    health_body = MAIN_SOURCE.split("def _miniapp_healthz()", 1)[1].split(
        "def _miniapp_readyz()", 1
    )[0]
    assert 'get_db_connection' not in health_body
    assert 'requests.' not in health_body


def test_readyz_has_startup_and_shutdown_gates():
    assert "'/readyz'" in MAIN_SOURCE
    assert "_shutdown_requested.is_set()" in MAIN_SOURCE
    assert "_startup_complete.is_set()" in MAIN_SOURCE
    assert "_startup_complete.clear()" in MAIN_SOURCE


def test_migration_failure_prevents_readiness_and_bot_start():
    startup = MAIN_SOURCE.split('async def main():', 1)[1]
    assert startup.index('run_migrations(DATABASE_URL)') < startup.index(
        'Application.builder()'
    )
    failure_block = startup.split('run_migrations(DATABASE_URL)', 1)[1].split(
        '# 3. Get Tokens from ENV', 1
    )[0]
    assert 'startup aborted' in failure_block
    assert 'return' in failure_block


def test_shutdown_tracks_workers_and_closes_pool():
    assert 'worker_tasks.append(asyncio.create_task' in MAIN_SOURCE
    assert 'task.cancel()' in MAIN_SOURCE
    assert 'await asyncio.wait_for(' in MAIN_SOURCE
    assert 'close_db_pool()' in MAIN_SOURCE
