import ast
import pathlib
import threading
import time
import unittest

from services.bot_reliability import (
    ThreadSafeFastCache,
    TieredConnectionGate,
    cached_search_lookup,
    format_file_extra_display,
    is_valid_temp_link_token,
)


MAIN_PY = pathlib.Path(__file__).resolve().parent.parent / "main.py"


class TestTempLinkTokenValidation(unittest.TestCase):
    def test_accepts_python_12_hex_and_node_16_hex_tokens(self):
        self.assertTrue(is_valid_temp_link_token("tmp_0123456789ab"))
        self.assertTrue(is_valid_temp_link_token("tmp_0123456789abcdef"))
        self.assertTrue(is_valid_temp_link_token("tmp_" + "a" * 32))

    def test_rejects_malformed_tokens(self):
        for bad in (
            None,
            "",
            "tmp_",
            "tmp_1234567890a",          # 11 hex (too short)
            "tmp_" + "a" * 33,          # 33 hex (too long)
            "tmp_0123456789AB",         # uppercase hex
            "tmp_0123456789ag",         # non-hex char
            "file_123_0",
            "tmp_0123456789ab\n",
        ):
            self.assertFalse(is_valid_temp_link_token(bad), f"Expected rejection for {bad!r}")


class TestThreadSafeFastCache(unittest.TestCase):
    def test_ttl_expiry(self):
        cache = ThreadSafeFastCache(ttl_seconds=1.0, max_entries=32)
        cache.set("k1", ["movie"])
        self.assertEqual(cache.get("k1"), ["movie"])
        # Backdate timestamp to test expiry without sleeping a full second
        cache.cache["k1"] = (["movie"], time.time() - 2.0)
        self.assertIsNone(cache.get("k1"))

    def test_bounded_lru_eviction(self):
        cache = ThreadSafeFastCache(ttl_seconds=60.0, max_entries=16)
        for i in range(20):
            cache.set(f"k{i}", i)
        self.assertLessEqual(len(cache.cache), 16)
        # Oldest 4 entries (k0..k3) should have been evicted
        for i in range(4):
            self.assertIsNone(cache.get(f"k{i}"))
        self.assertEqual(cache.get("k19"), 19)

    def test_concurrent_reads_and_writes(self):
        cache = ThreadSafeFastCache(ttl_seconds=60.0, max_entries=64)
        errors = []

        def worker(tid: int):
            try:
                for i in range(100):
                    key = f"t{tid % 4}_k{i % 30}"
                    cache.set(key, (tid, i))
                    _ = cache.get(key)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])


class TestTieredConnectionGate(unittest.TestCase):
    def test_background_cap_cannot_starve_bot_or_web(self):
        gate = TieredConnectionGate(max_connections=16, web_reserved=3, background_max=2)
        self.assertEqual(gate.bot_max, 13)
        self.assertEqual(gate.background_max, 2)

        # Acquire max background connections (2)
        self.assertTrue(gate.acquire("background", timeout=0.05))
        gate.register_lease(1, "background")
        self.assertTrue(gate.acquire("background", timeout=0.05))
        gate.register_lease(2, "background")

        # 3rd background worker must be rejected quickly, leaving 11 bot + 3 web slots free
        self.assertFalse(gate.acquire("background", timeout=0.02))

        # Interactive bot connections must still succeed (11 remaining in bot_max=13)
        bot_ids = []
        for idx in range(11):
            self.assertTrue(gate.acquire("bot", timeout=0.05), f"bot slot {idx} failed")
            cid = 100 + idx
            gate.register_lease(cid, "bot")
            bot_ids.append(cid)

        # 12th bot slot is rejected because 3 web_reserved slots stay protected
        self.assertFalse(gate.acquire("bot", timeout=0.02))

        # All 3 web slots must still succeed immediately
        web_ids = []
        for idx in range(3):
            self.assertTrue(gate.acquire("web", timeout=0.05), f"web slot {idx} failed")
            cid = 200 + idx
            gate.register_lease(cid, "web")
            web_ids.append(cid)

        self.assertEqual(gate.active_count(), 16)

        # Clean release (and verify releasing unknown/duplicate id is a safe no-op)
        for cid in [1, 2, *bot_ids, *web_ids]:
            role = gate.release_lease(cid)
            gate.release_role(role)
        self.assertIsNone(gate.release_lease(99999))
        gate.release_role(None)
        self.assertEqual(gate.active_count(), 0)


class TestCachedSearchLookup(unittest.TestCase):
    def test_does_not_cache_db_failure(self):
        cache = ThreadSafeFastCache(ttl_seconds=30.0)
        calls = {"n": 0}

        def failing_then_succeeding():
            calls["n"] += 1
            if calls["n"] == 1:
                return [], False  # transient DB timeout
            return [(42, "Inception", None, "file_42")], True

        rows1, ok1 = cached_search_lookup(cache, "sql_inception_100", failing_then_succeeding)
        self.assertEqual(rows1, [])
        self.assertFalse(ok1)
        self.assertIsNone(cache.get("sql_inception_100"))

        rows2, ok2 = cached_search_lookup(cache, "sql_inception_100", failing_then_succeeding)
        self.assertTrue(ok2)
        self.assertEqual(len(rows2), 1)
        self.assertEqual(rows2[0][1], "Inception")

        # Third call hits cache without invoking fetch_fn again
        rows3, ok3 = cached_search_lookup(cache, "sql_inception_100", failing_then_succeeding)
        self.assertTrue(ok3)
        self.assertEqual(rows3, rows2)
        self.assertEqual(calls["n"], 2)

    def test_caches_legitimate_empty_result_when_db_ok(self):
        cache = ThreadSafeFastCache(ttl_seconds=30.0)
        calls = {"n": 0}

        def empty_ok():
            calls["n"] += 1
            return [], True

        rows1, ok1 = cached_search_lookup(cache, "sql_nonexistent", empty_ok)
        self.assertEqual(rows1, [])
        self.assertTrue(ok1)
        rows2, ok2 = cached_search_lookup(cache, "sql_nonexistent", empty_ok)
        self.assertEqual(rows2, [])
        self.assertTrue(ok2)
        self.assertEqual(calls["n"], 1)


class TestFormatFileExtraDisplay(unittest.TestCase):
    def test_edition_and_season_episode_year_refinement(self):
        extra, yr = format_file_extra_display("Director's Uncut", None, "📅 <b>Year:</b> 2020\n")
        self.assertIn("Edition:", extra)
        self.assertEqual(yr, "📅 <b>Year:</b> 2020\n")

        seasons = {
            "2": {
                "year": "2023",
                "episodes": {
                    "5": {"air_date": "2024-01-12"},
                },
            }
        }
        extra2, yr2 = format_file_extra_display("S02E05", seasons, "📅 <b>Year:</b> 2021\n")
        self.assertIn("Season & Episode:", extra2)
        self.assertEqual(yr2, "📅 <b>Year:</b> 2024\n")

        extra3, yr3 = format_file_extra_display("", seasons, "📅 <b>Year:</b> 2021\n")
        self.assertEqual(extra3, "")
        self.assertEqual(yr3, "📅 <b>Year:</b> 2021\n")


class TestMainPyHotHandlersNonBlocking(unittest.TestCase):
    """AST regression guards so hot async handlers never re-introduce blocking DB calls."""

    @classmethod
    def setUpClass(cls):
        cls.source = MAIN_PY.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)
        cls.async_funcs = {
            node.name: node
            for node in cls.tree.body
            if isinstance(node, ast.AsyncFunctionDef)
        }

    def _direct_calls_in_async_body(self, func_node: ast.AsyncFunctionDef, target_name: str):
        """Find direct calls to `target_name(...)` in `func_node` excluding nested sync defs."""
        hits = []

        class Visitor(ast.NodeVisitor):
            def visit_FunctionDef(self, node):
                # Nested synchronous helper passed to run_interactive/run_async is allowed
                return

            def visit_Call(self, node):
                if isinstance(node.func, ast.Name) and node.func.id == target_name:
                    hits.append(node.lineno)
                self.generic_visit(node)

        for stmt in func_node.body:
            Visitor().visit(stmt)
        return hits

    def test_no_direct_get_db_connection_in_hot_async_handlers(self):
        hot_handlers = [
            "start",
            "send_movie_to_user",
            "deliver_movie_on_start",
            "deliver_movie_page_on_start",
            "main_menu",
            "search_movies",
            "handle_group_message",
            "button_callback",
            "handle_webapp_request_callback",
        ]
        for name in hot_handlers:
            self.assertIn(name, self.async_funcs, f"Missing expected async handler {name}")
            direct_calls = self._direct_calls_in_async_body(
                self.async_funcs[name], "get_db_connection"
            )
            self.assertEqual(
                direct_calls,
                [],
                f"{name}() still calls synchronous get_db_connection() on lines {direct_calls}",
            )

    def test_global_chat_relay_worker_does_not_use_run_interactive(self):
        node = self.async_funcs["global_chat_relay_worker"]
        hits = self._direct_calls_in_async_body(node, "run_interactive")
        self.assertEqual(
            hits,
            [],
            f"global_chat_relay_worker() should use run_async, not run_interactive (lines {hits})",
        )


if __name__ == "__main__":
    unittest.main()
