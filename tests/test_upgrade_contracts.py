"""Standard-library regression contracts for critical bot/app boundaries."""
import ast
import asyncio
import datetime
import json
import logging
import os
import re
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]


def function_node(source, name):
    tree = ast.parse(source)
    return next(node for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == name)


class Cache:
    def __init__(self):
        self.items = {}

    def get(self, key):
        return self.items.get(key)

    def set(self, key, value):
        self.items[key] = value


class RequestArgs:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None, type=None):
        value = self.values.get(key, default)
        if type is None:
            return value
        try:
            return type(value)
        except (TypeError, ValueError):
            return default


class FakeFlaskApp:
    def __init__(self):
        self.routes = {}

    def route(self, path, **_kwargs):
        def register(handler):
            self.routes[path] = handler
            return handler
        return register


class FakeProviderResponse:
    status_code = 200

    def __init__(self, items):
        self.items = items

    def raise_for_status(self):
        return None

    def json(self):
        return {'results': self.items}


def build_upcoming_route(provider):
    source = (ROOT / 'webapp_routes.py').read_text(encoding='utf-8')
    node = function_node(source, 'register_webapp_routes')
    app = FakeFlaskApp()
    api_cache = Cache()
    namespace = {
        'datetime': datetime.datetime,
        'timezone': datetime.timezone,
        'timedelta': datetime.timedelta,
        'json': json,
        'logging': logging,
        'os': os,
        're': re,
        'threading': threading,
        'time': time,
        'ThreadPoolExecutor': ThreadPoolExecutor,
        'as_completed': as_completed,
        'quote': quote,
        'requests': provider,
        'CANONICAL_GENRES': [],
        'GENRE_BY_ID': {},
        'request': SimpleNamespace(args=RequestArgs()),
        'jsonify': lambda value: value,
        'require_telegram_user': lambda: (None, None),
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'webapp_routes.py', 'exec'), namespace)
    namespace['register_webapp_routes'](
        app, api_movies_cache=api_cache, search_cache=Cache(),
        get_db_connection=lambda: None, close_db_connection=lambda _conn: None,
        store_user_request=lambda *_args: None, TMDB_API_KEY='fixture-key',
        logger=logging.getLogger('upgrade-contracts'),
    )
    return app.routes['/api/upcoming'], namespace, api_cache


class UpgradeContracts(unittest.TestCase):
    def test_superbatch_save_has_no_aggregate_wall_clock_deadline(self):
        source = (ROOT / 'main.py').read_text(encoding='utf-8')
        node = function_node(source, '_save_superbatch_files')
        timed_file_save = []
        for call in ast.walk(node):
            if not isinstance(call, ast.Call) or not call.args:
                continue
            callee = ast.unparse(call.func)
            target = call.args[0]
            if (callee == 'asyncio.wait_for'
                    and isinstance(target, ast.Call)
                    and ast.unparse(target.func) == '_save_one_superbatch_file'):
                timed_file_save.append(call)
        self.assertEqual(timed_file_save, [], 'file save must not have a whole-operation timeout')

    def test_hot_search_and_batch_handlers_do_not_run_sql_inline(self):
        source = (ROOT / 'main.py').read_text(encoding='utf-8')
        for name in ('main_menu_or_search', 'pm_file_listener'):
            node = function_node(source, name)
            direct_db = []
            stack = list(node.body)
            while stack:
                item = stack.pop()
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                    continue
                if isinstance(item, ast.Call):
                    callee = ast.unparse(item.func)
                    if callee == 'get_db_connection' or callee.endswith('.execute'):
                        direct_db.append((item.lineno, callee))
                stack.extend(ast.iter_child_nodes(item))
            self.assertEqual(direct_db, [], f'{name} must await executor-backed database helpers')

    def test_upcoming_keeps_valid_source_when_the_other_provider_fails(self):
        class Provider:
            RequestException = type('RequestException', (Exception,), {})

            def __init__(self):
                self.calls = []

            def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                if url.endswith('/movie'):
                    raise self.RequestException('movie discovery unavailable')
                return FakeProviderResponse([{
                    'id': 9, 'name': 'Future Series',
                    'first_air_date': (datetime.datetime.now(datetime.timezone(
                        datetime.timedelta(hours=5, minutes=30))).date()
                        + datetime.timedelta(days=2)).isoformat(),
                }])

        provider = Provider()
        route, namespace, _api_cache = build_upcoming_route(provider)
        response = route()
        self.assertEqual(response['status'], 'success')
        self.assertIn('partial', response)
        self.assertTrue(response['partial'])
        self.assertEqual([item['title'] for item in response['movies']], ['Future Series'])
        self.assertEqual(len(provider.calls), 2, 'one bounded attempt per media source')
        self.assertTrue(all(call[1]['timeout'] == (1, 3) for call in provider.calls))

    def test_upcoming_reports_retryable_error_when_both_sources_fail(self):
        class Provider:
            RequestException = type('RequestException', (Exception,), {})

            def __init__(self):
                self.calls = []

            def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                raise self.RequestException('provider unavailable')

        provider = Provider()
        route, _namespace, api_cache = build_upcoming_route(provider)
        result = route()
        response, status = result if isinstance(result, tuple) else (result, 200)
        self.assertEqual(status, 503)
        self.assertEqual(response['status'], 'error')
        self.assertEqual(len(provider.calls), 2)
        self.assertEqual(api_cache.items, {}, 'provider failure must not be cached as a valid empty list')


if __name__ == '__main__':
    unittest.main()
