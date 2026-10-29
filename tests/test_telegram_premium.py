"""Premium Telegram onboarding and command discoverability checks."""

import ast
import html
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


MAIN_SOURCE = Path(__file__).resolve().parents[1].joinpath('main.py').read_text(encoding='utf-8')


class Button:
    def __init__(self, text, *, callback_data=None, url=None, web_app=None):
        self.text = text
        self.callback_data = callback_data
        self.url = url
        self.web_app = web_app


def load_functions(*names, **dependencies):
    nodes = [
        node for node in ast.parse(MAIN_SOURCE).body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
    ]
    namespace = dict(
        os=os, re=re, html_escape=html.escape, datetime=datetime,
        timedelta=timedelta, timezone=timezone, InlineKeyboardButton=Button,
        InlineKeyboardMarkup=lambda rows: rows,
        WebAppInfo=lambda url: SimpleNamespace(url=url), WEB_APP_URL='https://example.test/app',
        Update=object, ContextTypes=SimpleNamespace(DEFAULT_TYPE=object),
        **dependencies,
    )
    module = ast.Module(body=nodes, type_ignores=[])
    exec(compile(module, 'main.py', 'exec'), namespace)
    return namespace


def flatten(rows):
    return [button for row in rows for button in row]


def run_immediate_coroutine(coroutine):
    """Run a coroutine with fully synchronous fakes without opening a loop."""
    try:
        coroutine.send(None)
    except StopIteration:
        return
    raise AssertionError('test coroutine unexpectedly yielded to an event loop')


def test_welcome_menu_uses_safe_names_and_real_actions(monkeypatch):
    monkeypatch.delenv('UPI_ID', raising=False)
    ns = load_functions('_configured_donation_upi', 'build_premium_start_menu')
    user = SimpleNamespace(id=101, first_name='<Mahi>')
    bot = SimpleNamespace(first_name='<Flimfy>', username='FlimfyBoxBot')

    text, keyboard = ns['build_premium_start_menu'](user, bot)

    assert '&lt;Mahi&gt;' in text
    assert '&lt;Flimfy&gt;' in text
    assert '24/7 uptime' not in text.lower()
    buttons = flatten(keyboard)
    assert 'start_search' in {button.callback_data for button in buttons}
    assert any(button.web_app and button.web_app.url == 'https://example.test/app' for button in buttons)
    assert any(button.url == 'https://t.me/FlimfyBoxBot?startgroup=true' for button in buttons)
    assert not any(button.callback_data == 'start_donate' for button in buttons)


def test_donation_only_appears_for_a_valid_configured_upi(monkeypatch):
    ns = load_functions('_configured_donation_upi', 'build_premium_start_menu')
    monkeypatch.setenv('UPI_ID', 'flimfybox@okaxis')
    assert ns['_configured_donation_upi']() == 'flimfybox@okaxis'
    _, keyboard = ns['build_premium_start_menu'](
        SimpleNamespace(id=1, first_name='Viewer'),
        SimpleNamespace(first_name='FlimfyBox', username='FlimfyBoxBot'),
    )
    assert any(button.callback_data == 'start_donate' for button in flatten(keyboard))

    monkeypatch.setenv('UPI_ID', 'default_id@ybl')
    assert ns['_configured_donation_upi']() == ''
    monkeypatch.setenv('UPI_ID', 'not a valid id')
    assert ns['_configured_donation_upi']() == ''


def test_search_command_passes_the_requested_title_into_normal_search_flow():
    calls = []

    async def run_menu(_update, _context, *, search_query):
        calls.append(search_query)

    ns = load_functions('search_command', main_menu_or_search=run_menu)
    context = SimpleNamespace(args=['Spider-Man:', 'Brand', 'New', 'Day'])
    run_immediate_coroutine(ns['search_command'](SimpleNamespace(effective_message=None), context))

    assert calls == ['Spider-Man: Brand New Day']


def test_help_and_search_are_registered_and_telegram_command_menu_is_configured():
    assert "CommandHandler('help', help_command)" in MAIN_SOURCE
    assert "CommandHandler('search', search_command, filters=filters.ChatType.PRIVATE)" in MAIN_SOURCE
    assert 'set_my_commands(PUBLIC_BOT_COMMANDS)' in MAIN_SOURCE
    assert "BotCommand('search', 'Find a movie or series')" in MAIN_SOURCE
    assert 'Delp Menu' not in MAIN_SOURCE
