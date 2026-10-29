import ast
import asyncio
import __future__
import logging
import re
from html import escape
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


TREE = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text(encoding='utf-8'))


def load(*names, **dependencies):
    nodes = [node for node in TREE.body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    namespace = {
        'asyncio': asyncio, 'logger': logging.getLogger(__name__), 'html_escape': escape,
        'SUPERBATCH_IDENTITY_TIMEOUT_SECONDS': 0.01,
        'SUPERBATCH_MOVIE_TIMEOUT_SECONDS': 0.01,
    }
    namespace.update(dependencies)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'main.py', 'exec',
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


async def immediate(function, *args, **kwargs):
    return function(*args, **kwargs)


def test_hung_identity_is_cancelled_and_local_evidence_is_used():
    async def scenario():
        cancelled = asyncio.Event()

        async def identity(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        fallback = {'title': 'Frieren Beyond Journeys End'}
        core = AsyncMock(return_value={'movie_id': 49})
        ns = load('_prepare_superbatch_movie', reconcile_evidence_with_gemini=identity,
                  _local_evidence_fallback=lambda *args: fallback, _core_movie_processor=core)
        assert await ns['_prepare_superbatch_movie']({}, fallback['title']) == {'movie_id': 49}
        assert cancelled.is_set()
        assert core.call_args.kwargs['reconciled_data'] == fallback

    asyncio.run(scenario())


def test_movie_49_timeout_does_not_stop_movies_50_to_110():
    async def scenario():
        processed, saved, failures, messages = [], [], [], []
        groups = {str(i): [{'batch_item_id': i, 'display_title': str(i), 'caption': str(i)}]
                  for i in range(1, 111)}

        async def core(raw, **kwargs):
            index = int(raw)
            processed.append(index)
            if index == 49:
                await asyncio.Event().wait()
            return dict(movie_id=index, title=f'Movie {index}', year=2026, genre='', rating='',
                        plot='', category='Movie', movie_lang='', poster_url=None, imdb_id=None)

        async def save(files, context, movie_id, progress=None):
            saved.append(movie_id)
            if progress:
                await progress(1, 1)
            return ['1080p']

        async def failure(item_id, exc, stage=None):
            failures.append((item_id, type(exc)))

        async def status(message, text, **kwargs):
            messages.append(text)
            return True

        ns = load('_prepare_superbatch_movie', '_superbatch_done_impl', '_superbatch_posted_summary',
                  reconcile_evidence_with_gemini=AsyncMock(return_value={'title': 'Movie'}),
                  _core_movie_processor=core, _local_evidence_fallback=lambda *args: {},
                  SUPER_BATCH_SESSION={'active': True, 'admin_id': 42, 'batch_id': 'b1', 'files': [1]},
                  BATCH_SESSION={}, _build_superbatch_groups=lambda _: groups,
                  _select_representative_file=lambda files: files[0], get_storage_channels=lambda: [1],
                  _safe_batch_reply=AsyncMock(return_value=object()), _safe_batch_status_edit=status,
                  _save_superbatch_files=save, _mark_superbatch_item_failed=failure,
                  run_async=immediate,
                  _batch_job_snapshot=lambda *args: (None,) * 6 + (109, 0, 1, 0), re=re)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=42), message=object())
        await asyncio.wait_for(ns['_superbatch_done_impl'](update, SimpleNamespace()), 2)
        assert processed == list(range(1, 111))
        assert saved == [i for i in range(1, 111) if i != 49]
        assert failures == [(49, TimeoutError)]
        assert not ns['BATCH_SESSION']['active']
        assert ns['BATCH_SESSION']['movie_id'] is None
        assert 'SUPER BATCH COMPLETED' in messages[-1]
        assert 'Failed:</b> 1' in messages[-1]
        assert '/batchretry b1' in messages[-1]

    asyncio.run(scenario())


@pytest.mark.parametrize('failed_stage', ['claim', 'save'])
def test_failed_claim_or_save_does_not_block_later_files(failed_stage):
    async def scenario():
        saved, updates = [], []

        def claim(item_id):
            return not (failed_stage == 'claim' and item_id == 1)

        def fetch(_item_id):
            row = [None] * 20
            row[12] = 'processing'
            row[13] = 'storage_copying'
            return row

        async def save(message, context, item_id):
            if failed_stage == 'save' and item_id == 1:
                raise TimeoutError('bounded storage request timed out')
            saved.append(item_id)
            return '1080p'

        async def no_sleep(_):
            pass

        ns = load('_save_one_superbatch_file', '_save_superbatch_files', '_mark_superbatch_item_failed',
                  run_async=immediate, _batch_item_claim=claim, _batch_item_fetch=fetch,
                  _batch_item_update=lambda item_id, **fields: updates.append((item_id, fields)),
                  _pm_save_file=save)
        ns['asyncio'] = SimpleNamespace(wait_for=asyncio.wait_for, sleep=no_sleep)
        files = [{'batch_item_id': i, 'message_obj': object()} for i in (1, 2)]
        assert await ns['_save_superbatch_files'](files, None, 7) == ['1080p']
        assert saved == [2]
        if failed_stage == 'save':
            assert any(item_id == 1 and fields.get('state') == 'failed'
                       for item_id, fields in updates)

    asyncio.run(scenario())


@pytest.mark.parametrize('state,pending,expected', [
    ('processing', 123, 'unrecoverable'), ('processing', None, 'failed'),
    ('succeeded', None, None), ('skipped', None, None), ('unrecoverable', 123, None),
])
def test_failure_preserves_success_and_unknown_storage_outcomes(state, pending, expected):
    updates = []
    row = [None] * 20
    row[12], row[13], row[18] = state, 'storage_copying', pending
    ns = load('_mark_superbatch_item_failed', run_async=immediate,
              _batch_item_fetch=lambda _: row,
              _batch_item_update=lambda item_id, **fields: updates.append(fields))
    asyncio.run(ns['_mark_superbatch_item_failed'](1, TimeoutError()))
    if expected:
        assert updates[0]['state'] == expected
    else:
        assert updates == []


def test_large_completion_list_is_bounded_and_html_safe():
    ns = load('_superbatch_posted_summary')
    summary = ns['_superbatch_posted_summary'](['<Movie & Series> ' + '🎬' * 100] * 400)
    assert len(summary.encode('utf-16-le')) // 2 < 2500
    assert '<Movie' not in summary
    assert '&lt;Movie &amp; Series&gt;' in summary
    assert 'aur ' in summary


def test_ingestion_metadata_skips_season_and_episode_http_requests():
    from urllib.parse import quote
    urls = []

    def get(url, **kwargs):
        urls.append(url)
        payload = ({'results': [{'id': 49, 'name': 'Frieren', 'media_type': 'tv',
                                 'first_air_date': '2023-09-29'}]}
                   if '/search/' in url else {'imdb_id': 'tt1234567'})
        return SimpleNamespace(json=lambda: payload)

    ns = load('fetch_movie_metadata', os=SimpleNamespace(environ={}), re=re, quote=quote,
              requests=SimpleNamespace(get=get), _find_best_tmdb_match=lambda rows, *args: rows[0])
    result = ns['fetch_movie_metadata']('Frieren', hint_category='Anime', include_seasons=False)
    assert result[0] == 'Frieren'
    assert result[-1] == {}
    assert len(urls) == 2
    assert all('/season/' not in url for url in urls)


def test_optional_provider_failure_does_not_prevent_file_ingestion():
    async def scenario():
        async def blocked_io(*args):
            await asyncio.Event().wait()

        async def fast_deadline(awaitable, timeout):
            return await asyncio.wait_for(awaitable, 0.01)

        ns = load('_optional_movie_lookup', run_async=blocked_io)
        ns['asyncio'] = SimpleNamespace(wait_for=fast_deadline)
        assert await ns['_optional_movie_lookup'](str, default=(None, None)) == (None, None)

    asyncio.run(scenario())
