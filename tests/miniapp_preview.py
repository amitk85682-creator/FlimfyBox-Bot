"""Local UI preview with a read-only proxy to the deployed public catalogue.

Run: python tests/miniapp_preview.py
Offline: python tests/miniapp_preview.py --catalogue /path/to/catalogue.json
Neither mode starts the bot or permits account mutations. Offline mode never
manufactures release dates, metadata, files, ratings, or successful actions.
"""
import argparse
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from flask import Flask, jsonify, redirect, render_template, request

ROOT = Path(__file__).resolve().parents[1]
DEPLOYED_ORIGIN = 'https://flimfybox-bot-yht0.onrender.com'
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
PUBLIC_GETS = {
    'movies': {'page', 'limit'},
    'home/trending': {'limit'},
    'home/new-releases': {'limit', 'days'},
    'home/catalogue-rows': {'limit'},
    'upcoming': {'limit'},
    'search': {'q', 'autocomplete', 'limit'},
    'suggest': {'q'},
    'genres': {'type'},
    'browse': {'type'},
    'browse/surprise': {'type'},
}
ACCOUNT_MESSAGE = 'Account actions are disabled in this read-only preview. Open the mini app in Telegram to use them.'


def public_query_keys(path):
    if path in PUBLIC_GETS:
        return PUBLIC_GETS[path]
    if re.fullmatch(r'genre/[a-z0-9-]+', path):
        return {'type'}
    if re.fullmatch(r'movie/[1-9][0-9]*(?:/rating)?', path):
        return set()
    if re.fullmatch(r'imdb_id/[1-9][0-9]*', path):
        return set()
    return None


def deployed_json(path, params):
    """No client credentials, cookies, redirects, netrc or arbitrary destinations."""
    deadline = time.monotonic() + 12
    with requests.Session() as session:
        session.trust_env = False
        with session.get(
            f'{DEPLOYED_ORIGIN}/api/{path}', params=params,
            headers={'Accept': 'application/json'}, timeout=(3, 8),
            allow_redirects=False, stream=True,
        ) as response:
            if not 200 <= response.status_code < 300:
                raise requests.RequestException('Deployed API returned an unsuccessful response')
            body = bytearray()
            for chunk in response.iter_content(chunk_size=32768):
                if time.monotonic() > deadline:
                    raise requests.Timeout('Deployed API response took too long')
                body.extend(chunk)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise ValueError('Deployed API response is too large')
            return json.loads(body)


def release_day(movie):
    value = movie.get('first_air_date') if str(movie.get('media_type', '')).lower() == 'tv' else movie.get('release_date')
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def genres_for(movie):
    value = movie.get('genre', '')
    parts = value if isinstance(value, list) else re.split(r'[,/|]', str(value))
    return {str(part).strip().lower() for part in parts if str(part).strip()}


def create_preview(catalogue=None):
    app = Flask(__name__, template_folder=str(ROOT / 'templates'), static_folder=str(ROOT / 'static'))
    offline = catalogue is not None
    movies = [dict(movie) for movie in (catalogue or {}).get('movies', [])]

    @app.after_request
    def mark_preview(response):
        # A reload must show the current UI, and public responses must not be
        # cached as if they were authenticated responses from the production app.
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Preview-Mode'] = 'offline-snapshot' if offline else 'deployed-read-only'
        return response

    @app.get('/')
    def home():
        return redirect('/preview')

    @app.get('/webapp')
    def webapp():
        return render_template('mini_app.html')

    @app.get('/preview')
    def preview():
        width = min(max(request.args.get('width', 1280, type=int), 320), 1600)
        label = ('Offline catalogue snapshot · read-only · account actions disabled' if offline
                 else 'Deployed catalogue API · read-only · account actions disabled')
        return f'''<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1"><title>FlimfyBox · Local preview</title></head>
        <body style="margin:0;background:#171820;color:#ddd;font:12px system-ui">
        <header style="min-height:48px;padding:0 12px;display:flex;align-items:center;justify-content:center;gap:16px;flex-wrap:wrap">
        <span>{label}</span>
        <a style="color:#c2b5ff" href="/preview?width=1280">Desktop</a>
        <a style="color:#c2b5ff" href="/preview?width=390">Mobile</a>
        <a style="color:#c2b5ff" href="/preview?width=320">Small phone</a>
        <a style="color:#c2b5ff" href="/webapp">Open app</a></header>
        <iframe title="Mini app preview" src="/webapp" style="display:block;width:{width}px;max-width:100%;height:calc(100dvh - 48px);border:0;margin:auto"></iframe></body></html>'''

    def snapshot_api(path):
        today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
        browse_type = request.args.get('type', 'all')

        def matches_type(movie):
            is_tv = (str(movie.get('content_type', '')).lower() in {'tv', 'series', 'web series'}
                     or str(movie.get('category', '')).lower() == 'web series'
                     or bool(movie.get('seasons_data')))
            return browse_type == 'all' or (is_tv if browse_type == 'tv' else not is_tv)

        typed = [movie for movie in movies if matches_type(movie)]
        if path == 'movies':
            page = max(1, request.args.get('page', 1, type=int))
            limit = min(100, max(1, request.args.get('limit', 40, type=int)))
            offset = (page - 1) * limit
            return jsonify(status='success', movies=movies[offset:offset + limit], has_more=offset + limit < len(movies))
        if path == 'home/trending':
            return jsonify(status='success', results=movies[:12], hero_limit=5, source='offline-snapshot')
        if path == 'home/new-releases':
            recent = [m for m in movies if release_day(m) and timedelta(0) <= today - release_day(m) <= timedelta(days=60)]
            return jsonify(status='success', movies=recent, window_days=60)
        if path == 'home/catalogue-rows':
            collections = {name: [m for m in movies if name in str(m.get('category', '')).lower() or name in genres_for(m)]
                           for name in ('hollywood', 'bollywood', 'anime')}
            return jsonify(status='success', collections=collections)
        if path == 'upcoming':
            upcoming = [m for m in movies if release_day(m) and release_day(m) > today and not m.get('is_available')]
            return jsonify(status='success', movies=sorted(upcoming, key=release_day))
        if path == 'search':
            query = request.args.get('q', '').strip().casefold()
            autocomplete = request.args.get('autocomplete') == '1'
            results = [m for m in movies if query and (str(m.get('title', '')).casefold().startswith(query) if autocomplete
                       else query in str(m.get('title', '')).casefold())]
            return jsonify(status='success', results=results[:20] if autocomplete else results)
        if path == 'suggest':
            return jsonify([])
        if path == 'genres':
            names = sorted(set().union(*(genres_for(m) for m in typed)))
            return jsonify(status='success', genres=[{'id': name.replace(' ', '-'), 'label': name.title(),
                           'count': sum(name in genres_for(m) for m in typed),
                           'posters': [m['image'] for m in typed if name in genres_for(m) and m.get('image')][:3]}
                           for name in names])
        if path.startswith('genre/'):
            name = path.split('/')[1].replace('-', ' ')
            selected = [m for m in typed if name in genres_for(m)]
            return jsonify(status='success', genre={'id': name, 'label': name.title(), 'count': len(selected)}, movies=selected)
        if path == 'browse':
            return jsonify(status='success', collections=[{'id': 'snapshot', 'label': 'In this catalogue snapshot', 'movies': typed}])
        if path == 'browse/surprise':
            if not typed:
                return jsonify(status='error', message='No matching titles in this snapshot'), 404
            return jsonify(status='success', movie=typed[0])
        if re.fullmatch(r'movie/[1-9][0-9]*', path):
            movie = next((m for m in movies if str(m.get('id')) == path.split('/')[1]), None)
            if movie:
                return jsonify(status='success', movie=movie)
            return jsonify(status='error', message='Title not included in this snapshot'), 404
        return jsonify(status='error', message='This information is not included in the offline snapshot'), 404

    @app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
    def api(path):
        allowed_keys = public_query_keys(path)
        if request.method != 'GET' or allowed_keys is None:
            return jsonify(status='error', message=ACCOUNT_MESSAGE), 403
        if offline:
            return snapshot_api(path)
        params = {key: request.args[key][:200] for key in allowed_keys if key in request.args}
        try:
            payload = deployed_json(path, params)
            if not isinstance(payload, (dict, list)):
                raise ValueError('Invalid deployed API response')
            return jsonify(payload)
        except requests.Timeout:
            return jsonify(status='error', message='The deployed API took too long. Please retry.'), 504
        except (requests.RequestException, ValueError):
            return jsonify(status='error', message='The deployed API is unavailable. Please retry.'), 502

    @app.get('/watch/<path:path>')
    def watch(path):
        return 'File delivery is disabled in this read-only preview. Open this title in the Telegram mini app.', 403

    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalogue', type=Path, help='Use a verified offline snapshot instead of the deployed API')
    parser.add_argument('--port', type=int, default=8787)
    args = parser.parse_args()
    catalogue = json.loads(args.catalogue.read_text(encoding='utf-8')) if args.catalogue else None
    create_preview(catalogue).run(host='127.0.0.1', port=args.port, threaded=True)
