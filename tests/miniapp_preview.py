"""Local UI fixture server. No bot startup, production writes, or credentials.

Run: python tests/miniapp_preview.py --catalogue /path/to/catalogue-snapshot.json
Open /preview for responsive QA. API mutations below affect memory only.
"""
import argparse
import json
from pathlib import Path

from flask import Flask, jsonify, render_template, request

ROOT = Path(__file__).resolve().parents[1]


def create_preview(catalogue):
    app = Flask(__name__, template_folder=str(ROOT / 'templates'), static_folder=str(ROOT / 'static'))
    movies = [dict(movie, is_available=True) for movie in catalogue['movies']]
    saved = set()
    chat = []

    @app.get('/webapp')
    def webapp():
        return render_template('mini_app.html')

    @app.get('/preview')
    def preview():
        width = min(max(request.args.get('width', 1280, type=int), 320), 1600)
        return f'''<!doctype html><html><head><title>FlimfyBox · Local preview</title></head>
        <body style="margin:0;background:#262729;color:#ddd;font:12px system-ui">
        <header style="height:42px;display:flex;align-items:center;justify-content:center;gap:20px">
        <span>LOCAL QA · catalogue snapshot · simulated actions</span>
        <a style="color:#ffab91" href="/preview?width=1280">Desktop</a>
        <a style="color:#ffab91" href="/preview?width=390">Mobile</a>
        <a style="color:#ffab91" href="/preview?width=320">Small phone</a></header>
        <iframe title="Mini app preview" src="/webapp" style="display:block;width:{width}px;max-width:100%;height:calc(100vh - 42px);border:0;margin:auto"></iframe></body></html>'''

    @app.route('/api/<path:path>', methods=['GET', 'POST', 'DELETE'])
    def api(path):
        if path == 'movies':
            return jsonify(status='success', movies=movies, has_more=False)
        if path == 'home/trending':
            featured = sorted(movies, key=lambda m: m.get('title') != 'Heart of Stone')
            return jsonify(status='success', results=featured[:12], hero_limit=5)
        if path == 'home/new-releases':
            return jsonify(status='success', movies=movies[8:20], window_days=60)
        if path == 'home/catalogue-rows':
            return jsonify(status='success', collections={'hollywood': movies[:8], 'bollywood': movies[8:16], 'anime': movies[16:24]})
        if path == 'upcoming':
            return jsonify(status='success', movies=[dict(m, id=f'tmdb_{m["id"]}', source='tmdb', is_upcoming=True, is_available=False, release_date='2027-01-01') for m in movies[12:18]])
        if path == 'search':
            query = request.args.get('q', '').lower()
            if query == 'error':
                return jsonify(status='error', message='Preview network failure'), 503
            return jsonify(status='success', results=[m for m in movies if query in m['title'].lower()])
        if path == 'suggest':
            return jsonify([])
        if path == 'genres':
            return jsonify(status='success', genres=[{'id': name, 'count': len(movies), 'posters': []} for name in ['action','anime','comedy','drama','thriller','romance']])
        if path.startswith('genre/'):
            return jsonify(status='success', genre={'count': len(movies)}, movies=movies)
        if path == 'browse':
            return jsonify(status='success', collections=[{'label':'Modern favourites', 'movies': movies[:8]}])
        if path == 'browse/surprise':
            return jsonify(status='success', movie=movies[0])
        if path.startswith('movie/') and path.endswith('/rating'):
            own = (request.get_json(silent=True) or {}).get('rating', 0)
            return jsonify(status='success', average=4.2, count=12, user_rating=own, can_rate=True)
        if path.startswith('movie/'):
            movie = next((m for m in movies if str(m['id']) == path.split('/')[1]), None)
            if not movie:
                return jsonify(status='error', message='Movie not found'), 404
            return jsonify(status='success', movie=dict(movie,
                description='Local preview story: an unexpected mission brings a team together, testing their loyalties as the stakes rise. This is sample detail content for layout and interaction testing.',
                cast='Sample cast member, Second cast member', trailer_key=None,
                files=[{'id':1,'quality':'1080p WEB-DL','size':'2.4 GB','extra_info':''}, {'id':2,'quality':'720p WEB-DL','size':'940 MB','extra_info':''}]))
        if path.startswith('my-list/'):
            movie_id = path.split('/')[1]
            if request.method == 'DELETE':
                saved.discard(movie_id)
            return jsonify(status='success', saved=movie_id in saved, movie_id=movie_id)
        if path == 'my-list':
            if request.method == 'POST':
                movie_id = str(request.json['movie_id']); saved.add(movie_id)
                return jsonify(status='success', saved=True, movie_id=movie_id)
            return jsonify(status='success', movies=[m for m in movies if str(m['id']) in saved])
        if path == 'global-chat':
            if request.method == 'POST':
                chat.append(dict(first_name='Preview user', message=request.json['message']))
            return jsonify(status='success', messages=chat)
        if path == 'upcoming/reminder':
            return jsonify(status='success', reminder=request.method == 'POST', availability_state='upcoming')
        if path in {'request', 'recommendation-events'}:
            return jsonify(status='success', guest=True)
        return jsonify(status='error', message=f'No fixture for {path}'), 404

    @app.get('/watch/<path:path>')
    def watch(path):
        return f'Preview only: download link routed correctly to /watch/{path}. No file was sent.'

    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalogue', required=True, type=Path)
    args = parser.parse_args()
    create_preview(json.loads(args.catalogue.read_text(encoding='utf-8'))).run(host='127.0.0.1', port=8787, threaded=True)
