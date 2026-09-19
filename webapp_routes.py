from flask import Blueprint, jsonify, request, send_file, render_template, redirect
from flask_cors import CORS
import os
import logging
import json
import psycopg2
from datetime import datetime, timedelta
import requests
import time
from urllib.parse import quote
from urllib.parse import parse_qsl
import random
import re
import secrets
import hashlib
import hmac
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from fuzzywuzzy import process, fuzz

CANONICAL_GENRES = [
    ('action', 'Action', 'Action & Adventure', 'Fast, explosive, and high-stakes viewing.'),
    ('adventure', 'Adventure', 'Action & Adventure', 'Quest-driven stories and faraway journeys.'),
    ('animation', 'Animation', 'Animation & Anime', 'Bold visual storytelling and inventive worlds.'),
    ('anime', 'Anime', 'Animation & Anime', 'Anime, manga-inspired worlds and beloved fandoms.'),
    ('biography', 'Biography', 'Drama & Documentary', 'Real-life stories with emotional depth and context.'),
    ('comedy', 'Comedy', 'Comedy', 'Light, witty, and crowd-pleasing entertainment.'),
    ('crime', 'Crime', 'Crime & Mystery', 'Sharp investigations, pressure, and power plays.'),
    ('documentary', 'Documentary', 'Documentary & Reality', 'Truth, culture, and world-expanding nonfiction.'),
    ('drama', 'Drama', 'Drama', 'Character-first stories with emotional weight.'),
    ('erotic', 'Erotic', 'Adult / Mature', 'Mature, romantic, and explicit content.'),
    ('family', 'Family', 'Family & Feel-Good', 'Warm and accessible storytelling for every age.'),
    ('fantasy', 'Fantasy', 'Sci-Fi & Fantasy', 'Magic, myth, and imaginative wonder.'),
    ('history', 'History', 'Drama & Documentary', 'Historic eras, legacies, and major turning points.'),
    ('horror', 'Horror', 'Horror', 'Suspense, dread, and fear-driven cinema.'),
    ('music', 'Music', 'Music & Culture', 'Performance, rhythm, and creative energy.'),
    ('mystery', 'Mystery', 'Crime & Mystery', 'Clues, tension, and cinematic intrigue.'),
    ('reality', 'Reality', 'Documentary & Reality', 'Authentic, unscripted, and high-interest stories.'),
    ('romance', 'Romance', 'Romance', 'Emotional connection, chemistry, and longing.'),
    ('scifi', 'Sci-Fi', 'Sci-Fi & Fantasy', 'Future worlds, tech, and speculative fiction.'),
    ('short', 'Short', 'Short-form', 'Compact, sharp, and instantly watchable stories.'),
    ('sport', 'Sport', 'Sport & Lifestyle', 'Competition, ambition, and underdog energy.'),
    ('thriller', 'Thriller', 'Thriller', 'Tension, danger, and edge-of-the-seat suspense.'),
    ('war', 'War', 'Action & Adventure', 'Conflict, endurance, and high-stakes battlefields.'),
    ('western', 'Western', 'Classic & Cinematic', 'Dust, law, and frontier storytelling.'),
    ('musical', 'Musical', 'Music & Culture', 'Songs, emotion, and performance-led stories.'),
    ('cyberpunk', 'Cyberpunk', 'Sci-Fi & Fantasy', 'Neon futures, corporate tension, and dystopian energy.'),
    ('dystopian', 'Dystopian', 'Sci-Fi & Fantasy', 'Oppressive worlds and fragile resistance.'),
    ('space-opera', 'Space Opera', 'Sci-Fi & Fantasy', 'Epic cosmic scale and heroic journeys.'),
    ('psychological-horror', 'Psychological Horror', 'Horror', 'Fear driven by the mind, guilt, and dread.'),
    ('supernatural-horror', 'Supernatural Horror', 'Horror', 'Hauntings, curses, and unknown forces.'),
    ('political-drama', 'Political Drama', 'Drama', 'Power, consequence, and high-pressure choices.'),
    ('historical-drama', 'Historical Drama', 'Drama', 'Large-scale narratives shaped by eras and events.'),
    ('romantic-comedy', 'Romantic Comedy', 'Comedy', 'Warm chemistry and easygoing fun.'),
    ('dark-comedy', 'Dark Comedy', 'Comedy', 'Humor built on chaos, irony, and edge.'),
    ('detective', 'Detective', 'Crime & Mystery', 'Clues, suspects, and moral puzzles.'),
    ('procedural', 'Procedural', 'Crime & Mystery', 'Case-by-case tension and investigative structure.'),
    ('heist', 'Heist', 'Action & Adventure', 'Strategy, stealing, and adrenaline.'),
    ('survival', 'Survival', 'Action & Adventure', 'Danger, resilience, and impossible odds.'),
    ('superhero', 'Superhero', 'Action & Adventure', 'Legend, power, and larger-than-life conflict.'),
    ('spy', 'Spy', 'Action & Adventure', 'Espionage, stealth, and geopolitical thrill.'),
    ('isekai', 'Isekai', 'Animation & Anime', 'Reborn into a new world and adventure unfolds.'),
    ('shonen', 'Shonen', 'Animation & Anime', 'High energy, ambition, and action-driven arcs.'),
    ('shojo', 'Shojo', 'Animation & Anime', 'Romance, emotion, and layered character arcs.'),
    ('seinen', 'Seinen', 'Animation & Anime', 'Mature tone and layered themes.'),
    ('kaiju', 'Kaiju', 'Animation & Anime', 'Monster-scale spectacle and destruction.'),
    ('zombie', 'Zombie', 'Horror', 'Apocalypse, panic, and survival instincts.'),
    ('vampire', 'Vampire', 'Horror', 'Dark romance, bloodlust, and nightfall.')
]

GENRE_ALIASES = {
    'action': {'action', 'martial arts', 'superhero', 'spy', 'heist', 'survival', 'battle'},
    'adventure': {'adventure', 'quest', 'expedition', 'journey'},
    'animation': {'animation', 'animated', 'cartoons', 'cartoon'},
    'anime': {'anime', 'isekai', 'shonen', 'shojo', 'seinen', 'kaiju'},
    'biography': {'biography', 'bio'},
    'comedy': {'comedy', 'comdey', 'conedy', 'omedy', 'action comedy'},
    'crime': {'crime', 'cime', 'gangster', 'detective', 'procedural', 'heist'},
    'documentary': {'documentary', 'docudrama', 'real life'},
    'drama': {'drama', 'dram', 'drame', 'family drama', 'political drama', 'medical drama', 'legal drama', 'teen drama', 'coming-of-age'},
    'erotic': {'erotic'},
    'family': {'family', 'kids', 'children'},
    'fantasy': {'fantasy', 'myth', 'dark fantasy'},
    'history': {'history', 'histry', 'historical', 'period'},
    'horror': {'horror', 'slasher', 'monster', 'zombie', 'vampire', 'haunting'},
    'music': {'music'},
    'mystery': {'mystery', 'investigation', 'whodunit', 'detective'},
    'reality': {'reality', 'reality show', 'talk show', 'game show'},
    'romance': {'romance', 'romantic', 'love story'},
    'scifi': {'sci-fi', 'sci fi', 'science fiction', 'science-fiction', 'cyberpunk', 'dystopian', 'time travel', 'space opera', 'mecha'},
    'short': {'short', 'short film'},
    'sport': {'sport', 'sports', 'wrestling'},
    'thriller': {'thriller', 'suspense', 'psychological', 'crime thriller'},
    'war': {'war', 'war action thriller'},
    'western': {'western', 'frontier'},
    'musical': {'musical', 'song'},
    'cyberpunk': {'cyberpunk'},
    'dystopian': {'dystopian'},
    'space-opera': {'space opera', 'space-opera'},
    'psychological-horror': {'psychological horror', 'psychological-horror'},
    'supernatural-horror': {'supernatural horror', 'supernatural-horror'},
    'political-drama': {'political drama', 'political-drama'},
    'historical-drama': {'historical drama', 'historical-drama'},
    'romantic-comedy': {'romantic comedy', 'romantic-comedy'},
    'dark-comedy': {'dark comedy', 'dark-comedy'},
    'detective': {'detective'},
    'procedural': {'procedural'},
    'heist': {'heist'},
    'survival': {'survival'},
    'superhero': {'superhero'},
    'spy': {'spy', 'espionage'},
    'isekai': {'isekai'},
    'shonen': {'shonen'},
    'shojo': {'shojo'},
    'seinen': {'seinen'},
    'kaiju': {'kaiju'},
    'zombie': {'zombie'},
    'vampire': {'vampire'}
}

GENRE_GARBAGE = {'', 'n/a', 'na', 'a', 'n', 'unknown', 'dubbed', 'bluray', 'hdcam', 'hdrip', 'hdtc', 'hdts', 'web-dl'}

def normalize_catalogue_genres(raw_value):
    if not raw_value or not isinstance(raw_value, str):
        return set()
    result = set()
    for token in re.split(r'[,&/|]', raw_value):
        value = re.sub(r'\s+', ' ', token.replace('–', '-').replace('—', '-')).strip().lower()
        value = re.sub(r'[^a-z0-9\s-]', ' ', value)
        value = re.sub(r'\s+', ' ', value).strip()
        if not value or value in GENRE_GARBAGE or value.startswith('quality '):
            continue
        for genre_id, aliases in GENRE_ALIASES.items():
            if value in aliases:
                result.add(genre_id)
        if 'science fiction' in value or value == 'sci fi':
            result.add('scifi')
        if 'dark comedy' in value:
            result.add('dark-comedy')
        if 'romantic comedy' in value:
            result.add('romantic-comedy')
        if 'psychological horror' in value:
            result.add('psychological-horror')
        if 'supernatural horror' in value:
            result.add('supernatural-horror')
        if 'historical drama' in value:
            result.add('historical-drama')
        if 'political drama' in value:
            result.add('political-drama')
    return result

def is_catalogue_tv(category, seasons_data):
    """Use the existing episodic signals for Browse media classification."""
    if isinstance(seasons_data, str):
        try:
            seasons_data = json.loads(seasons_data)
        except (TypeError, ValueError):
            seasons_data = None
    return (
        str(category or '').strip().lower() == 'web series'
        or (isinstance(seasons_data, dict) and bool(seasons_data))
    )

def browse_type_matches(browse_type, category, seasons_data):
    if browse_type == 'all':
        return True
    is_tv = is_catalogue_tv(category, seasons_data)
    return is_tv if browse_type == 'tv' else not is_tv

GENRE_BY_ID = {item[0]: item for item in CANONICAL_GENRES}

def register_webapp_routes(
    flask_app,
    *,
    api_movies_cache,
    search_cache,
    get_db_connection,
    close_db_connection,
    store_user_request,
    TMDB_API_KEY,
    logger
):
    release_cache = {}
    release_cache_lock = threading.Lock()
    new_releases_refresh_lock = threading.Lock()
    release_success_ttl = 7 * 24 * 60 * 60
    release_negative_ttl = 6 * 60 * 60
    release_discovery_ttl = 6 * 60 * 60

    def tmdb_cached_request(path, params, cache_key, negative=False, ttl_seconds=None):
        now = datetime.utcnow().timestamp()
        with release_cache_lock:
            cached = release_cache.get(cache_key)
            if cached and cached[0] > now:
                return cached[1], True
        request_params = dict(params)
        request_params['api_key'] = TMDB_API_KEY
        response_data = None
        for attempt in range(2):
            try:
                response = requests.get(
                    f'https://api.themoviedb.org/3/{path}',
                    params=request_params,
                    headers={'User-Agent': 'FlimfyBox/1.0'},
                    timeout=6
                )
                if response.status_code in (429, 500, 502, 503, 504):
                    if attempt == 0:
                        continue
                    response.raise_for_status()
                response.raise_for_status()
                response_data = response.json()
                break
            except (requests.RequestException, ValueError) as error:
                if attempt == 1:
                    logger.warning('TMDB release lookup failed for %s: %s', cache_key, error)
        ttl = release_negative_ttl if negative or response_data is None else (ttl_seconds or release_success_ttl)
        with release_cache_lock:
            release_cache[cache_key] = (now + ttl, response_data)
        return response_data, False

    def normalize_release_title(value):
        return re.sub(r'[^a-z0-9]', '', str(value or '').lower())

    def get_release_details(candidate, metrics=None):
        media_type = candidate.get('media_type')
        tmdb_id = candidate.get('id')
        if media_type not in {'movie', 'tv'} or not tmdb_id:
            return None
        details, cache_hit = tmdb_cached_request(
            f'{media_type}/{tmdb_id}',
            {'append_to_response': 'release_dates,external_ids'},
            f'details:{media_type}:{tmdb_id}'
        )
        if metrics is not None:
            with metrics['lock']:
                metrics['cache_hits'] += int(cache_hit)
                metrics['requests'] += int(not cache_hit)
        if not details:
            return None
        if media_type == 'tv':
            release_date = details.get('first_air_date') or candidate.get('first_air_date')
            release_type = 'first air'
        else:
            release_date = candidate.get('release_date') or details.get('release_date')
            india_dates = []
            for country in (details.get('release_dates') or {}).get('results', []):
                if country.get('iso_3166_1') == 'IN':
                    india_dates.extend(country.get('release_dates') or [])
            preferred_dates = [
                item['release_date'][:10]
                for item in india_dates
                if item.get('type') in {2, 3, 4} and item.get('release_date')
            ]
            if preferred_dates:
                release_date = min(preferred_dates)
            release_type = 'India release' if india_dates else 'release'
        if not release_date:
            return None
        return {
            'tmdb_id': tmdb_id,
            'media_type': media_type,
            'imdb_id': (details.get('external_ids') or {}).get('imdb_id'),
            'title': details.get('title') or details.get('name') or candidate.get('title') or candidate.get('name'),
            'release_date': release_date[:10],
            'release_type': release_type
        }

    def get_release_discovery_pool(media_type, days, today, metrics):
        """Load the complete TMDB release window once, then reuse its candidates."""
        cache_key = f'release-discovery:{media_type}:{days}:{today.isoformat()}'
        now = time.time()
        with release_cache_lock:
            cached = release_cache.get(cache_key)
            stale_pool = cached[1] if cached and cached[1] else None
            if cached and cached[0] > now:
                metrics['discovery_cache_hits'] += 1
                return cached[1]

        if media_type == 'movie':
            base_params = {
                'primary_release_date.gte': (today - timedelta(days=days)).isoformat(),
                'primary_release_date.lte': today.isoformat(),
                'region': 'IN',
                'with_release_type': '2|3|4',
                'sort_by': 'primary_release_date.desc'
            }
        else:
            base_params = {
                'first_air_date.gte': (today - timedelta(days=days)).isoformat(),
                'first_air_date.lte': today.isoformat(),
                'sort_by': 'first_air_date.desc'
            }

        def fetch_page(page):
            params = {**base_params, 'page': page}
            return tmdb_cached_request(
                f'discover/{media_type}',
                params,
                f'discover:{media_type}:{days}:{today.isoformat()}:page:{page}',
                ttl_seconds=release_discovery_ttl
            )

        first_page, first_hit = fetch_page(1)
        metrics['discovery_pages'] += 1
        metrics['discovery_requests'] += int(not first_hit)
        metrics['discovery_cache_hits'] += int(first_hit)
        if not isinstance(first_page, dict):
            if stale_pool:
                metrics['discovery_stale_fallbacks'] += 1
                return stale_pool
            return {'results': [], 'total_pages': 0, 'total_results': 0}

        try:
            total_pages = max(1, min(int(first_page.get('total_pages') or 1), 500))
        except (TypeError, ValueError):
            total_pages = 1
        pages = {1: first_page}
        page_numbers = range(2, total_pages + 1)
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_page, page): page for page in page_numbers}
            for future in as_completed(futures):
                page = futures[future]
                page_data, page_hit = future.result()
                metrics['discovery_pages'] += 1
                metrics['discovery_requests'] += int(not page_hit)
                metrics['discovery_cache_hits'] += int(page_hit)
                if not isinstance(page_data, dict):
                    if stale_pool:
                        metrics['discovery_stale_fallbacks'] += 1
                        return stale_pool
                    return {'results': [], 'total_pages': 0, 'total_results': 0}
                pages[page] = page_data

        seen_ids = set()
        results = []
        for page in range(1, total_pages + 1):
            for item in pages[page].get('results') or []:
                item_id = item.get('id')
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)
                    results.append(item)
        pool = {
            'results': results,
            'total_pages': total_pages,
            'total_results': first_page.get('total_results', len(results))
        }
        with release_cache_lock:
            release_cache[cache_key] = (now + release_discovery_ttl, pool)
        return pool

    tmdb_ranking_cache = {}
    tmdb_ranking_cache_lock = threading.Lock()

    def normalize_title_lookup(value):
        return re.sub(r'[^a-z0-9]+', '', str(value or '').lower())

    def fetch_tmdb_home_ranking(source, max_pages=5):
        source = source if source in {'day', 'week', 'popular'} else 'day'
        now = time.time()
        ttl_by_source = {
            'day': int(os.environ.get('TMDB_TRENDING_DAY_TTL', '180')),
            'week': int(os.environ.get('TMDB_TRENDING_WEEK_TTL', '600')),
            'popular': int(os.environ.get('TMDB_POPULAR_TTL', '600')),
        }
        stale = None
        with tmdb_ranking_cache_lock:
            cached = tmdb_ranking_cache.get(source)
            if cached:
                stale = cached
                if cached.get('expires_at', 0) > now:
                    return cached['items'], 'cached', cached.get('fetched_at')

        endpoint_map = {
            'day': ('trending/movie/day', {}),
            'week': ('trending/movie/week', {}),
            'popular': ('movie/popular', {}),
        }
        endpoint, base_params = endpoint_map[source]
        ranked = []
        seen_ids = set()
        for page in range(1, max_pages + 1):
            page_data, _ = tmdb_cached_request(
                endpoint,
                {**base_params, 'page': page},
                f'tmdb-ranking:{source}:{page}'
            )
            if not page_data:
                if stale and stale.get('items'):
                    logger.warning('TMDB %s ranking stale fallback used after failed refresh', source)
                    return stale['items'], 'stale', stale.get('fetched_at')
                return ranked, 'fresh', None
            for item in (page_data or {}).get('results', []):
                tmdb_id = item.get('id')
                if not tmdb_id or tmdb_id in seen_ids:
                    continue
                seen_ids.add(tmdb_id)
                ranked.append({
                    'id': tmdb_id,
                    'title': item.get('title') or item.get('name') or '',
                    'release_date': item.get('release_date') or item.get('first_air_date') or '',
                    'year': item.get('release_date')[:4] if isinstance(item.get('release_date'), str) and item.get('release_date') else (item.get('first_air_date')[:4] if isinstance(item.get('first_air_date'), str) and item.get('first_air_date') else ''),
                    'poster_path': item.get('poster_path'),
                    'backdrop_path': item.get('backdrop_path'),
                })

        if not ranked and stale and stale.get('items'):
            logger.warning('TMDB %s ranking returned no results; reusing stale cache', source)
            return stale['items'], 'stale', stale.get('fetched_at')

        if ranked:
            detail_ids = [item['id'] for item in ranked]
            details = []
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(
                    lambda tmdb_id, _=None: tmdb_cached_request(
                        f'movie/{tmdb_id}',
                        {'append_to_response': 'external_ids'},
                        f'tmdb-ranking-detail:{tmdb_id}'
                    )[0],
                    tmdb_id
                ) for tmdb_id in detail_ids]
                details = [future.result() for future in as_completed(futures)]
            detail_map = {}
            for detail in details:
                if not detail:
                    continue
                tmdb_id = detail.get('id')
                if tmdb_id is None:
                    continue
                detail_map[tmdb_id] = detail
            for item in ranked:
                detail = detail_map.get(item['id'])
                item['imdb_id'] = (detail.get('external_ids') or {}).get('imdb_id') if detail else ''
                item['overview'] = detail.get('overview') if detail else ''
                item['vote_average'] = detail.get('vote_average') if detail else 0

        result = {
            'items': ranked,
            'fresh': True,
            'fetched_at': datetime.utcnow().isoformat(),
            'expires_at': now + ttl_by_source[source],
        }
        with tmdb_ranking_cache_lock:
            tmdb_ranking_cache[source] = result
        return ranked, 'fresh', result['fetched_at']

    @flask_app.route('/api/home/trending', methods=['GET'])
    def get_home_trending():
        source = request.args.get('source', 'day')
        if source not in {'day', 'week', 'popular'}:
            source = 'day'
        hero_limit = int(os.environ.get('HOME_TRENDING_HERO_LIMIT', '10'))
        ranking, cache_state, fetched_at = fetch_tmdb_home_ranking(source)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, title, year, poster_url, rating, genre, category, language, imdb_id
                FROM movies
                WHERE poster_url IS NOT NULL AND poster_url <> ''
            """)
            local_rows = cur.fetchall()
        finally:
            close_db_connection(conn)

        local_by_imdb = {str(row[8]).strip().lower(): row for row in local_rows if row[8]}
        local_by_title_year = {}
        for row in local_rows:
            key = (normalize_title_lookup(row[1]), str(row[2] or '')[:4])
            if key[0] and key[1].isdigit():
                local_by_title_year.setdefault(key, []).append(row)

        results = []
        seen_ids = set()
        for item in ranking:
            imdb_id = str(item.get('imdb_id') or '').strip().lower()
            local_row = local_by_imdb.get(imdb_id) if imdb_id else None
            if not local_row:
                title_key = (normalize_title_lookup(item.get('title')), str(item.get('year') or '')[:4])
                matches = local_by_title_year.get(title_key, [])
                if len(matches) == 1:
                    local_row = matches[0]
                elif len(matches) > 1:
                    continue
            if not local_row:
                continue
            movie_id = local_row[0]
            if movie_id in seen_ids:
                continue
            seen_ids.add(movie_id)
            results.append({
                'id': movie_id,
                'title': local_row[1],
                'year': local_row[2] or '',
                'image': local_row[3] or '/static/miniapp/poster-placeholder.svg',
                'rating': local_row[4] or 'N/A',
                'genre': local_row[5] or 'Unknown',
                'category': local_row[6] or 'Movie',
                'language': local_row[7] or '',
                'source': 'local',
                'tmdb_id': item.get('id'),
                'tmdb_rank': len(results) + 1,
                'tmdb_source': source,
                'cached': cache_state in {'cached', 'stale'},
                'fetched_at': fetched_at,
            })
            if len(results) >= hero_limit:
                break

        return jsonify({
            'status': 'success',
            'source': source,
            'fresh': cache_state == 'fresh',
            'results': results,
            'total_ranked': len(ranking),
            'local_matches': len(results),
            'cached': cache_state in {'cached', 'stale'},
            'fetched_at': fetched_at,
            'hero_limit': hero_limit,
        })

    @flask_app.route('/api/home/new-releases', methods=['GET'])
    def get_home_new_releases():
        cache_key = 'home_new_releases_v1'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)

        with new_releases_refresh_lock:
            cached = api_movies_cache.get(cache_key)
            if cached:
                return jsonify(cached)

            today = datetime.utcnow().date()
            windows = [(30, '30-day'), (60, '60-day fallback')]
            conn = get_db_connection()
            if not conn:
                return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT id, title, year, poster_url, rating, genre, category, language, imdb_id
                    FROM movies
                """)
                local_rows = cur.fetchall()
                cur.close()
            finally:
                close_db_connection(conn)

            local_by_imdb = {str(row[8]).strip().lower(): row for row in local_rows if row[8]}
            local_by_title_year = {}
            local_by_title = {}
            for row in local_rows:
                title_key = normalize_release_title(row[1])
                key = (title_key, str(row[2] or '')[:4])
                if key[0] and key[1].isdigit():
                    local_by_title_year.setdefault(key, []).append(row)
                if title_key:
                    local_by_title.setdefault(title_key, []).append(row)

            selected = []
            enriched = 0
            unmatched = 0
            matched_local_ids = set()
            selected_window_days = None
            discovery_metrics = {
                'discovery_pages': 0,
                'discovery_requests': 0,
                'discovery_cache_hits': 0,
                'discovery_stale_fallbacks': 0
            }
            detail_metrics = {'lock': threading.Lock(), 'requests': 0, 'cache_hits': 0}
            candidate_count = 0
            for days, window_label in windows:
                start = today - timedelta(days=days)
                candidates = []
                for media_type in ('movie', 'tv'):
                    pool = get_release_discovery_pool(media_type, days, today, discovery_metrics)
                    pool_results = pool.get('results') or []
                    candidate_count += len(pool_results)
                    for item in pool_results:
                        title = item.get('title') or item.get('name')
                        date_value = item.get('release_date') or item.get('first_air_date') or ''
                        key = (normalize_release_title(title), str(date_value)[:4])
                        if key in local_by_title_year or key[0] in local_by_title:
                            candidates.append({**item, 'media_type': media_type})

                candidates_by_id = {}
                for candidate in candidates:
                    candidates_by_id[(candidate['media_type'], candidate['id'])] = candidate
                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = [
                        executor.submit(get_release_details, candidate, detail_metrics)
                        for candidate in candidates_by_id.values()
                    ]
                    details = [future.result() for future in as_completed(futures)]
                for detail in details:
                    if not detail:
                        continue
                    row = local_by_imdb.get(str(detail.get('imdb_id') or '').lower())
                    if not row:
                        key = (normalize_release_title(detail['title']), detail['release_date'][:4])
                        matches = local_by_title_year.get(key, [])
                        row = matches[0] if len(matches) == 1 else None
                    if not row:
                        unmatched += 1
                        continue
                    enriched += 1
                    matched_local_ids.add(row[0])
                    release_date = datetime.strptime(detail['release_date'], '%Y-%m-%d').date()
                    if start <= release_date <= today:
                        selected.append((release_date, row, detail))
                if selected:
                    selected_window_days = days
                    break

            unique = {}
            for release_date, row, detail in selected:
                unique[row[0]] = (release_date, row, detail)
            ordered = sorted(unique.values(), key=lambda item: (item[0], item[1][0]), reverse=True)[:12]
            movies = []
            for release_date, row, detail in ordered:
                age = (today - release_date).days
                release_label = 'Released today' if age == 0 else f'Released {age} day{"s" if age != 1 else ""} ago'
                movies.append({
                    'id': row[0], 'title': row[1], 'year': row[2] or '',
                    'image': row[3] or '/static/miniapp/poster-placeholder.svg',
                    'rating': row[4] or 'N/A', 'genre': row[5] or 'Unknown',
                    'category': row[6] or 'Movie', 'language': row[7] or '', 'source': 'local',
                    'release_date': release_date.isoformat(), 'release_label': release_label,
                    'release_type': detail['release_type']
                })
            result = {
                'status': 'success', 'movies': movies, 'window_days': selected_window_days or 60,
                'stats': {'local_catalogue': len(local_rows), 'imdb_matched': len(local_by_imdb),
                          'enriched': enriched, 'unmatched': unmatched,
                          'matched_local_count': len(matched_local_ids),
                          'candidate_count': candidate_count,
                          'discovery_pages': discovery_metrics['discovery_pages'],
                          'discovery_requests': discovery_metrics['discovery_requests'],
                          'discovery_cache_hits': discovery_metrics['discovery_cache_hits'],
                          'discovery_stale_fallbacks': discovery_metrics['discovery_stale_fallbacks'],
                          'tmdb_requests': discovery_metrics['discovery_requests'] + detail_metrics['requests'],
                          'cache_hits': discovery_metrics['discovery_cache_hits'] + detail_metrics['cache_hits']}
            }
            api_movies_cache.set(cache_key, result)
            return jsonify(result)

    def telegram_user_from_request():
        """Validate Telegram WebApp initData and return its signed-in user."""
        init_data = request.headers.get('X-Telegram-Init-Data', '')
        bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        if not init_data or not bot_token:
            return None
        values = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = values.pop('hash', '')
        auth_date_raw = values.get('auth_date', '')
        try:
            auth_date = int(auth_date_raw)
            max_age = int(os.environ.get('TELEGRAM_AUTH_MAX_AGE_SECONDS', '86400'))
        except (TypeError, ValueError):
            return None
        if not received_hash or max_age <= 0:
            return None
        now = time.time()
        if auth_date > now + 60 or now - auth_date > max_age:
            return None
        data_check_string = '\n'.join(f'{key}={value}' for key, value in sorted(values.items()))
        secret_key = hmac.new(b'WebAppData', bot_token.encode(), hashlib.sha256).digest()
        expected_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected_hash, received_hash):
            return None
        try:
            user = json.loads(values.get('user', '{}'))
            return user if user.get('id') else None
        except (TypeError, ValueError):
            return None

    def upsert_miniapp_user(cur, user):
        cur.execute("""
            INSERT INTO miniapp_users (user_id, username, first_name, last_seen)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_seen = CURRENT_TIMESTAMP
        """, (user['id'], user.get('username', ''), user.get('first_name', '')))

    def require_telegram_user():
        user = telegram_user_from_request()
        if not user:
            return None, (jsonify({'status': 'error', 'message': 'Open My List inside Telegram.'}), 401)
        return user, None

    def chat_user_from_request():
        """Return Telegram identity or a stable browser guest identity for public chat."""
        telegram_user = telegram_user_from_request()
        if telegram_user:
            return telegram_user
        guest_token = request.headers.get('X-Guest-Token', '').strip()
        if not re.fullmatch(r'[A-Za-z0-9_-]{16,128}', guest_token):
            return None
        digest = hashlib.sha256(guest_token.encode('utf-8')).digest()
        guest_id = -max(1, int.from_bytes(digest[:8], byteorder='big', signed=False))
        return {
            'id': guest_id,
            'username': '',
            'first_name': 'Guest',
        }

    @flask_app.route('/api/recommendation-events', methods=['POST'])
    def record_recommendation_event_api():
        user, error = require_telegram_user()
        if error:
            return error
        payload = request.get_json(silent=True) or {}
        event_type = str(payload.get('event_type') or '').strip()
        source = str(payload.get('source') or 'miniapp').strip()
        allowed_event_types = {
            'miniapp_open_details', 'miniapp_download', 'watchlist_add',
            'watchlist_remove', 'rating_submitted', 'surprise_impression',
            'surprise_click', 'surprise_skip',
        }
        if event_type not in allowed_event_types:
            return jsonify({'status': 'error', 'message': 'Unsupported recommendation event.'}), 400
        movie_id = payload.get('movie_id')
        if movie_id is not None:
            try:
                movie_id = int(movie_id)
            except (TypeError, ValueError):
                return jsonify({'status': 'error', 'message': 'Invalid movie.'}), 400
            if movie_id <= 0:
                return jsonify({'status': 'error', 'message': 'Invalid movie.'}), 400
        metadata = payload.get('metadata') or {}
        if not isinstance(metadata, dict):
            return jsonify({'status': 'error', 'message': 'Invalid event metadata.'}), 400
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            upsert_miniapp_user(cur, user)
            if movie_id is not None:
                cur.execute("SELECT 1 FROM movies WHERE id = %s", (movie_id,))
                if not cur.fetchone():
                    return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            cur.execute("""
                INSERT INTO user_recommendation_events
                    (user_id, movie_id, event_type, source, metadata)
                VALUES (%s, %s, %s, %s, %s::jsonb)
            """, (user['id'], movie_id, event_type, source[:40],
                  json.dumps(metadata, ensure_ascii=True)))
            conn.commit()
            cur.close()
            return jsonify({'status': 'success'})
        except Exception:
            conn.rollback()
            logger.exception('Recommendation event API failed')
            return jsonify({'status': 'error', 'message': 'Could not record activity.'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/global-chat', methods=['GET', 'POST'])
    def global_chat_api():
        user = chat_user_from_request()
        if not user:
            return jsonify({
                'status': 'error',
                'message': 'Chat identity is unavailable. Refresh the app and try again.',
            }), 400
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            upsert_miniapp_user(cur, user)
            if request.method == 'POST':
                payload = request.get_json(silent=True) or {}
                message = str(payload.get('message') or '').strip()
                if not message or len(message) > 500:
                    return jsonify({'status': 'error', 'message': 'Message must be between 1 and 500 characters.'}), 400
                cur.execute("""
                    INSERT INTO global_chat_messages (user_id, username, first_name, message)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, username, first_name, message, created_at
                """, (user['id'], user.get('username', ''), user.get('first_name', ''), message))
                conn.commit()
            cur.execute("""
                SELECT id, user_id, username, first_name, message, created_at
                FROM global_chat_messages
                ORDER BY created_at DESC, id DESC
                LIMIT 100
            """)
            messages = [{
                'id': row[0], 'user_id': row[1], 'username': row[2] or '',
                'first_name': row[3] or 'User', 'message': row[4],
                'created_at': row[5].isoformat() if row[5] else ''
            } for row in reversed(cur.fetchall())]
            return jsonify({'status': 'success', 'messages': messages})
        except Exception:
            conn.rollback()
            logger.exception('Global chat request failed')
            return jsonify({'status': 'error', 'message': 'Chat is temporarily unavailable.'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/upcoming', methods=['GET'])
    def get_upcoming_titles():
        limit = min(max(request.args.get('limit', 18, type=int), 1), 30)
        today = datetime.utcnow().date()
        cache_key = f'api-upcoming:{today.isoformat()}'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
        results = []
        seen = set()
        end_date = today + timedelta(days=180)
        for media_type in ('movie', 'tv'):
            params = {
                'page': 1,
                'sort_by': 'primary_release_date.asc' if media_type == 'movie' else 'first_air_date.asc',
                'vote_count.gte': 1,
                'with_release_type': '2|3|4',
                'primary_release_date.gte': today.isoformat(),
                'primary_release_date.lte': end_date.isoformat(),
            } if media_type == 'movie' else {
                'page': 1,
                'sort_by': 'first_air_date.asc',
                'vote_count.gte': 1,
                'first_air_date.gte': today.isoformat(),
                'first_air_date.lte': end_date.isoformat(),
            }
            data, _ = tmdb_cached_request(
                f'discover/{media_type}', params,
                f'upcoming:{media_type}:{today.isoformat()}',
                ttl_seconds=6 * 60 * 60
            )
            for item in (data or {}).get('results', []):
                tmdb_id = item.get('id')
                release_date = item.get('release_date') or item.get('first_air_date') or ''
                if not tmdb_id or not release_date or (media_type, tmdb_id) in seen:
                    continue
                seen.add((media_type, tmdb_id))
                results.append({
                    'id': f'tmdb_{media_type}_{tmdb_id}',
                    'tmdb_id': tmdb_id,
                    'title': item.get('title') or item.get('name') or 'Unknown',
                    'year': release_date[:4],
                    'release_date': release_date,
                    'image': f"https://image.tmdb.org/t/p/w500{item['poster_path']}" if item.get('poster_path') else '/static/miniapp/poster-placeholder.svg',
                    'rating': round(float(item.get('vote_average') or 0), 1),
                    'category': 'Movie' if media_type == 'movie' else 'TV Series',
                    'source': 'tmdb',
                    'description': item.get('overview') or ''
                })
        results.sort(key=lambda item: item['release_date'])
        response = {'status': 'success', 'movies': results[:limit], 'updated_at': datetime.utcnow().isoformat()}
        api_movies_cache.set(cache_key, response)
        return jsonify(response)

    def rating_summary(cur, movie_id, user_id):
        cur.execute("""
            SELECT COALESCE(AVG(rating), 0), COUNT(*)
            FROM movie_ratings
            WHERE movie_id = %s
        """, (movie_id,))
        average, count = cur.fetchone()
        cur.execute("""
            SELECT rating FROM movie_ratings
            WHERE movie_id = %s AND user_id = %s
        """, (movie_id, user_id))
        own = cur.fetchone()
        return {
            'average': round(float(average), 1) if count else 0,
            'count': int(count),
            'user_rating': int(own[0]) if own else None,
            'can_rate': own is None
        }

    @flask_app.route('/healthz', methods=['GET', 'HEAD'])
    def healthz():
        """Fast Render health probe; intentionally independent of TMDB/DB."""
        response = jsonify({'status': 'ok', 'service': 'flimfybox-miniapp'})
        response.headers['Cache-Control'] = 'no-store'
        return response, 200

    # Root remains useful for a browser or external uptime monitor.
    @flask_app.route('/', methods=['GET', 'HEAD'])
    def home():
        return redirect('/webapp', code=302)

    @flask_app.route('/api/genres', methods=['GET'])
    def get_genres():
        """Return canonical genres with counts computed from the full catalogue."""
        browse_type = request.args.get('type', 'all').lower()
        if browse_type not in {'all', 'movies', 'tv'}:
            return jsonify({'status': 'error', 'message': 'Invalid genre type'}), 400
        cache_key = f'api_genres_{browse_type}_v1'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT genre, category, seasons_data, poster_url
                FROM movies
                WHERE genre IS NOT NULL AND genre <> ''
                ORDER BY id DESC
            """)
            counts = {genre_id: 0 for genre_id, *_ in CANONICAL_GENRES}
            posters = {genre_id: [] for genre_id, *_ in CANONICAL_GENRES}
            for raw_genre, category, seasons_data, poster_url in cur.fetchall():
                if not browse_type_matches(browse_type, category, seasons_data):
                    continue
                for genre_id in normalize_catalogue_genres(raw_genre):
                    if genre_id in counts:
                        counts[genre_id] += 1
                        if poster_url and poster_url not in posters[genre_id] and len(posters[genre_id]) < 3:
                            posters[genre_id].append(poster_url)
            genres = [
                {
                    'id': genre_id,
                    'label': label,
                    'group': group,
                    'description': description,
                    'count': counts[genre_id],
                    'posters': posters[genre_id]
                }
                for genre_id, label, group, description in CANONICAL_GENRES
            ]
            result = {'status': 'success', 'genres': genres, 'source': 'database'}
            api_movies_cache.set(cache_key, result)
            cur.close()
            return jsonify(result)
        except Exception as e:
            logger.error(f"Genre catalogue error: {e}")
            return jsonify({'status': 'error', 'message': 'Could not load genres'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/genre/<genre_id>', methods=['GET'])
    def get_genre_catalogue(genre_id):
        """Return every real catalogue title mapped to one canonical genre."""
        if genre_id not in GENRE_BY_ID:
            return jsonify({'status': 'error', 'message': 'Unknown genre'}), 404
        browse_type = request.args.get('type', 'all').lower()
        if browse_type not in {'all', 'movies', 'tv'}:
            return jsonify({'status': 'error', 'message': 'Invalid genre type'}), 400
        cache_key = f'api_genre_{genre_id}_{browse_type}_v1'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, title, year, poster_url, rating, genre, category, language, seasons_data
                FROM movies
                ORDER BY id DESC
            """)
            movies = []
            for row in cur.fetchall():
                if genre_id not in normalize_catalogue_genres(row[5]) or not browse_type_matches(browse_type, row[6], row[8]):
                    continue
                movies.append({
                    'id': row[0],
                    'title': row[1],
                    'year': row[2] or '',
                    'image': row[3] or '/static/miniapp/poster-placeholder.svg',
                    'rating': row[4] or 'N/A',
                    'genre': row[5] or '',
                    'category': row[6] or 'Movies',
                    'language': row[7] or '',
                    'source': 'local'
                })
            result = {
                'status': 'success',
                'genre': {
                    'id': genre_id,
                    'label': GENRE_BY_ID[genre_id][1],
                    'group': GENRE_BY_ID[genre_id][2],
                    'description': GENRE_BY_ID[genre_id][3],
                    'count': len(movies)
                },
                'movies': movies,
                'source': 'database'
            }
            api_movies_cache.set(cache_key, result)
            cur.close()
            return jsonify(result)
        except Exception as e:
            logger.error(f"Genre detail error for %s: %s", genre_id, e)
            return jsonify({'status': 'error', 'message': 'Could not load genre catalogue'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/browse', methods=['GET'])
    def get_browse():
        browse_type = request.args.get('type', 'all').lower()
        if browse_type not in {'all', 'movies', 'tv'}:
            return jsonify({'status': 'error', 'message': 'Invalid browse type'}), 400
        cache_key = f'api_browse_{browse_type}_v1'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, title, year, poster_url, rating, genre, category, language, seasons_data
                FROM movies
                WHERE poster_url IS NOT NULL AND poster_url <> '' AND year IS NOT NULL
                ORDER BY year DESC NULLS LAST, id DESC
            """)
            rows = cur.fetchall()
            decades = [
                ('2020s', '2020s Standouts', 2020, 2029),
                ('2010s', '2010s Essentials', 2010, 2019),
                ('2000s', '2000s Classics', 2000, 2009),
                ('pre-2000', 'Golden Oldies', None, 1999)
            ]
            collections = []
            for decade_id, label, start_year, end_year in decades:
                items = []
                for row in rows:
                    year = int(row[2] or 0)
                    if (start_year is not None and not (start_year <= year <= end_year)) or (start_year is None and year > end_year):
                        continue
                    if not browse_type_matches(browse_type, row[6], row[8]):
                        continue
                    items.append({
                        'id': row[0], 'title': row[1], 'year': year,
                        'image': row[3], 'rating': row[4] or 'N/A',
                        'genre': row[5] or '', 'category': row[6] or 'Movies',
                        'language': row[7] or '', 'source': 'local'
                    })
                items.sort(key=lambda item: (float(item['rating']) if str(item['rating']).replace('.', '', 1).isdigit() else -1, item['year'], item['id']), reverse=True)
                collections.append({'id': decade_id, 'label': label, 'movies': items[:12]})
            result = {'status': 'success', 'type': browse_type, 'collections': collections, 'source': 'database'}
            api_movies_cache.set(cache_key, result)
            cur.close()
            return jsonify(result)
        except Exception as error:
            logger.error('Browse catalogue error: %s', error)
            return jsonify({'status': 'error', 'message': 'Could not load Browse'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/browse/surprise', methods=['GET'])
    def get_browse_surprise():
        browse_type = request.args.get('type', 'all').lower()
        if browse_type not in {'all', 'movies', 'tv'}:
            return jsonify({'status': 'error', 'message': 'Invalid browse type'}), 400
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            classification_sql = "category = 'Web Series' OR (seasons_data IS NOT NULL AND seasons_data <> '{}'::jsonb)"
            where_type = '' if browse_type == 'all' else (f' AND ({classification_sql})' if browse_type == 'tv' else f' AND NOT ({classification_sql})')
            signed_in_user = telegram_user_from_request()
            if signed_in_user:
                cur.execute("""
                    SELECT m.genre, m.language, m.category, e.event_type, COUNT(*)
                    FROM user_recommendation_events e
                    JOIN movies m ON m.id = e.movie_id
                    WHERE e.user_id = %s
                      AND e.created_at >= CURRENT_TIMESTAMP - INTERVAL '180 days'
                    GROUP BY m.genre, m.language, m.category, e.event_type
                """, (signed_in_user['id'],))
                profile_rows = cur.fetchall()
                event_weights = {
                    'rating_submitted': 5,
                    'watchlist_add': 4,
                    'miniapp_download': 4,
                    'pm_exact_match': 3,
                    'miniapp_open_details': 2,
                    'group_selection': 2,
                    'surprise_click': 2,
                    'pm_search': 1,
                    'group_search': 1,
                }
                genre_scores = {}
                language_scores = {}
                category_scores = {}
                for genre, language, category, event_type, count in profile_rows:
                    weight = event_weights.get(event_type, 1) * count
                    for token in re.split(r'[,/&|]', (genre or '').lower()):
                        token = token.strip()
                        if token:
                            genre_scores[token] = genre_scores.get(token, 0) + weight
                    language_key = (language or '').strip().lower()
                    if language_key:
                        language_scores[language_key] = language_scores.get(language_key, 0) + weight
                    category_key = (category or '').strip().lower()
                    if category_key:
                        category_scores[category_key] = category_scores.get(category_key, 0) + weight
                cur.execute("""
                    SELECT movie_id
                    FROM user_recommendation_events
                    WHERE user_id = %s
                      AND event_type IN ('surprise_impression', 'surprise_click')
                      AND created_at >= CURRENT_TIMESTAMP - INTERVAL '14 days'
                    ORDER BY created_at DESC
                    LIMIT 80
                """, (signed_in_user['id'],))
                recent_ids = {row[0] for row in cur.fetchall()}
                cur.execute(f"""
                    SELECT m.id, m.title, m.year, m.poster_url, m.rating, m.genre, m.category, m.language
                    FROM movies m
                    WHERE m.poster_url IS NOT NULL AND m.poster_url <> ''
                      AND (
                          (m.file_id IS NOT NULL AND m.file_id <> '')
                          OR EXISTS (SELECT 1 FROM movie_files mf WHERE mf.movie_id = m.id
                                    AND (mf.file_id IS NOT NULL OR mf.url IS NOT NULL))
                      ){where_type}
                    ORDER BY m.id DESC
                    LIMIT 500
                """)
                candidates = []
                for row in cur.fetchall():
                    movie_id, title, year, poster, rating, genre, category, language = row
                    if movie_id in recent_ids:
                        continue
                    score = 0
                    for token in re.split(r'[,/&|]', (genre or '').lower()):
                        score += genre_scores.get(token.strip(), 0)
                    score += language_scores.get((language or '').strip().lower(), 0)
                    score += category_scores.get((category or '').strip().lower(), 0)
                    rating_score = float(rating) if str(rating or '').replace('.', '', 1).isdigit() else 0
                    candidates.append((score + rating_score * 0.25, row))
                if candidates:
                    candidates.sort(key=lambda item: (item[0], item[1][0]), reverse=True)
                    top_score = candidates[0][0]
                    shortlist = [item[1] for item in candidates if item[0] >= top_score - 3][:20]
                    row = random.choice(shortlist)
                    movie = {
                        'id': row[0], 'title': row[1], 'year': row[2] or '',
                        'image': row[3], 'rating': row[4] or 'N/A', 'genre': row[5] or '',
                        'category': row[6] or 'Movies', 'language': row[7] or '', 'source': 'local',
                    }
                    cur.close()
                    return jsonify({
                        'status': 'success', 'type': browse_type, 'movie': movie,
                        'personalized': bool(profile_rows),
                    })
            cur.execute(f"""
                SELECT COUNT(*) FROM movies
                WHERE poster_url IS NOT NULL AND poster_url <> ''{where_type}
            """)
            total = cur.fetchone()[0]
            if not total:
                return jsonify({'status': 'success', 'type': browse_type, 'movie': None, 'message': 'No eligible local titles are available.'})
            offset = random.randrange(total)
            cur.execute(f"""
                SELECT id, title, year, poster_url, rating, genre, category, language
                FROM movies
                WHERE poster_url IS NOT NULL AND poster_url <> ''{where_type}
                ORDER BY id
                LIMIT 1 OFFSET %s
            """, (offset,))
            row = cur.fetchone()
            movie = {
                'id': row[0], 'title': row[1], 'year': row[2] or '',
                'image': row[3], 'rating': row[4] or 'N/A', 'genre': row[5] or '',
                'category': row[6] or 'Movies', 'language': row[7] or '', 'source': 'local'
            } if row else None
            cur.close()
            return jsonify({'status': 'success', 'type': browse_type, 'movie': movie})
        except Exception as error:
            logger.error('Browse surprise error: %s', error)
            return jsonify({'status': 'error', 'message': 'Could not select a title'}), 500
        finally:
            close_db_connection(conn)
    
    @flask_app.route('/api/movies', methods=['GET'])
    def get_movies():
        """
        Return list of movies with pagination (Infinite Scroll).
        """
        try:
            page = int(request.args.get('page', '1'))
            limit = int(request.args.get('limit', '40'))
        except (TypeError, ValueError):
            return jsonify({
                'status': 'error',
                'message': 'Page and limit must be integers.'
            }), 400
        if page < 1 or limit < 1 or limit > 100:
            return jsonify({
                'status': 'error',
                'message': 'Page must be positive and limit must be between 1 and 100.'
            }), 400
        
        cache_key = f"api_movies_{page}_{limit}"
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
            
        offset = (page - 1) * limit
    
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, title, year, poster_url, rating, genre, category,
                       COALESCE(language, '') as language, created_at
                FROM movies
                WHERE poster_url IS NOT NULL AND poster_url != ''
                ORDER BY created_at DESC NULLS LAST, id DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            
            rows = cur.fetchall()
            movies = []
            for r in rows:
                movies.append({
                    'id': r[0],
                    'title': r[1],
                    'year': r[2] if r[2] else '',
                    'image': r[3] if r[3] else '/static/miniapp/poster-placeholder.svg',
                    'rating': r[4] if r[4] else 'N/A',
                    'genre': r[5] if r[5] else 'Unknown',
                    'category': r[6] if r[6] else 'Movie',
                    'language': r[7],
                    'created_at': r[8].isoformat() if r[8] else None
                })
            cur.close()
            close_db_connection(conn)
            
            # Check if more movies exist
            has_more = len(movies) == limit 
            
            result = {'status': 'success', 'movies': movies, 'has_more': has_more}
            api_movies_cache.set(cache_key, result)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Error in /api/movies: {e}")
            close_db_connection(conn)
            return jsonify({'status': 'error', 'message': 'Could not load movies'}), 500
    
    
    @flask_app.route('/api/movie/<int:movie_id>/rating', methods=['GET'])
    def get_movie_rating(movie_id):
        user, error = require_telegram_user()
        if error:
            return error
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM movies WHERE id = %s", (movie_id,))
            if not cur.fetchone():
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            result = rating_summary(cur, movie_id, user['id'])
            conn.commit()
            cur.close()
            return jsonify({'status': 'success', **result})
        except Exception as exc:
            logger.error(f'Rating summary error for movie {movie_id}: {exc}')
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Rating temporarily unavailable'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/movie/<int:movie_id>/rating', methods=['POST'])
    def submit_movie_rating(movie_id):
        user, error = require_telegram_user()
        if error:
            return error
        data = request.get_json(silent=True) or {}
        rating = data.get('rating')
        if isinstance(rating, bool) or not isinstance(rating, int) or rating < 1 or rating > 5:
            return jsonify({'status': 'error', 'message': 'Rating must be an integer from 1 to 5.'}), 400
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM movies WHERE id = %s", (movie_id,))
            if not cur.fetchone():
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            upsert_miniapp_user(cur, user)
            try:
                cur.execute("""
                    INSERT INTO movie_ratings (movie_id, user_id, rating)
                    VALUES (%s, %s, %s)
                """, (movie_id, user['id'], rating))
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                return jsonify({'status': 'error', 'message': 'You have already rated this title.'}), 409
            result = rating_summary(cur, movie_id, user['id'])
            conn.commit()
            cur.close()
            return jsonify({'status': 'success', **result})
        except Exception as exc:
            logger.error(f'Rating submission error for movie {movie_id}: {exc}')
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Could not save rating'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/movie/<int:movie_id>', methods=['GET'])
    def get_movie_details(movie_id):
        if movie_id <= 0:
            return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
        # Details include live movie_files rows. Do not serve a stale cached
        # response that was generated before files finished being ingested.
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, title, year, poster_url, rating, genre, description, category, language, "cast", trailer_key, seasons_data
                FROM movies WHERE id = %s
            """, (movie_id,))
            row = cur.fetchone()
            if not row:
                cur.close()
                close_db_connection(conn)
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
    
            movie = {
                'id': row[0],
                'title': row[1],
                'year': row[2] if row[2] else '',
                'image': row[3] if row[3] else '/static/miniapp/poster-placeholder.svg',
                'rating': row[4] if row[4] else 'N/A',
                'genre': row[5] if row[5] else 'Unknown',
                'description': row[6] if row[6] else 'No description available.',
                'category': row[7] if row[7] else 'Movie',
                'language': row[8] if row[8] else '',
                'cast': row[9] if row[9] else '',
                'trailer_key': row[10] if row[10] else None,
                'seasons_data': row[11] if len(row) > 11 and row[11] else {}
            }
    
            # Get files
            # Updated to fetch extra_info for Season/Episode parsing
            cur.execute("""
                SELECT id, quality, file_size, COALESCE(extra_info, '')
                FROM movie_files
                WHERE movie_id = %s
                ORDER BY id ASC
            """, (movie_id,))
            files = [
                {'id': f[0], 'quality': f[1], 'size': f[2], 'extra_info': f[3] if len(f) > 3 else ''}
                for f in cur.fetchall()
            ]
            movie['files'] = files
    
            cur.close()
            close_db_connection(conn)
    
            # Keep details responsive: remote trailer/backdrop enrichment must not
            # block the local detail response. A poster is a reliable backdrop
            # fallback, while saved trailer keys remain available immediately.
            movie['backdrop'] = movie['image']

            result = {'status': 'success', 'movie': movie}
            return jsonify(result)
        except Exception as e:
            logger.error(f"Error in /api/movie/{movie_id}: {e}")
            close_db_connection(conn)
            return jsonify({'status': 'error', 'message': 'Could not load movie details'}), 500

    @flask_app.route('/api/search', methods=['GET'])
    def search_movies_api():
        query = request.args.get('q', '').strip()
        if not query:
            return jsonify({'status': 'error', 'message': 'Missing query'}), 400
        if len(query) > 200:
            return jsonify({'status': 'error', 'message': 'Query is too long'}), 400
    
        cache_key = f"api_search_{query}"
        cached = search_cache.get(cache_key)
        if cached:
            return jsonify(cached)
    
        conn = get_db_connection()
        local_results = []
        # Retain the local catalogue candidates so TMDB results can be mapped
        # back to their available local record after a misspelled search.
        catalog_candidates = []
        if not conn:
            # Do not label TMDB titles as request-only just because the local
            # catalogue connection is temporarily unavailable.
            return jsonify({
                'status': 'error',
                'message': 'Catalogue is temporarily unavailable. Please try again.'
            }), 503
        try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT id, title, year, poster_url, rating, genre, category
                    FROM movies
                    WHERE title ILIKE %s OR title ILIKE %s
                    LIMIT 20
                """, (f'%{query}%', f'%{query.replace(" ", "%")}%'))
                rows = cur.fetchall()
                for r in rows:
                    local_results.append({
                        'id': r[0],
                        'title': r[1],
                        'year': r[2] if r[2] else '',
                        'image': r[3] if r[3] else '/static/miniapp/poster-placeholder.svg',
                        'rating': r[4] if r[4] else 'N/A',
                        'genre': r[5] if r[5] else 'Unknown',
                        'category': r[6] if r[6] else 'Movie',
                        'source': 'local'
                    })
                # A typo such as "rechar" has no ILIKE match, so score the
                # local titles before declaring it unavailable.
                if not local_results:
                    cur.execute("""
                        SELECT id, title, year, poster_url, rating, genre, category
                        FROM movies WHERE title IS NOT NULL
                    """)
                    candidates = cur.fetchall()
                    catalog_candidates = candidates
                    titles = [row[1] for row in candidates]
                    for _, score, index in process.extract(query, titles, scorer=fuzz.WRatio, limit=8):
                        if score < 58:
                            continue
                        r = candidates[index]
                        local_results.append({
                            'id': r[0], 'title': r[1], 'year': r[2] if r[2] else '',
                            'image': r[3] if r[3] else '/static/miniapp/poster-placeholder.svg',
                            'rating': r[4] if r[4] else 'N/A', 'genre': r[5] if r[5] else 'Unknown',
                            'category': r[6] if r[6] else 'Movie', 'source': 'local'
                        })
                cur.close()
        except Exception as e:
            logger.error(f"Local search error: {e}")
            return jsonify({
                'status': 'error',
                'message': 'Catalogue search failed. Please try again.'
            }), 500
        finally:
            close_db_connection(conn)
    
        tmdb_results = []
        if len(local_results) < 15:
            try:
                # Retry TMDB with server-side Google suggestions. This works in
                # Telegram WebView too, unlike a browser-side JSONP callback.
                search_terms = [query]
                if not local_results:
                    suggest_url = 'https://suggestqueries.google.com/complete/search'
                    suggest_response = requests.get(
                        suggest_url,
                        params={'client': 'firefox', 'q': f'{query} movie'},
                        headers={'User-Agent': 'Mozilla/5.0'}, timeout=3
                    ).json()
                    for suggestion in (suggest_response[1] if len(suggest_response) > 1 else [])[:6]:
                        # Suggestions like "Reacher movie cast" are Google
                        # query completions, not actual title names.
                        corrected = re.sub(
                            r'\s+(?:movie|film|series|web\s+series)(?:\s+(?:cast|trailer|release\s+date|review|episodes?|season\s*\d+|\d{4}))*\s*$',
                            '', str(suggestion), flags=re.I
                        ).strip()
                        if (
                            corrected
                            and fuzz.WRatio(query, corrected) >= 60
                            and corrected.lower() not in {term.lower() for term in search_terms}
                        ):
                            search_terms.append(corrected)
                        if len(search_terms) >= 4:
                            break
                for search_term in search_terms:
                    tmdb_url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(search_term)}"
                    resp = requests.get(tmdb_url, timeout=5).json()
                    for item in resp.get('results', [])[:8]:
                        img_path = item.get('poster_path') or item.get('backdrop_path')
                        if not img_path:
                            continue
                        tmdb_results.append({
                            'id': 'tmdb_' + str(item['id']),
                            'title': item.get('title') or item.get('name') or 'Unknown',
                            'year': (item.get('release_date') or item.get('first_air_date') or '')[:4],
                            'image': f"https://image.tmdb.org/t/p/w500{img_path}",
                            'rating': round(item.get('vote_average', 0), 1),
                            'genre': 'Action, Drama',
                            'category': 'Movie' if item.get('media_type') == 'movie' else 'TV Series',
                            'source': 'tmdb',
                            'description': item.get('overview', '')
                        })
            except Exception as e:
                logger.error(f"TMDB search error: {e}")
    
        # Deduplicate: normalize title (remove punctuation, spaces, lowercase)
        def normalize_title(t):
            return re.sub(r'[^\w\s]', '', t).lower().replace(" ", "")

        # The user may type "Dhurandra", while TMDB correctly returns
        # "Dhurandhar". If that TMDB title is effectively the same as a local
        # title, use the local record and mark it Available—not Request.
        if catalog_candidates and tmdb_results:
            candidate_titles = [row[1] for row in catalog_candidates]
            already_local_ids = {movie['id'] for movie in local_results}
            for tmdb_movie in tmdb_results:
                best = process.extractOne(tmdb_movie['title'], candidate_titles, scorer=fuzz.ratio)
                if not best:
                    continue
                _, score, index = best
                if score < 88:
                    continue
                row = catalog_candidates[index]
                local_year = str(row[2] or '')[:4]
                tmdb_year = str(tmdb_movie.get('year') or '')[:4]
                if local_year.isdigit() and tmdb_year.isdigit() and abs(int(local_year) - int(tmdb_year)) > 1:
                    continue
                if row[0] not in already_local_ids:
                    local_results.append({
                        'id': row[0], 'title': row[1], 'year': row[2] if row[2] else '',
                        'image': row[3] or tmdb_movie['image'],
                        'rating': row[4] if row[4] else 'N/A',
                        'genre': row[5] if row[5] else 'Unknown',
                        'category': row[6] if row[6] else 'Movie', 'source': 'local'
                    })
                    already_local_ids.add(row[0])
    
        seen = set()
        combined = []
        # Local movies first
        for m in local_results:
            key = normalize_title(m['title'])
            if key not in seen:
                seen.add(key)
                combined.append(m)
        # Then TMDB movies (only if not already seen)
        for m in tmdb_results:
            key = normalize_title(m['title'])
            if key not in seen and len(combined) < 30:
                seen.add(key)
                combined.append(m)
    
        result = {'status': 'success', 'results': combined}
        search_cache.set(cache_key, result)
        return jsonify(result)
    
    
    @flask_app.route('/api/request', methods=['POST'])
    def request_movie_api():
        """
        Store a user request from web app AND Notify Admin.
        """
        data = request.get_json(silent=True) or {}
        if not data or 'title' not in data:
            return jsonify({'status': 'error', 'message': 'Missing movie title'}), 400
        
        title = data.get('title')
        if not isinstance(title, str):
            return jsonify({'status': 'error', 'message': 'Movie title must be text'}), 400
        title = title.strip()[:200]
        if not title:
            return jsonify({'status': 'error', 'message': 'Missing movie title'}), 400
        authenticated_user = telegram_user_from_request()
        if authenticated_user:
            user_id = authenticated_user['id']
            username = authenticated_user.get('username', '')
            first_name = authenticated_user.get('first_name', 'WebApp User')
        else:
            user_id = 0
            username = ''
            first_name = 'WebApp User'
        
        success = store_user_request(user_id, username, first_name, title, None, None)
        
        if success:
            # 🔥 FIX: Web App se aayi request ko turant Admin Channel me send karein
            bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
            request_channel = os.environ.get('REQUEST_CHANNEL_ID')
            
            if bot_token and request_channel:
                try:
                    # Beautiful Admin Notification Format
                    msg_text = (
                        f"🎬 <b>New WebApp Request!</b> 🎬\n\n"
                        f"Movie: <b>{title}</b>\n"
                        f"User: {first_name} (<code>{user_id}</code>)\n"
                    )
                    if username:
                        msg_text += f"Username: @{username}\n"
                    msg_text += f"From: 🌐 Web Portal"
    
                    # Inline Buttons for Admin
                    short_title = title[:15].replace('_', ' ')
                    reply_markup = {
                        "inline_keyboard": [
                            [{"text": "✅ Movie Add Kar Di Gai Hai", "callback_data": f"reqA_{user_id}_{short_title}"}],
                            [{"text": "❌ Nahi Mili", "callback_data": f"reqN_{user_id}_{short_title}"}]
                        ]
                    }
                    
                    # Direct Telegram API Call
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    payload = {
                        "chat_id": request_channel,
                        "text": msg_text,
                        "parse_mode": "HTML",
                        "reply_markup": reply_markup
                    }
                    requests.post(url, json=payload, timeout=5)
                except Exception as e:
                    logger.error(f"Failed to notify admin from WebApp: {e}")
    
            return jsonify({'status': 'success', 'message': 'Request saved & Admin Notified'})
        else:
            return jsonify({'status': 'error', 'message': 'Could not save request'}), 500

    @flask_app.route('/api/my-list', methods=['GET'])
    def get_my_list():
        user, error = require_telegram_user()
        if error:
            return error
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            upsert_miniapp_user(cur, user)
            cur.execute("""
                SELECT m.id, m.title, m.year, m.poster_url, m.rating, m.genre, m.category
                FROM user_watchlist w
                JOIN movies m ON m.id = w.movie_id
                WHERE w.user_id = %s
                ORDER BY w.created_at DESC
            """, (user['id'],))
            movies = [
                {
                    'id': row[0], 'title': row[1], 'year': row[2] or '',
                    'image': row[3] or '/static/miniapp/poster-placeholder.svg',
                    'rating': row[4] or 'N/A', 'genre': row[5] or 'Unknown',
                    'category': row[6] or 'Movie', 'source': 'local'
                }
                for row in cur.fetchall()
            ]
            conn.commit()
            cur.close()
            return jsonify({'status': 'success', 'movies': movies})
        except Exception as e:
            logger.error(f"My List fetch error: {e}")
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Could not load My List'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/my-list', methods=['POST'])
    def add_to_my_list():
        user, error = require_telegram_user()
        if error:
            return error
        data = request.get_json(silent=True) or {}
        try:
            movie_id = int(data.get('movie_id'))
        except (TypeError, ValueError):
            return jsonify({'status': 'error', 'message': 'Invalid movie'}), 400
        if movie_id <= 0:
            return jsonify({'status': 'error', 'message': 'Invalid movie'}), 400
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            upsert_miniapp_user(cur, user)
            cur.execute("SELECT 1 FROM movies WHERE id = %s", (movie_id,))
            if not cur.fetchone():
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            cur.execute("""
                INSERT INTO user_watchlist (user_id, movie_id)
                VALUES (%s, %s) ON CONFLICT (user_id, movie_id) DO NOTHING
            """, (user['id'], movie_id))
            conn.commit()
            cur.close()
            return jsonify({'status': 'success', 'saved': True, 'movie_id': movie_id})
        except Exception as e:
            logger.error(f"My List add error: {e}")
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Could not save to My List'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/my-list/<int:movie_id>/status', methods=['GET'])
    def get_my_list_status(movie_id):
        user, error = require_telegram_user()
        if error:
            return error
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM user_watchlist
                    WHERE user_id = %s AND movie_id = %s
                )
            """, (user['id'], movie_id))
            saved = bool(cur.fetchone()[0])
            cur.close()
            return jsonify({'status': 'success', 'saved': saved})
        except Exception:
            logger.exception("My List status error for movie %s", movie_id)
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Could not check My List'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/my-list/<int:movie_id>', methods=['DELETE'])
    def remove_from_my_list(movie_id):
        user, error = require_telegram_user()
        if error:
            return error
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM user_watchlist WHERE user_id = %s AND movie_id = %s", (user['id'], movie_id))
            conn.commit()
            cur.close()
            return jsonify({'status': 'success', 'saved': False, 'movie_id': movie_id})
        except Exception as e:
            logger.error(f"My List removal error: {e}")
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Could not update My List'}), 500
        finally:
            close_db_connection(conn)
    
    # 🤖 GOOGLE AUTO-SUGGEST PROXY (Spelling Fixer)
    @flask_app.route('/api/suggest', methods=['GET'])
    def get_suggestions():
        q = request.args.get('q', '').strip()
        if not q:
            return jsonify([])
        try:
            # Firefox client wali API direct JSON list deti hai, jo use karne me aasan hai
            url = f"https://suggestqueries.google.com/complete/search?client=firefox&q={quote(q + ' movie')}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            resp = requests.get(url, headers=headers, timeout=3).json()
            
            # resp ka format: ["query", ["suggestion1", "suggestion2", ...]]
            suggestions = resp[1] if len(resp) > 1 else []
            
            # Keep the provider's useful title corrections, while removing
            # common search-intent suffixes and duplicates.
            clean_suggs = []
            for suggestion in suggestions:
                title = re.sub(
                    r'\s+(?:movie|film|series|web\s+series)(?:\s+(?:cast|trailer|release\s+date|review|episodes?|season\s*\d+|\d{4}))*\s*$',
                    '', str(suggestion), flags=re.I
                ).strip()
                if title and title.lower() not in {x.lower() for x in clean_suggs}:
                    clean_suggs.append(title)
            clean_suggs = clean_suggs[:6]
            return jsonify(clean_suggs)
        except Exception as e:
            logger.error(f"Suggest API Error: {e}")
            return jsonify([])
    
    @flask_app.route('/api/smart-merge', methods=['POST'])
    def smart_merge_api():
        """Hybrid Search Endpoint: Checks Local DB first (raw_query top priority), then fetches TMDB concurrently."""
        import re as re_mod
        data = request.json or {}
        queries = data.get('queries', [])
        raw_query = data.get('raw_query', '').strip()
        
        if not queries and not raw_query:
            return jsonify({'status': 'success', 'results': []})
        
        def normalize(s):
            """Strip special chars for comparison: 'Avengers: Infinity War' -> 'avengers infinity war'"""
            return re_mod.sub(r'[^a-z0-9\s]', '', s.lower()).strip()
        
        conn = get_db_connection()
        local_results = []
        found_normalized = set()
        
        # Define search queries (raw_query top priority)
        search_queries = []
        if raw_query:
            search_queries.append(raw_query)
        search_queries.extend([q for q in queries if q != raw_query])
        
        if conn:
            try:
                cur = conn.cursor()
                # Fuzzy search: Use % wildcards around each word for flexible matching
                for q in search_queries:
                    words = q.split()
                    if not words:
                        continue
                    # Build a LIKE pattern: %word1%word2%word3%
                    like_pattern = '%' + '%'.join(words) + '%'
                    cur.execute("""
                        SELECT id, title, year, poster_url, rating, genre, category 
                        FROM movies 
                        WHERE title ILIKE %s
                        LIMIT 1
                    """, (like_pattern,))
                    row = cur.fetchone()
                    if row:
                        title = row[1]
                        norm_title = normalize(title)
                        if norm_title not in found_normalized:
                            found_normalized.add(norm_title)
                            local_results.append({
                                'id': row[0],
                                'title': title,
                                'year': row[2] if row[2] else '',
                                'image': row[3] if row[3] else '/static/miniapp/poster-placeholder.svg',
                                'rating': row[4] if row[4] else 'N/A',
                                'genre': row[5] if row[5] else 'Unknown',
                                'category': row[6] if row[6] else 'Movie',
                                'source': 'local',
                                '_query': q  # Track which query matched
                            })
                cur.close()
            except Exception as e:
                logger.error(f"Error in smart_merge local check: {e}")
            finally:
                close_db_connection(conn)
    
        # Find which queries still need TMDB lookup
        matched_queries = {r['_query'] for r in local_results}
        missing_queries = [q for q in search_queries if q not in matched_queries]
        tmdb_results = []
        
        def fetch_tmdb(q):
            try:
                url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(q)}"
                resp = requests.get(url, timeout=3).json()
                results = resp.get('results', [])
                if results:
                    item = results[0]
                    img_path = item.get('poster_path') or item.get('backdrop_path')
                    if img_path:
                        return {
                            'id': 'tmdb_' + str(item['id']),
                            'title': item.get('title') or item.get('name') or 'Unknown',
                            'year': (item.get('release_date') or item.get('first_air_date') or '')[:4],
                            'image': f"https://image.tmdb.org/t/p/w500{img_path}",
                            'rating': round(item.get('vote_average', 0), 1),
                            'genre': 'Action, Drama',
                            'category': 'Movie' if item.get('media_type') == 'movie' else 'TV Series',
                            'source': 'tmdb',
                            'description': item.get('overview', ''),
                            '_query': q
                        }
            except Exception as e:
                logger.error(f"TMDB fetch error for {q}: {e}")
            return None
    
        # Fetch concurrently for maximum speed
        if missing_queries:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_tmdb, q): q for q in missing_queries}
                for future in concurrent.futures.as_completed(futures):
                    res = future.result()
                    if res:
                        tmdb_results.append(res)
        
        # Build final results ordered by original query order (raw_query first)
        all_fetched = local_results + tmdb_results
        final_results = []
        seen_ids = set()
        
        for q in search_queries:
            # Find the result that was fetched for this query
            match = next((r for r in all_fetched if r.get('_query') == q), None)
            if match and match['id'] not in seen_ids:
                seen_ids.add(match['id'])
                # Remove internal tracking key before sending to frontend
                result_copy = {k: v for k, v in match.items() if k != '_query'}
                final_results.append(result_copy)
                    
        return jsonify({'status': 'success', 'results': final_results})
    
    @flask_app.route('/api/imdb_id/<int:tmdb_id>', methods=['GET'])
    def get_imdb_id(tmdb_id):
        """Fetch IMDB ID from TMDB API for the Web Player streamimdb.ru requirement."""
        try:
            url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}"
            resp = requests.get(url, timeout=5).json()
            imdb_id = resp.get('imdb_id')
            
            if imdb_id:
                return jsonify({'status': 'success', 'imdb_id': imdb_id})
            
            # Fallback to TV show if movie returns no IMDB ID
            url_tv = f"https://api.themoviedb.org/3/tv/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}"
            resp_tv = requests.get(url_tv, timeout=5).json()
            imdb_id_tv = resp_tv.get('imdb_id')
            
            if imdb_id_tv:
                 return jsonify({'status': 'success', 'imdb_id': imdb_id_tv})
                 
            return jsonify({'status': 'error', 'message': 'No IMDB ID found'}), 404
        except Exception as e:
            logger.error(f"Error fetching IMDB ID for tmdb_{tmdb_id}: {e}")
            return jsonify({'status': 'error', 'message': 'Could not fetch external movie details'}), 500
    
    # ==================== MAIN WEB APP PAGE (Premium HTML) ====================
    
    # 🛡️ MIDDLEMAN REDIRECT PAGE (Anti-Bot)
    @flask_app.route('/watch/<int:movie_id>')
    @flask_app.route('/watch/<int:movie_id>/file/<int:movie_file_id>')
    def secure_watch(movie_id, movie_file_id=None):
        if movie_id <= 0 or (movie_file_id is not None and movie_file_id <= 0):
            return jsonify({'status': 'error', 'message': 'Invalid movie or file'}), 400
        # Yeh HTML page user ko dikhega. Bots JS run nahi kar pate.
        html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>FlimfyBox - Verifying Secure Connection...</title>
            <style>
                body { background: #09090b; color: white; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; font-family: sans-serif; }
                .loader { border: 4px solid rgba(255,255,255,0.1); border-top: 4px solid #f43f5e; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin-bottom: 20px; }
                @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
            </style>
        </head>
        <body>
            <div class="loader"></div>
            <h3>Securely verifying your connection...</h3>
            <p style="color: #a1a1aa; font-size: 13px;">Please wait 2 seconds. You will be redirected automatically.</p>
            
            <script>
                // Invisible JS Challenge
                setTimeout(() => {
                    fetch('/api/gen_link/""" + str(movie_id) + (f"/file/{movie_file_id}" if movie_file_id else "") + """', { method: 'POST' })
                    .then(response => response.json())
                    .then(data => {
                        if(data.url) {
                            window.location.href = data.url; 
                        } else {
                            document.body.innerHTML = "<h3>❌ Server Error. Please try again.</h3>";
                        }
                    }).catch(e => {
                        document.body.innerHTML = "<h3>❌ Connection failed.</h3>";
                    });
                }, 1500); 
            </script>
        </body>
        </html>
        """
        return html
    
    # 🔐 SECRET LINK GENERATOR API (Auto Delete Logic)
    @flask_app.route('/api/gen_link/<int:movie_id>', methods=['POST'])
    @flask_app.route('/api/gen_link/<int:movie_id>/file/<int:movie_file_id>', methods=['POST'])
    def gen_secure_link(movie_id, movie_file_id=None):
        if movie_id <= 0 or (movie_file_id is not None and movie_file_id <= 0):
            return jsonify({'status': 'error', 'message': 'Invalid movie or file'}), 400
        token = "tmp_" + secrets.token_hex(6)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            # Delete old tokens (1 minute se purane)
            cur.execute("DELETE FROM temp_links WHERE created_at < NOW() - INTERVAL '1 minute'")
            if movie_file_id:
                cur.execute(
                    "SELECT 1 FROM movie_files WHERE id = %s AND movie_id = %s",
                    (movie_file_id, movie_id)
                )
                if not cur.fetchone():
                    return jsonify({'status': 'error', 'message': 'Selected file not found'}), 404
            else:
                cur.execute("SELECT 1 FROM movies WHERE id = %s", (movie_id,))
                if not cur.fetchone():
                    return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            # Save a short-lived token for this exact movie/file pair.
            cur.execute(
                "INSERT INTO temp_links (token, movie_id, movie_file_id) VALUES (%s, %s, %s)",
                (token, movie_id, movie_file_id)
            )
            conn.commit()
            cur.close()
        except Exception as e:
            conn.rollback()
            logger.error(f"Token Error: {e}")
            return jsonify({'status': 'error', 'message': 'Could not create secure link'}), 500
        finally:
            close_db_connection(conn)
                
        bot_username = os.environ.get('BOT_USERNAME', 'FlimfyBoxBot')
        tg_url = f"tg://resolve?domain={bot_username}&start={token}"
        return jsonify({"url": tg_url})
    
    @flask_app.route('/webapp')
    def serve_mini_app():
        return render_template("mini_app.html")
    
    # ==================== RUN FLASK ====================
    
