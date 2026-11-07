from flask import Blueprint, jsonify, request, send_file, render_template, redirect
from flask_cors import CORS
import os
import logging
import json
import math
import psycopg2
import functools
from datetime import datetime, timedelta, timezone
import requests
import time
from html import escape as html_escape, unescape as html_unescape
from urllib.parse import urlparse
from urllib.parse import quote
from urllib.parse import parse_qsl
import random
import re
import secrets
import hashlib
import hmac
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from fuzzywuzzy import process, fuzz

_RATING_SOURCE_EXECUTOR = ThreadPoolExecutor(
    max_workers=8, thread_name_prefix='miniapp-rating-source'
)
_PROVIDER_REFRESH_EXECUTOR = ThreadPoolExecutor(
    max_workers=4, thread_name_prefix='miniapp-provider-refresh'
)
_RATING_SOURCE_CAPACITY = threading.BoundedSemaphore(64)
_PROVIDER_REFRESH_CAPACITY = threading.BoundedSemaphore(64)


def _submit_limited(executor, capacity, function, *args, **kwargs):
    """Bound running plus queued provider jobs; return None when saturated."""
    if not capacity.acquire(blocking=False):
        return None
    try:
        future = executor.submit(function, *args, **kwargs)
    except RuntimeError:
        capacity.release()
        return None
    except Exception:
        capacity.release()
        raise
    future.add_done_callback(lambda _future: capacity.release())
    return future

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

def is_catalogue_tv(category, seasons_data, content_type=None):
    """Use the existing episodic signals for Browse media classification."""
    if isinstance(seasons_data, str):
        try:
            seasons_data = json.loads(seasons_data)
        except (TypeError, ValueError):
            seasons_data = None
    return (
        str(content_type or '').strip().lower() in {'web series', 'tv series'}
        or str(category or '').strip().lower() == 'web series'
        or (isinstance(seasons_data, dict) and bool(seasons_data))
    )

def browse_type_matches(browse_type, category, seasons_data, content_type=None):
    if browse_type == 'all':
        return True
    is_tv = is_catalogue_tv(category, seasons_data, content_type)
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
    tmdb_request_inflight = {}
    rating_source_cache = {}
    rating_source_cache_lock = threading.Lock()
    rating_source_inflight = {}
    movie_detail_response_cache = {}
    movie_detail_response_cache_lock = threading.Lock()
    movie_detail_request_locks = [threading.Lock() for _ in range(64)]
    movie_detail_cache_ttl = 2.0
    omdb_rating_cache = {}
    omdb_rating_cache_lock = threading.Lock()
    omdb_rating_inflight = {}
    provider_failure_ttl = 60
    provider_stale_retry_ttl = 30
    rating_source_cache_ttl = 5 * 60
    new_releases_refresh_lock = threading.Lock()
    release_success_ttl = 7 * 24 * 60 * 60
    release_negative_ttl = 6 * 60 * 60
    release_failure_ttl = 60
    release_stale_retry_ttl = 30
    release_discovery_ttl = 6 * 60 * 60
    home_response_cache = {}
    home_response_cache_lock = threading.Lock()
    home_response_ttl = int(os.environ.get('MINIAPP_HOME_CACHE_SECONDS', '600'))
    availability_response_ttl = min(home_response_ttl, 60)

    def configured_ttl(name, default):
        try:
            return max(1, min(int(os.environ.get(name, default)), 30 * 24 * 60 * 60))
        except (TypeError, ValueError):
            logger.warning('Invalid cache TTL setting %s; using default', name)
            return default

    tmdb_rating_cache_ttl = configured_ttl('MINIAPP_TMDB_RATING_CACHE_SECONDS', 6 * 60 * 60)
    omdb_rating_cache_ttl = configured_ttl('MINIAPP_OMDB_RATING_CACHE_SECONDS', 6 * 60 * 60)
    tmdb_review_cache_ttl = configured_ttl('MINIAPP_TMDB_REVIEW_CACHE_SECONDS', 6 * 60 * 60)
    tmdb_review_db_refresh_lock = threading.Lock()
    tmdb_review_db_refreshing = set()
    rating_source_cache_ttl = configured_ttl('MINIAPP_RATING_RESPONSE_CACHE_SECONDS', 5 * 60)
    provider_failure_ttl = configured_ttl('MINIAPP_PROVIDER_FAILURE_CACHE_SECONDS', 60)
    provider_stale_retry_ttl = configured_ttl('MINIAPP_PROVIDER_STALE_RETRY_SECONDS', 30)

    detail_api_path = re.compile(
        r'^/api/(?:movie/\d+(?:/(?:rating-sources|rating|reviews|audience-reviews))?'
        r'|tmdb/(?:movie|tv)/\d+/(?:rating-sources|audience-reviews))$'
    )

    @flask_app.before_request
    def start_detail_api_timer():
        if detail_api_path.fullmatch(request.path):
            request.environ['flimfy_detail_api_started'] = time.perf_counter()

    @flask_app.after_request
    def log_detail_api_duration(response):
        started = request.environ.pop('flimfy_detail_api_started', None)
        if started is not None:
            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.info(
                'miniapp_detail_api route=%s status=%s duration_ms=%.1f',
                request.path, response.status_code, elapsed_ms,
            )
        return response

    def singleflight_movie_details(route_function):
        """Coalesce rapid duplicate opens and briefly cache live file details."""
        @functools.wraps(route_function)
        def wrapped(movie_id):
            key = int(movie_id)
            request_lock = movie_detail_request_locks[key % len(movie_detail_request_locks)]
            with request_lock:
                now = time.monotonic()
                with movie_detail_response_cache_lock:
                    cached = movie_detail_response_cache.get(key)
                    if cached and cached[0] > now:
                        logger.info('miniapp_movie_details id=%s state=cache_hit', key)
                        return jsonify(cached[1])
                    if cached:
                        movie_detail_response_cache.pop(key, None)

                response = route_function(movie_id)
                response_object = response[0] if isinstance(response, tuple) else response
                status_code = response[1] if isinstance(response, tuple) else getattr(response_object, 'status_code', 200)
                if status_code == 200:
                    payload = response_object.get_json(silent=True)
                    if isinstance(payload, dict) and payload.get('status') == 'success':
                        with movie_detail_response_cache_lock:
                            movie_detail_response_cache[key] = (
                                time.monotonic() + movie_detail_cache_ttl, payload
                            )
                            if len(movie_detail_response_cache) > 512:
                                expired = [
                                    cache_key for cache_key, (expires_at, _value)
                                    in movie_detail_response_cache.items()
                                    if expires_at <= time.monotonic()
                                ]
                                for cache_key in expired[:128]:
                                    movie_detail_response_cache.pop(cache_key, None)
                                while len(movie_detail_response_cache) > 512:
                                    movie_detail_response_cache.pop(next(iter(movie_detail_response_cache)))
                return response
        return wrapped

    def local_movie_has_file_sql(alias='m'):
        return f"""(
            COALESCE(NULLIF({alias}.url, ''), NULLIF({alias}.file_id, '')) IS NOT NULL
            OR EXISTS (
                SELECT 1
                FROM movie_files availability_file
                WHERE availability_file.movie_id = {alias}.id
                  AND (
                      NULLIF(availability_file.url, '') IS NOT NULL
                      OR NULLIF(availability_file.file_id, '') IS NOT NULL
                  )
            )
        )"""

    def local_movie_availability_filter_sql(availability, alias='m'):
        has_file = local_movie_has_file_sql(alias)
        if availability == 'available':
            return has_file
        if availability == 'request':
            return f'NOT {has_file}'
        return 'TRUE'

    def tmdb_cached_request(
        path, params, cache_key, negative=False, ttl_seconds=None,
        attempts=2, request_timeout=6, stale_while_revalidate=False,
        include_cache_state=False, _refresh_event=None, response_validator=None,
    ):
        started_at = time.perf_counter()
        is_detail_provider = cache_key.startswith((
            'movie-rating-details:', 'movie-audience-reviews:'
        ))

        def packed(data, cache_hit, cache_state):
            if include_cache_state:
                return data, cache_hit, cache_state
            return data, cache_hit

        now = time.time()
        owner = False
        stale_data = None
        wait_event = None
        with release_cache_lock:
            cached = release_cache.get(cache_key)
            stale_data = cached[1] if cached and cached[1] is not None else None
            if _refresh_event is not None:
                wait_event = _refresh_event
                owner = True
            elif cached and cached[0] > now:
                logger.debug('TMDB cache hit: %s', cache_key)
                if is_detail_provider:
                    logger.info('miniapp_provider_cache provider=TMDb key=%s state=hit', cache_key)
                cache_state = cached[2] if len(cached) > 2 else (
                    'fresh' if cached[1] is not None else 'unavailable'
                )
                if cache_state == 'stale' and stale_while_revalidate:
                    cache_state = 'stale_cached'
                return packed(
                    cached[1], True, cache_state,
                )
            if not owner:
                wait_event = tmdb_request_inflight.get(cache_key)
            if not owner and stale_data is not None and stale_while_revalidate:
                if wait_event is None:
                    wait_event = threading.Event()
                    tmdb_request_inflight[cache_key] = wait_event
                    submitted = _submit_limited(
                        _PROVIDER_REFRESH_EXECUTOR, _PROVIDER_REFRESH_CAPACITY,
                        tmdb_cached_request,
                        path, params, cache_key, negative, ttl_seconds,
                        attempts, request_timeout, False, include_cache_state,
                        wait_event, response_validator,
                    )
                    if submitted is None:
                        tmdb_request_inflight.pop(cache_key, None)
                        wait_event.set()
                        if stale_data is not None:
                            release_cache[cache_key] = (
                                time.time() + provider_stale_retry_ttl, stale_data, 'stale'
                            )
                        logger.warning('TMDB background refresh queue full: %s', cache_key)
                logger.debug('TMDB stale cache served; refresh scheduled: %s', cache_key)
                if is_detail_provider:
                    logger.info('miniapp_provider_cache provider=TMDb key=%s state=stale refresh=scheduled', cache_key)
                return packed(stale_data, True, 'stale')
            if not owner and wait_event is None:
                wait_event = threading.Event()
                tmdb_request_inflight[cache_key] = wait_event
                owner = True
                logger.debug('TMDB cache miss: %s', cache_key)
                if is_detail_provider:
                    logger.info('miniapp_provider_cache provider=TMDb key=%s state=miss', cache_key)
            else:
                logger.debug('TMDB request joined in-flight fetch: %s', cache_key)
                if is_detail_provider:
                    logger.info('miniapp_provider_cache provider=TMDb key=%s state=inflight_join', cache_key)

        if not owner:
            if isinstance(request_timeout, (tuple, list)):
                wait_seconds = sum(float(part) for part in request_timeout)
            else:
                wait_seconds = float(request_timeout)
            wait_seconds = min(max(wait_seconds * max(1, int(attempts)) + 0.5, 1), 20)
            wait_event.wait(wait_seconds)
            with release_cache_lock:
                current = release_cache.get(cache_key)
                if current and current[1] is not None:
                    state = current[2] if len(current) > 2 else ('fresh' if current[0] > time.time() else 'stale')
                    return packed(current[1], True, state)
            return packed(stale_data, stale_data is not None, 'unavailable' if stale_data is None else 'stale')

        request_params = dict(params)
        request_params['api_key'] = TMDB_API_KEY
        response_data = None
        attempt_limit = max(1, int(attempts))
        failure_reason = 'Unavailable'
        try:
            for attempt in range(attempt_limit if TMDB_API_KEY else 0):
                try:
                    provider_started = time.perf_counter()
                    response = requests.get(
                        f'https://api.themoviedb.org/3/{path}',
                        params=request_params,
                        headers={'User-Agent': 'FlimfyBox/1.0'},
                        timeout=request_timeout,
                    )
                    if is_detail_provider:
                        logger.info(
                            'miniapp_provider_request provider=TMDb key=%s http_status=%s duration_ms=%.1f',
                            cache_key, response.status_code,
                            (time.perf_counter() - provider_started) * 1000,
                        )
                    if response.status_code == 429:
                        failure_reason = 'Rate limited'
                        logger.warning('TMDB provider rate limited request: %s', cache_key)
                        break
                    if response.status_code >= 500 and attempt + 1 < attempt_limit:
                        time.sleep(min(0.2 * (attempt + 1), 0.5))
                        continue
                    response.raise_for_status()
                    response_data = response.json()
                    if (
                        not isinstance(response_data, dict)
                        or response_data.get('success') is False
                        or (
                            response_validator is not None
                            and not response_validator(response_data)
                        )
                    ):
                        failure_reason = 'Invalid provider response'
                        response_data = None
                        logger.warning('TMDB provider returned unusable data: %s', cache_key)
                        break
                    break
                except (requests.RequestException, ValueError) as error:
                    failure_reason = type(error).__name__
                    if is_detail_provider:
                        logger.warning(
                            'miniapp_provider_request provider=TMDb key=%s outcome=failure type=%s duration_ms=%.1f',
                            cache_key, failure_reason,
                            (time.perf_counter() - provider_started) * 1000
                            if 'provider_started' in locals() else 0.0,
                        )
                    if attempt + 1 == attempt_limit:
                        # Never log request exception text; it may contain the API key.
                        logger.warning('TMDB provider request failed: %s (%s)', cache_key, failure_reason)

            completed_at = time.time()
            if response_data is not None:
                ttl = ttl_seconds or release_success_ttl
                with release_cache_lock:
                    release_cache[cache_key] = (completed_at + ttl, response_data, 'fresh')
                logger.debug('TMDB cache refreshed: %s', cache_key)
                return packed(response_data, False, 'fresh')

            if stale_data is not None:
                with release_cache_lock:
                    release_cache[cache_key] = (completed_at + provider_stale_retry_ttl, stale_data, 'stale')
                logger.warning('TMDB refresh failed; retaining cached data: %s (%s)', cache_key, failure_reason)
                return packed(stale_data, True, 'stale')

            if not TMDB_API_KEY:
                failure_reason = 'Not configured'
            failure_ttl = release_negative_ttl if negative else provider_failure_ttl
            with release_cache_lock:
                release_cache[cache_key] = (completed_at + failure_ttl, None, 'unavailable')
            return packed(None, False, 'unavailable')
        finally:
            if is_detail_provider and _refresh_event is not None:
                logger.info(
                    'miniapp_provider_refresh provider=TMDb key=%s duration_ms=%.1f',
                    cache_key, (time.perf_counter() - started_at) * 1000,
                )
            with release_cache_lock:
                tmdb_request_inflight.pop(cache_key, None)
            wait_event.set()

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

    def rating_number(value):
        if isinstance(value, bool) or value is None:
            return None
        try:
            score = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(score) or not 0 <= score <= 10:
            return None
        return score

    def rating_vote_text(value):
        if value is None:
            return None
        text = str(value).strip()
        return text[:32] if text and text.lower() not in {'n/a', 'na', 'none'} else None

    def persist_rating_source(provider, provider_item_id, media_type, source_data):
        if provider not in {'imdb', 'tmdb'} or not provider_item_id or media_type not in {'movie', 'tv'}:
            return False
        conn = get_db_connection()
        if not conn:
            return False
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO movie_provider_ratings_cache
                    (provider, provider_item_id, media_type, score, votes, via,
                     fetched_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP + (%s * INTERVAL '1 second'))
                ON CONFLICT (provider, provider_item_id, media_type) DO UPDATE SET
                    score = EXCLUDED.score,
                    votes = EXCLUDED.votes,
                    via = EXCLUDED.via,
                    fetched_at = CURRENT_TIMESTAMP,
                    expires_at = EXCLUDED.expires_at
            """, (
                provider, str(provider_item_id), media_type, source_data['score'],
                rating_vote_text(source_data.get('votes')),
                str(source_data.get('via') or ('TMDb' if provider == 'tmdb' else 'OMDb')),
                tmdb_rating_cache_ttl if provider == 'tmdb' else omdb_rating_cache_ttl,
            ))
            conn.commit()
            return True
        except Exception as error:
            conn.rollback()
            logger.warning('Could not persist %s rating snapshot (%s)', provider, type(error).__name__)
            return False
        finally:
            close_db_connection(conn)

    def read_rating_source(provider, provider_item_id, media_type):
        if not provider_item_id:
            return None
        conn = get_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT score, votes, expires_at > CURRENT_TIMESTAMP
                FROM movie_provider_ratings_cache
                WHERE provider = %s AND provider_item_id = %s AND media_type = %s
            """, (provider, str(provider_item_id), media_type))
            row = cur.fetchone()
            conn.commit()
            if not row:
                return None
            score = rating_number(row[0])
            votes = rating_vote_text(row[1])
            if score is None:
                return None
            return {'score': score, 'votes': votes, 'fresh': bool(row[2])}
        except Exception as error:
            conn.rollback()
            logger.warning('Provider rating cache read failed (%s)', type(error).__name__)
            return None
        finally:
            close_db_connection(conn)

    def clean_tmdb_audience_review(review):
        if not isinstance(review, dict):
            return None
        content = html_unescape(str(review.get('content') or ''))
        content = re.sub(r'<[^>]*>', ' ', content)
        content = re.sub(r'\s+', ' ', content).strip()
        if not content:
            return None
        author = html_unescape(str(review.get('author') or 'TMDb member'))
        author = re.sub(r'\s+', ' ', author).strip()[:100] or 'TMDb member'
        raw_url = str(review.get('url') or '').strip()
        parsed_url = urlparse(raw_url)
        safe_url = (
            raw_url[:500]
            if parsed_url.scheme == 'https'
            and parsed_url.hostname == 'www.themoviedb.org'
            and parsed_url.port in (None, 443)
            and parsed_url.username is None
            and parsed_url.password is None
            and re.fullmatch(r'/review/[A-Za-z0-9_-]+/?', parsed_url.path)
            else ''
        )
        author_details = review.get('author_details') or {}
        review_rating = rating_number(author_details.get('rating')) if isinstance(author_details, dict) else None
        review_id = str(review.get('id') or '').strip()
        return {
            'id': review_id[:100] if re.fullmatch(r'[A-Za-z0-9_-]{1,100}', review_id) else None,
            'author': author,
            'content': content[:1200],
            'rating': review_rating,
            'created_at': str(review.get('created_at') or '')[:40],
            'url': safe_url,
            'source': 'TMDb',
        }

    def read_tmdb_reviews_cache(media_type, tmdb_id):
        conn = get_db_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT status, review_count, fetched_at, last_success_at,
                       expires_at, last_error, expires_at > CURRENT_TIMESTAMP
                FROM movie_provider_review_sync
                WHERE provider = 'tmdb' AND provider_item_id = %s AND media_type = %s
            """, (str(tmdb_id), media_type))
            sync_row = cur.fetchone()
            if not sync_row:
                return None
            cur.execute("""
                SELECT review_id, author, content, rating, provider_created_at,
                       review_url
                FROM movie_provider_reviews_cache
                WHERE provider = 'tmdb' AND provider_item_id = %s AND media_type = %s
                ORDER BY provider_created_at DESC NULLS LAST, review_id
                LIMIT 20
            """, (str(tmdb_id), media_type))
            reviews = []
            for row in cur.fetchall():
                created_at = row[4].isoformat() if row[4] else ''
                reviews.append({
                    'id': row[0], 'author': row[1], 'content': row[2],
                    'rating': float(row[3]) if row[3] is not None else None,
                    'created_at': created_at, 'url': row[5], 'source': 'TMDb',
                })
            conn.commit()
            return {
                'status': sync_row[0], 'review_count': int(sync_row[1] or 0),
                'fetched_at': sync_row[2], 'last_success_at': sync_row[3],
                'expires_at': sync_row[4], 'last_error': sync_row[5],
                'is_fresh': bool(sync_row[6]), 'reviews': reviews,
            }
        except Exception as error:
            conn.rollback()
            logger.warning('TMDb review database cache read failed (%s)', type(error).__name__)
            return None
        finally:
            close_db_connection(conn)

    def store_tmdb_reviews_cache(media_type, tmdb_id, payload=None, failure_type=None):
        conn = get_db_connection()
        if not conn:
            return False
        key = str(tmdb_id)
        try:
            cur = conn.cursor()
            if failure_type:
                cur.execute("""
                    INSERT INTO movie_provider_review_sync
                        (provider, provider_item_id, media_type, status, review_count,
                         fetched_at, last_attempt_at, expires_at, last_error)
                    VALUES ('tmdb', %s, %s, 'error', 0, CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '60 seconds', %s)
                    ON CONFLICT (provider, provider_item_id, media_type) DO UPDATE SET
                        status = 'error', last_attempt_at = CURRENT_TIMESTAMP,
                        expires_at = CURRENT_TIMESTAMP + INTERVAL '60 seconds',
                        last_error = EXCLUDED.last_error
                """, (key, media_type, str(failure_type)[:64]))
            else:
                if not isinstance(payload, dict) or not isinstance(payload.get('results'), list):
                    raise ValueError('Invalid successful TMDb review payload')
                raw_reviews = payload.get('results') or []
                try:
                    review_count = max(0, int(payload.get('total_results', len(raw_reviews))))
                except (TypeError, ValueError):
                    review_count = len(raw_reviews)
                # This endpoint returns page one. A successful response replaces
                # that provider snapshot; a failed request never reaches here.
                cur.execute("""
                    DELETE FROM movie_provider_reviews_cache
                    WHERE provider = 'tmdb' AND provider_item_id = %s AND media_type = %s
                """, (key, media_type))
                for raw_review in raw_reviews[:20]:
                    review = clean_tmdb_audience_review(raw_review)
                    if not review or not review.get('id'):
                        continue
                    provider_created_at = None
                    try:
                        raw_date = str(review.get('created_at') or '').replace('Z', '+00:00')
                        provider_created_at = datetime.fromisoformat(raw_date) if raw_date else None
                    except (TypeError, ValueError):
                        provider_created_at = None
                    cur.execute("""
                        INSERT INTO movie_provider_reviews_cache
                            (provider, provider_item_id, media_type, review_id, author,
                             content, rating, review_url, provider_created_at, fetched_at)
                        VALUES ('tmdb', %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (provider, provider_item_id, media_type, review_id)
                        DO UPDATE SET author = EXCLUDED.author,
                                      content = EXCLUDED.content,
                                      rating = EXCLUDED.rating,
                                      review_url = EXCLUDED.review_url,
                                      provider_created_at = EXCLUDED.provider_created_at,
                                      fetched_at = CURRENT_TIMESTAMP
                    """, (key, media_type, review['id'], review['author'],
                          review['content'], review['rating'], review['url'], provider_created_at))
                cur.execute("""
                    DELETE FROM movie_provider_reviews_cache
                    WHERE provider = 'tmdb' AND provider_item_id = %s AND media_type = %s
                      AND fetched_at < CURRENT_TIMESTAMP - INTERVAL '180 days'
                """, (key, media_type))
                cur.execute("""
                    INSERT INTO movie_provider_review_sync
                        (provider, provider_item_id, media_type, status, review_count,
                         fetched_at, last_success_at, last_attempt_at, expires_at, last_error)
                    VALUES ('tmdb', %s, %s, 'success', %s, CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP + (%s * INTERVAL '1 second'), NULL)
                    ON CONFLICT (provider, provider_item_id, media_type) DO UPDATE SET
                        status = 'success', review_count = EXCLUDED.review_count,
                        fetched_at = CURRENT_TIMESTAMP, last_success_at = CURRENT_TIMESTAMP,
                        last_attempt_at = CURRENT_TIMESTAMP, expires_at = EXCLUDED.expires_at,
                        last_error = NULL
                """, (key, media_type, review_count, tmdb_review_cache_ttl))
            conn.commit()
            return True
        except Exception as error:
            conn.rollback()
            logger.warning('TMDb review database cache write failed (%s)', type(error).__name__)
            return False
        finally:
            close_db_connection(conn)

    def fetch_and_store_tmdb_reviews(media_type, tmdb_id, background=False):
        payload, _cache_hit, cache_status = tmdb_cached_request(
            f'{media_type}/{tmdb_id}/reviews', {'language': 'en-US', 'page': 1},
            f'movie-audience-reviews:{media_type}:{tmdb_id}',
            False, tmdb_review_cache_ttl, 1, (1, 1.5),
            stale_while_revalidate=True, include_cache_state=True,
            response_validator=lambda value: isinstance(value.get('results'), list),
        )
        if not isinstance(payload, dict):
            store_tmdb_reviews_cache(media_type, tmdb_id, failure_type='provider_unavailable')
            return None
        if cache_status in {'fresh', 'database_hit'}:
            store_tmdb_reviews_cache(media_type, tmdb_id, payload=payload)
        elif background:
            # A stale in-process response is useful to serve, but does not renew
            # the persistent provider cache's freshness timestamp.
            return None
        return payload, cache_status

    def refresh_tmdb_reviews_in_background(media_type, tmdb_id):
        key = (media_type, str(tmdb_id))
        try:
            fetch_and_store_tmdb_reviews(media_type, tmdb_id, background=True)
        finally:
            with tmdb_review_db_refresh_lock:
                tmdb_review_db_refreshing.discard(key)

    def format_tmdb_review_cache(cache_row, cache_status):
        return {
            'status': 'success', 'source': 'TMDb',
            'reviews': cache_row['reviews'][:5],
            'review_count': cache_row['review_count'],
            'cache_status': cache_status,
        }

    def load_tmdb_audience_reviews(media_type, tmdb_id):
        persistent = read_tmdb_reviews_cache(media_type, tmdb_id)
        if persistent and persistent['is_fresh'] and persistent['status'] == 'success':
            logger.info('miniapp_provider_cache provider=TMDb reviews=%s state=db_hit', tmdb_id)
            return format_tmdb_review_cache(persistent, 'fresh')

        if (
            persistent and persistent['is_fresh']
            and persistent['status'] == 'error'
            and persistent['last_success_at'] is None
        ):
            logger.info('miniapp_provider_cache provider=TMDb reviews=%s state=failure_backoff', tmdb_id)
            return None

        has_stale_success = bool(persistent and persistent['last_success_at'] is not None)
        if has_stale_success:
            key = (media_type, str(tmdb_id))
            with tmdb_review_db_refresh_lock:
                should_refresh = (
                    not (persistent['status'] == 'error' and persistent['is_fresh'])
                    and key not in tmdb_review_db_refreshing
                )
                if should_refresh:
                    tmdb_review_db_refreshing.add(key)
            if should_refresh:
                future = _submit_limited(
                    _PROVIDER_REFRESH_EXECUTOR, _PROVIDER_REFRESH_CAPACITY,
                    refresh_tmdb_reviews_in_background, media_type, tmdb_id,
                )
                if future is None:
                    with tmdb_review_db_refresh_lock:
                        tmdb_review_db_refreshing.discard(key)
            logger.info('miniapp_provider_cache provider=TMDb reviews=%s state=stale refresh=scheduled', tmdb_id)
            return format_tmdb_review_cache(persistent, 'stale')

        fetched = fetch_and_store_tmdb_reviews(media_type, tmdb_id)
        if not fetched:
            return None
        payload, cache_status = fetched
        raw_reviews = payload.get('results') or []
        reviews = [
            safe_review for safe_review in
            (clean_tmdb_audience_review(item) for item in raw_reviews[:5]) if safe_review
        ]
        try:
            review_count = max(0, int(payload.get('total_results', len(raw_reviews))))
        except (TypeError, ValueError):
            review_count = len(raw_reviews)
        return {
            'status': 'success', 'source': 'TMDb', 'reviews': reviews,
            'review_count': review_count, 'cache_status': cache_status,
        }

    def fetch_omdb_rating(imdb_id, api_key):
        if not api_key or not re.fullmatch(r'tt\d{7,8}', str(imdb_id or '')):
            return None
        provider_started = time.perf_counter()
        try:
            response = requests.get(
                'https://www.omdbapi.com/',
                params={'i': imdb_id, 'apikey': api_key},
                headers={'User-Agent': 'FlimfyBox/1.0'},
                timeout=(1, 1.5),
            )
            logger.info(
                'miniapp_provider_request provider=OMDb key=%s http_status=%s duration_ms=%.1f',
                imdb_id, response.status_code,
                (time.perf_counter() - provider_started) * 1000,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError, TypeError) as error:
            # Never include the requests error text: it can contain OMDb's key.
            logger.warning(
                'miniapp_provider_request provider=OMDb key=%s outcome=failure type=%s duration_ms=%.1f',
                imdb_id, type(error).__name__,
                (time.perf_counter() - provider_started) * 1000,
            )
            return None
        if not isinstance(payload, dict) or payload.get('Response') != 'True':
            logger.info('IMDb score unavailable from OMDb for %s', imdb_id)
            return None
        score = rating_number(payload.get('imdbRating'))
        if score is None:
            return None
        return {'score': score, 'votes': rating_vote_text(payload.get('imdbVotes'))}

    def _finish_omdb_rating_refresh(imdb_id, api_key, event, stale_data=None):
        refresh_started = time.perf_counter()
        try:
            result = fetch_omdb_rating(imdb_id, api_key)
            completed_at = time.time()
            if result is not None:
                cache_value = (completed_at + omdb_rating_cache_ttl, result, 'fresh')
                logger.debug('OMDb rating cache refreshed for %s', imdb_id)
            elif stale_data is not None:
                cache_value = (completed_at + provider_stale_retry_ttl, stale_data, 'stale')
                logger.warning('OMDb rating refresh failed; retaining cached data for %s', imdb_id)
            else:
                cache_value = (completed_at + provider_failure_ttl, None, 'unavailable')
            with omdb_rating_cache_lock:
                omdb_rating_cache[imdb_id] = cache_value
            return result
        finally:
            if stale_data is not None:
                logger.info(
                    'miniapp_provider_refresh provider=OMDb key=%s duration_ms=%.1f',
                    imdb_id, (time.perf_counter() - refresh_started) * 1000,
                )
            with omdb_rating_cache_lock:
                omdb_rating_inflight.pop(imdb_id, None)
            event.set()

    def cached_omdb_rating(imdb_id, api_key):
        now = time.time()
        with omdb_rating_cache_lock:
            cached = omdb_rating_cache.get(imdb_id)
            if cached and cached[0] > now:
                logger.debug('OMDb rating cache hit for %s', imdb_id)
                logger.info('miniapp_provider_cache provider=OMDb key=%s state=hit', imdb_id)
                state = cached[2] if len(cached) > 2 else 'fresh'
                return cached[1], 'stale_cached' if state == 'stale' else state
            stale_data = cached[1] if cached and cached[1] is not None else None
            event = omdb_rating_inflight.get(imdb_id)
            if stale_data is not None:
                if event is None:
                    event = threading.Event()
                    omdb_rating_inflight[imdb_id] = event
                    submitted = _submit_limited(
                        _PROVIDER_REFRESH_EXECUTOR, _PROVIDER_REFRESH_CAPACITY,
                        _finish_omdb_rating_refresh, imdb_id, api_key, event, stale_data
                    )
                    if submitted is None:
                        omdb_rating_inflight.pop(imdb_id, None)
                        event.set()
                        omdb_rating_cache[imdb_id] = (
                            time.time() + provider_stale_retry_ttl, stale_data, 'stale'
                        )
                        logger.warning('OMDb background refresh queue full for %s', imdb_id)
                logger.debug('OMDb stale rating served; refresh scheduled for %s', imdb_id)
                logger.info('miniapp_provider_cache provider=OMDb key=%s state=stale refresh=scheduled', imdb_id)
                return stale_data, 'stale'
            if event is None:
                event = threading.Event()
                omdb_rating_inflight[imdb_id] = event
                owner = True
                logger.debug('OMDb rating cache miss for %s', imdb_id)
                logger.info('miniapp_provider_cache provider=OMDb key=%s state=miss', imdb_id)
            else:
                owner = False
                logger.debug('OMDb rating request joined in-flight fetch for %s', imdb_id)
                logger.info('miniapp_provider_cache provider=OMDb key=%s state=inflight_join', imdb_id)

        if not owner:
            event.wait(3.0)
            with omdb_rating_cache_lock:
                current = omdb_rating_cache.get(imdb_id)
                if current:
                    state = current[2] if len(current) > 2 else (
                        'fresh' if current[0] > time.time() else 'stale'
                    )
                    return current[1], state
            return None, 'unavailable'

        result = _finish_omdb_rating_refresh(imdb_id, api_key, event)
        with omdb_rating_cache_lock:
            current = omdb_rating_cache.get(imdb_id)
            cache_state = current[2] if current and len(current) > 2 else (
                'fresh' if current and current[0] > time.time() else 'unavailable'
            )
        return result, cache_state

    def _fetch_rating_sources(cache_key, media_type, tmdb_id, imdb_id, previous=None):
        tmdb_id = str(tmdb_id or '').strip()
        imdb_id = str(imdb_id or '').strip()
        tmdb_id = tmdb_id if re.fullmatch(r'\d+', tmdb_id) else ''
        imdb_id = imdb_id if re.fullmatch(r'tt\d{7,8}', imdb_id) else ''
        omdb_key = os.environ.get('OMDB_API_KEY', '').strip()
        deadline = time.monotonic() + 2.35
        futures = {}
        provider_queue_full = set()

        if TMDB_API_KEY and tmdb_id:
            future = _submit_limited(
                _RATING_SOURCE_EXECUTOR, _RATING_SOURCE_CAPACITY,
                tmdb_cached_request,
                f'{media_type}/{tmdb_id}',
                {'append_to_response': 'external_ids'},
                f'movie-rating-details:{media_type}:{tmdb_id}',
                False, tmdb_rating_cache_ttl, 2, (1, 1.5), True, True,
                response_validator=lambda value: value.get('id') is not None,
            )
            if future:
                futures['tmdb'] = future
            else:
                provider_queue_full.add('tmdb')
                logger.warning('Rating provider queue full: provider=TMDb key=%s', cache_key)
        if omdb_key and imdb_id:
            future = _submit_limited(
                _RATING_SOURCE_EXECUTOR, _RATING_SOURCE_CAPACITY,
                cached_omdb_rating, imdb_id, omdb_key,
            )
            if future:
                futures['imdb'] = future
            else:
                provider_queue_full.add('imdb')
                logger.warning('Rating provider queue full: provider=OMDb key=%s', cache_key)

        if futures:
            wait(tuple(futures.values()), timeout=max(0, deadline - time.monotonic()))

        detail_data = None
        tmdb_score = None
        tmdb_state = (
            'refreshing' if 'tmdb' in provider_queue_full
            else 'unlinked' if not tmdb_id
            else 'unavailable' if TMDB_API_KEY
            else 'not_configured'
        )
        if futures.get('tmdb') and futures['tmdb'].done():
            try:
                detail_result = futures['tmdb'].result()
                if len(detail_result) == 3:
                    detail_data, _cache_hit, tmdb_state = detail_result
                else:
                    detail_data, _cache_hit = detail_result
                    tmdb_state = 'fresh' if detail_data else 'unavailable'
                detail_data = detail_data if isinstance(detail_data, dict) else None
            except Exception as error:
                logger.debug('TMDb rating future failed for %s (%s)', cache_key, type(error).__name__)
                tmdb_state = 'unavailable'
        elif futures.get('tmdb'):
            tmdb_state = 'refreshing'

        if detail_data:
            vote_count = detail_data.get('vote_count')
            try:
                vote_count = int(vote_count) if vote_count is not None else 0
            except (TypeError, ValueError):
                vote_count = 0
            score = rating_number(detail_data.get('vote_average'))
            if score is not None and vote_count > 0:
                tmdb_score = {'score': score, 'votes': vote_count}
                persist_rating_source('tmdb', tmdb_id, media_type, tmdb_score)

        if not imdb_id and detail_data:
            external_ids = detail_data.get('external_ids') or {}
            if isinstance(external_ids, dict):
                imdb_id = str(external_ids.get('imdb_id') or '').strip()
        if omdb_key and re.fullmatch(r'tt\d{7,8}', imdb_id or '') and not futures.get('imdb'):
            future = _submit_limited(
                _RATING_SOURCE_EXECUTOR, _RATING_SOURCE_CAPACITY,
                cached_omdb_rating, imdb_id, omdb_key,
            )
            if future:
                futures['imdb'] = future
                remaining = max(0, deadline - time.monotonic())
                if remaining:
                    wait((future,), timeout=remaining)
            else:
                provider_queue_full.add('imdb')
                logger.warning('Rating provider queue full: provider=OMDb key=%s', cache_key)

        imdb_score = None
        imdb_state = (
            'refreshing' if 'imdb' in provider_queue_full
            else 'unavailable' if omdb_key and imdb_id
            else 'not_configured' if imdb_id
            else 'refreshing' if futures.get('tmdb') and not futures['tmdb'].done()
            else 'unlinked'
        )
        if futures.get('imdb') and futures['imdb'].done():
            try:
                imdb_score, imdb_state = futures['imdb'].result()
                if isinstance(imdb_score, dict) and imdb_score.get('score') is not None:
                    persist_rating_source('imdb', imdb_id, media_type, {
                        'score': imdb_score['score'], 'votes': imdb_score.get('votes'),
                        'via': 'OMDb', 'imdb_id': imdb_id,
                    })
            except Exception as error:
                logger.debug('OMDb rating future failed for %s (%s)', imdb_id, type(error).__name__)
                imdb_state = 'unavailable'
        elif futures.get('imdb'):
            imdb_state = 'refreshing'

        old_sources = previous or {}
        if imdb_score is None and old_sources.get('imdb', {}).get('score') is not None:
            imdb_score = old_sources['imdb']
            imdb_state = 'stale'
        if tmdb_score is None and old_sources.get('tmdb', {}).get('score') is not None:
            tmdb_score = old_sources['tmdb']
            tmdb_state = 'stale'

        statuses = {tmdb_state, imdb_state}
        if 'stale' in statuses:
            cache_status = 'stale'
        elif 'stale_cached' in statuses:
            cache_status = 'stale_cached'
        elif any(future and not future.done() for future in futures.values()) or 'refreshing' in statuses:
            cache_status = 'refreshing'
        elif 'unavailable' in statuses and (tmdb_score or imdb_score):
            cache_status = 'partial'
        elif not tmdb_score and not imdb_score:
            if statuses == {'unlinked'}:
                cache_status = 'unlinked'
            elif statuses <= {'unlinked', 'not_configured'}:
                cache_status = 'not_configured'
            elif 'unavailable' in statuses:
                cache_status = 'unavailable'
            else:
                cache_status = 'empty'
        else:
            cache_status = 'fresh'

        result = {
            'status': 'success',
            'imdb': imdb_score or {'score': None, 'votes': None},
            'tmdb': tmdb_score or {'score': None, 'votes': None},
            'source_status': {'imdb': imdb_state, 'tmdb': tmdb_state},
            'cache_status': cache_status,
        }
        if cache_status == 'stale':
            # Recombine promptly after the provider-level background refresh finishes.
            ttl = 1
        elif cache_status in {'unavailable', 'partial'}:
            ttl = provider_failure_ttl
        elif cache_status == 'refreshing':
            # Let a detail-page refresh poll pick up a slow provider response quickly.
            ttl = 1
        else:
            ttl = rating_source_cache_ttl
        with rating_source_cache_lock:
            rating_source_cache[cache_key] = (time.time() + ttl, result)
        return result

    def _refresh_rating_sources(cache_key, media_type, tmdb_id, imdb_id, event, previous=None, background=False):
        refresh_started = time.perf_counter()
        try:
            result = _fetch_rating_sources(cache_key, media_type, tmdb_id, imdb_id, previous)
            if background:
                logger.info(
                    'miniapp_provider_refresh key=%s cache_status=%s duration_ms=%.1f',
                    cache_key, result.get('cache_status'),
                    (time.perf_counter() - refresh_started) * 1000,
                )
            return result
        finally:
            with rating_source_cache_lock:
                rating_source_inflight.pop(cache_key, None)
            event.set()

    def load_rating_sources(cache_key, media_type, tmdb_id=None, imdb_id=None):
        now = time.time()
        stale_result = None
        with rating_source_cache_lock:
            cached = rating_source_cache.get(cache_key)
            if cached and cached[0] > now:
                logger.debug('Rating response cache hit: %s', cache_key)
                logger.info('miniapp_rating_cache key=%s state=hit', cache_key)
                cached_result = cached[1]
                if cached_result.get('cache_status') == 'stale':
                    return {
                        **cached_result,
                        'cache_status': 'stale_cached',
                        'source_status': {
                            key: 'stale_cached' if value == 'stale' else value
                            for key, value in cached_result.get('source_status', {}).items()
                        },
                    }
                return cached_result
            stale_result = cached[1] if cached and any(
                (cached[1].get(provider) or {}).get('score') is not None
                for provider in ('imdb', 'tmdb')
            ) else None
            event = rating_source_inflight.get(cache_key)
            if stale_result is not None:
                if event is None:
                    event = threading.Event()
                    rating_source_inflight[cache_key] = event
                    submitted = _submit_limited(
                        _PROVIDER_REFRESH_EXECUTOR, _PROVIDER_REFRESH_CAPACITY,
                        _refresh_rating_sources, cache_key, media_type,
                        tmdb_id, imdb_id, event, stale_result, True,
                    )
                    if submitted is None:
                        rating_source_inflight.pop(cache_key, None)
                        event.set()
                        stale_cached_result = {**stale_result, 'cache_status': 'stale_cached'}
                        rating_source_cache[cache_key] = (
                            time.time() + provider_stale_retry_ttl, stale_cached_result
                        )
                        logger.warning('Rating background refresh queue full: %s', cache_key)
                        return stale_cached_result
                logger.debug('Stale rating response served; provider refresh scheduled: %s', cache_key)
                logger.info('miniapp_rating_cache key=%s state=stale refresh=scheduled', cache_key)
                return {**stale_result, 'cache_status': 'stale'}
            if event is None:
                event = threading.Event()
                rating_source_inflight[cache_key] = event
                owner = True
                logger.debug('Rating response cache miss: %s', cache_key)
                logger.info('miniapp_rating_cache key=%s state=miss', cache_key)
            else:
                owner = False
                logger.debug('Rating request joined in-flight refresh: %s', cache_key)
                logger.info('miniapp_rating_cache key=%s state=inflight_join', cache_key)

        if owner and stale_result is None:
            tmdb_saved = read_rating_source('tmdb', tmdb_id, media_type) if tmdb_id else None
            imdb_saved = read_rating_source('imdb', imdb_id, media_type) if imdb_id else None
            omdb_configured = bool(os.environ.get('OMDB_API_KEY', '').strip())
            needs_tmdb = bool(TMDB_API_KEY and tmdb_id)
            needs_imdb = bool(omdb_configured and imdb_id)
            has_saved_rating = bool(tmdb_saved or imdb_saved)
            saved_sources_fresh = (
                (not needs_tmdb or bool(tmdb_saved and tmdb_saved['fresh']))
                and (not needs_imdb or bool(imdb_saved and imdb_saved['fresh']))
            )
            if has_saved_rating:
                saved_result = {
                    'status': 'success',
                    'imdb': ({'score': imdb_saved['score'], 'votes': imdb_saved['votes']}
                             if imdb_saved else {'score': None, 'votes': None}),
                    'tmdb': ({'score': tmdb_saved['score'], 'votes': tmdb_saved['votes']}
                             if tmdb_saved else {'score': None, 'votes': None}),
                    'source_status': {
                        'imdb': 'fresh' if imdb_saved and imdb_saved['fresh'] else
                                'stale_cached' if imdb_saved else
                                'unavailable' if needs_imdb else 'not_configured' if imdb_id else 'unlinked',
                        'tmdb': 'fresh' if tmdb_saved and tmdb_saved['fresh'] else
                                'stale_cached' if tmdb_saved else
                                'unavailable' if needs_tmdb else 'not_configured' if TMDB_API_KEY else 'unlinked',
                    },
                    'cache_status': 'fresh' if saved_sources_fresh else 'stale',
                }
                if saved_sources_fresh:
                    with rating_source_cache_lock:
                        rating_source_cache[cache_key] = (time.time() + rating_source_cache_ttl, saved_result)
                        rating_source_inflight.pop(cache_key, None)
                    event.set()
                    logger.info('miniapp_rating_cache key=%s state=persistent_db_hit', cache_key)
                    return saved_result

                submitted = _submit_limited(
                    _PROVIDER_REFRESH_EXECUTOR, _PROVIDER_REFRESH_CAPACITY,
                    _refresh_rating_sources, cache_key, media_type, tmdb_id, imdb_id,
                    event, saved_result, True,
                )
                if submitted is not None:
                    with rating_source_cache_lock:
                        rating_source_cache[cache_key] = (time.time() + 1, saved_result)
                    logger.info('miniapp_rating_cache key=%s state=persistent_stale refresh=scheduled', cache_key)
                    return saved_result
                with rating_source_cache_lock:
                    rating_source_inflight.pop(cache_key, None)
                    saved_result['cache_status'] = 'stale_cached'
                    rating_source_cache[cache_key] = (time.time() + provider_stale_retry_ttl, saved_result)
                event.set()
                return saved_result

        if not owner:
            event.wait(3.5)
            with rating_source_cache_lock:
                current = rating_source_cache.get(cache_key)
                if current:
                    return current[1]
            return {
                'status': 'success',
                'imdb': {'score': None, 'votes': None},
                'tmdb': {'score': None, 'votes': None},
                'source_status': {'imdb': 'refreshing', 'tmdb': 'refreshing'},
                'cache_status': 'refreshing',
            }
        return _refresh_rating_sources(
            cache_key, media_type, tmdb_id, imdb_id, event
        )

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
        cache_key = f'home_trending_v4:{source}'
        now = time.time()
        with home_response_cache_lock:
            cached = home_response_cache.get(cache_key)
            if cached and cached[0] > now:
                return jsonify(cached[1])
        hero_limit = int(os.environ.get('HOME_TRENDING_HERO_LIMIT', '10'))
        ranking, cache_state, fetched_at = fetch_tmdb_home_ranking(source)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT m.id, m.title, m.year, m.poster_url, m.backdrop_poster_url,
                       m.rating, m.genre, m.category, m.language, m.imdb_id,
                       {local_movie_has_file_sql('m')} AS is_available
                FROM movies m
                WHERE m.poster_url IS NOT NULL AND m.poster_url <> ''
            """)
            local_rows = cur.fetchall()
        finally:
            close_db_connection(conn)

        local_by_imdb = {str(row[9]).strip().lower(): row for row in local_rows if row[9]}
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
                'backdrop': local_row[4] or local_row[3] or '/static/miniapp/poster-placeholder.svg',
                'rating': local_row[5] or 'N/A',
                'genre': local_row[6] or 'Unknown',
                'category': local_row[7] or 'Movie',
                'language': local_row[8] or '',
                'source': 'local',
                'tmdb_id': item.get('id'),
                'tmdb_rank': len(results) + 1,
                'tmdb_source': source,
                'is_available': bool(local_row[10]),
                'availability_state': 'available' if local_row[10] else 'unavailable',
                'cached': cache_state in {'cached', 'stale'},
                'fetched_at': fetched_at,
            })
            if len(results) >= hero_limit:
                break

        result = {
            'status': 'success',
            'source': source,
            'fresh': cache_state == 'fresh',
            'results': results,
            'total_ranked': len(ranking),
            'local_matches': len(results),
            'cached': cache_state in {'cached', 'stale'},
            'fetched_at': fetched_at,
            'hero_limit': hero_limit,
        }
        with home_response_cache_lock:
            home_response_cache[cache_key] = (time.time() + availability_response_ttl, result)
        return jsonify(result)

    @flask_app.route('/api/home/new-releases', methods=['GET'])
    def get_home_new_releases():
        cache_key = 'home_new_releases_v1'
        home_cache_key = 'home_new_releases_v4'
        now = time.time()
        with home_response_cache_lock:
            cached = home_response_cache.get(home_cache_key)
            if cached and cached[0] > now:
                return jsonify(cached[1])

        with new_releases_refresh_lock:
            now = time.time()
            with home_response_cache_lock:
                cached = home_response_cache.get(home_cache_key)
                if cached and cached[0] > now:
                    return jsonify(cached[1])

            today = datetime.utcnow().date()
            windows = [(30, '30-day'), (60, '60-day fallback')]
            conn = get_db_connection()
            if not conn:
                return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
            try:
                cur = conn.cursor()
                cur.execute(f"""
                    SELECT m.id, m.title, m.year, m.poster_url, m.backdrop_poster_url,
                           m.rating, m.genre, m.category, m.language, m.imdb_id,
                           {local_movie_has_file_sql('m')} AS is_available
                    FROM movies m
                """)
                local_rows = cur.fetchall()
                cur.close()
            finally:
                close_db_connection(conn)

            local_by_imdb = {str(row[9]).strip().lower(): row for row in local_rows if row[9]}
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
                    'backdrop': row[4] or row[3] or '/static/miniapp/poster-placeholder.svg',
                    'rating': row[5] or 'N/A', 'genre': row[6] or 'Unknown',
                    'category': row[7] or 'Movie', 'language': row[8] or '', 'source': 'local',
                    'release_date': release_date.isoformat(), 'release_label': release_label,
                    'release_type': detail['release_type'],
                    'is_available': bool(row[10]),
                    'availability_state': 'available' if row[10] else 'unavailable'
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
            with home_response_cache_lock:
                home_response_cache[home_cache_key] = (
                    time.time() + availability_response_ttl,
                    result,
                )
            return jsonify(result)

    @flask_app.route('/api/home/catalogue-rows', methods=['GET'])
    def get_home_catalogue_rows():
        """Return independent home collections instead of one paginated slice."""
        cache_key = 'home_catalogue_rows_v5'
        now = time.time()
        with home_response_cache_lock:
            cached = home_response_cache.get(cache_key)
            if cached and cached[0] > now:
                return jsonify(cached[1])

        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute(f"""
                WITH classified AS (
                    SELECT m.id, m.title, m.year, m.poster_url, m.backdrop_poster_url, m.rating, m.genre, m.category,
                           COALESCE(m.language, '') AS language, m.created_at,
                           {local_movie_has_file_sql('m')} AS is_available,
                           CASE
                               WHEN LOWER(COALESCE(category, '')) ~ '(^|[^a-z])anime([^a-z]|$)'
                                    OR LOWER(COALESCE(genre, '')) ~ '(^|[^a-z])anime([^a-z]|$)'
                                   THEN 'anime'
                               WHEN LOWER(COALESCE(category, '')) ~ '(^|[^a-z])(bollywood|hindi)([^a-z]|$)'
                                    OR (
                                        LOWER(COALESCE(language, '')) ~ '(^|[^a-z])hindi([^a-z]|$)'
                                        AND LOWER(CONCAT_WS(' ', COALESCE(category, ''), COALESCE(genre, ''))) !~ '(korean|japan|japanese|china|chinese|anime)'
                                    )
                                   THEN 'bollywood'
                               WHEN LOWER(COALESCE(category, '')) ~ '(^|[^a-z])hollywood([^a-z]|$)'
                                    OR (
                                        LOWER(COALESCE(category, '')) IN ('english', 'english movie', 'english movies', 'movie', 'movies', 'film', 'films')
                                        AND LOWER(COALESCE(language, '')) ~ '(^|[^a-z])english([^a-z]|$)'
                                        AND LOWER(CONCAT_WS(' ', COALESCE(category, ''), COALESCE(genre, ''))) !~ '(korean|japan|japanese|china|chinese|anime)'
                                    )
                                   THEN 'hollywood'
                               ELSE NULL
                           END AS collection,
                           CASE
                               WHEN BTRIM(COALESCE(rating, '')) ~ '^[0-9]+([.][0-9]+)?$'
                                   THEN BTRIM(rating)::numeric
                               ELSE -1
                           END AS rating_value
                    FROM movies m
                    WHERE m.poster_url IS NOT NULL AND m.poster_url <> ''
                ),
                ranked AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY collection
                        ORDER BY rating_value DESC, id DESC
                    ) AS row_number
                    FROM classified
                    WHERE collection IS NOT NULL
                )
                SELECT id, title, year, poster_url, backdrop_poster_url, rating, genre, category,
                       language, collection, is_available
                FROM ranked
                WHERE row_number <= 12
                ORDER BY collection, row_number
            """)
            rows = cur.fetchall()
            collections = {'hollywood': [], 'bollywood': [], 'anime': []}
            for row in rows:
                collections[row[9]].append({
                    'id': row[0],
                    'title': row[1],
                    'year': row[2] or '',
                    'image': row[3] or '/static/miniapp/poster-placeholder.svg',
                    'backdrop': row[4] or row[3] or '/static/miniapp/poster-placeholder.svg',
                    'rating': row[5] or 'N/A',
                    'genre': row[6] or 'Unknown',
                    'category': row[7] or 'Movie',
                    'language': row[8] or '',
                    'source': 'local',
                    'is_available': bool(row[10]),
                    'availability_state': 'available' if row[10] else 'unavailable',
                })
            result = {'status': 'success', 'collections': collections}
            with home_response_cache_lock:
                home_response_cache[cache_key] = (time.time() + availability_response_ttl, result)
            return jsonify(result)
        except Exception as error:
            logger.error('Home catalogue rows error: %s', error)
            return jsonify({'status': 'error', 'message': 'Could not load catalogue rows'}), 500
        finally:
            close_db_connection(conn)

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

    def telegram_photo_url(user):
        photo_url = str(user.get('photo_url') or '').strip()[:2048]
        parsed_photo_url = urlparse(photo_url)
        if (
            parsed_photo_url.scheme.lower() != 'https'
            or not parsed_photo_url.netloc
            or parsed_photo_url.username
            or parsed_photo_url.password
        ):
            return ''
        return photo_url

    def upsert_miniapp_user(cur, user):
        photo_url = telegram_photo_url(user)
        cur.execute("""
            INSERT INTO miniapp_users (user_id, username, first_name, photo_url, last_seen)
            VALUES (%s, %s, %s, NULLIF(%s, ''), CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                photo_url = COALESCE(EXCLUDED.photo_url, miniapp_users.photo_url),
                last_seen = CURRENT_TIMESTAMP
        """, (user['id'], user.get('username', ''), user.get('first_name', ''), photo_url))

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
        guest_number = int.from_bytes(digest[:8], byteorder='big', signed=False) & ((1 << 63) - 1)
        guest_id = -max(1, guest_number)
        return {
            'id': guest_id,
            'username': '',
            'first_name': 'Guest',
        }

    @flask_app.route('/api/home/daily-picks', methods=['GET'])
    def get_home_daily_picks():
        """Return a stable three-title shortlist for each user and India-local day."""
        india_tz = timezone(timedelta(hours=5, minutes=30))
        now_india = datetime.now(india_tz)
        today = now_india.date()
        tomorrow = datetime.combine(today + timedelta(days=1), datetime.min.time(), india_tz)
        signed_in_user = telegram_user_from_request()
        user_key = str(signed_in_user['id']) if signed_in_user else 'guest'
        cache_key = f'home_daily_picks_v1:{today.isoformat()}:{user_key}'
        now = time.time()
        with home_response_cache_lock:
            cached = home_response_cache.get(cache_key)
            if cached and cached[0] > now:
                return jsonify(cached[1])

        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            profile_rows = []
            genre_scores = {}
            language_scores = {}
            category_scores = {}
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
                    'rating_submitted': 5, 'watchlist_add': 4, 'miniapp_download': 4,
                    'miniapp_open_details': 2, 'surprise_click': 2, 'daily_pick_click': 3,
                    'mood_pick_liked': 6, 'mood_pick_skipped': -4,
                    'surprise_skip': -3, 'surprise_impression': 0,
                }
                for genre, language, category, event_type, count in profile_rows:
                    weight = event_weights.get(event_type, 1) * count
                    for token in re.split(r'[,/&|]', (genre or '').lower()):
                        token = token.strip()
                        if token:
                            genre_scores[token] = genre_scores.get(token, 0) + weight
                    for token in re.split(r'[,/&|]', (language or '').lower()):
                        token = token.strip()
                        if token:
                            language_scores[token] = language_scores.get(token, 0) + weight
                    category_key = (category or '').strip().lower()
                    if category_key:
                        category_scores[category_key] = category_scores.get(category_key, 0) + weight

            has_file_sql = local_movie_has_file_sql('m')
            cur.execute(f"""
                SELECT m.id, m.title, m.year, m.poster_url, m.backdrop_poster_url,
                       m.rating, m.genre, m.category, m.language, m.content_type,
                       {has_file_sql} AS is_available
                FROM movies m
                WHERE NOT COALESCE(m.is_unreleased, FALSE)
                  AND {has_file_sql}
                ORDER BY m.id DESC
                LIMIT 1200
            """)
            scored = []
            for row in cur.fetchall():
                genre_tokens = {token.strip().lower() for token in re.split(r'[,/&|]', str(row[6] or '')) if token.strip()}
                category_tokens = {token.strip().lower() for token in re.split(r'[,/&|]', str(row[7] or '')) if token.strip()}
                language_tokens = {token.strip().lower() for token in re.split(r'[,/&|]', str(row[8] or '')) if token.strip()}
                score = sum(genre_scores.get(token, 0) for token in genre_tokens)
                score += sum(category_scores.get(token, 0) for token in category_tokens)
                score += sum(language_scores.get(token, 0) for token in language_tokens)
                rating_text = str(row[5] or '').strip()
                rating_score = float(rating_text) if re.fullmatch(r'\d+(?:\.\d+)?', rating_text) else 0
                scored.append((score + rating_score * 0.25, row))

            scored.sort(key=lambda item: (item[0], item[1][0]), reverse=True)
            top_pool = [row for _score, row in scored[:min(80, len(scored))]]
            seed_text = f'{today.isoformat()}:{user_key}'
            seed = int.from_bytes(hashlib.sha256(seed_text.encode('utf-8')).digest()[:8], 'big')
            random.Random(seed).shuffle(top_pool)
            movies = [{
                'id': row[0], 'title': row[1], 'year': row[2] or '',
                'image': row[3] or '/static/miniapp/poster-placeholder.svg',
                'backdrop': row[4] or row[3] or '/static/miniapp/poster-placeholder.svg',
                'rating': row[5] or 'N/A', 'genre': row[6] or 'Unknown',
                'category': row[7] or 'Movie', 'language': row[8] or '',
                'content_type': row[9] or 'Movie', 'source': 'local',
                'is_available': bool(row[10]), 'availability_state': 'available',
            } for row in top_pool[:3]]
            cur.close()
            result = {
                'status': 'success', 'date': today.isoformat(),
                'date_label': today.strftime('%a, %d %b'),
                'refreshes_at': tomorrow.isoformat(), 'movies': movies,
                'personalized': any(row[3] != 'surprise_impression' for row in profile_rows),
            }
            ttl = max(1, min(21600, (tomorrow - now_india).total_seconds()))
            with home_response_cache_lock:
                daily_keys = []
                for stale_key, cache_value in list(home_response_cache.items()):
                    if not stale_key.startswith('home_daily_picks_v1:'):
                        continue
                    if stale_key != cache_key and (cache_value[0] <= time.time() or f':{today.isoformat()}:' not in stale_key):
                        del home_response_cache[stale_key]
                    elif stale_key != cache_key:
                        daily_keys.append(stale_key)
                while len(daily_keys) >= 500:
                    home_response_cache.pop(daily_keys.pop(0), None)
                home_response_cache[cache_key] = (time.time() + ttl, result)
            return jsonify(result)
        except Exception as error:
            logger.error('Daily picks error: %s', error)
            return jsonify({'status': 'error', 'message': 'Could not load today\'s picks'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/home/daily-checkin', methods=['GET', 'POST'])
    def post_home_daily_checkin():
        user, error = require_telegram_user()
        if error:
            return error
        india_tz = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(india_tz).date()
        today_text = today.isoformat()
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            new_checkin = False
            if request.method == 'POST':
                # Lock the user's row so simultaneous check-ins remain
                # idempotent: one user can earn only one visit per India day.
                upsert_miniapp_user(cur, user)
                cur.execute("""
                    SELECT 1
                    FROM user_recommendation_events
                    WHERE user_id = %s AND event_type = 'daily_checkin'
                      AND metadata->>'date' = %s
                    LIMIT 1
                """, (user['id'], today_text))
                new_checkin = cur.fetchone() is None
                if new_checkin:
                    cur.execute("""
                        INSERT INTO user_recommendation_events
                            (user_id, movie_id, event_type, source, metadata)
                        VALUES (%s, NULL, 'daily_checkin', 'miniapp', %s::jsonb)
                    """, (user['id'], json.dumps({'date': today_text})))

            cur.execute("""
                SELECT DISTINCT metadata->>'date'
                FROM user_recommendation_events
                WHERE user_id = %s AND event_type = 'daily_checkin'
                  AND metadata->>'date' <= %s
            """, (user['id'], today_text))
            checkin_dates = set()
            for row in cur.fetchall():
                try:
                    checkin_dates.add(datetime.strptime(row[0], '%Y-%m-%d').date())
                except (TypeError, ValueError):
                    continue

            # Before today's explicit tap, an unbroken streak ends yesterday.
            streak_day = today if today in checkin_dates else today - timedelta(days=1)
            current_streak = 0
            while streak_day in checkin_dates:
                current_streak += 1
                streak_day -= timedelta(days=1)

            ticket_earned = False
            tickets_earned_count = 0
            if request.method == 'POST' and current_streak >= 7:
                run_start = (today - timedelta(days=current_streak - 1)).isoformat()
                cur.execute("""
                    SELECT metadata->>'streak_days'
                    FROM user_recommendation_events
                    WHERE user_id = %s AND event_type = 'priority_ticket_earned'
                      AND metadata->>'run_start' = %s
                """, (user['id'], run_start))
                already_awarded = {row[0] for row in cur.fetchall() if row[0]}
                for milestone_day in range(7, current_streak + 1, 7):
                    if str(milestone_day) in already_awarded:
                        continue
                    reward_day = (today - timedelta(days=current_streak - milestone_day)).isoformat()
                    cur.execute("""
                        INSERT INTO user_recommendation_events
                            (user_id, movie_id, event_type, source, metadata)
                        VALUES (%s, NULL, 'priority_ticket_earned', 'streak', %s::jsonb)
                    """, (user['id'], json.dumps({
                        'date': reward_day,
                        'run_start': run_start,
                        'streak_days': milestone_day,
                    })))
                    ticket_earned = True
                    tickets_earned_count += 1
            cur.execute("""
                SELECT COALESCE(SUM(CASE
                    WHEN event_type = 'priority_ticket_earned' THEN 1
                    WHEN event_type = 'priority_ticket_used' THEN -1
                    ELSE 0
                END), 0)
                FROM user_recommendation_events
                WHERE user_id = %s
                  AND event_type IN ('priority_ticket_earned', 'priority_ticket_used')
            """, (user['id'],))
            priority_tickets = max(0, int((cur.fetchone() or [0])[0] or 0))
            if request.method == 'POST':
                conn.commit()

            def longest_streak(days):
                best = run_length = 0
                previous_day = None
                for checkin_day in sorted(days):
                    run_length = (
                        run_length + 1
                        if previous_day and checkin_day == previous_day + timedelta(days=1)
                        else 1
                    )
                    best = max(best, run_length)
                    previous_day = checkin_day
                return best

            best_streak = longest_streak(checkin_dates)
            best_before_today = longest_streak(checkin_dates - {today})

            milestones = [
                {'days': 3, 'name': 'Fresh Start'},
                {'days': 7, 'name': 'Weekly Regular'},
                {'days': 14, 'name': 'Film Fan'},
                {'days': 30, 'name': 'Flimfy Legend'},
                {'days': 60, 'name': 'Super Fan'},
                {'days': 100, 'name': 'Century Club'},
            ]
            next_milestone = next(
                (item for item in milestones if item['days'] > current_streak),
                None,
            )
            if next_milestone is None:
                next_days = ((current_streak // 100) + 1) * 100
                next_milestone = {'days': next_days, 'name': f'{next_days}-Day Legend'}
            earned_badges = [
                {
                    **item,
                    'unlocked': best_streak >= item['days'],
                    'newly_unlocked': (
                        new_checkin
                        and best_before_today < item['days'] <= best_streak
                    ),
                }
                for item in milestones
            ]
            week = []
            for days_ago in range(6, -1, -1):
                day = today - timedelta(days=days_ago)
                day_text = day.isoformat()
                week.append({
                    'date': day_text, 'label': day.strftime('%a')[:2],
                    'done': day in checkin_dates, 'is_today': day == today,
                })
            cur.close()
            return jsonify({
                'status': 'success', 'current_streak': current_streak,
                'best_streak': best_streak,
                'total_checkins': len(checkin_dates),
                'checked_in_today': today in checkin_dates,
                'new_checkin': new_checkin,
                'ticket_earned': ticket_earned,
                'tickets_earned_count': tickets_earned_count,
                'priority_tickets': priority_tickets,
                'next_milestone': next_milestone,
                'badges': earned_badges,
                'week': week,
            })
        except Exception as error:
            conn.rollback()
            logger.error('Daily check-in error: %s', error)
            return jsonify({'status': 'error', 'message': 'Could not update today\'s streak'}), 500
        finally:
            close_db_connection(conn)

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
            'surprise_click', 'surprise_skip', 'mood_pick_liked',
            'mood_pick_skipped', 'daily_pick_click',
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
                cur.execute("SELECT is_unreleased FROM movies WHERE id = %s", (movie_id,))
                movie_row = cur.fetchone()
                if not movie_row:
                    return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
                if movie_row[0]:
                    conn.rollback()
                    return jsonify({
                        'status': 'error',
                        'message': 'Rating is available after release.',
                        'rating_locked': True
                    }), 409
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
        payload = (request.get_json(silent=True) or {}) if request.method == 'POST' else {}
        if request.method == 'POST' and not isinstance(payload, dict):
            return jsonify({'status': 'error', 'message': 'Invalid message payload.'}), 400
        message = str(payload.get('message') or '').strip() if request.method == 'POST' else ''
        if request.method == 'POST' and (not message or len(message) > 3800):
            return jsonify({'status': 'error', 'message': 'Message must be between 1 and 3800 characters.'}), 400
        client_message_id = None
        if request.method == 'POST':
            try:
                client_message_id = str(uuid.UUID(str(payload.get('client_id') or '')))
            except (ValueError, TypeError, AttributeError):
                return jsonify({'status': 'error', 'message': 'Invalid message id. Please retry.'}), 400
        after_id = request.args.get('after_id', type=int) if request.method == 'GET' else None

        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            if request.method == 'POST':
                upsert_miniapp_user(cur, user)
                cur.execute("""
                    INSERT INTO global_chat_messages
                        (user_id, username, first_name, message, source, client_message_id)
                    VALUES (%s, %s, %s, %s, 'miniapp', %s)
                    ON CONFLICT (user_id, client_message_id) DO NOTHING
                    RETURNING id, user_id, username, first_name, message, created_at,
                              source, client_message_id
                """, (user['id'], user.get('username', ''), user.get('first_name', ''),
                      message, client_message_id))
                row = cur.fetchone()
                if row:
                    cur.execute("""
                        INSERT INTO global_chat_relay_outbox (message_id)
                        VALUES (%s)
                    """, (row[0],))
                    relay_status = 'pending'
                else:
                    cur.execute("""
                        SELECT m.id, m.user_id, m.username, m.first_name, m.message,
                               m.created_at, m.source, m.client_message_id,
                               COALESCE(o.status, 'sent')
                        FROM global_chat_messages m
                        LEFT JOIN global_chat_relay_outbox o ON o.message_id = m.id
                        WHERE m.user_id = %s AND m.client_message_id = %s
                    """, (user['id'], client_message_id))
                    row = cur.fetchone()
                    if not row or row[4] != message:
                        conn.rollback()
                        return jsonify({'status': 'error', 'message': 'Message retry did not match.'}), 409
                    relay_status = row[8]
                    if relay_status == 'failed':
                        cur.execute("""
                            UPDATE global_chat_relay_outbox
                            SET status = 'pending', attempts = 0,
                                next_attempt_at = CURRENT_TIMESTAMP, locked_at = NULL,
                                last_error = NULL, updated_at = CURRENT_TIMESTAMP
                            WHERE message_id = %s
                        """, (row[0],))
                        relay_status = 'pending'
                conn.commit()
                created_at = row[5]
                if created_at and created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                return jsonify({
                    'status': 'success',
                    'message': {
                        'id': row[0], 'is_mine': True, 'username': row[2] or '',
                        'first_name': row[3] or 'User', 'message': row[4],
                        'photo_url': telegram_photo_url(user),
                        'created_at': created_at.isoformat() if created_at else '',
                        'source': row[6] or 'miniapp', 'client_id': str(row[7]),
                        'relay_status': relay_status,
                    }
                })

            if after_id is None or after_id < 0:
                upsert_miniapp_user(cur, user)
                conn.commit()
            if after_id is not None and after_id >= 0:
                cur.execute("""
                    SELECT m.id, m.user_id, m.username, m.first_name, m.message,
                       m.created_at, m.source, m.client_message_id,
                       COALESCE(o.status, 'sent'), u.photo_url
                    FROM global_chat_messages m
                    LEFT JOIN global_chat_relay_outbox o ON o.message_id = m.id
                    LEFT JOIN miniapp_users u ON u.user_id = m.user_id
                    WHERE m.id > %s
                    ORDER BY m.id ASC
                    LIMIT 100
                """, (after_id,))
                rows = cur.fetchall()
            else:
                cur.execute("""
                    SELECT m.id, m.user_id, m.username, m.first_name, m.message,
                       m.created_at, m.source, m.client_message_id,
                       COALESCE(o.status, 'sent'), u.photo_url
                    FROM global_chat_messages m
                    LEFT JOIN global_chat_relay_outbox o ON o.message_id = m.id
                    LEFT JOIN miniapp_users u ON u.user_id = m.user_id
                    ORDER BY m.id DESC
                    LIMIT 80
                """)
                rows = list(reversed(cur.fetchall()))

            messages = []
            for row in rows:
                created_at = row[5]
                if created_at and created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                messages.append({
                    'id': row[0], 'is_mine': row[1] == user['id'],
                    'username': row[2] or '', 'first_name': row[3] or 'Community member',
                    'message': row[4], 'created_at': created_at.isoformat() if created_at else '',
                    'source': row[6] or 'miniapp',
                    'client_id': str(row[7]) if row[7] else '', 'relay_status': row[8] or 'sent',
                    'photo_url': row[9] or '',
                })

            cur.execute("""
                SELECT o.message_id, o.status
                FROM global_chat_relay_outbox o
                JOIN global_chat_messages m ON m.id = o.message_id
                WHERE m.user_id = %s
                  AND m.created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                ORDER BY o.message_id DESC
                LIMIT 50
            """, (user['id'],))
            relay_updates = [{'id': row[0], 'relay_status': row[1]} for row in cur.fetchall()]
            return jsonify({
                'status': 'success', 'messages': messages,
                'relay_updates': relay_updates,
            })
        except Exception:
            conn.rollback()
            logger.exception('Global chat request failed')
            return jsonify({'status': 'error', 'message': 'Chat is temporarily unavailable.'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/upcoming', methods=['GET'])
    def get_upcoming_titles():
        limit = min(max(request.args.get('limit', 18, type=int), 1), 30)
        # Releases are calendar dates. Use the catalogue's India timezone so a
        # title released at midnight does not remain upcoming until 05:30 IST.
        today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
        cache_key = f'api-upcoming:v3:{today.isoformat()}:{limit}'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
        results = []
        seen = set()
        end_date = today + timedelta(days=180)
        failed_sources = 0
        for media_type in ('movie', 'tv'):
            params = {
                'page': 1,
                'sort_by': 'primary_release_date.asc' if media_type == 'movie' else 'first_air_date.asc',
                'vote_count.gte': 1,
                'with_release_type': '2|3|4',
                'primary_release_date.gte': (today + timedelta(days=1)).isoformat(),
                'primary_release_date.lte': end_date.isoformat(),
            } if media_type == 'movie' else {
                'page': 1,
                'sort_by': 'first_air_date.asc',
                'vote_count.gte': 1,
                'first_air_date.gte': (today + timedelta(days=1)).isoformat(),
                'first_air_date.lte': end_date.isoformat(),
            }
            data, _ = tmdb_cached_request(
                f'discover/{media_type}', params,
                f'upcoming:{media_type}:{today.isoformat()}',
                ttl_seconds=6 * 60 * 60,
                attempts=1,
                request_timeout=(1, 3),
            )
            source_items = data.get('results') if isinstance(data, dict) else None
            if not isinstance(source_items, list):
                failed_sources += 1
                continue
            for item in source_items:
                tmdb_id = item.get('id')
                release_date = item.get('release_date' if media_type == 'movie' else 'first_air_date') or ''
                if not tmdb_id or not release_date or (media_type, tmdb_id) in seen:
                    continue
                try:
                    if not isinstance(release_date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', release_date):
                        continue
                    release_day = datetime.strptime(release_date, '%Y-%m-%d').date()
                except (TypeError, ValueError):
                    continue
                # Discovery can contain re-releases and stale provider rows.
                # Never fabricate a date or display an already released title.
                if not today < release_day <= end_date:
                    continue
                seen.add((media_type, tmdb_id))
                is_unreleased = True
                results.append({
                    'id': f'tmdb_{tmdb_id}',
                    'tmdb_id': tmdb_id,
                    'media_type': media_type,
                    'title': item.get('title') or item.get('name') or 'Unknown',
                    'year': release_date[:4],
                    'release_date': release_date,
                    'image': f"https://image.tmdb.org/t/p/w500{item['poster_path']}" if item.get('poster_path') else '/static/miniapp/poster-placeholder.svg',
                    'backdrop': f"https://image.tmdb.org/t/p/original{item['backdrop_path']}" if item.get('backdrop_path') else None,
                    'rating': round(float(item.get('vote_average') or 0), 1),
                    'category': 'Movie' if media_type == 'movie' else 'TV Series',
                    'source': 'tmdb',
                    'description': item.get('overview') or '',
                    'is_unreleased': is_unreleased,
                    'is_upcoming': is_unreleased,
                    'is_released': not is_unreleased,
                    'is_available': False,
                    'availability_state': 'upcoming' if is_unreleased else 'released',
                    'release_state': 'released' if not is_unreleased else 'upcoming'
                })
        if failed_sources == 2:
            return jsonify({
                'status': 'error',
                'message': 'Upcoming titles are temporarily unavailable. Please retry.',
            }), 503
        results.sort(key=lambda item: item['release_date'])
        response = {
            'status': 'success',
            'movies': results[:limit],
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'partial': failed_sources > 0,
        }
        if not failed_sources:
            api_movies_cache.set(cache_key, response)
        return jsonify(response)

    @flask_app.route('/api/upcoming/reminder', methods=['GET', 'POST'])
    def toggle_upcoming_reminder():
        user, error = require_telegram_user()
        if error:
            return error
        payload = request.get_json(silent=True) or {}
        raw_tmdb_id = payload.get('tmdb_id') or payload.get('id') or request.args.get('tmdb_id')
        action = (payload.get('action') or 'set').lower()
        stage = (payload.get('stage') or request.args.get('stage') or 'release').lower()
        if stage not in {'release', 'availability'}:
            return jsonify({'status': 'error', 'message': 'Invalid notification stage.'}), 400
        title = payload.get('title') or request.args.get('title') or 'Upcoming title'
        user_id = user['id']

        if raw_tmdb_id is None:
            return jsonify({'status': 'error', 'message': 'Missing release id.'}), 400

        tmdb_id = str(raw_tmdb_id).replace('tmdb_', '').strip()
        if not tmdb_id or not re.fullmatch(r'\d+', tmdb_id):
            return jsonify({'status': 'error', 'message': 'Invalid release id.'}), 400

        release_date_raw = payload.get('release_date') or request.args.get('release_date')
        release_date = None
        if release_date_raw:
            try:
                release_date = datetime.strptime(str(release_date_raw), '%Y-%m-%d').date()
            except (TypeError, ValueError):
                return jsonify({'status': 'error', 'message': 'A valid release date is required.'}), 400
        elif request.method == 'POST':
            return jsonify({'status': 'error', 'message': 'A valid release date is required.'}), 400

        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT movie_title, release_date, release_notification_requested,
                           availability_notification_requested, release_notified_at,
                           availability_notified_at
                    FROM upcoming_notifications
                    WHERE user_id = %s AND tmdb_id = %s
                    """,
                    (user_id, tmdb_id),
                )
                existing = cur.fetchone()
                if action == 'remove':
                    if stage == 'release':
                        cur.execute("""
                            UPDATE upcoming_notifications
                            SET release_notification_requested = FALSE
                            WHERE user_id = %s AND tmdb_id = %s
                        """, (user_id, tmdb_id))
                    else:
                        cur.execute("""
                            UPDATE upcoming_notifications
                            SET availability_notification_requested = FALSE
                            WHERE user_id = %s AND tmdb_id = %s
                        """, (user_id, tmdb_id))
                    conn.commit()
                    return jsonify({'status': 'success', 'message': 'Notification removed', 'reminder': False, 'tmdb_id': tmdb_id})

                if request.method == 'GET':
                    release_date_value = existing[1] if existing else release_date
                    today = datetime.utcnow().date()
                    is_released = bool(release_date_value and release_date_value <= today)
                    local_available = False
                    if existing or title:
                        cur.execute("""
                            SELECT EXISTS(
                                SELECT 1
                                FROM movies m
                                JOIN movie_files mf ON mf.movie_id = m.id
                                WHERE (
                                    m.tmdb_id = %s
                                    OR (
                                        m.tmdb_id IS NULL
                                        AND LOWER(REGEXP_REPLACE(m.title, '[^a-z0-9]', '', 'g'))
                                            = LOWER(REGEXP_REPLACE(%s, '[^a-z0-9]', '', 'g'))
                                    )
                                )
                                AND (
                                    NULLIF(mf.url, '') IS NOT NULL
                                    OR NULLIF(mf.file_id, '') IS NOT NULL
                                )
                            )
                        """, (int(tmdb_id), existing[0] if existing else title))
                        local_available = bool(cur.fetchone()[0])
                    state = 'available' if local_available else ('released' if is_released else 'upcoming')
                    return jsonify({
                        'status': 'success',
                        'reminder': bool(existing and (existing[2] if stage == 'release' else existing[3])),
                        'release_notification_set': bool(existing and existing[2]),
                        'availability_notification_set': bool(existing and existing[3]),
                        'release_notified': bool(existing and existing[4]),
                        'availability_notified': bool(existing and existing[5]),
                        'availability_state': state,
                        'is_available': local_available,
                        'tmdb_id': tmdb_id
                    })
                if stage == 'availability':
                    cur.execute("""
                        SELECT EXISTS(
                            SELECT 1 FROM movies m
                            JOIN movie_files mf ON mf.movie_id = m.id
                            WHERE (
                                m.tmdb_id = %s
                                OR (
                                    m.tmdb_id IS NULL
                                    AND LOWER(REGEXP_REPLACE(m.title, '[^a-z0-9]', '', 'g'))
                                        = LOWER(REGEXP_REPLACE(%s, '[^a-z0-9]', '', 'g'))
                                )
                            )
                            AND (
                                NULLIF(mf.url, '') IS NOT NULL
                                OR NULLIF(mf.file_id, '') IS NOT NULL
                            )
                        )
                    """, (int(tmdb_id), title))
                    if cur.fetchone()[0]:
                        return jsonify({'status': 'error', 'message': 'This title is already available.'}), 409
                cur.execute(
                    """
                    INSERT INTO upcoming_notifications
                        (user_id, tmdb_id, movie_title, release_date,
                         release_notification_requested, availability_notification_requested)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, tmdb_id)
                    DO UPDATE SET movie_title = EXCLUDED.movie_title,
                                  release_date = EXCLUDED.release_date,
                                  release_notification_requested = CASE
                                      WHEN %s THEN TRUE ELSE upcoming_notifications.release_notification_requested END,
                                  availability_notification_requested = CASE
                                      WHEN %s THEN TRUE ELSE upcoming_notifications.availability_notification_requested END
                    """,
                    (user_id, tmdb_id, title, release_date, stage == 'release', stage == 'availability',
                     stage == 'release', stage == 'availability'),
                )
                conn.commit()
                return jsonify({
                    'status': 'success',
                    'message': 'Notification already enabled.' if existing and (existing[2] if stage == 'release' else existing[3]) else 'Notification set.',
                    'reminder': True, 'already_enabled': bool(existing),
                    'stage': stage,
                    'tmdb_id': tmdb_id
                })
            except Exception:
                conn.rollback()
                logger.exception('Upcoming reminder update failed')
                return jsonify({'status': 'error', 'message': 'Could not update reminder.'}), 500
            finally:
                close_db_connection(conn)

        return jsonify({'status': 'error', 'message': 'Notification service is temporarily unavailable.'}), 503

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
            'average': float(average) if count else 0,
            'count': int(count),
            'user_rating': int(own[0]) if own else None,
            'can_rate': own is None
        }

    @flask_app.route('/healthz', methods=['GET', 'HEAD'])
    def healthz():
        """Fast Render health probe; intentionally independent of TMDB/DB."""
        response = jsonify({
            'status': 'ok', 'service': 'flimfybox-miniapp',
            'revision': os.environ.get('RENDER_GIT_COMMIT', 'local'),
        })
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
        cache_key = f'api_genres_{browse_type}_v2'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT genre, category, content_type, seasons_data, poster_url, rating, id
                FROM movies
                WHERE genre IS NOT NULL AND genre <> ''
                ORDER BY
                    CASE
                        WHEN BTRIM(COALESCE(rating, '')) ~ '^[0-9]+([.][0-9]+)?$'
                            THEN BTRIM(rating)::numeric
                        ELSE -1
                    END DESC,
                    id DESC
            """)
            counts = {genre_id: 0 for genre_id, *_ in CANONICAL_GENRES}
            posters = {genre_id: [] for genre_id, *_ in CANONICAL_GENRES}
            poster_candidates = {genre_id: [] for genre_id, *_ in CANONICAL_GENRES}
            for raw_genre, category, content_type, seasons_data, poster_url, _rating, _movie_id in cur.fetchall():
                if not browse_type_matches(browse_type, category, seasons_data, content_type):
                    continue
                for genre_id in normalize_catalogue_genres(raw_genre):
                    if genre_id in counts:
                        counts[genre_id] += 1
                        if (
                            poster_url
                            and poster_url not in poster_candidates[genre_id]
                            and len(poster_candidates[genre_id]) < 12
                        ):
                            poster_candidates[genre_id].append(poster_url)

            # A title can belong to several genres. Reserve the best unique
            # poster for the first matching genre so the Browse cards do not
            # visually repeat the same artwork across the grid.
            used_posters = set()
            for genre_id, *_ in CANONICAL_GENRES:
                unique = [
                    poster for poster in poster_candidates[genre_id]
                    if poster not in used_posters
                ][:3]
                posters[genre_id] = unique
                used_posters.update(unique)
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
        cache_key = f'api_genre_{genre_id}_{browse_type}_v2'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT id, title, year, poster_url, backdrop_poster_url, rating, genre, category, language, content_type, seasons_data,
                       {local_movie_has_file_sql('movies')} AS is_available
                FROM movies
                ORDER BY id DESC
            """)
            movies = []
            for row in cur.fetchall():
                if genre_id not in normalize_catalogue_genres(row[6]) or not browse_type_matches(browse_type, row[7], row[10], row[9]):
                    continue
                movies.append({
                    'id': row[0],
                    'title': row[1],
                    'year': row[2] or '',
                    'image': row[3] or '/static/miniapp/poster-placeholder.svg',
                    'backdrop': row[4] or row[3] or '/static/miniapp/poster-placeholder.svg',
                    'rating': row[5] or 'N/A',
                    'genre': row[6] or '',
                    'category': row[7] or 'Movies',
                    'language': row[8] or '',
                    'content_type': row[9] or 'Movie',
                    'source': 'local',
                    'is_available': bool(row[11]),
                    'availability_state': 'available' if row[11] else 'unavailable'
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
        cache_key = f'api_browse_{browse_type}_v2'
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT id, title, year, poster_url, rating, genre, category, language, content_type, seasons_data,
                       {local_movie_has_file_sql('movies')} AS is_available
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
                    if not browse_type_matches(browse_type, row[6], row[9], row[8]):
                        continue
                    items.append({
                        'id': row[0], 'title': row[1], 'year': year,
                        'image': row[3], 'rating': row[4] or 'N/A',
                        'genre': row[5] or '', 'category': row[6] or 'Movies',
                        'language': row[7] or '', 'content_type': row[8] or 'Movie', 'source': 'local',
                        'is_available': bool(row[10]),
                        'availability_state': 'available' if row[10] else 'unavailable'
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
        mood = request.args.get('mood', 'any').strip().lower()
        mood_genres = {
            'any': set(),
            'fun': {'comedy', 'animation', 'animated', 'family', 'musical'},
            'mind': {'thriller', 'mystery', 'crime', 'sci-fi', 'sci fi', 'science fiction', 'detective'},
            'action': {'action', 'adventure', 'war', 'fantasy', 'superhero'},
            'heart': {'romance', 'romantic', 'drama', 'family'},
            'anime': {'anime', 'animation', 'animated', 'isekai', 'shonen', 'shojo', 'seinen'},
        }
        if mood not in mood_genres:
            return jsonify({'status': 'error', 'message': 'Invalid mood'}), 400
        language = request.args.get('language', 'any').strip().lower()
        if language not in {'any', 'hindi', 'english'}:
            return jsonify({'status': 'error', 'message': 'Invalid language'}), 400
        availability = request.args.get('availability', 'available').strip().lower()
        if availability not in {'available', 'all'}:
            return jsonify({'status': 'error', 'message': 'Availability must be available or all'}), 400
        excluded_ids = []
        for raw_id in request.args.get('exclude', '').split(',')[:8]:
            if not raw_id.strip():
                continue
            try:
                movie_id = int(raw_id)
            except (TypeError, ValueError):
                return jsonify({'status': 'error', 'message': 'Invalid excluded title'}), 400
            if movie_id <= 0:
                return jsonify({'status': 'error', 'message': 'Invalid excluded title'}), 400
            excluded_ids.append(movie_id)
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            classification_sql = "(LOWER(COALESCE(m.content_type, '')) IN ('web series', 'tv series') OR LOWER(COALESCE(m.category, '')) = 'web series' OR (m.seasons_data IS NOT NULL AND m.seasons_data <> '{}'::jsonb))"
            where_type = '' if browse_type == 'all' else (f' AND {classification_sql}' if browse_type == 'tv' else f' AND NOT {classification_sql}')
            has_file_sql = local_movie_has_file_sql('m')
            where_availability = f' AND {has_file_sql}' if availability == 'available' else ''
            excluded_set = set(excluded_ids)
            signed_in_user = telegram_user_from_request()
            profile_rows = []
            recent_ids = set()
            genre_scores = {}
            language_scores = {}
            category_scores = {}
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
                    'mood_pick_liked': 6,
                    'mood_pick_skipped': -4,
                    'surprise_skip': -3,
                    'surprise_impression': 0,
                    'pm_search': 1,
                    'group_search': 1,
                }
                for genre, language, category, event_type, count in profile_rows:
                    weight = event_weights.get(event_type, 1) * count
                    for token in re.split(r'[,/&|]', (genre or '').lower()):
                        token = token.strip()
                        if token:
                            genre_scores[token] = genre_scores.get(token, 0) + weight
                    for language_key in re.split(r'[,/&|]', (language or '').lower()):
                        language_key = language_key.strip()
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
                    SELECT m.id, m.title, m.year, m.poster_url, m.rating, m.genre,
                           m.category, m.language, m.content_type,
                           {has_file_sql} AS is_available
                    FROM movies m
                    WHERE NOT COALESCE(m.is_unreleased, FALSE)
                      {where_availability}{where_type}
                    ORDER BY m.id DESC
                    LIMIT 1200
                """)
            rows = cur.fetchall()
            target_genres = mood_genres[mood]
            candidates = []
            for row in rows:
                movie_id, title, year, poster, rating, genre, category, movie_language, content_type, is_available = row
                if movie_id in excluded_set:
                    continue
                language_value = str(movie_language or '').lower()
                if language != 'any' and not re.search(rf'\b{re.escape(language)}\b', language_value):
                    continue
                genre_tokens = {token.strip().lower() for token in re.split(r'[,/&|]', str(genre or '')) if token.strip()}
                category_tokens = {token.strip().lower() for token in re.split(r'[,/&|]', str(category or '')) if token.strip()}
                content_tokens = {token.strip().lower() for token in re.split(r'[,/&|]', str(content_type or '')) if token.strip()}
                tags = genre_tokens | category_tokens | content_tokens
                matched_genres = sorted(tags & target_genres)
                rating_text = str(rating or '').strip()
                try:
                    rating_score = float(rating_text) if re.fullmatch(r'\d+(?:\.\d+)?', rating_text) else 0
                except (TypeError, ValueError):
                    rating_score = 0
                profile_score = 0
                for token in genre_tokens:
                    profile_score += genre_scores.get(token, 0)
                for token in category_tokens:
                    profile_score += category_scores.get(token, 0)
                for token in re.split(r'[,/&|]', language_value):
                    profile_score += language_scores.get(token.strip(), 0)
                score = profile_score + rating_score * 0.25
                if matched_genres:
                    score += 8 + len(matched_genres)
                candidates.append({
                    'score': score, 'row': row, 'matched_genres': matched_genres,
                    'mood_match': bool(matched_genres) if mood != 'any' else False,
                })

            if not candidates:
                message = 'No titles match these filters. Try changing the language, Browse type, or availability.'
                cur.close()
                return jsonify({'status': 'success', 'type': browse_type, 'movie': None, 'message': message})

            mood_matches = [candidate for candidate in candidates if candidate['mood_match']]
            mood_pool = mood_matches if mood_matches else candidates
            fresh_pool = [candidate for candidate in mood_pool if candidate['row'][0] not in recent_ids]
            pool = fresh_pool or mood_pool
            pool.sort(key=lambda candidate: (candidate['score'], candidate['row'][0]), reverse=True)
            best_score = pool[0]['score']
            shortlist = [candidate for candidate in pool if candidate['score'] >= best_score - 3][:24]
            selected = random.choice(shortlist)
            row = selected['row']
            movie = {
                'id': row[0], 'title': row[1], 'year': row[2] or '',
                'image': row[3] or '/static/miniapp/poster-placeholder.svg',
                'rating': row[4] or 'N/A', 'genre': row[5] or '',
                'category': row[6] or 'Movies', 'language': row[7] or '',
                'content_type': row[8] or 'Movie', 'source': 'local',
                'is_available': bool(row[9]),
                'availability_state': 'available' if row[9] else 'unavailable',
            }
            cur.close()
            return jsonify({
                'status': 'success', 'type': browse_type, 'movie': movie,
                'mood_match': selected['mood_match'],
                'matched_genres': selected['matched_genres'],
                'personalized': any(row[3] != 'surprise_impression' for row in profile_rows),
            })
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
        availability = request.args.get('availability', 'all').strip().lower()
        if availability not in {'all', 'available', 'request'}:
            return jsonify({
                'status': 'error',
                'message': 'Availability must be all, available, or request.'
            }), 400
        if page < 1 or limit < 1 or limit > 100:
            return jsonify({
                'status': 'error',
                'message': 'Page must be positive and limit must be between 1 and 100.'
            }), 400
        
        cache_key = f"api_movies_v5_{availability}_{page}_{limit}"
        cached = api_movies_cache.get(cache_key)
        if cached:
            return jsonify(cached)
            
        offset = (page - 1) * limit
    
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            has_file_sql = local_movie_has_file_sql('m')
            availability_sql = local_movie_availability_filter_sql(availability, 'm')
            cur.execute(f"""
                SELECT m.id, m.title, m.year, m.poster_url, m.backdrop_poster_url,
                       m.rating, m.genre, m.category,
                       COALESCE(m.language, '') AS language,
                       COALESCE(m.content_type, 'Movie') AS content_type,
                       m.created_at,
                       {has_file_sql} AS is_available
                FROM movies m
                WHERE m.poster_url IS NOT NULL AND m.poster_url <> ''
                  AND ({availability_sql})
                ORDER BY m.created_at DESC NULLS LAST, m.id DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            
            rows = cur.fetchall()
            movies = []
            for r in rows:
                is_available = bool(r[11])
                movies.append({
                    'id': r[0],
                    'title': r[1],
                    'year': r[2] if r[2] else '',
                    'image': r[3] if r[3] else '/static/miniapp/poster-placeholder.svg',
                    'backdrop': r[4] or r[3] or '/static/miniapp/poster-placeholder.svg',
                    'rating': r[5] if r[5] else 'N/A',
                    'genre': r[6] if r[6] else 'Unknown',
                    'category': r[7] if r[7] else 'Movie',
                    'language': r[8],
                    'content_type': r[9],
                    'created_at': r[10].isoformat() if r[10] else None,
                    'is_available': is_available,
                    'is_upcoming': False,
                    'availability_state': 'available' if is_available else 'unavailable'
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
        user = telegram_user_from_request()
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute("SELECT is_unreleased FROM movies WHERE id = %s", (movie_id,))
            movie_row = cur.fetchone()
            if not movie_row:
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            if movie_row[0]:
                return jsonify({
                    'status': 'success', 'average': 0, 'count': 0,
                    'user_rating': None, 'can_rate': False,
                    'is_upcoming': True, 'rating_locked': True
                })
            result = rating_summary(cur, movie_id, user['id'] if user else None)
            result['can_rate'] = bool(user)
            conn.commit()
            cur.close()
            return jsonify({'status': 'success', **result})
        except Exception as exc:
            logger.error(f'Rating summary error for movie {movie_id}: {exc}')
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Rating temporarily unavailable'}), 500
        finally:
            close_db_connection(conn)

    @flask_app.route('/api/movie/<int:movie_id>/reviews', methods=['GET'])
    def get_movie_reviews(movie_id):
        if movie_id <= 0:
            return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        cur = None
        try:
            cur = conn.cursor()
            cur.execute('SELECT is_unreleased FROM movies WHERE id = %s', (movie_id,))
            movie_row = cur.fetchone()
            if not movie_row:
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404

            user = telegram_user_from_request()
            cur.execute("""
                SELECT r.id,
                       r.review,
                       COALESCE(NULLIF(u.first_name, ''), 'FlimfyBox member'),
                       r.created_at,
                       r.updated_at,
                       mr.rating
                FROM movie_reviews r
                LEFT JOIN miniapp_users u ON u.user_id = r.user_id
                LEFT JOIN movie_ratings mr
                  ON mr.movie_id = r.movie_id AND mr.user_id = r.user_id
                WHERE r.movie_id = %s
                ORDER BY r.updated_at DESC, r.id DESC
                LIMIT 10
            """, (movie_id,))
            reviews = [
                {
                    'id': row[0],
                    'content': row[1],
                    'author': row[2],
                    'created_at': row[3],
                    'updated_at': row[4],
                    'rating': row[5],
                    'review_source': 'flimfybox',
                }
                for row in cur.fetchall()
            ]
            cur.execute('SELECT COUNT(*) FROM movie_reviews WHERE movie_id = %s', (movie_id,))
            count_row = cur.fetchone()
            review_count = int(count_row[0] or 0) if count_row else 0
            user_review = None
            if user:
                cur.execute(
                    'SELECT review FROM movie_reviews WHERE movie_id = %s AND user_id = %s',
                    (movie_id, user['id']),
                )
                own_row = cur.fetchone()
                user_review = own_row[0] if own_row else None
            conn.commit()
            return jsonify({
                'status': 'success',
                'reviews': reviews,
                'review_count': review_count,
                'user_review': user_review,
                'can_review': bool(user and not movie_row[0]),
                'is_upcoming': bool(movie_row[0]),
            })
        except Exception as exc:
            logger.error('FlimfyBox review list failed for movie %s (%s)', movie_id, type(exc).__name__)
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Community reviews are temporarily unavailable'}), 500
        finally:
            if cur is not None:
                cur.close()
            close_db_connection(conn)

    @flask_app.route('/api/movie/<int:movie_id>/reviews', methods=['POST'])
    def submit_movie_review(movie_id):
        user, error = require_telegram_user()
        if error:
            return error
        if movie_id <= 0:
            return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
        data = request.get_json(silent=True) or {}
        raw_review = data.get('review')
        if not isinstance(raw_review, str):
            return jsonify({'status': 'error', 'message': 'Write a review between 20 and 800 characters.'}), 400
        review = '\n'.join(line.rstrip() for line in raw_review.replace('\r\n', '\n').replace('\r', '\n').split('\n')).strip()
        if not 20 <= len(review) <= 800:
            return jsonify({'status': 'error', 'message': 'Write a review between 20 and 800 characters.'}), 400

        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        cur = None
        try:
            cur = conn.cursor()
            cur.execute('SELECT is_unreleased FROM movies WHERE id = %s', (movie_id,))
            movie_row = cur.fetchone()
            if not movie_row:
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            if movie_row[0]:
                return jsonify({'status': 'error', 'message': 'Reviews open after release.'}), 409
            upsert_miniapp_user(cur, user)
            cur.execute("""
                INSERT INTO movie_reviews (movie_id, user_id, review)
                VALUES (%s, %s, %s)
                ON CONFLICT (movie_id, user_id) DO UPDATE SET
                    review = EXCLUDED.review,
                    updated_at = CURRENT_TIMESTAMP
            """, (movie_id, user['id'], review))
            conn.commit()
            return jsonify({'status': 'success', 'review': review, 'message': 'Your FlimfyBox review is saved.'})
        except Exception as exc:
            logger.error('FlimfyBox review submission failed for movie %s (%s)', movie_id, type(exc).__name__)
            conn.rollback()
            return jsonify({'status': 'error', 'message': 'Could not save your review'}), 500
        finally:
            if cur is not None:
                cur.close()
            close_db_connection(conn)

    @flask_app.route('/api/movie/<int:movie_id>/rating-sources', methods=['GET'])
    def get_local_movie_rating_sources(movie_id):
        if movie_id <= 0:
            return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute(
                'SELECT imdb_id, tmdb_id, category, content_type FROM movies WHERE id = %s',
                (movie_id,),
            )
            row = cur.fetchone()
            cur.close()
            if not row:
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            imdb_id, tmdb_id, category, content_type = row
            media_type = 'tv' if is_catalogue_tv(category, None, content_type) else 'movie'
            key = ('local', movie_id, str(tmdb_id or ''), str(imdb_id or ''), media_type)
            close_db_connection(conn)
            conn = None
            return jsonify(load_rating_sources(key, media_type, tmdb_id, imdb_id))
        except Exception as error:
            logger.warning('Could not load provider ratings for movie %s: %s', movie_id, error)
            return jsonify({'status': 'error', 'message': 'Ratings are temporarily unavailable'}), 503
        finally:
            if conn is not None:
                close_db_connection(conn)

    @flask_app.route('/api/tmdb/<media_type>/<int:tmdb_id>/rating-sources', methods=['GET'])
    def get_tmdb_movie_rating_sources(media_type, tmdb_id):
        if media_type not in {'movie', 'tv'} or tmdb_id <= 0:
            return jsonify({'status': 'error', 'message': 'Invalid TMDb title'}), 404
        key = ('tmdb', media_type, tmdb_id)
        return jsonify(load_rating_sources(key, media_type, tmdb_id))

    @flask_app.route('/api/movie/<int:movie_id>/audience-reviews', methods=['GET'])
    def get_local_movie_audience_reviews(movie_id):
        if movie_id <= 0:
            return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute(
                'SELECT tmdb_id, category, content_type FROM movies WHERE id = %s',
                (movie_id,),
            )
            row = cur.fetchone()
            cur.close()
            if not row:
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            tmdb_id, category, content_type = row
            media_type = 'tv' if is_catalogue_tv(category, None, content_type) else 'movie'
            close_db_connection(conn)
            conn = None
            if not re.fullmatch(r'\d+', str(tmdb_id or '')):
                return jsonify({
                    'status': 'success', 'source': 'TMDb', 'reviews': [],
                    'review_count': 0, 'linked': False,
                })
            result = load_tmdb_audience_reviews(media_type, str(tmdb_id))
            if result is None:
                return jsonify({'status': 'error', 'message': 'TMDb reviews are temporarily unavailable'}), 503
            return jsonify({**result, 'linked': True})
        except Exception as error:
            logger.warning('Could not load TMDb audience reviews for movie %s (%s)', movie_id, type(error).__name__)
            return jsonify({'status': 'error', 'message': 'TMDb reviews are temporarily unavailable'}), 503
        finally:
            if conn is not None:
                close_db_connection(conn)

    @flask_app.route('/api/tmdb/<media_type>/<int:tmdb_id>/audience-reviews', methods=['GET'])
    def get_tmdb_title_audience_reviews(media_type, tmdb_id):
        if media_type not in {'movie', 'tv'} or tmdb_id <= 0:
            return jsonify({'status': 'error', 'message': 'Invalid TMDb title'}), 404
        result = load_tmdb_audience_reviews(media_type, tmdb_id)
        if result is None:
            return jsonify({'status': 'error', 'message': 'TMDb reviews are temporarily unavailable'}), 503
        return jsonify({**result, 'linked': True})

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
            cur.execute("SELECT is_unreleased FROM movies WHERE id = %s", (movie_id,))
            movie_row = cur.fetchone()
            if not movie_row:
                return jsonify({'status': 'error', 'message': 'Movie not found'}), 404
            if movie_row[0]:
                return jsonify({'status': 'error', 'message': 'Ratings open after release.'}), 409
            upsert_miniapp_user(cur, user)
            cur.execute("""
                INSERT INTO movie_ratings (movie_id, user_id, rating)
                VALUES (%s, %s, %s)
                ON CONFLICT (movie_id, user_id) DO UPDATE SET
                    rating = EXCLUDED.rating
            """, (movie_id, user['id'], rating))
            result = rating_summary(cur, movie_id, user['id'])
            result['can_rate'] = True
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
    @singleflight_movie_details
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
                SELECT id, title, year, poster_url, backdrop_poster_url, rating, genre, description, category, language, content_type, "cast", trailer_key, seasons_data, is_unreleased, url, file_id
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
                'backdrop': row[4] or row[3] or '/static/miniapp/poster-placeholder.svg',
                'rating': row[5] if row[5] else 'N/A',
                'genre': row[6] if row[6] else 'Unknown',
                'description': row[7] if row[7] else 'No description available.',
                'category': row[8] if row[8] else 'Movie',
                'language': row[9] if row[9] else '',
                'content_type': row[10] if row[10] else 'Movie',
                'cast': row[11] if row[11] else '',
                'trailer_key': (row[12] if row[12] and
                                re.fullmatch(r'[A-Za-z0-9_-]{11}', row[12]) and
                                row[12].lower() != 'not_found' else None),
                'seasons_data': row[13] if len(row) > 13 and row[13] else {}
            }
            # Get files
            # Updated to fetch extra_info for Season/Episode parsing
            cur.execute("""
                SELECT id, quality, file_size, COALESCE(extra_info, '')
                FROM movie_files
                WHERE movie_id = %s
                  AND (
                      NULLIF(url, '') IS NOT NULL
                      OR NULLIF(file_id, '') IS NOT NULL
                  )
                ORDER BY id ASC
            """, (movie_id,))
            files = [
                {'id': f[0], 'quality': f[1], 'size': f[2], 'extra_info': f[3] if len(f) > 3 else ''}
                for f in cur.fetchall()
            ]
            movie['files'] = files
            has_root_file = bool(row[15] or row[16])
            movie['is_available'] = has_root_file or any(
                file_item.get('id') is not None for file_item in files
            )
            movie['is_upcoming'] = bool(row[14]) and not movie['is_available']
            movie['is_released'] = not movie['is_upcoming']
            movie['availability_state'] = (
                'available' if movie['is_available']
                else ('upcoming' if movie['is_upcoming'] else 'unavailable')
            )
    
            cur.close()
            close_db_connection(conn)
    
            # Keep details responsive: remote trailer/backdrop enrichment must not
            # block the local detail response. A poster is a reliable backdrop
            # fallback, while saved trailer keys remain available immediately.
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

        autocomplete = request.args.get('autocomplete', '').lower() in {'1', 'true'} or len(query) < 3
        limit = min(max(request.args.get('limit', 20, type=int), 1), 20)

        normalized_query = re.sub(
            r"'s\b|’s\b", "", query.lower()
        )
        normalized_query = re.sub(r"[^a-z0-9]", "", normalized_query)
        if not normalized_query:
            return jsonify({'status': 'success', 'results': []})
    
        cache_key = f"api-search:v3:{'prefix' if autocomplete else 'full'}:{limit}:{query.casefold()}"
        cached = search_cache.get(cache_key)
        if cached:
            return jsonify(cached)
    
        conn = get_db_connection()
        local_results = []
        local_failed = not bool(conn)
        # Retain the local catalogue candidates so TMDB results can be mapped
        # back to their available local record after a misspelled search.
        catalog_candidates = []
        if not conn:
            # TMDB is also the discovery source, so a temporary catalogue
            # outage must not block title discovery or request submission.
            logger.warning('Local catalogue unavailable during search; continuing with TMDB')
        else:
            try:
                cur = conn.cursor()
                select_sql = """
                    SELECT id, title, year, poster_url, rating, genre, category,
                           tmdb_id, content_type, is_unreleased,
                           (
                               COALESCE(NULLIF(url, ''), NULLIF(file_id, '')) IS NOT NULL
                               OR EXISTS (
                                   SELECT 1
                                   FROM movie_files mf
                                   WHERE mf.movie_id = movies.id
                                     AND (
                                         NULLIF(mf.url, '') IS NOT NULL
                                         OR NULLIF(mf.file_id, '') IS NOT NULL
                                     )
                               )
                           ) AS is_available
                    FROM movies
                """
                if autocomplete:
                    # The punctuation-insensitive matching expression has a
                    # text_pattern_ops B-tree index (migration 14). Normalizing
                    # first also strips SQL wildcard characters from user input.
                    normalized_prefix = normalized_query + '%'
                    cur.execute(select_sql + """
                        WHERE regexp_replace(LOWER(title), '[^a-z0-9]', '', 'g') LIKE %s
                        ORDER BY year DESC NULLS LAST, LOWER(title), id DESC
                        LIMIT %s
                    """, (normalized_prefix, limit))
                else:
                    cur.execute(select_sql + """
                    WHERE title ILIKE %s
                       OR title ILIKE %s
                       OR (%s <> '' AND regexp_replace(LOWER(title), '[^a-z0-9]', '', 'g') LIKE %s)
                    ORDER BY year DESC NULLS LAST, id DESC
                    LIMIT %s
                """, (
                    f'%{query}%',
                    f'%{query.replace(" ", "%")}%',
                    normalized_query,
                    f'%{normalized_query}%',
                    limit,
                ))
                rows = cur.fetchall()
                for r in rows:
                    is_available = bool(r[10])
                    local_results.append({
                        'id': r[0],
                        'title': r[1],
                        'year': r[2] if r[2] else '',
                        'image': r[3] if r[3] else '/static/miniapp/poster-placeholder.svg',
                        'rating': r[4] if r[4] else 'N/A',
                        'genre': r[5] if r[5] else 'Unknown',
                        'category': r[6] if r[6] else 'Movie',
                        'source': 'local',
                        'tmdb_id': r[7],
                        'media_type': 'tv' if str(r[8] or '').lower() in {'web series', 'tv series'} else 'movie',
                        'is_available': is_available,
                        'is_upcoming': bool(r[9]) and not is_available,
                        'availability_state': 'available' if is_available else ('upcoming' if r[9] else 'unavailable'),
                    })
                # A typo such as "rechar" has no ILIKE match, so score the
                # local titles before declaring it unavailable.
                if not local_results and not autocomplete:
                    # The trigram index narrows candidates in PostgreSQL;
                    # never transfer and score the entire catalogue per key.
                    cur.execute(select_sql + """
                        WHERE title %% %s
                        ORDER BY SIMILARITY(title, %s) DESC, id DESC
                        LIMIT 80
                    """, (query, query))
                    candidates = cur.fetchall()
                    catalog_candidates = candidates
                    titles = {index: row[1] for index, row in enumerate(candidates)}
                    for _, score, index in process.extract(query, titles, scorer=fuzz.WRatio, limit=8):
                        if score < 75:
                            continue
                        r = candidates[index]
                        is_available = bool(r[10])
                        local_results.append({
                            'id': r[0], 'title': r[1], 'year': r[2] if r[2] else '',
                            'image': r[3] if r[3] else '/static/miniapp/poster-placeholder.svg',
                            'rating': r[4] if r[4] else 'N/A', 'genre': r[5] if r[5] else 'Unknown',
                            'category': r[6] if r[6] else 'Movie', 'source': 'local',
                            'tmdb_id': r[7],
                            'media_type': 'tv' if str(r[8] or '').lower() in {'web series', 'tv series'} else 'movie',
                            'is_available': is_available,
                            'is_upcoming': bool(r[9]) and not is_available,
                            'availability_state': 'available' if is_available else ('upcoming' if r[9] else 'unavailable'),
                        })
                cur.close()
            except Exception as e:
                local_failed = True
                # A local schema/connection failure should not hide a valid TMDB
                # result. The title can still be shown as request-only.
                logger.error(f"Local search error; continuing with TMDB: {e}")
            finally:
                close_db_connection(conn)

        # Keystrokes are a local prefix lookup only. Likewise, a successful
        # catalogue search must never wait for TMDB or a Google suggestion.
        if autocomplete or local_results:
            if local_failed and not local_results:
                return jsonify({'status': 'error', 'message': 'Search is temporarily unavailable. Please try again.'}), 503
            result = {'status': 'success', 'results': local_results[:limit]}
            search_cache.set(cache_key, result)
            return jsonify(result)
    
        tmdb_results = []
        discovery_failed = False
        # Only an explicit full search with no local result needs discovery.
        # One bounded provider lookup replaces Google + up to four serial calls.
        if TMDB_API_KEY:
            try:
                search_terms = [query]
                for search_term in search_terms:
                    tmdb_url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(search_term)}"
                    provider_response = requests.get(tmdb_url, timeout=(1, 2))
                    provider_response.raise_for_status()
                    resp = provider_response.json()
                    for item in resp.get('results', [])[:8]:
                        # Skip non-movie/TV results (e.g. person)
                        if item.get('media_type') not in ('movie', 'tv', None):
                            continue
                        media_type = item.get('media_type') or 'movie'
                        release_date = item.get('first_air_date' if media_type == 'tv' else 'release_date') or ''
                        is_upcoming = False
                        try:
                            if not isinstance(release_date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', release_date):
                                raise ValueError('Unknown release date')
                            release_day = datetime.strptime(release_date, '%Y-%m-%d').date()
                            is_upcoming = release_day > datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
                        except (TypeError, ValueError):
                            release_date = ''
                        img_path = item.get('poster_path') or item.get('backdrop_path')
                        poster_url = f"https://image.tmdb.org/t/p/w500{img_path}" if img_path else '/static/miniapp/poster-placeholder.svg'
                        tmdb_results.append({
                            'id': f"tmdb_{media_type}_{item['id']}",
                            'tmdb_id': item.get('id'),
                            'title': item.get('title') or item.get('name') or 'Unknown',
                            'year': release_date[:4],
                            'release_date': release_date,
                            'image': poster_url,
                            'rating': round(item.get('vote_average', 0), 1),
                            'genre': '',
                            'category': 'Movie' if media_type == 'movie' else 'TV Series',
                            'media_type': media_type,
                            'source': 'tmdb',
                            'description': item.get('overview', ''),
                            'is_available': False,
                            'is_upcoming': is_upcoming,
                            'availability_state': 'upcoming' if is_upcoming else 'unavailable',
                        })
            except Exception as e:
                discovery_failed = True
                logger.error(f"TMDB search error: {e}")
    
        # Only verified, typed provider IDs identify the same work. A local
        # title/year match may still be a different film or an unreviewed upload.
        local_by_tmdb = {}
        for movie in local_results:
            if movie.get('tmdb_id') is not None:
                key = (movie.get('media_type') or 'movie', str(movie['tmdb_id']))
                local_by_tmdb.setdefault(key, []).append(movie)
        merged_tmdb_results = []
        for movie in tmdb_results:
            key = (movie.get('media_type') or 'movie', str(movie.get('tmdb_id')))
            if key not in local_by_tmdb:
                merged_tmdb_results.append(movie)

        seen = set()
        combined = []
        # Local movies first
        for m in local_results:
            # Same-title movies are valid distinct catalogue records. Use the
            # stable movie ID for deduplication, not the display title.
            key = ('local', m['id'])
            if key not in seen:
                seen.add(key)
                combined.append(m)
        # Then TMDB movies (only if not already seen)
        for m in merged_tmdb_results:
            key = ('tmdb', m.get('media_type') or 'movie', m['id'])
            if key not in seen and len(combined) < 30:
                seen.add(key)
                combined.append(m)
    
        if not combined and (discovery_failed or (local_failed and not TMDB_API_KEY)):
            return jsonify({'status': 'error', 'message': 'Search is temporarily unavailable. Please try again.'}), 503
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

        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Could not save request'}), 500
        priority_request = False
        priority_ticket_used = False
        priority_tickets = 0
        try:
            cur = conn.cursor()
            use_ticket = False
            if authenticated_user:
                # This row lock serializes spending against check-ins and other
                # requests so one priority ticket cannot be used twice.
                upsert_miniapp_user(cur, authenticated_user)
                cur.execute("""
                    SELECT COALESCE(SUM(CASE
                        WHEN event_type = 'priority_ticket_earned' THEN 1
                        WHEN event_type = 'priority_ticket_used' THEN -1
                        ELSE 0
                    END), 0)
                    FROM user_recommendation_events
                    WHERE user_id = %s
                      AND event_type IN ('priority_ticket_earned', 'priority_ticket_used')
                """, (user_id,))
                priority_tickets = max(0, int((cur.fetchone() or [0])[0] or 0))
                use_ticket = priority_tickets > 0

            cur.execute("""
                INSERT INTO user_requests
                    (user_id, username, first_name, movie_title, group_id,
                     message_id, priority)
                VALUES (%s, %s, %s, %s, NULL, NULL, %s)
                ON CONFLICT ON CONSTRAINT user_requests_unique_constraint DO NOTHING
                RETURNING id
            """, (user_id, username, first_name, title, use_ticket))
            inserted = cur.fetchone()
            if inserted:
                request_id = inserted[0]
                priority_request = use_ticket
                if use_ticket:
                    cur.execute("""
                        INSERT INTO user_recommendation_events
                            (user_id, movie_id, event_type, source, metadata)
                        VALUES (%s, NULL, 'priority_ticket_used', 'movie_request', %s::jsonb)
                    """, (user_id, json.dumps({
                        'request_id': request_id, 'title': title,
                    })))
                    priority_ticket_used = True
                    priority_tickets -= 1
            else:
                # Repeating an existing title keeps its status but never spends
                # another reward ticket.
                cur.execute("""
                    UPDATE user_requests
                    SET requested_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s AND movie_title = %s
                    RETURNING id, priority
                """, (user_id, title))
                existing_request = cur.fetchone()
                if not existing_request:
                    raise RuntimeError('The request could not be confirmed after saving.')
                request_id = existing_request[0]
                priority_request = bool(existing_request[1])

            conn.commit()
            cur.close()
        except Exception as error:
            conn.rollback()
            logger.error('Could not save Mini App request: %s', error)
            return jsonify({'status': 'error', 'message': 'Could not save request'}), 500
        finally:
            close_db_connection(conn)
        
        if request_id:
            # 🔥 FIX: Web App se aayi request ko turant Admin Channel me send karein
            bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
            request_channel = os.environ.get('REQUEST_CHANNEL_ID')
            admin_notified = False
            
            if bot_token and request_channel:
                try:
                    # Beautiful Admin Notification Format
                    msg_text = (
                        f"🎬 <b>New WebApp Request!</b> 🎬\n\n"
                        f"Movie: <b>{html_escape(title)}</b>\n"
                        f"User: {html_escape(first_name)} (<code>{user_id}</code>)\n"
                    )
                    if priority_request:
                        msg_text += (
                            "\n⚡ <b>PRIORITY REQUEST</b>\n"
                            "Boost earned from a FlimfyBox streak.\n"
                        )
                    if username:
                        msg_text += f"Username: @{html_escape(username)}\n"
                    msg_text += f"From: 🌐 Web Portal"
    
                    # Inline Buttons for Admin
                    reply_markup = {
                        "inline_keyboard": [
                            [{"text": "✅ Movie Add Kar Di Gai Hai", "callback_data": f"reqA_{request_id}"}],
                            [{"text": "❌ Nahi Mili", "callback_data": f"reqN_{request_id}"}]
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
                    response = requests.post(url, json=payload, timeout=5)
                    response.raise_for_status()
                    admin_notified = bool(response.json().get('ok'))
                except Exception as e:
                    logger.error(f"Failed to notify admin from WebApp: {e}")
    
            response = {
                'status': 'success',
                'message': (
                    'Request saved & Admin notified.'
                    if admin_notified else
                    'Request saved, but admin notification could not be sent.'
                ),
                'request_id': request_id,
                'guest': user_id <= 0,
                'admin_notified': admin_notified,
                'priority_request': priority_request,
                'priority_ticket_used': priority_ticket_used,
                'priority_tickets': priority_tickets,
            }
            if priority_ticket_used:
                response['message'] = (
                    'Priority request sent.' if admin_notified
                    else 'Priority request saved, but team notification could not be sent.'
                )
            if user_id <= 0:
                response['message'] = (
                    'Request saved. Open this page inside Telegram to receive a DM.'
                )
            return jsonify(response)
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
            cur.execute(f"""
                SELECT m.id, m.title, m.year, m.poster_url, m.rating, m.genre, m.category,
                       {local_movie_has_file_sql('m')} AS is_available
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
                    'category': row[6] or 'Movie', 'source': 'local',
                    'is_available': bool(row[7]),
                    'availability_state': 'available' if row[7] else 'unavailable'
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

    @flask_app.route('/api/my-list/<int:movie_id>/availability-notification', methods=['GET', 'POST'])
    def toggle_my_list_availability_notification(movie_id):
        user, error = require_telegram_user()
        if error:
            return error
        payload = request.get_json(silent=True) or {}
        if request.method == 'POST' and not isinstance(payload.get('enabled'), bool):
            return jsonify({'status': 'error', 'message': 'Choose whether notifications are enabled.'}), 400

        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        try:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT m.title, {local_movie_has_file_sql('m')}
                FROM movies m
                WHERE m.id = %s
            """, (movie_id,))
            movie = cur.fetchone()
            if not movie:
                return jsonify({'status': 'error', 'message': 'Title not found.'}), 404
            upsert_miniapp_user(cur, user)

            cur.execute("""
                SELECT availability_notification_requested, availability_notified_at
                FROM user_watchlist
                WHERE user_id = %s AND movie_id = %s
            """, (user['id'], movie_id))
            existing = cur.fetchone()
            saved = existing is not None
            requested = bool(existing and existing[0])
            notified = bool(existing and existing[1])
            available = bool(movie[1])

            if request.method == 'GET':
                conn.commit()
                return jsonify({
                    'status': 'success', 'saved': saved, 'requested': requested,
                    'notified': notified, 'available': available
                })

            enabled = payload['enabled']
            if enabled and available:
                conn.commit()
                return jsonify({
                    'status': 'success', 'saved': saved, 'requested': requested,
                    'notified': notified, 'available': True, 'already_available': True,
                    'message': 'This title is already available.'
                })

            if enabled:
                cur.execute("""
                    INSERT INTO user_watchlist (user_id, movie_id)
                    VALUES (%s, %s) ON CONFLICT (user_id, movie_id) DO NOTHING
                """, (user['id'], movie_id))
                cur.execute("""
                    UPDATE user_watchlist
                    SET availability_notification_requested = TRUE,
                        availability_notified_at = CASE
                            WHEN availability_notification_requested THEN availability_notified_at
                            ELSE NULL END,
                        availability_notification_attempts = CASE
                            WHEN availability_notification_requested THEN availability_notification_attempts
                            ELSE 0 END,
                        availability_notification_next_attempt_at = CASE
                            WHEN availability_notification_requested THEN availability_notification_next_attempt_at
                            ELSE CURRENT_TIMESTAMP END,
                        availability_notification_lease_until = CASE
                            WHEN availability_notification_requested THEN availability_notification_lease_until
                            ELSE NULL END,
                        availability_notification_last_error = CASE
                            WHEN availability_notification_requested THEN availability_notification_last_error
                            ELSE NULL END
                    WHERE user_id = %s AND movie_id = %s
                """, (user['id'], movie_id))
            elif saved:
                cur.execute("""
                    UPDATE user_watchlist
                    SET availability_notification_requested = FALSE,
                        availability_notification_lease_until = NULL,
                        availability_notification_last_error = NULL
                    WHERE user_id = %s AND movie_id = %s
                """, (user['id'], movie_id))

            conn.commit()
            return jsonify({
                'status': 'success', 'saved': saved or enabled,
                'requested': enabled, 'notified': notified,
                'available': available, 'already_enabled': requested and enabled
            })
        except Exception:
            conn.rollback()
            logger.exception('My List availability notification update failed for movie %s', movie_id)
            return jsonify({'status': 'error', 'message': 'Could not update notification.'}), 500
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
    
