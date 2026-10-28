"""Conservative identity rules shared by ingestion and metadata enrichment.

Popularity, language and artwork are not evidence that two titles are the same.
"""
import re
import unicodedata


class AmbiguousMovieIdentity(ValueError):
    """Keep the source file for admin review instead of guessing a destination."""


def title_key(value):
    value = unicodedata.normalize('NFKC', str(value or '')).casefold()
    return ' '.join(re.sub(r'[^\w\s]', ' ', value).split())


def media_kind(value='', extra=''):
    value = str(value or '').strip().casefold()
    if value in {'tv', 'series', 'web series', 'tv series', 'television'}:
        return 'tv'
    if re.search(r'\b(?:s\d{1,2}(?:e\d+)?|season\s*\d+|episode\s*\d+|ep\s*\d+)\b', str(extra), re.I):
        return 'tv'
    if value in {'movie', 'movies', 'film', 'bollywood', 'hollywood', 'south'}:
        return 'movie'
    return ''  # Anime and Adult describe a genre, not a TMDB namespace.


def release_year(value):
    match = re.match(r'^(\d{4})', str(value or '').strip())
    return match.group(1) if match else ''


def stored_kind(row):
    # Older catalogue rows used Anime as content_type for both films and TV.
    seasons = row[14] if len(row) > 14 else None
    return media_kind(row[13], 'S01' if seasons and seasons != '{}' else '')


def pick_tmdb_identity(results, title, year='', kind=''):
    """Only a unique exact title/year/type match may supply identity or artwork."""
    key, year, kind = title_key(title), release_year(year), media_kind(kind)
    matches = {}
    for item in results or []:
        item_kind = item.get('media_type') or ('tv' if 'first_air_date' in item else 'movie')
        if item_kind not in {'movie', 'tv'} or (kind and kind != item_kind):
            continue
        names = (item.get('title'), item.get('name'), item.get('original_title'), item.get('original_name'))
        if not key or not any(key == title_key(name) for name in names if name):
            continue
        item_year = release_year(item.get('release_date') or item.get('first_air_date'))
        if year and year != item_year:
            continue
        matches[(item_kind, item.get('id'))] = item
    return next(iter(matches.values())) if len(matches) == 1 else None


def lookup_omdb_identity(requests, api_key, title, year='', kind=''):
    """OMDB's t= endpoint silently chooses one namesake; enumerate before i=."""
    params = {'apikey': api_key, 's': title}
    if release_year(year):
        params['y'] = release_year(year)
    kind = media_kind(kind)
    if kind:
        params['type'] = 'series' if kind == 'tv' else 'movie'
    data = requests.get('https://www.omdbapi.com/', params=params, timeout=10).json()
    # An incomplete search page cannot establish uniqueness.
    if int(data.get('totalResults') or 0) > len(data.get('Search') or []):
        return None
    matches = {item.get('imdbID'): item for item in data.get('Search', [])
               if title_key(item.get('Title')) == title_key(title)
               and (not release_year(year) or release_year(item.get('Year')) == release_year(year))
               and (not kind or media_kind(item.get('Type')) == kind)
               and item.get('imdbID')}
    if len(matches) != 1:
        return None
    imdb_id = next(iter(matches))
    result = requests.get('https://www.omdbapi.com/', params={
        'apikey': api_key, 'i': imdb_id, 'plot': 'full'}, timeout=10).json()
    if result.get('Response') != 'True' or result.get('imdbID') != imdb_id:
        return None
    if release_year(year) and release_year(result.get('Year')) != release_year(year):
        return None
    if kind and media_kind(result.get('Type')) != kind:
        return None
    return result


def evidence_conflict(first, second):
    """Explicit conflicting evidence must survive AI reconciliation."""
    a, b = release_year(first.get('year')), release_year(second.get('year'))
    if a and b and a != b:
        return 'Filename and caption have different years'
    a = media_kind(first.get('category'), first.get('extra_info'))
    b = media_kind(second.get('category'), second.get('extra_info'))
    # A bare Movies label is often only the parser's default. Explicit season or
    # episode structure in the other source is stronger evidence of a series.
    first_extra = str(first.get('extra_info') or '')
    second_extra = str(second.get('extra_info') or '')
    explicit_first_tv = media_kind('', first_extra) == 'tv'
    explicit_second_tv = media_kind('', second_extra) == 'tv'
    if a == 'movie' and b == 'tv' and explicit_second_tv and not explicit_first_tv:
        a = 'tv'
    elif b == 'movie' and a == 'tv' and explicit_first_tv and not explicit_second_tv:
        b = 'tv'
    if a and b and a != b:
        return 'Filename and caption disagree on Movie / Series'
    return ''


def find_existing_upload(connect, close, evidence):
    """Reuse a unique local identity before consulting external title search.

    This preserves admin corrections and avoids network requests for known titles.
    Unknown years/types never act as wildcards across a same-name catalogue.
    """
    conn = connect()
    if not conn:
        raise RuntimeError('Database unavailable while checking movie identity')
    try:
        cur = conn.cursor()
        cur.execute("SET LOCAL statement_timeout = '10s'")
        cur.execute('''SELECT id, title, year, genre, rating, description, category,
                              language, poster_url, backdrop_poster_url, imdb_id,
                              tmdb_id, "cast", content_type, seasons_data, trailer_key
                       FROM movies WHERE LOWER(BTRIM(title)) = LOWER(BTRIM(%s))''',
                    (evidence.get('title'),))
        rows = cur.fetchall()
        if not rows:
            return None
        year = release_year(evidence.get('year'))
        kind = media_kind(evidence.get('category'), evidence.get('extra_info'))
        explicit_episode_identity = media_kind('', evidence.get('extra_info')) == 'tv'
        compatible = [row for row in rows
                      if (not year or not release_year(row[2]) or year == release_year(row[2]))
                      and (not kind or not stored_kind(row) or kind == stored_kind(row))]
        if not compatible and year and kind:
            return None  # A genuinely different year/type can establish its own provider ID.
        if not kind or len(compatible) != 1 or (not year and not explicit_episode_identity):
            options = ', '.join(f'#{r[0]} ({r[2] or "?"}, {r[13] or "?"})' for r in rows[:8])
            raise AmbiguousMovieIdentity(f'Ambiguous {evidence.get("title")}: {options}. Use /batchresolve <item ID> <catalogue ID> after reviewing the file.')
        row = compatible[0]
        if (year and not release_year(row[2])) or not stored_kind(row) or not (row[10] or row[11]):
            raise AmbiguousMovieIdentity(f'Catalogue #{row[0]} needs identity review; use /batchresolve <item ID> {row[0]} only after reviewing the file.')
        keys = ('movie_id', 'title', 'year', 'genre', 'rating', 'plot', 'category',
                'movie_lang', 'poster_url', 'backdrop_poster_url', 'imdb_id',
                'tmdb_id', 'cast_str', 'content_type')
        result = dict(zip(keys, row))
        result['trailer_key'] = row[15] if len(row) > 15 else None
        return result
    finally:
        close(conn)


def validate_file_target(connect, close, movie_id, caption, filename):
    """Check every file, not just the first/representative file of a batch."""
    conflict = evidence_conflict(caption, filename)
    if conflict:
        raise AmbiguousMovieIdentity(conflict)
    conn = connect()
    if not conn:
        raise RuntimeError('Database unavailable while validating file identity')
    try:
        cur = conn.cursor()
        cur.execute("SET LOCAL statement_timeout = '10s'")
        cur.execute('SELECT title, year, content_type FROM movies WHERE id = %s', (movie_id,))
        row = cur.fetchone()
        if not row:
            raise AmbiguousMovieIdentity('The selected catalogue title no longer exists')
        for item in (caption, filename):
            name = title_key(item.get('title'))
            if name and name not in {'unknown', 'unknown movie', 'file'} and name != title_key(row[0]):
                raise AmbiguousMovieIdentity(f'File title conflicts with catalogue #{movie_id} ({row[0]})')
            year = release_year(item.get('year'))
            kind = media_kind(item.get('category'), item.get('extra_info'))
            if year and release_year(row[1]) and year != release_year(row[1]):
                raise AmbiguousMovieIdentity(f'File year {year} conflicts with catalogue #{movie_id} ({row[1]})')
            if kind and media_kind(row[2]) and kind != media_kind(row[2]):
                raise AmbiguousMovieIdentity(f'File Movie / Series type conflicts with catalogue #{movie_id}')
    finally:
        close(conn)


def pending_identity_reviews(connect, close, batch_id, admin_id):
    conn = connect()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute('''SELECT i.id, i.file_name, i.error_detail
                       FROM ingestion_batch_items i JOIN ingestion_batches b ON b.id=i.batch_id
                       WHERE b.id=%s AND b.admin_id=%s AND i.state='failed'
                         AND i.error_class='AmbiguousMovieIdentity'
                       ORDER BY i.id LIMIT 8''', (batch_id, admin_id))
        return cur.fetchall()
    finally:
        close(conn)


def resolve_review_item(connect, close, item_id, movie_id, admin_id):
    """Admin chooses a destination; /batchretry performs the actual saved work."""
    conn = connect()
    if not conn:
        raise RuntimeError('Database unavailable')
    try:
        cur = conn.cursor()
        cur.execute("SET LOCAL lock_timeout = '5s'")
        cur.execute("SET LOCAL statement_timeout = '10s'")
        cur.execute('''UPDATE ingestion_batch_items i
                       SET movie_id=%s, evidence=COALESCE(i.evidence,'{}'::jsonb) ||
                           jsonb_build_object('reviewed_movie_id', %s),
                           error_class='IdentityReviewed', error_detail='Admin selected catalogue identity',
                           updated_at=NOW()
                       FROM ingestion_batches b
                       WHERE b.id=i.batch_id AND b.admin_id=%s AND i.id=%s
                         AND i.state='failed' AND i.error_class='AmbiguousMovieIdentity'
                         AND i.storage_pending_chat IS NULL
                         AND EXISTS(SELECT 1 FROM movies WHERE id=%s)
                       RETURNING i.batch_id''', (movie_id, movie_id, admin_id, item_id, movie_id))
        row = cur.fetchone()
        if not row:
            raise ValueError('No unresolved identity review found for this admin/item, or catalogue ID is invalid')
        conn.commit()
        return row[0]
    except Exception:
        conn.rollback()
        raise
    finally:
        close(conn)
