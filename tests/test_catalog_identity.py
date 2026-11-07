import asyncio
from types import SimpleNamespace

import pytest

from catalog_identity import (AmbiguousMovieIdentity, evidence_conflict,
    find_existing_upload, lookup_omdb_identity, pick_tmdb_identity, validate_file_target,
    resolve_review_item)
from tests.test_filename_identity_ingestion import _load_functions


def candidate(id, year, kind='movie', title='Obsession', **extra):
    return dict(id=id, title=title, media_type=kind, release_date=f'{year}-01-01', **extra)


@pytest.mark.parametrize('year,kind,expected', [('2026','Movie',1),('2023','Web Series',2),('2025','Movie',None),('','',None)])
def test_namesakes_require_exact_year_and_media_type(year, kind, expected):
    rows=[candidate(1,2026,popularity=9999), candidate(2,2023,'tv')]
    match=pick_tmdb_identity(rows,'Obsession',year,kind)
    assert (match['id'] if match else None)==expected


def test_same_year_namesakes_and_subset_titles_are_not_resolved_by_popularity():
    assert pick_tmdb_identity([candidate(1,2025),candidate(2,2025,popularity=900)],'Obsession','2025','Movie') is None
    assert pick_tmdb_identity([candidate(1,2025,title='Secret Obsession')],'Obsession','2025','Movie') is None


class Connection:
    def __init__(self, rows=(), single=None):
        self.rows=rows; self.single=single; self.queries=[]; self.closed=False
        self.commits=0; self.rollbacks=0
    def cursor(self): return self
    def execute(self, sql, params=None): self.queries.append((sql,params))
    def fetchall(self): return self.rows
    def fetchone(self): return self.single
    def commit(self): self.commits+=1
    def rollback(self): self.rollbacks+=1
    def close(self): self.closed=True


def local_row(id,year,kind,imdb='tt1234567'):
    return (id,'Obsession',year,'Drama','7','Manual plot','Bollywood','Hindi',
            'manual-poster','manual-backdrop',imdb,None,'Manual cast',kind)


def test_local_manual_identity_is_reused_without_any_writes_or_provider_calls():
    conn=Connection([local_row(1,2026,'Movie'),local_row(2,2023,'Web Series'),local_row(3,2025,'Web Series')])
    result=find_existing_upload(lambda:conn,lambda c:c.close(),{'title':'Obsession','year':'2025','category':'Web Series'})
    assert result['movie_id']==3 and result['plot']=='Manual plot'
    assert conn.closed and all('UPDATE' not in sql for sql,_ in conn.queries)


@pytest.mark.parametrize('rows,evidence', [
    ([local_row(1,2026,'Movie'),local_row(2,2026,'Movie',None)], {'year':'2026','category':'Movies'}),
    ([local_row(1,2026,'Movie')], {'category':'Movies'}),
    ([local_row(1,2025,'Movie',None)], {'year':'2025','category':'Movies'}),
])
def test_ambiguous_or_unverified_local_identity_holds_files(rows,evidence):
    conn=Connection(rows)
    with pytest.raises(AmbiguousMovieIdentity):
        find_existing_upload(lambda:conn,lambda c:c.close(),{'title':'Obsession',**evidence})
    assert conn.closed and conn.commits==0


@pytest.mark.parametrize('evidence', [{'year':'2025'}, {'category':'Web Series','extra_info':'S01 COMBINED'}, {'title':'Secret Obsession','year':'2026','category':'Movie'}])
def test_every_file_is_checked_against_the_target_not_only_batch_representative(evidence):
    conn=Connection(single=('Obsession',2026,'Movie'))
    with pytest.raises(AmbiguousMovieIdentity):
        validate_file_target(lambda:conn,lambda c:c.close(),6111,{},evidence)
    assert conn.closed


def test_admin_selected_title_variant_is_allowed_but_year_and_type_stay_checked():
    selected = Connection(single=('Paachuvum Albhuthavilakkum', 2023, 'Movie'))
    validate_file_target(
        lambda:selected, lambda c:c.close(), 6111,
        {'title':'Pachuvum Athbutha Vilakkum','year':'2023','category':'Movies'},
        {'title':'Pachuvum Athbutha Vilakkum','year':'2023','category':'Movies'},
        allow_title_variant=True,
    )
    assert selected.closed

    unrelated = Connection(single=('Paachuvum Albhuthavilakkum', 2023, 'Movie'))
    with pytest.raises(AmbiguousMovieIdentity, match='title conflicts'):
        validate_file_target(
            lambda:unrelated, lambda c:c.close(), 6111,
            {'title':'Dabangg 2','year':'2023','category':'Movies'},
            {'title':'Dabangg 2','year':'2023','category':'Movies'},
            allow_title_variant=True,
        )
    assert unrelated.closed

    wrong_year = Connection(single=('Paachuvum Albhuthavilakkum', 2023, 'Movie'))
    with pytest.raises(AmbiguousMovieIdentity, match='year 2024'):
        validate_file_target(
            lambda:wrong_year, lambda c:c.close(), 6111,
            {'year':'2024','category':'Movies'},
            {'year':'2024','category':'Movies'},
            allow_title_variant=True,
        )
    assert wrong_year.closed


def test_existing_anime_series_can_use_its_stored_seasons_to_identify_tv():
    row=local_row(3,2025,'Anime')+({'1':{'year':2025}},)
    conn=Connection([row])
    result=find_existing_upload(lambda:conn,lambda c:c.close(),{'title':'Obsession','year':'2025','category':'Anime','extra_info':'S01 E01'})
    assert result['movie_id']==3


def test_caption_and_filename_disagreement_is_not_hidden_by_ai():
    assert evidence_conflict({'year':'2025'},{'year':'2026'})
    assert evidence_conflict({'category':'Movies'},{'category':'Web Series'})


def test_superbatch_keeps_movie_series_year_and_subset_titles_separate():
    ns=_load_functions({'_build_superbatch_groups'}, {
        '_best_local_identity':lambda r:(r['title'],r['year'],r['title'].lower()),
        '_evidence_record_score':lambda r:1,
        '_local_evidence_fallback':lambda a,b:a,
    })
    rows=[dict(title=t,year=y,caption_evidence={'category':kind}) for t,y,kind in [
        ('Obsession','2025','Movie'),('Obsession','2025','Web Series'),
        ('Obsession','2026','Movie'),('Obsession','','Movie'),
        ('Secret Obsession','2025','Movie'),('Obsession','2025','Movie')]]
    groups=ns['_build_superbatch_groups'](rows)
    assert sorted(map(len,groups.values()))==[1,1,1,1,2]


def test_known_imdb_missing_from_tmdb_never_borrows_another_titles_trailer_or_art():
    calls=[]
    def get(url,**kwargs):
        calls.append(url)
        return SimpleNamespace(json=lambda:{'movie_results':[],'tv_results':[]})
    from urllib.parse import quote
    from typing import Optional
    ns=_load_functions({'_tmdb_find_by_imdb_id','fetch_tmdb_artwork','fetch_tmdb_trailer_key',
        'normalize_artwork_url','normalize_trailer_key'},
        {'requests':SimpleNamespace(get=get),'TMDB_API_KEY':'test','quote':quote,'Optional':Optional})
    assert ns['fetch_tmdb_artwork']('Obsession','2025','tt39330776','Web Series')==(None,None)
    assert ns['fetch_tmdb_trailer_key']('Obsession','2025','tt39330776','Web Series') is None
    assert len(calls)==1 and all('/find/tt39330776' in url for url in calls)


def test_tmdb_find_uses_requested_media_type_and_rejects_sentinel_trailer_keys():
    calls=[]
    def get(url,**kwargs):
        calls.append(url)
        if '/find/' in url:
            return SimpleNamespace(json=lambda:{
                'movie_results':[{'id':10,'title':'Obsession','release_date':'2025-01-01','poster_path':'/movie.jpg'}],
                'tv_results':[{'id':20,'name':'Obsession','first_air_date':'2025-01-01','poster_path':'/tv.jpg'}],
            })
        if '/tv/20/videos' in url:
            return SimpleNamespace(json=lambda:{'results':[
                {'site':'YouTube','key':'NOT_FOUND','type':'Trailer','official':True},
                {'site':'YouTube','key':'AbCdEf12345','type':'Trailer','official':True},
            ]})
        if '/tv/20' in url:
            return SimpleNamespace(json=lambda:{'id':20,'poster_path':'/tv.jpg'})
        raise AssertionError(url)
    from urllib.parse import quote
    from typing import Optional
    ns=_load_functions({'_tmdb_find_by_imdb_id','fetch_tmdb_artwork','fetch_tmdb_trailer_key',
        'normalize_artwork_url','normalize_trailer_key'},
        {'requests':SimpleNamespace(get=get),'TMDB_API_KEY':'test','quote':quote,'Optional':Optional})
    assert ns['fetch_tmdb_artwork']('Obsession','2025','tt39330776','Web Series') == (
        'https://image.tmdb.org/t/p/original/tv.jpg', None
    )
    assert ns['fetch_tmdb_trailer_key']('Obsession','2025','tt39330776','Web Series') == 'AbCdEf12345'
    assert any('/tv/20/videos' in url for url in calls)


def test_tmdb_imdb_resolution_is_cached_for_related_metadata_lookups():
    calls=[]
    payload={'movie_results':[{'id':816590}], 'tv_results':[]}
    from urllib.parse import quote
    ns=_load_functions({'_tmdb_find_by_imdb_id'}, {
        'requests':SimpleNamespace(get=lambda url, **kwargs: (
            calls.append((url, kwargs)) or SimpleNamespace(json=lambda:payload)
        )),
        'TMDB_API_KEY':'test', 'quote':quote,
    })
    first=ns['_tmdb_find_by_imdb_id']('tt13781128')
    second=ns['_tmdb_find_by_imdb_id']('tt13781128')
    assert first==second==payload
    assert len(calls)==1


def test_tmdb_find_never_uses_movie_art_or_trailer_for_a_verified_series():
    def get(url,**kwargs):
        assert '/find/' in url
        return SimpleNamespace(json=lambda:{
            'movie_results':[{'id':10,'title':'Obsession','release_date':'2025-01-01','poster_path':'/wrong.jpg'}],
            'tv_results':[],
        })
    from urllib.parse import quote
    from typing import Optional
    ns=_load_functions({'_tmdb_find_by_imdb_id','fetch_tmdb_artwork','fetch_tmdb_trailer_key',
        'normalize_artwork_url','normalize_trailer_key'},
        {'requests':SimpleNamespace(get=get),'TMDB_API_KEY':'test','quote':quote,'Optional':Optional})
    assert ns['fetch_tmdb_artwork']('Obsession','2025','tt39330776','Web Series') == (None,None)
    assert ns['fetch_tmdb_trailer_key']('Obsession','2025','tt39330776','Web Series') is None


def test_omdb_search_disambiguates_before_fetching_by_id():
    calls=[]
    def get(url,params,**kwargs):
        calls.append(params)
        payload=({'totalResults':'1','Search':[{'Title':'Obsession','Year':'2025','Type':'series','imdbID':'tt39330776'}]}
            if 's' in params else {'Response':'True','Title':'Obsession','Year':'2025','Type':'series','imdbID':'tt39330776'})
        return SimpleNamespace(json=lambda:payload)
    result=lookup_omdb_identity(SimpleNamespace(get=get),'test','Obsession','2025','Web Series')
    assert result['imdbID']=='tt39330776'
    assert calls[0]['type']=='series' and calls[1]['i']=='tt39330776'


def test_provider_id_conflicts_are_rejected_and_tmdb_lookup_is_typed():
    ns=_load_functions({'_find_movie_by_provider_identity'})
    conn=Connection([(1,'tt1111111',99)])
    with pytest.raises(ValueError):
        ns['_find_movie_by_provider_identity'](conn,'tt2222222',99,'Web Series')
    assert 'LOWER(content_type) = ANY' in conn.queries[0][0]
    assert conn.queries[0][1][-1]==['web series','tv series','tv','series']


def test_review_assignment_is_owned_failed_state_only_and_transactional():
    conn=Connection(single=('batch-1',))
    assert resolve_review_item(lambda:conn,lambda c:c.close(),10,6111,99)=='batch-1'
    sql,params=conn.queries[-1]
    assert "i.state='failed'" in sql and "b.admin_id=%s" in sql
    assert params==(6111,6111,99,10,6111) and conn.commits==1 and conn.closed
    conn=Connection()
    with pytest.raises(ValueError): resolve_review_item(lambda:conn,lambda c:c.close(),10,6111,100)
    assert conn.commits==0 and conn.rollbacks==1


def test_missing_metadata_repair_uses_verified_snapshot_and_preserves_manual_fields():
    row=(16972,'Obsession','tt39330776',None,2025,'Web Series','Bollywood',
         'manual-poster','manual-backdrop','manual-trailer','Drama','N/A',
         'Manual plot','Manual cast',{})
    reads=Connection(single=row)
    writes=Connection()
    writes.rowcount=1
    pool=iter([reads,writes])
    snapshot={
        'imdb_id':'tt39330776','title':'Obsession','year':'2025','media_type':'tv',
        'rating':'7.1','votes':'1,234','genre':'Drama','category':'Bollywood',
        'poster_url':None,'description':None,'cast':None,
    }
    cached=[]
    ns=_load_functions({
        'auto_fetch_and_update_metadata','_metadata_identity_kind',
        '_metadata_identity_matches',
    }, {
        'get_db_connection':lambda:next(pool), 'close_db_connection':lambda c:c.close(),
        'clean_catalog_title':lambda value,*_args:value,
        'is_valid_imdb_id':lambda value:str(value or '').startswith('tt'),
        '_fetch_omdb_snapshot':lambda imdb_id:(snapshot,'success'),
        '_fetch_verified_tmdb_snapshot':lambda *_args:(None,'unavailable'),
        '_upsert_movie_provider_snapshot':lambda *args:cached.append(args) or True,
        '_refresh_tmdb_reviews_for_catalogue':lambda *_args:True,
        'math':__import__('math'), 'json':__import__('json'),
    })
    assert ns['auto_fetch_and_update_metadata'](16972,'Obsession')
    assert cached[0][0:3]==('imdb','tt39330776','tv')
    sql,params=writes.queries[-1]
    assert 'poster_url = COALESCE(%s, poster_url)' in sql
    assert 'title IS NOT DISTINCT FROM %s' in sql
    assert sql.count('%s')==len(params)==23
    assert params[7] is None and params[8] is None and params[9] is None
    assert params[17:]==(16972,'Obsession',2025,'Web Series','tt39330776',None)
    assert writes.commits==1


def test_idless_tmdb_lookup_requires_unique_exact_title_year_and_type():
    class Response:
        def __init__(self,payload): self.payload=payload
        def raise_for_status(self): pass
        def json(self): return self.payload

    records=[
        {'id':10,'media_type':'movie','title':'Obsession','release_date':'2023-01-01'},
        {'id':11,'media_type':'movie','title':'Obsession','release_date':'2026-01-01'},
    ]
    ns=_load_functions({'_search_tmdb_catalogue_identity'}, {
        'TMDB_API_KEY':'test','clean_catalog_title':lambda value,*_args:value,
        'requests':SimpleNamespace(get=lambda *_args,**_kwargs:Response({'total_pages':1,'results':records})),
    })
    candidate,title,status=ns['_search_tmdb_catalogue_identity']({
        'id':99,'title':'Obsession','year':2026,'content_type':'Movie',
    })
    assert candidate['id']==11 and title=='Obsession' and status=='success'

    records.append({'id':12,'media_type':'tv','name':'Obsession','first_air_date':'2026-01-01'})
    candidate,_,status=ns['_search_tmdb_catalogue_identity']({
        'id':99,'title':'Obsession','year':2026,'content_type':'Movie',
    })
    assert candidate is None and status=='unmatched'


@pytest.mark.parametrize('country,language,genre,expected', [
    ([{'iso_3166_1':'IN','name':'India'}],'hi','Drama','Bollywood'),
    ([{'iso_3166_1':'IN','name':'India'}],'ta','Drama','South'),
    ([{'iso_3166_1':'JP','name':'Japan'}],'ja','Animation','Anime'),
    ([{'iso_3166_1':'US','name':'United States'}],'en','Drama','Hollywood'),
    ([], 'en', 'Drama', ''),
])
def test_provider_signals_only_assign_supported_catalogue_categories(country,language,genre,expected):
    ns=_load_functions({'_metadata_category_from_signals'})
    assert ns['_metadata_category_from_signals'](country,language,genre)==expected


def test_conflicting_file_stops_before_storage_copy():
    from tests.test_batch_recovery import _saver_harness
    saver,state,message,_session=_saver_harness()
    async def evidence(_raw): return {'year':'2025','category':'Movie'}
    saver.__globals__['fallback_extraction']=evidence
    assert asyncio.run(saver(message,SimpleNamespace(),batch_item_id=1)) is None
    assert state['copies']==0 and state['upserts']==0
    assert state['item'][12]=='failed'
    assert any(item.get('error_class')=='AmbiguousMovieIdentity' for item in state['updates'])
