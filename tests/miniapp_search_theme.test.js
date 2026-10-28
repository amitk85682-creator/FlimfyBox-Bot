const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const UI = require('../static/miniapp/ui-core.js');
const source = fs.readFileSync(path.join(__dirname, '../static/miniapp/app.js'), 'utf8');
const movies = [{id:1,title:'Reacher'}, {id:2,title:'Red Notice'}, {id:3,title:'The Red Queen'}, {id:4,title:'Arrival'}];

test('same-name movie and series remain distinct and display their year and type', () => {
    const namesakes = [{id:6111,title:'Obsession',year:2026,content_type:'Movie'},
        {id:15499,title:'Obsession',year:2023,media_type:'tv'},
        {id:16972,title:'Obsession',year:2025,content_type:'Web Series'}];
    assert.equal(UI.prefixMatches(namesakes,'obs').length,3);
    assert.deepEqual(namesakes.map(UI.identityLabel),['2026 · Movie','2023 · Series','2025 · Series']);
});

test('prefix suggestions match re and refine without substring noise or duplicate IDs', () => {
    assert.deepEqual(UI.prefixMatches([...movies, movies[0]], ' RE ').map(m => m.title), ['Reacher', 'Red Notice']);
    assert.deepEqual(UI.prefixMatches(movies, 'rea').map(m => m.title), ['Reacher']);
    assert.equal(UI.prefixMatches(movies, '').length, 0);
    assert.equal(UI.prefixMatches(movies, 're', 1).length, 1);
});

test('prefix suggestions ignore punctuation in movie titles', () => {
    const spider = [{id:6815,title:'Spider-Man: Brand New Day'}];
    assert.deepEqual(UI.prefixMatches(spider, 'Spider Man').map(m => m.title), ['Spider-Man: Brand New Day']);
    assert.deepEqual(UI.prefixMatches(spider, 'Spider Man Brand New Day').map(m => m.title), ['Spider-Man: Brand New Day']);
});

test('upcoming excludes past, today, malformed dates and already available files in India calendar', () => {
    const now = new Date('2026-10-25T20:00:00Z'); // October 26 in India.
    for (const release_date of ['2026-10-25', '2026-10-26', '2026-02-30', '2026', '']) {
        assert.equal(UI.futureRelease({release_date, is_upcoming:true}, now), false);
    }
    assert.equal(UI.futureRelease({release_date:'2026-10-27'}, now), true);
    assert.equal(UI.futureRelease({release_date:'2026-10-27',is_available:true}, now), false);
    assert.equal(UI.futureRelease({media_type:'tv',first_air_date:'2020-01-01',release_date:'2026-11-01'}, now), false);
});

test('availability labels distinguish missing files from a known catalogue title', () => {
    assert.equal(UI.availabilityLabel({is_available:false}), 'Request');
    assert.equal(UI.availabilityLabel({source:'tmdb',release_date:'2000-01-01',is_upcoming:true}), 'Request');
    assert.equal(UI.availabilityLabel({is_available:true}), 'In your catalogue');
});

test('appearance persists and follows OS changes only when System is selected', () => {
    const storage = new Map([['flimfybox-theme','light']]);
    const root = { dataset:{}, style:{} };
    let change;
    const media = {matches:true, addEventListener:(_name, cb) => change = cb};
    const context = {document:{documentElement:root,querySelector:() => null,querySelectorAll:() => []},
        matchMedia:() => media,localStorage:{getItem:key => storage.get(key),setItem:(key,value) => storage.set(key,value)}};
    context.window = context;
    vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../static/miniapp/ui-core.js'),'utf8'), context);
    assert.equal(root.dataset.theme,'light');
    change();
    assert.equal(root.dataset.theme,'light');
    context.FlimfyUI.applyTheme('system');
    assert.equal(root.dataset.theme,'dark');
    media.matches = false; change();
    assert.equal(root.dataset.theme,'light');
    context.FlimfyUI.applyTheme('dark');
    change();
    assert.equal(root.dataset.theme,'dark');
    assert.equal(storage.get('flimfybox-theme'),'dark');
    assert.equal(UI.resolveTheme('invalid',true),'dark');
});

function searchHarness() {
    let timer;
    const calls = [], renders = [], recent = [];
    const input = {value:'',style:{},addEventListener(){},setAttribute(){}};
    const context = {FlimfyUI:UI, allMovies:movies, searchController:null, AbortController,
        setTimeout:fn => {timer=fn;return 1;},clearTimeout:() => {timer=null;},
        document:{getElementById:() => input}, addRecentSearch:value => recent.push(value),
        renderSearchDiscovery(){},isAbortError:error => error.name === 'AbortError',
        fetchWithTimeout:(url,options) => new Promise(resolve => calls.push({url,options,resolve})),
        Date,console};
    context.window=context;
    vm.runInNewContext(source.slice(source.indexOf('// Hybrid Search'),source.indexOf('// Hide dropdown if clicked outside')),context);
    context.renderSearchMatches = (query, results, flags) => renders.push({query,titles:results.map(m=>m.title),flags});
    return {context,calls,renders,recent,flush:() => timer?.()};
}

test('typing paints local prefixes immediately and sends one debounced autocomplete request', async () => {
    const h=searchHarness();
    await h.context.runTitleSearch('r');
    await h.context.runTitleSearch('re');
    assert.equal(h.calls.length,0);
    assert.deepEqual(h.renders.at(-1).titles,['Reacher','Red Notice']);
    const pending=h.flush();
    assert.match(h.calls[0].url,/q=re&autocomplete=1$/);
    h.calls[0].resolve({ok:true,json:async()=>({status:'success',results:movies})});
    await pending;
    assert.deepEqual(h.renders.at(-1).titles,['Reacher','Red Notice']);
    assert.equal(h.recent.length,0,'Partial queries must not fill history');
    await h.context.runTitleSearch('re');
    h.flush();
    assert.equal(h.calls.length,1,'Fresh cache should be immediate without a new request');
});

test('a stale response cannot replace a newer query even when transport ignores abort', async () => {
    const h=searchHarness();
    await h.context.runTitleSearch('re'); const old=h.flush();
    await h.context.runTitleSearch('arr'); const current=h.flush();
    assert.equal(h.calls[0].options.signal.aborted,true);
    h.calls[1].resolve({ok:true,json:async()=>({status:'success',results:[movies[3]]})}); await current;
    const count=h.renders.length;
    h.calls[0].resolve({ok:true,json:async()=>({status:'success',results:[movies[0]]})}); await old;
    assert.equal(h.renders.length,count);
    assert.equal(h.renders.at(-1).query,'arr');
    assert.deepEqual(h.renders.at(-1).titles,['Arrival']);
});

test('explicit search discovers non-prefix results and failures remain retryable', async () => {
    const h=searchHarness();
    const first=h.context.runTitleSearch('queen',true);
    assert.equal(h.calls[0].url,'/api/search?q=queen');
    h.calls[0].resolve({ok:false,json:async()=>({status:'error'})}); await first;
    assert.equal(h.renders.at(-1).flags.failed,true);
    const retry=h.context.runTitleSearch('queen',true);
    h.calls[1].resolve({ok:true,json:async()=>({status:'success',results:[movies[2]]})}); await retry;
    assert.deepEqual(h.renders.at(-1).titles,['The Red Queen']);
    assert.equal(h.recent[0],'queen');
});
