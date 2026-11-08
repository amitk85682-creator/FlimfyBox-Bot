from types import SimpleNamespace

import telegram

from tests.test_filename_identity_ingestion import _load_functions


class AsyncioShim:
    async def wait_for(self, awaitable, timeout):
        return await awaitable

    async def sleep(self, _delay):
        return None


def run_immediate(coroutine):
    try:
        coroutine.send(None)
    except StopIteration as finished:
        return finished.value
    raise AssertionError('The test coroutine unexpectedly suspended.')


def test_broadcast_recipients_include_all_tracked_user_sources():
    class Cursor:
        def __init__(self):
            self.queries=[]
        def __enter__(self): return self
        def __exit__(self,*_args): return False
        def execute(self,sql,params=None): self.queries.append((sql,params))
        def fetchall(self): return [(22,),(11,),(-1001,),(0,),(22,)]

    class Connection:
        def __init__(self): self.cur=Cursor(); self.closed=False
        def cursor(self): return self.cur

    conn=Connection()
    ns=_load_functions({'_get_broadcast_recipient_ids'}, {
        'get_db_connection':lambda:conn,
        'close_db_connection':lambda value:setattr(value,'closed',True),
    })

    assert ns['_get_broadcast_recipient_ids']()==[11,22]
    sql=conn.cur.queries[-1][0]
    assert 'FROM user_activity' in sql
    assert 'FROM miniapp_users' in sql
    assert 'FROM user_requests' in sql
    assert 'user_id > 0' in sql
    assert conn.closed


def test_broadcast_returns_an_immediate_ack_when_database_lookup_fails():
    events=[]

    class Status:
        async def edit_text(self,text): events.append(('edit',text))

    class Message:
        text='/broadcast Hello'
        async def reply_text(self,text):
            events.append(('reply',text))
            return Status()

    async def run_interactive(_func):
        events.append(('lookup',None))
        raise RuntimeError('database unavailable')

    ns=_load_functions({'broadcast_message'}, {
        'ADMIN_IDS':{11},'asyncio':AsyncioShim(),'re':__import__('re'),
        'telegram':telegram,'logger':__import__('logging').getLogger('test_broadcast'),
        'Update':object,'ContextTypes':SimpleNamespace(DEFAULT_TYPE=object),
        'run_interactive':run_interactive,
        '_get_broadcast_recipient_ids':lambda:[],
    })
    message=Message()
    update=SimpleNamespace(
        effective_user=SimpleNamespace(id=11),
        effective_message=message,
        message=message,
    )
    context=SimpleNamespace(args=['Hello'],bot=SimpleNamespace())

    run_immediate(ns['broadcast_message'](update,context))

    assert events[0][0]=='reply'
    assert events[1][0]=='lookup'
    assert events[2][0]=='edit'
    assert 'could not start' in events[2][1].lower()


def test_invalid_broadcast_html_retries_as_plain_text():
    calls=[]

    class Bot:
        async def send_message(self,**kwargs):
            calls.append(kwargs)
            if len(calls)==1:
                raise telegram.error.BadRequest("Can't parse entities: Unsupported start tag")

    ns=_load_functions({
        '_send_broadcast_to_user','_broadcast_is_html_parse_error',
    }, {
        'telegram':telegram,'asyncio':AsyncioShim(),
        'BeautifulSoup':__import__('bs4').BeautifulSoup,
    })

    used_fallback=run_immediate(ns['_send_broadcast_to_user'](
        Bot(),22,'Hello & everyone <b>there</i>'
    ))

    assert used_fallback is True
    assert len(calls)==2
    assert calls[0]['parse_mode']=='HTML'
    assert calls[1]['parse_mode'] is None
    assert calls[1]['text']=='Hello & everyone there'
