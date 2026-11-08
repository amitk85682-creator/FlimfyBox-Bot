from types import SimpleNamespace

import telegram
from telegram import Chat, Message, PhotoSize, Update
from telegram.ext import filters
from datetime import datetime, timezone

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


def test_caption_command_filter_matches_photo_broadcast_caption():
    broadcast_caption=filters.CaptionRegex(r'(?i)^/broadcast(?:@\w+)?(?:\s|$)')
    caption_filter=filters.CAPTION & broadcast_caption
    message=Message(
        message_id=1,
        date=datetime.now(timezone.utc),
        chat=Chat(id=22,type='private'),
        caption='/broadcast@FlimfyBoxBot Happy Diwali',
        photo=[PhotoSize(file_id='photo',file_unique_id='photo-unique',width=20,height=20)],
    )
    update=Update(update_id=1,message=message)

    assert caption_filter.check_update(update)
    assert not (filters.PHOTO & filters.ChatType.PRIVATE & ~broadcast_caption).check_update(update)
    assert not (
        filters.ChatType.PRIVATE
        & (filters.Document.ALL | filters.VIDEO | filters.PHOTO | (filters.TEXT & ~filters.COMMAND))
        & ~broadcast_caption
    ).check_update(update)


def test_broadcast_returns_an_immediate_ack_when_database_lookup_fails():
    events=[]

    class Status:
        async def edit_text(self,text): events.append(('edit',text))

    class Message:
        text=None
        caption='/broadcast Hello'
        photo=[SimpleNamespace(file_id='source-photo')]
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


def test_caption_broadcast_sends_photo_with_readable_fallback_caption():
    calls=[]

    class Bot:
        async def send_photo(self,**kwargs):
            calls.append(kwargs)
            if len(calls)==1:
                raise telegram.error.BadRequest("Can't parse entities: Unsupported start tag")

    ns=_load_functions({
        '_send_broadcast_to_user','_broadcast_is_html_parse_error',
    }, {
        'telegram':telegram,'asyncio':AsyncioShim(),
        'BeautifulSoup':__import__('bs4').BeautifulSoup,
    })
    photo_message=SimpleNamespace(photo=[SimpleNamespace(file_id='source-photo')])

    used_fallback=run_immediate(ns['_send_broadcast_to_user'](
        Bot(),22,'Happy <b>Diwali</b>',photo_message
    ))

    assert used_fallback is True
    assert calls[0]['photo']=='source-photo'
    assert calls[0]['caption']=='Happy <b>Diwali</b>'
    assert calls[1]['caption']=='Happy Diwali'
    assert calls[1]['parse_mode'] is None
