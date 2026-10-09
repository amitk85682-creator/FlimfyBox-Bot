import ast
import asyncio
import html
import logging
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / 'main.py'
MAIN_TREE = ast.parse(MAIN_PATH.read_text(encoding='utf-8'))
CALLBACK_NODE = next(
    node for node in MAIN_TREE.body
    if isinstance(node, ast.AsyncFunctionDef)
    and node.name == 'handle_webapp_request_callback'
)


class FakeRequestCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = 0

    def execute(self, query, params=None):
        if query.lstrip().startswith('SELECT user_id, first_name, movie_title, notified'):
            request_id = params[0]
            self.connection.selected_id = request_id
            self.connection.row = self.connection.requests.get(request_id)
        elif query.startswith('UPDATE user_requests SET notified'):
            request_id = params[0]
            row = self.connection.requests.get(request_id)
            if row and not row['notified']:
                self.connection.pending_notified_id = request_id
                self.rowcount = 1
            else:
                self.rowcount = 0
        else:
            raise AssertionError(f'Unexpected SQL: {query}')

    def fetchone(self):
        row = self.connection.row
        if row is None:
            return None
        return (
            row['user_id'],
            row['first_name'],
            row['movie_title'],
            row['notified'],
        )


class FakeRequestConnection:
    def __init__(self, requests):
        self.requests = requests
        self.row = None
        self.pending_notified_id = None
        self.selected_id = None
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeRequestCursor(self)

    def commit(self):
        self.commits += 1
        if self.pending_notified_id is not None:
            self.requests[self.pending_notified_id]['notified'] = True
            self.pending_notified_id = None

    def rollback(self):
        self.rollbacks += 1
        self.pending_notified_id = None


class FakeQuery:
    def __init__(self, user_id, data):
        self.from_user = SimpleNamespace(id=user_id)
        self.data = data
        self.message = SimpleNamespace(text='New request', edited_text=None)
        self.answers = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))

    async def edit_message_text(self, text, **_kwargs):
        self.message.edited_text = text


def _load_callback(requests, *, admin_ids=(7,), send_result=True):
    connection = FakeRequestConnection(requests)
    sent_messages = []

    async def send_multi_bot_message(user_id, text):
        sent_messages.append((user_id, text))
        return send_result

    namespace = {
        'ADMIN_IDS': set(admin_ids),
        'get_db_connection': lambda: connection,
        'close_db_connection': lambda _connection: None,
        'send_multi_bot_message': send_multi_bot_message,
        'html_escape': html.escape,
        'ADMIN_USERNAME': 'Admin',
        'logger': logging.getLogger('test_webapp_request_callback'),
    }
    exec(
        compile(
            ast.Module(body=[CALLBACK_NODE], type_ignores=[]),
            str(MAIN_PATH),
            'exec',
        ),
        namespace,
    )
    return namespace['handle_webapp_request_callback'], connection, sent_messages


def test_reqA_sends_one_message_to_db_requester_and_marks_notified():
    requests = {
        25: {
            'user_id': 123,
            'first_name': 'Real User',
            'movie_title': 'Requested Film',
            'notified': False,
        }
    }
    callback, connection, sent = _load_callback(requests)
    query = FakeQuery(7, 'reqA_25')

    asyncio.run(callback(query, 'reqA', 25))
    asyncio.run(callback(FakeQuery(7, 'reqA_25'), 'reqA', 25))

    assert len(sent) == 1
    assert sent[0][0] == 123
    assert 'Requested Film' in sent[0][1]
    assert requests[25]['notified'] is True
    assert connection.commits == 1


def test_reqN_sends_one_message_and_repeated_click_is_idempotent():
    requests = {
        26: {
            'user_id': 456,
            'first_name': 'Requester',
            'movie_title': 'Unavailable Film',
            'notified': False,
        }
    }
    callback, _connection, sent = _load_callback(requests)

    asyncio.run(callback(FakeQuery(7, 'reqN_26'), 'reqN', 26))
    asyncio.run(callback(FakeQuery(7, 'reqN_26'), 'reqN', 26))

    assert len(sent) == 1
    assert sent[0][0] == 456
    assert 'Unavailable Film' in sent[0][1]
    assert requests[26]['notified'] is True


def test_unknown_request_id_is_safe():
    callback, connection, sent = _load_callback({})
    query = FakeQuery(7, 'reqA_999')

    asyncio.run(callback(query, 'reqA', 999))

    assert sent == []
    assert connection.commits == 0
    assert query.answers[-1][0] == '❌ Request not found.'


def test_non_admin_cannot_use_request_callback():
    requests = {
        27: {
            'user_id': 123,
            'first_name': 'Requester',
            'movie_title': 'Film',
            'notified': False,
        }
    }
    callback, connection, sent = _load_callback(requests)
    query = FakeQuery(8, 'reqA_27')

    asyncio.run(callback(query, 'reqA', 27))

    assert sent == []
    assert connection.selected_id is None
    assert requests[27]['notified'] is False
    assert query.answers[-1] == ('❌ Admin only!', True)


def test_failed_telegram_notification_keeps_request_retryable():
    requests = {
        28: {
            'user_id': 123,
            'first_name': 'Requester',
            'movie_title': 'Film',
            'notified': False,
        }
    }
    callback, connection, sent = _load_callback(requests, send_result=False)
    query = FakeQuery(7, 'reqA_28')

    asyncio.run(callback(query, 'reqA', 28))

    assert len(sent) == 1
    assert requests[28]['notified'] is False
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert 'retryable' in query.answers[-1][0]


def test_guest_request_is_not_sent_to_telegram():
    requests = {
        29: {
            'user_id': 0,
            'first_name': 'WebApp User',
            'movie_title': 'Guest Film',
            'notified': False,
        }
    }
    callback, connection, sent = _load_callback(requests)
    query = FakeQuery(7, 'reqA_29')

    asyncio.run(callback(query, 'reqA', 29))

    assert sent == []
    assert requests[29]['notified'] is False
    assert connection.commits == 0
    assert 'no Telegram identity' in query.answers[-1][0]
