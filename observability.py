"""Small request-correlation helpers shared by Flask and bot code."""

from contextvars import ContextVar
import secrets


_request_id = ContextVar('flimfybox_request_id', default='-')


def new_request_id():
    """Return an opaque request ID that contains no user or request data."""
    return secrets.token_hex(8)


def bind_request_id(request_id):
    return _request_id.set(request_id or '-')


def current_request_id():
    return _request_id.get()


def reset_request_id(token):
    _request_id.reset(token)


def log_search_timing(logger, update_id, timings):
    """Log search stage timings without recording the searched title."""
    logger.info(
        'search_timing update_id=%s timings=%s',
        update_id if update_id is not None else '-',
        timings,
    )
