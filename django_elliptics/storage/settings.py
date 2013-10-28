# coding: utf-8

from django import conf


DJANGO_ENABLED = True
try:
    getattr(conf.settings, 'ANYTHING', 3)
except ImportError:
    # мы вне джанги и настройки не могут быть проимпортированы
    DJANGO_ENABLED = False

# timeout of http-session on read requests
ELLIPTICS_GET_CONNECTION_TIMEOUT = 3
# number of retries in http-session on read requests
ELLIPTICS_GET_CONNECTION_RETRIES = 3
# timeout of http-session on save requests
ELLIPTICS_POST_CONNECTION_TIMEOUT = 5
# number of retries in http-session on save requests
ELLIPTICS_POST_CONNECTION_RETRIES = 9
# size of a chunk in bytes, to split content into
ELLIPTICS_UPLOAD_CHUNK_SIZE = 3 * 1024 * 1024
# maximum number of instantaneous http-sessions to elliptics
ELLIPTICS_MAX_SESSIONS = 5


if DJANGO_ENABLED:
    ELLIPTICS_GET_CONNECTION_TIMEOUT = getattr(
        conf.settings,
        'ELLIPTICS_GET_CONNECTION_TIMEOUT',
        ELLIPTICS_GET_CONNECTION_TIMEOUT
    )
    ELLIPTICS_GET_CONNECTION_RETRIES = getattr(
        conf.settings,
        'ELLIPTICS_GET_CONNECTION_RETRIES',
        ELLIPTICS_GET_CONNECTION_RETRIES
    )
    ELLIPTICS_POST_CONNECTION_TIMEOUT = getattr(
        conf.settings,
        'ELLIPTICS_POST_CONNECTION_TIMEOUT',
        ELLIPTICS_POST_CONNECTION_TIMEOUT
    )
    ELLIPTICS_POST_CONNECTION_RETRIES = getattr(
        conf.settings,
        'ELLIPTICS_POST_CONNECTION_RETRIES',
        ELLIPTICS_POST_CONNECTION_RETRIES
    )
    ELLIPTICS_UPLOAD_CHUNK_SIZE = getattr(
        conf.settings,
        'ELLIPTICS_UPLOAD_CHUNK_SIZE',
        ELLIPTICS_UPLOAD_CHUNK_SIZE
    )
    ELLIPTICS_MAX_SESSIONS = getattr(
        conf.settings,
        'ELLIPTICS_MAX_SESSIONS',
        ELLIPTICS_MAX_SESSIONS
    )
