# coding: utf-8
import logging
import threading

from django import conf

from .base import SaveError, BaseError
from .simple import EllipticsStorage
from .errors import *

logger = logging.getLogger(__name__)


class ThreadedEllipticsStorage(EllipticsStorage):
    """

    Uses _upload_a_chunk from parent class to upload in multiple threads.
    All configuration params from parent have power.

    """
    # maximum number of instantaneous http-sessions to elliptics
    MAX_HTTP_SESSIONS = getattr(conf.settings, 'ELLIPTICS_MAX_SESSIONS', 5)
    # active upload sessions
    __active_threads = None

    def __init__(self, **kwargs):
        super(ThreadedEllipticsStorage, self).__init__(**kwargs)
        self.session.config['pool_connections'] = self.MAX_HTTP_SESSIONS
        self.session.config['pool_maxsize'] = self.MAX_HTTP_SESSIONS
        self.__active_threads = {}

    def __wait_till_all_threads_finish(self):
        exception = None
        for thread in tuple(self.__active_threads.keys()):
            try:
                self.__collect_thread_status_and_kill(thread)
            except BaseError as exc:
                exception = exc
            except Exception as exc:
                logger.exception(
                    'Unhandled exception when joining threads "%s"',
                    repr(exc)
                )
        if exception:
            raise exception

    def __collect_thread_status_and_kill(self, thread):
        thread.join(self.timeout_post * self.retries_post + 100)
        result = self.__active_threads[thread]
        del self.__active_threads[thread]
        if result is None:
            logger.warning(
                'Thread "%s" did not make it in time to upload into Elliptics',
                repr(thread)
            )
            # did not make it in time
            raise BaseError('One of requests timed out')
        elif isinstance(result, BaseError):
            raise result
        else:
            if result.status_code != 200:
                raise SaveError(result)

    def _upload_chunk_in_thread(self, url, chunk):
        """
        Do the request to Elliptics.

        Start a thread with HTTP-request. When the pool is full it waits till
        anyone from the pool has finished and reuses it.

        @raise: SaveError
        """

        if len(self.__active_threads) == self.MAX_HTTP_SESSIONS:
            # wait till anyone exits. Expected time is slightly bigger
            # than the timeout time.
            for worker in tuple(self.__active_threads.keys()):
                if not worker.is_alive():
                    # get it out of the queue
                    self.__collect_thread_status_and_kill(worker)
            logger.info(
                '%d threads left in the pool after cleanup',
                len(self.__active_threads)
            )

        if len(self.__active_threads) == self.MAX_HTTP_SESSIONS:
            # force anyone to quit
            worker = list(self.__active_threads.keys())[0]
            logger.warning(
                'Every thread is busy uploading, finishing "%s"', worker
            )
            self.__collect_thread_status_and_kill(worker)

        thread = threading.Thread(
            target=self._timeout_request_with_result,
            name='elliptics-loader-%s' % len(self.__active_threads),
            args=('POST', url),
            kwargs=dict(data=chunk)
        )
        self.__active_threads[thread] = None
        thread.start()

    def _timeout_request_with_result(self, *args, **kwargs):
        try:
            response = self._timeout_request(*args, **kwargs)
        except BaseError as exc:
            self.__active_threads[threading.current_thread()] = exc
        else:
            self.__active_threads[threading.current_thread()] = response

    def _save_file(self, name, content, length=None, **args):
        """
        Simple wrapper which knows about threading.
        """
        try:
            return super(ThreadedEllipticsStorage, self)._save_file(
                name, content, length, **args
            )
        finally:
            self.__wait_till_all_threads_finish()

    def _upload_a_chunk(self, url, chunk, synchronous=False):
        """
        Upload a chunk and raise SaveError on errors.

        @param url:
        @param chunk:
        @param synchronous: is upload in parallel allowed or not
        @return: None
        @raise: SaveError
        """
        if synchronous:
            self.__wait_till_all_threads_finish()
            response = self._timeout_request(
                'POST', url, data=chunk
            )
            if response.status_code != 200:
                raise SaveError(response)
        else:
            self._upload_chunk_in_thread(url, chunk)
