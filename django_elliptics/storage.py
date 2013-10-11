import requests
import urllib
import logging
import time
import socket
import threading
from cStringIO import StringIO

from django import conf
from django.core.files import base, storage


logger = logging.getLogger(__name__)


class BaseError(Exception):
    """Generic error for EllipticsStorage backend."""


class ModeError (BaseError):
    """File operation incompatible with file access mode."""


class HTTPError(BaseError):
    """Elliptics request failed."""


class SaveError(HTTPError):
    """Failed to store file to the backend."""

    def __str__(self):
        response = self.args[0]
        return 'got status code %s while sending to %s' % (
            response.status_code, response.url)


class ReadError(HTTPError):
    """Failed to read from the backend."""

    def __str__(self):
        response = self.args[0]
        return 'got status code %s while reading %s' % (
            response.status_code, response.url)


class TimeoutError(ReadError, SaveError):
    """Timeout error."""

    # ReadError and SaveError override __str__, because they get object with a
    # response to the input. In TimeoutError is impossible to pass the object
    # with a response, therefore overriding is introduced again.
    def __str__(self):
        return super(HTTPError, self).__str__()


class BaseEllipticsStorage(storage.Storage):
    """
    Base Django file storage backend for Elliptics via HTTP API.

    Configuration parameters:

    ELLIPTICS_PREFIX - prefix to prepend to the Django names before passing them to the storage.
    ELLIPTICS_PUBLIC_URL - URL pointing to public interface of the Elliptics cluster to serve files from.

    ELLIPTICS_PRIVATE_URL - URL to send modification requests to.
    """

    default_settings = {
        'prefix': '',
        'public_url': 'http://localhost:8080/',
        'private_url': 'http://localhost:9000/',
    }

    def __init__(self, **kwargs):
        self.settings = self._build_settings(kwargs)
        self.session = requests.session()
        self.session.config['keep_alive'] = True

    def _build_settings(self, settings):
        return type('settings', (), dict(
            (name, settings.get(name, self._get_default(name)))
            for name in self.default_settings))

    def _get_default(self, name):
        setting_name = 'ELLIPTICS_%s' % (name.upper(),)
        return getattr(conf.settings, setting_name, self.default_settings[name])

    def delete(self, name):
        url = self._make_private_url('delete', name)
        self.session.get(url)

    def exists(self, name):
        """
        Returns True if the given name already exists in the storage system.

        Note: override this method with False return value if you want to
        overwrite the contents with the given name.
        This will save your application from unnecessary request in the storage system.
        """
        url = self._make_private_url('get', name)
        r = self.session.head(url)
        return r.status_code == 200

    def url(self, name):
        return self._make_public_url('get', name)

    def _open(self, name, mode):
        return EllipticsFile(name, self, mode)

    def _save(self, name, content, append=False):
        args = {}

        if append:
            args['ioflags'] = 2  # DNET_IO_FLAGS_APPEND = (1<<1)

        url = self._make_private_url('upload', name, **args)
        r = self.session.post(url, data=content)

        if r.status_code != 200:
            raise SaveError(r)

        return name

    def _fetch(self, name):
        url = self._make_private_url('get', name)
        r = self.session.get(url)
        if r.status_code != 200:
            raise ReadError(r)

        return r.content

    def _make_private_url(self, command, *parts, **args):
        return self._make_url(
            self.settings.private_url, command, self.settings.prefix, *parts, **args
        )

    def _make_public_url(self, command, *parts, **args):
        return self._make_url(
            self.settings.public_url, command, self.settings.prefix, *parts, **args
        )

    def _make_url(self, *parts, **args):
        url = '/'.join(part.strip('/') for part in parts if part)

        if args:
            url += '?' + urllib.urlencode(args)

        return url


class EllipticsFile (base.File):
    def __init__(self, name, storage, mode):
        self.name = name
        self._storage = storage
        self._stream = None

        if 'r' in mode:
            self._mode = 'r'
        elif 'w' in mode:
            self._mode = 'w'
        elif 'a' in mode:
            self._mode = 'a'
        else:
            raise ValueError('mode must contain at least one of "r", "w" or "a"')

        if '+' in mode:
            raise ValueError('mixed mode access not supported yet.')

    def read(self, num_bytes=None):
        if self._mode != 'r':
            raise ModeError('reading from a file opened for writing.')

        if self._stream is None:
            content = self._storage._fetch(self.name)
            self._stream = StringIO(content)

        if num_bytes is None:
            return self._stream.read()

        return self._stream.read(num_bytes)

    def write(self, content):
        if self._mode not in ('w', 'a'):
            raise ModeError('writing to a file opened for reading.')

        if self._stream is None:
            self._stream = StringIO()

        return self._stream.write(content)

    def close(self):
        if self._stream is None:
            return

        if self._mode in ('w', 'a'):
            self._storage._save(
                self.name, self._stream.getvalue(), append=(self._mode == 'a')
            )

    @property
    def size(self):
        raise NotImplementedError

    @property
    def closed(self):
        return bool(self._stream is None)

    def seek(self, offset, mode=0):
        self._stream.seek(offset, mode)


FAILED_MESSAGE = (
    '%s failed attempts of %s to connect to Elliptics '
    '(%s %s). Timeout: %s seconds. "%s"'
)


class EllipticsStorage(BaseEllipticsStorage):
    """
    Django storage backend to Elliptics.

    Configuration: same as in base class + some more.
    Supports timeouts and retries on failure (see the config).
    """
    # timeout of http-session on read requests
    timeout_get = getattr(conf.settings, 'ELLIPTICS_GET_CONNECTION_TIMEOUT', 3)
    # number of retries in http-session on read requests
    retries_get = getattr(conf.settings, 'ELLIPTICS_GET_CONNECTION_RETRIES', 3)
    # timeout of http-session on save requests
    timeout_post = getattr(conf.settings, 'ELLIPTICS_POST_CONNECTION_TIMEOUT', 5)
    # number of retries in http-session on save requests
    retries_post = getattr(conf.settings, 'ELLIPTICS_POST_CONNECTION_RETRIES', 9)
    # size of a chunk in bytes, to split content into
    MAX_CHUNK_SIZE = getattr(
        conf.settings, 'ELLIPTICS_UPLOAD_CHUNK_SIZE', 3 * 1024 * 1024
    )
    # Average size of your file. Is used to reserve space in storage.
    # This helps against fragmentation, so choose it carefully.
    AVERAGE_FILE_SIZE = getattr(
        conf.settings, 'ELLIPTICS_AVERAGE_FILE_SIZE', 1024 * 10
    )

    def _request(self, method, url, *args, **kwargs):
        if method in ('POST', 'GET', 'HEAD'):
            logger.debug('Sending "%s" to url of Elliptics "%s"', method, url)
            return getattr(self.session, method.lower())(url, *args, **kwargs)

        else:
            raise NotImplementedError('The requested method is not acceptable')

    def _timeout_request(self, method, url, *args, **kwargs):
        global retry_count, retry_count
        error_message = ''
        if method == 'POST':
            retries = self.retries_post
            timeout = self.timeout_post
        else:
            retries = self.retries_get
            timeout = self.timeout_get

        for retry_count in xrange(retries):
            try:
                started = time.time()
                response = self._request(method, url, *args, timeout=timeout, **kwargs)
            except socket.gaierror as exc:
                raise BaseError(
                    'incorrect elliptics request {0} "{1}": {2}'.format(
                        method, url, repr(exc))
                )
            except requests.Timeout, exception:
                error_message = str(exception)
            else:
                logger.debug(
                    'Success with "%s" to Elliptics "%s" at try %d in time=%.4f',
                    method, url, retry_count, time.time() - started
                )
                break
        else:
            logger.error(
                FAILED_MESSAGE,
                retry_count + 1, retries, method, url, timeout, error_message
            )
            raise TimeoutError(error_message)

        if retry_count:
            logger.warning(
                FAILED_MESSAGE,
                retry_count, retries, method, url, timeout, error_message
            )

        return response

    def _fetch(self, name):
        url = self._make_private_url('get', name)
        response = self._timeout_request('GET', url)

        if response.status_code != 200:
            logger.warning('Elliptics read error status %d, url %s',
                           response.status_code, url, extra={'stack': True})
            raise ReadError(response)

        return response.content

    def _save(self, name, content, append=False):
        """

        @raise: BaseError
        """
        args = {}
        if append:
            return self._save_with_append(name, content, **args)

        try:
            content, length = self.__guess_content_size(content)
        except NotImplementedError:
            length = None
        return self._save_file(name, content, length, **args)

    def _save_with_append(self, name, content, **args):
        args['ioflags'] = 2  # DNET_IO_FLAGS_APPEND = (1<<1)
        url = self._make_private_url('upload', name, **args)
        response = self._timeout_request('POST', url, data=content)

        if response.status_code != 200:
            raise SaveError(response)

        return name

    def __guess_content_size(self, content):
        """
        Return content and its size.

        @param content:
        @rtype: tuple
        @raise: NotImplementedError
        """
        if hasattr(content, 'size'):
            if content.size is not None:
                return content, content.size
            logger.info(
                'The size of content is None, content type is "%s"',
                type(content)
            )

        try:
            # may be a string or a bytestring
            return content, len(content)
        except Exception, exc:
            raise NotImplementedError(
                'The size of object cannot be guessed: "%s"', repr(exc)
            )

    def _save_file(self, name, content, length=None, **args):
        """
        Save the file into Elliptics.

        Splits file into chunks and allows to upload them in parallel or
        consequently.

        @param name: name of entity in Elliptics
        @param content: the File-like object, or a (byte-)string, iterable.
        @param length: length of content. May be unknown!
        @param args: additional args for the POST-request.
        @return: final name of entity
        @type name: str
        @type length: int
        @rtype: str
        """
        uploaded = 0
        logger.debug('Uploading %d bytes into Elliptics', length)

        next_chunk = self._create_chunk(content, uploaded, self.MAX_CHUNK_SIZE)
        next_chunk_length = len(next_chunk)

        while next_chunk_length > 0:
            request_args = args.copy()
            chunk = next_chunk
            chunk_length = next_chunk_length
            # get chunk, probably shorter than MAX_CHUNK_SIZE
            next_chunk = self._create_chunk(content, uploaded, self.MAX_CHUNK_SIZE)
            next_chunk_length = len(next_chunk)

            if not (uploaded == 0 and next_chunk_length == 0):
                # not the only one request
                request_args['offset'] = uploaded
                request_args['size'] = chunk_length

            if uploaded == 0 and next_chunk_length > 0:
                # the first request and more to come
                # reserve space in storage
                if length is not None and self.MAX_CHUNK_SIZE < length:
                    # there will be more than 1 of requests
                    request_args['prepare'] = length
                else:
                    request_args['prepare'] = max(
                        self.AVERAGE_FILE_SIZE * 2, chunk_length
                    )

            if uploaded > 0 and next_chunk_length == 0:  # the file is exhausted
                # this is the last request from a series of, we should commit
                request_args['commit'] = uploaded + chunk_length

            url = self._make_private_url('upload', name, **request_args)

            # this is the place to implement parallel uploads
            self._upload_a_chunk(
                url, chunk,
                # the first and the last requests are synchronous.
                synchronous=uploaded == 0 or next_chunk_length == 0
            )

            uploaded += chunk_length

        return name

    def _upload_a_chunk(self, url, chunk, synchronous=False):
        """
        Upload a chunk and raise SaveError on errors.

        Override this in child, to allow upload in threads.

        @param url:
        @param chunk:
        @param synchronous: upload in parallel is allowed.
        @type synchronous: bool
        @return: None
        @raise: SaveError
        """
        response = self._timeout_request(
            'POST', url, data=chunk
        )
        if response.status_code != 200:
            raise SaveError(response)

    def _create_chunk(self, content, from_byte, chunk_length):
        """
        Create chunk for uploading.

        @param content: File-like object or a string
        @type from_byte: int
        @type chunk_length: int
        """
        if hasattr(content, 'read'):
            return content.read(chunk_length)
        return content[from_byte:from_byte + chunk_length]

    def _make_url(self, *parts, **args):
        """
        Return URL.

        Quotes the path section of a URL.
        @return: str
        """
        if not isinstance(parts, list):
            parts = list(parts)

        for index in xrange(1, len(parts)):
            parts[index] = urllib.quote(parts[index])

        url = super(EllipticsStorage, self)._make_url(
            *parts,
            **args
        )
        return url


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
        for thread in tuple(self.__active_threads.keys()):
            self.__collect_thread_status_and_kill(thread)

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
