# coding: utf-8
import logging
import time
import socket
import urllib

import requests

from .base import BaseEllipticsStorage
from .errors import *
from .settings import (
    ELLIPTICS_GET_CONNECTION_TIMEOUT, ELLIPTICS_GET_CONNECTION_RETRIES,
    ELLIPTICS_POST_CONNECTION_RETRIES, ELLIPTICS_POST_CONNECTION_TIMEOUT,
    ELLIPTICS_UPLOAD_CHUNK_SIZE
)

logger = logging.getLogger(__name__)


FAILED_MESSAGE = (
    '%s failed attempts of %s to connect to Elliptics '
    '(%s %s). Timeout: %s seconds. "%s"'
)

# It is a very bad to write big files into elliptics without knowing it's size.
# That is because E does not have transactions and you will read what has not
# been written completely.
BAD_IDEA_TO_WRITE_MESSAGE = (
    'It is a very bad idea to write to Elliptics'
    'entities of unknown size'
)


class EllipticsStorage(BaseEllipticsStorage):
    """
    Django storage backend to Elliptics.

    Configuration: same as in base class + some more.
    Supports timeouts and retries on failure (see the config).
    """

    timeout_get = ELLIPTICS_GET_CONNECTION_TIMEOUT
    retries_get = ELLIPTICS_GET_CONNECTION_RETRIES
    timeout_post = ELLIPTICS_POST_CONNECTION_TIMEOUT
    retries_post = ELLIPTICS_POST_CONNECTION_RETRIES
    MAX_CHUNK_SIZE = ELLIPTICS_UPLOAD_CHUNK_SIZE

    def _request(self, method, url, *args, **kwargs):
        if method in ('POST', 'GET', 'HEAD'):
            logger.debug('Sending "%s" to url of Elliptics "%s"', method, url)
            return getattr(self.session, method.lower())(url, *args, **kwargs)

        else:
            raise NotImplementedError('The requested method is not acceptable')

    def _timeout_request(self, method, url, *args, **kwargs):
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
                response = self._request(
                    method, url, *args, timeout=timeout, **kwargs
                )
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
        You should have content.size attribute set.
        This is desired, cause that is the most safe way to upload.

        @raise: BaseError
        """
        args = {}
        if append:
            self._save_with_append(name, content, **args)
            return name

        try:
            content, length = self.__guess_content_size(content)
        except NotImplementedError:
            logger.error(BAD_IDEA_TO_WRITE_MESSAGE)
            # length is unknown, so we append
            uploaded = 0
            while True:
                chunk = self._create_chunk(
                    content, uploaded, self.MAX_CHUNK_SIZE
                )
                if not chunk:
                    return name
                self._save_with_append(name, chunk, **args)
                uploaded += len(chunk)

        self._save_file(name, content, length, **args)
        return name

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

    def _save_file(self, name, content, length, **args):
        """
        Save the file into Elliptics.

        Splits file into chunks and allows to upload them in parallel or
        consequently.

        @param name: name of entity in Elliptics
        @param content: the File-like object, or a (byte-)string, iterable.
        @param length: length of content.
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
            next_chunk = self._create_chunk(
                content, uploaded + chunk_length, self.MAX_CHUNK_SIZE
            )
            next_chunk_length = len(next_chunk)

            if not (uploaded == 0 and next_chunk_length == 0):
                # not the only one request
                request_args['offset'] = uploaded
                request_args['size'] = chunk_length

            if uploaded == 0 and next_chunk_length > 0:
                # the first request and more to come
                # reserve space in storage
                if chunk_length < length:
                    # there will be more than 1 of requests
                    request_args['prepare'] = length

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
