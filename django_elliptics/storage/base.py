# coding: utf-8
import urllib
from cStringIO import StringIO

import requests
from django.core.files import base, storage
from django import conf

from .errors import *


class BaseEllipticsStorage(storage.Storage):
    """
    Base Django file storage backend for Elliptics via HTTP API.

    Configuration parameters in your settings.py:

    ELLIPTICS_PREFIX - name of every entity put into or read from E will be prefixed with this string.
    It works like this: desired_key -> ELLIPTICS_PREFIX + '/' + desired_key.

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

    def _make_url(self, *parts, **query_params):
        url = '/'.join(part.strip('/') for part in parts if part)

        if query_params:
            url += '?' + urllib.urlencode(query_params)

        return url


class EllipticsFile(base.File):
    """
    A File-like object.

    You never instantiate it manually, it serves specific purposes.
    """
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
        return self._stream is None

    def seek(self, offset, mode=0):
        """
        @param mode: this value is passed as is into StringIO.seek
        """
        self._stream.seek(offset, mode)
