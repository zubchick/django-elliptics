# -*- coding: utf-8 -*-

try:
    from ujson import dumps, loads
except ImportError:
    from django.utils.simplejson import loads, dumps

from django.db import models
from django.conf import settings
from django.core.files.uploadedfile import SimpleUploadedFile


def configure_storage(prefix=None, **kwargs):
    """
    Get instance of a class specified in <prefix>_STORAGE_CLASS setting.
    Default setting is just STORAGE_CLASS.

    @type prefix: str
    @param prefix: prefix for storage class name in settings.
                   Will be uppercased and separated with 'STORAGE_CLASS' by '_'.
                   example: if prefix='file', storage class setting name used is
                   FILE_STORAGE_CLASS

    @return: storage instance
    """
    storage_class_name = 'STORAGE_CLASS'
    if prefix:
        assert isinstance(prefix, str), 'prefix must be a sting'
        storage_class_name = '_'.join([prefix.upper(), storage_class_name])

    storage = getattr(settings, storage_class_name, 'django_elliptics.storage.EllipticsStorage')

    module_name, class_name = storage.rsplit('.', 1)
    try:
        storage_class = getattr(
            __import__(module_name, {}, {}, [class_name]),
            class_name
        )
    except ImportError:
        raise
    return storage_class(**kwargs)


STORAGE = configure_storage()


class SerializedPropsMixInManager(models.Manager):
    def get_field_from_storage(self, data, single_field=None):
        """
        Read fields' data from key-value storage wihtout creating model's object.

        string, string|None -> anything

        """
        # Get data from storage
        if data:
            _data = self.model._storage_loads(STORAGE._open(data, 'r').read())
        else:
            _data = {}
        # Detect default value
        if isinstance(self.model._serialized_props_defaults, dict):
            get_default = True
        else:
            get_default = False
            default_value = self.model._serialized_props_defaults
        # Return one field's value
        if single_field:
            return _data.get(
                single_field,
                self.model._serialized_props_defaults.get(single_field, None)
                    if get_default else default_value
            )
        # Return all fields' values
        return dict(
            (f, _data.get(f,
                self.model._serialized_props_defaults.get(f, None)
                if get_default else default_value)
            )
            for f in self.model._serialized_props
        )

    def save_storage_fields(self, **kwargs):
        m = self.model()
        for k in kwargs:
            setattr(m, k, kwargs[k])
        return m.elliptics_id.storage.save(
            m.make_elliptics_id(),
            m._storage_dumps(m._data)
        )


class SerializedPropsMixIn(models.Model):
    """
    @todo comment
    """
    # List of properties
    _serialized_props = tuple()

    # Dict with defaults for every property or scalar for all
    _serialized_props_defaults = None

    # Have serialized properties been modified?
    _serialized_props_modified = False

    @staticmethod
    def _storage_loads(data):
        return loads(data)

    @staticmethod
    def _storage_dumps(data):
        return SimpleUploadedFile('fake_uploaded_file', dumps(data))

    objects = SerializedPropsMixInManager()

    def __getattr__(self, name):
        """
        Returns value of model's field from key-value storage.
        If field isn't stored into storage, returns default value
        based on model's _serialized_props_defaults property.
        """
        if name in self._serialized_props:
            self._init_data()
            return self._data.get(
                name,
                self._serialized_props_defaults.get(name, None)
                if isinstance(self._serialized_props_defaults, dict)
                else self._serialized_props_defaults
            )
        return super(SerializedPropsMixIn, self).__getattribute__(name)

    def __setattr__(self, name, value):
        """
        Set value for model's field from key-value storage.
        """
        if name in self._serialized_props:
            self._init_data()
            self._data[name] = value
            self._serialized_props_modified = True
        return super(SerializedPropsMixIn, self).__setattr__(name, value)

    def _init_data(self):
        """
        Implements lazy loading of data from key-value storage.
        """
        if not hasattr(self, '_data'):
            if self.elliptics_id:
                self._data = self._storage_loads(self.elliptics_id.read())
            else:
                self._data = {}

    def save(self, *args, **kwargs):
        """
        Saves model's data to RDBMS and key-value storage both.
        See also receivers of pre_save and post_save signals in this file.
        """
        lock = False

        # Save serialized properties to storage only if they have been
        # loaded and changed.
        elliptics_id = self.make_elliptics_id()
        if hasattr(self, '_data') and self._serialized_props_modified:
            self.elliptics_id = self.elliptics_id.storage.save(
                elliptics_id,
                self._storage_dumps(self._data)
            )

        res = super(SerializedPropsMixIn, self).save(*args, **kwargs)
        if self._serialized_props_modified:
            self._serialized_props_modified = False
        return res

    class Meta:
        abstract = True
