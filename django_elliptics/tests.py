from __future__ import with_statement
from django.test import TestCase
from django_elliptics import storage

class EllipticsStorageTest (TestCase):
    prefix = ''
    storage_class_name = 'EllipticsStorage'

    def setUp(self):
        self.storage = getattr(__import__('django_elliptics.storage', {}, {}, [self.storage_class_name]),
                               self.storage_class_name)  (prefix=self.prefix)
        self.sample1 = '<xml>test data</xml>'
        self.sample2 = '<xml>more test data</xml>'

    def tearDown(self):
        self.storage.delete('test.xml')

    def test_save(self):
        name = self.storage.save('test.xml', self.sample1)
        self.assertEquals(name, 'test.xml')
    
    def test_open_existing(self):
        name = self.storage.save('test.xml', self.sample1)

        with self.storage.open('test.xml', 'r') as stream:
            self.assertEquals(stream.read(), self.sample1)

        with self.storage.open('test.xml', 'w') as stream:
            n = stream.write(self.sample2)

        with self.storage.open('test.xml', 'r') as stream:
            self.assertEquals(stream.read(), self.sample2)

    def test_open_new(self):
        with self.storage.open('test.xml', 'w') as stream:
            stream.write(self.sample1)

        with self.storage.open('test.xml', 'r') as stream:
            self.assertEquals(stream.read(), self.sample1)

    def test_append(self):
        self.storage.save('test.xml', self.sample1)
        with self.storage.open('test.xml', 'r') as stream:
            self.assertEquals(stream.read(), self.sample1)

        with self.storage.open('test.xml', 'a') as stream:
            stream.write(self.sample2)

        with self.storage.open('test.xml', 'r') as stream:
            self.assertEquals(stream.read(), self.sample1 + self.sample2)

    def test_mode_protect(self):
        with self.storage.open('test.xml', 'r') as stream:
            self.assertRaises(storage.ModeError, stream.write, self.sample1)

        with self.storage.open('test.xml', 'w') as stream:
            self.assertRaises(storage.ModeError, stream.read)

        with self.storage.open('test.xml', 'a') as stream:
            self.assertRaises(storage.ModeError, stream.read)

    def test_delete(self):
        self.storage.save('test.xml', self.sample1)
        self.assertTrue(self.storage.exists('test.xml'))
        self.storage.delete('test.xml')
        self.assertFalse(self.storage.exists('test.xml'))


class PrefixTest (EllipticsStorageTest):
    prefix = 'prefix'


class LongPrefixTest (EllipticsStorageTest):
    prefix = 'long/prefix'


class TimeoutAwareEllipticsStorageTest(EllipticsStorageTest):
    prefix = ''
    storage_class_name = 'TimeoutAwareEllipticsStorage'
