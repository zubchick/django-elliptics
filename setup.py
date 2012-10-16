from setuptools import setup, find_packages

setup(
    name = 'django-elliptics',
    version = '1.0',
    packages = find_packages(),
    
    author = 'Vickenty Fesunov <kent@setattr.net>',
    license = 'BSD',
    description = 'Elliptics file storage backend for Django.',
    url = 'http://github.com/vickenty/django-elliptics',

    install_requires = [ 'requests >= 0.13.6' ]
)
