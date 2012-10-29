import os, sys
from django import conf

conf.settings = conf.global_settings
conf.settings.USE_I18N = False
conf.settings.SETTINGS_MODULE = 'django.conf.global_settings'
conf.settings.INSTALLED_APPS = [
    'django_elliptics'
]

conf.settings.DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
    }
}

env_elliptics_url = os.environ.get('ELLIPTICS_URL')
if env_elliptics_url:
    conf.settings.ELLIPTICS_PUBLIC_URL = env_elliptics_url
    conf.settings.ELLIPTICS_PRIVATE_URL = env_elliptics_url

from django.test import utils

def main():
    runner_class = utils.get_runner(conf.settings)
    runner = runner_class()
    return runner.run_tests(['django_elliptics'])

if __name__ == '__main__':
    sys.exit(main())
