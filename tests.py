import sys
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

from django.test import utils

def main():
    runner_class = utils.get_runner(conf.settings)
    runner = runner_class()
    return runner.run_tests(['django_elliptics'])

if __name__ == '__main__':
    sys.exit(main())
