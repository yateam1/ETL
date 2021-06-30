from .base import *

DEBUG = True

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'filters': {
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
        }
    },
    'formatters': {
        'default': {
            'format': '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]',
        },
    },
    'handlers': {
        'debug-console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'filters': ['require_debug_true'],
        },
    },
    'loggers': {
        'django.db.backends': {
            'level': 'DEBUG',
            'handlers': ['debug-console'],
            'propagate': False,
        }
    },
}

INSTALLED_APPS.append('debug_toolbar')
INSTALLED_APPS.append('django_extensions')

MIDDLEWARE.append('debug_toolbar.middleware.DebugToolbarMiddleware')

try:
    INTERNAL_IPS.append('127.0.0.1')
except:
    INTERNAL_IPS = [
        '127.0.0.1',
    ]

STATIC_ROOT = 'static'
