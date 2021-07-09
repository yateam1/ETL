import logging
import sys

from elasticsearch import Elasticsearch
from psycopg2.extensions import make_dsn
from redis import Redis

from postgres_to_es.config import POSTGRES_URI, REDIS_HOST, STATE_DB, ELASTICSEARCH_HOST, ELASTICSEARCH_PORT
from postgres_to_es.state import RedisStorage
from postgres_to_es.utils import get_args


dsn = make_dsn(POSTGRES_URI)
storage = RedisStorage(Redis(REDIS_HOST), STATE_DB)
es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])
index = 'movies'

try:
    batch_size, interval, test_pass = get_args()  # Получаем аргументы командной строки
except ValueError as e:
    logging.error(f'Exception {e}')
    sys.exit()


__all__ = (
    'dsn',
    'storage',
    'es',
    'index',
    'batch_size',
    'interval',
    'test_pass',
)
