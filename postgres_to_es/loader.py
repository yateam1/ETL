import logging
import sys

from elasticsearch import Elasticsearch
from psycopg2.extensions import make_dsn
from redis import Redis

from config import POSTGRES_URI, REDIS_HOST, STATE_DB, ELASTICSEARCH_HOST, ELASTICSEARCH_PORT
from state import RedisStorage
from utils import get_args

"""
:dsn: параметры подключения к СУБД Postgres
:storage: объект хранилища Redis
:es: объект Elastic Search
:batch_size: размер пачки данных, чтобы не перегружать сервисы
:interval: интервал в секундах, через который запускается каждый новый цикл
:test_pass: если данный параметр указан, то цикл остановится через указанное в нем кол-во повторений
"""

dsn = make_dsn(POSTGRES_URI)
storage = RedisStorage(Redis(REDIS_HOST), STATE_DB)
es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])



try:
    index, batch_size, interval, debug = get_args()  # Получаем аргументы командной строки
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
    'debug',
)
