import logging
import sys

from elasticsearch import Elasticsearch
from psycopg2.extensions import make_dsn
from redis import Redis

from es_settings import es_settings, es_mappings
from config import POSTGRES_URI, REDIS_HOST, STATE_DB, ELASTICSEARCH_HOST, ELASTICSEARCH_PORT
from config import INDEX_MOVIES, INDEX_GENRES, INDEX_PERSONS
from state import RedisStorage
from utils import get_args

"""
:dsn: параметры подключения к СУБД Postgres
:storage: объект хранилища Redis
:es: объект ElasticSearch
:batch_size: размер пачки данных, чтобы не перегружать сервисы
:interval: интервал в секундах, через который запускается каждый новый цикл
:test_pass: если данный параметр указан, то цикл остановится через указанное в нем кол-во повторений
:index_genres: наименование индекса жанов в ElasticSearch
:index_persons: наименование индекса персоналий в ElasticSearch
:index_movies: наименование индекса кинопроизведений в ElasticSearch
"""

dsn = make_dsn(POSTGRES_URI)
storage = RedisStorage(Redis(REDIS_HOST), STATE_DB)
es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])
index_movies = INDEX_MOVIES
index_genres = INDEX_GENRES
index_persons = INDEX_PERSONS


try:
    batch_size, interval, debug = get_args()  # Получаем аргументы командной строки
except ValueError as e:
    logging.error(f'Exception {e}')
    sys.exit()


__all__ = (
    'dsn',
    'storage',
    'es',
    'es_settings',
    'es_mappings',
    'batch_size',
    'interval',
    'debug',
    'index_genres',
    'index_persons',
    'index_movies',
)
