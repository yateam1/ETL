from elasticsearch import Elasticsearch
from environs import Env
from psycopg2.extensions import make_dsn
from redis import Redis

from postgres_to_es.state import RedisStorage

env = Env()
env.read_env()

# Параметры подключения к базе данных
POSTGRES_HOST = env.str('POSTGRES_HOST', default='localhost')
POSTGRES_PORT = env.int('POSTGRES_PORT', default=5432)
POSTGRES_DB = env.str('POSTGRES_DB')
POSTGRES_USER = env.str('POSTGRES_USER')
POSTGRES_PASSWORD = env.str('POSTGRES_PASSWORD')
POSTGRES_URI = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
dsn = make_dsn(POSTGRES_URI)

# Параметры подключения к Redis
REDIS_HOST = env.str('REDIS_HOST', default='localhost')
STATE_DB = 'Movie_ETL'
storage = RedisStorage(Redis(REDIS_HOST), STATE_DB)

# Параметры подключения к Elastic Search
ELASTICSEARCH_HOST = env.str('ELASTICSEARCH_HOST', default='localhost')
ELASTICSEARCH_PORT = env.str('ELASTICSEARCH_PORT', default='9200')
ELASTICSEARCH_INDEX = env.str('ELASTICSEARCH_INDEX', default='movies')
es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])
