from elasticsearch import Elasticsearch
from environs import Env
from psycopg2.extensions import make_dsn

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
STATE_KEY = 'producer'

# Параметры подключения к Elastic Search
ELASTICSEARCH_HOST = env.str('ELASTICSEARCH_HOST', default='localhost')
ELASTICSEARCH_PORT = env.str('ELASTICSEARCH_PORT', default='9200')
ELASTICSEARCH_INDEX = env.str('ELASTICSEARCH_INDEX', default='movies')
es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])
es.indices.create(index=ELASTICSEARCH_INDEX, ignore=400)



