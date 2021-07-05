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
ES_HOST = env.str('ES_HOST', default='localhost')
ES_PORT = env.str('ES_PORT', default='9200')
ES_INDEX = env.str('INDEX_NAME', default='movies')

