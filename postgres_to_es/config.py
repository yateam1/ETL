from environs import Env

env = Env()
env.read_env()

# Параметры подключения к базе данных
POSTGRES_HOST = env.str('POSTGRES_HOST', default='localhost')
POSTGRES_PORT = env.int('POSTGRES_PORT', default=5432)
POSTGRES_DB = env.str('POSTGRES_DB')
POSTGRES_USER = env.str('POSTGRES_USER')
POSTGRES_PASSWORD = env.str('POSTGRES_PASSWORD')
POSTGRES_URI = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

# Параметры подключения к Redis
REDIS_HOST = env.str('REDIS_HOST', default='localhost')
STATE_DB = 'Movie_ETL'

# Параметры подключения к Elastic Search
ELASTICSEARCH_HOST = env.str('ELASTICSEARCH_HOST', default='localhost')
ELASTICSEARCH_PORT = env.str('ELASTICSEARCH_PORT', default='9200')
