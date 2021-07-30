from environs import Env
from pydantic import BaseModel

env = Env()
env.read_env()


class PostgresSettings(BaseModel):
    host: str
    port: int


class RedisSettings(BaseModel):
    host: str
    port: int


class ElasticSettings(BaseModel):
    host: str
    port: int


class Config(BaseModel):
    postgres_db: PostgresSettings
    redis: RedisSettings
    elasticsearch: ElasticSettings


config = Config.parse_file('config.json')

# Параметры подключения к базе данных
POSTGRES_HOST = config.postgres_db.host
POSTGRES_PORT = config.postgres_db.port
POSTGRES_DB = env.str('POSTGRES_DB')
POSTGRES_USER = env.str('POSTGRES_USER')
POSTGRES_PASSWORD = env.str('POSTGRES_PASSWORD')
POSTGRES_URI = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

# Параметры подключения к Redis
REDIS_HOST = config.redis.host
REDIS_PORT = config.redis.port
STATE_DB = 'Movie_ETL'

# Параметры подключения к Elasticsearch
ELASTICSEARCH_HOST = config.elasticsearch.host
ELASTICSEARCH_PORT = config.elasticsearch.port

# Наименования индексов
INDEX_MOVIES = 'movies'
INDEX_GENRES = 'genres'
INDEX_PERSONS = 'persons'

