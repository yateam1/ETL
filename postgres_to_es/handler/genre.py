import logging
from datetime import datetime

import backoff
import elasticsearch
import psycopg2
import redis
from redis import Redis

from postgres_to_es.config import REDIS_HOST, ELASTICSEARCH_INDEX, es
from postgres_to_es.process import ETLGenre
from postgres_to_es.state import State, RedisStorage


@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def genre_etl(batch_size):
    storage = RedisStorage(Redis(REDIS_HOST))
    state = State(storage)
    last_created = state.get_state(__name__)
    now = datetime.now()
    logging.info(f'{__name__}: looking for updates in from {last_created} to {now}')
    
    es.indices.create(index=ELASTICSEARCH_INDEX, ignore=400)
    
    etl_genre = ETLGenre(last_created, now, batch_size)
    load_movies = etl_genre.load()
    transform_movies = etl_genre.transform(load_movies)

    movies = etl_genre.extract_movie(transform_movies)
    etl_genre.extract_movie_id(movies)

    serilas = etl_genre.extract_serial(transform_movies)
    etl_genre.extract_serial_id(serilas)

    state.set_state(__name__, now)