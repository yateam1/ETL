import logging
from datetime import datetime

import backoff
import elasticsearch
import psycopg2
import redis

from postgres_to_es.config import ELASTICSEARCH_INDEX, es, storage
from postgres_to_es.process import ETLMovie
from postgres_to_es.state import State


@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def movie_etl(batch_size):
    
    state = State(storage)
    last_created = state.get_state(__name__)
    now = datetime.now()
    logging.info(f'{__name__}: looking for updates in from {last_created} to {now}')
    
    es.indices.create(index=ELASTICSEARCH_INDEX, ignore=400)
    
    etl_movie = ETLMovie(last_created, now, batch_size)
    load_movies = etl_movie.load()
    transform_movies = etl_movie.transform(load_movies)
    etl_movie.extract(transform_movies)

    state.set_state(__name__, now)
