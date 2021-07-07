import logging
from datetime import datetime

import backoff
import elasticsearch
import psycopg2
import redis
from redis import Redis

from postgres_to_es.config import REDIS_HOST, es, ELASTICSEARCH_INDEX
from postgres_to_es.process import ETLSerial
from postgres_to_es.state import RedisStorage, State


@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def serial_etl(batch_size):
    storage = RedisStorage(Redis(REDIS_HOST))
    state = State(storage)
    last_created = state.get_state(__name__)
    now = datetime.now()
    logging.info(f'{__name__}: looking for updates in from {last_created} to {now}')
    
    es.indices.create(index=ELASTICSEARCH_INDEX, ignore=400)
    
    etl_serial = ETLSerial(last_created, now, batch_size)
    load_movies = etl_serial.load()
    transform_movies = etl_serial.transform(load_movies)
    etl_serial.extract(transform_movies)

    state.set_state(__name__, now)
