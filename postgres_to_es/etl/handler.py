import logging
from datetime import datetime

import backoff
import elasticsearch
import psycopg2
import redis

from .transform_load import load, transform
from .movie import extract_movies
from .serial import extract_serials
from ..config import ELASTICSEARCH_INDEX
from ..loader import storage, es
from ..state import State


@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def launch_etl():
    state = State(storage)
    last_created = state.get_state(__name__)
    now = datetime.now()
    logging.info(f'{__name__}: looking for updates in from {last_created} to {now}')
    
    es.indices.create(index=ELASTICSEARCH_INDEX, ignore=400)
    
    load_filmworks = load()
    transform_filmworks = transform(load_filmworks)

    extract_movies(transform_filmworks, last_created, now)
    extract_serials(transform_filmworks, last_created, now)

    state.set_state(__name__, now)