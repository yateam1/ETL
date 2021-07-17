import logging
from datetime import datetime

import backoff
import elasticsearch
import psycopg2
import redis

from .transform import transform_to_filmworks_index, transform_to_genres_index
from .load import load
from .genre import extract_genres
from .movie import extract_movies
from .serial import extract_serials
from loader import storage, es, index_movies, index_genres, index_persons
from state import State

STATE_KEY = 'movies'


@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def launch_etl():
    """
    Основной метод запуска корутин ETL. Предварительно из хранилища Redis достается дата и время последней успещной
    операции ETL. Если всё проходит хорошо, данные в хранилище обновляются.
    :return:
    """
    state = State(storage)
    last_created = state.get_state(STATE_KEY)
    now = datetime.now()
    logging.info(f'KEY {STATE_KEY}: looking for updates in from {last_created} to {now}')
    
    # Запускаем корутины перегрузки кинопроизведений
    es.indices.create(index=index_movies, ignore=400)
    
    load_filmworks = load()
    transform_filmworks = transform_to_filmworks_index(load_filmworks)

    extract_movies(transform_filmworks, last_created, now)
    extract_serials(transform_filmworks, last_created, now)

    # Запускаем корутины перегрузки жанров
    es.indices.create(index=index_genres, ignore=400)
    load_genres = load()
    transform_genres = transform_to_genres_index(load_genres)
    extract_genres(transform_genres, last_created, now)

    # Запускаем корутины перегрузки персон
    es.indices.create(index=index_persons, ignore=400)
    # load_persons()

    state.set_state(STATE_KEY, now)
