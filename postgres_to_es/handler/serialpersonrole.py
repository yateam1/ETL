import logging
from datetime import datetime

import backoff
import elasticsearch
import psycopg2
import redis

from postgres_to_es.config import ELASTICSEARCH_INDEX
from postgres_to_es.loader import storage, es
from postgres_to_es.process import ETLSerialPersonRole
from postgres_to_es.state import State


@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def serialpersonrole_etl(batch_size: int):
    """
    Запускаем ETL-процесс поиска изменений в таблице участия персоналий в сериалах(serialpersonrole).
    Временной период выборки изменений определяем между датой в storage и текущими датой и временем.
    Если все требуемые сервисы доступны и данные перегружены, обновляем дату в storage.
    :param batch_size: размер пачки данных
    :return:
    """
    
    state = State(storage)
    last_created = state.get_state(__name__)
    now = datetime.now()
    logging.info(f'{__name__}: looking for updates in from {last_created} to {now}')
    
    es.indices.create(index=ELASTICSEARCH_INDEX, ignore=400)

    etl_serialpersonrole = ETLSerialPersonRole(last_created, now, batch_size)
    load_movies = etl_serialpersonrole.load()
    transform_movies = etl_serialpersonrole.transform(load_movies)
    serials = etl_serialpersonrole.extract_serial(transform_movies)
    etl_serialpersonrole.extract_serial_id(serials)

    state.set_state(__name__, now)

