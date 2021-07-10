import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from ...loader import dsn, batch_size
from .query_ids import queries
from .quiery_filmworks import SQL_GET_SERIALS_BY_IDS
from ..coroutine import coroutine


def extract_filmwork_ids(batch, query: str, date_from: datetime, date_to: datetime):
    """
    Основной метод извлечения записей из базы данных
    :param batch: пачка извлеченных из БД данных
    :return: порция данных из БД, которые были изменены в заданный временной интервал
    """
    with psycopg2.connect(dsn=dsn) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            if not date_from:
                date_from = datetime(1900, 1, 1, 0, 0, 0, 0)
            cursor.execute(f"""{query}""", {'date_from': date_from, 'date_to': date_to})
            
            filmwork_ids = cursor.fetchmany(batch_size)
            while filmwork_ids:
                batch.send(filmwork_ids)
                filmwork_ids = cursor.fetchmany(batch_size)


@coroutine
def extract_filmworks_by_ids(batch):
    """
    Основной метод извлечения записей из базы данных
    :param batch: пачка извлеченных из БД данных
    :return: порция данных из БД, которые были изменены в заданный временной интервал
    """
    with psycopg2.connect(dsn=dsn) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            while filmwork_ids := (yield):
                filmwork_ids = [movie['serial_id'] for movie in filmwork_ids]
                cursor.execute(f"""{SQL_GET_SERIALS_BY_IDS}""", {'serial_ids': filmwork_ids})
                filmworks = cursor.fetchmany(batch_size)
                while filmworks:
                    batch.send(filmworks)
                    filmworks = cursor.fetchmany(batch_size)


def extract_serials(transform_function, last_created, now):
    for query in queries:
        logging.info(f'Search for modified records: {query}')
        extract_filmworks = extract_filmworks_by_ids(transform_function)
        extract_filmwork_ids(extract_filmworks, query, last_created, now)
