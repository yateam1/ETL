import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from loader import dsn, batch_size
from .query_ids import queries
from .quiery_filmworks import SQL_GET_SERIALS_BY_IDS
from ..coroutine import coroutine


def extract_filmwork_ids(batch, query: str, date_from: datetime, date_to: datetime):
    """
    Извлекает из таблицы БД, которая (таблица) указана в query, id'шники кинопроизведений, которые изменились в
    в заданный временной период.
    ПЕРВЫЙ МЕТОД КОРУТИНЫ ИДЕТ БЕЗ ДЕКОРАТОРА.
    :param batch: пачка данных, содержащая id'шники кинопроизведений и которая будет передана в последующую корутину
    :param query: запрос, который ищет изменения в указанной в нем модели(таблице) в заданный временной период
    :param date_from: начало (дата+время) временного периода
    :param date_to: окончание (дата+время) временного периода
    :return:
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
    Метод извлечения кинопроизведенийзаписей из базы данных по переданному из корутины списку filmwork_ids
    :param batch: пачка извлеченных из БД данных
    :return: send порция данных из БД, которые были изменены в заданный временной интервал
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
    """
    Метод, настраивающий запуск корутин получения id'шников кинопроизведений и самих кинопроизведений
    в заданный временной период.
    В методе используется импортированные запросы, которые ищут изменения по всем моделям базы данных.
    ЭТО ЕЩЁ НЕ ЗАПУСК КОРУТИНЫ, поэтому метод без декоратора
    :param transform_function: функция transoform основного ETL-процесса (см.импорт)
    :param last_created: начало (дата+время) временного периода
    :param now: окончание (дата+время) временного периода
    :return:
    """
    for query in queries:
        logging.info(f'Search for modified records: {query}')
        extract_filmworks = extract_filmworks_by_ids(transform_function)
        extract_filmwork_ids(extract_filmworks, query, last_created, now)
