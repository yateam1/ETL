import logging
from datetime import datetime

import psycopg2
from loader import dsn, batch_size
from psycopg2.extras import RealDictCursor

from .query import query


def extract_persons(batch, date_from: datetime, date_to: datetime):
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

    logging.info(f'Search for modified records in persons tables: {query}')
    with psycopg2.connect(dsn=dsn) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            if not date_from:
                date_from = datetime(1900, 1, 1, 0, 0, 0, 0)
            cursor.execute(f"""{query}""", {'date_from': date_from, 'date_to': date_to})
            
            persons = cursor.fetchmany(batch_size)
            while persons:
                batch.send(persons)
                persons = cursor.fetchmany(batch_size)
