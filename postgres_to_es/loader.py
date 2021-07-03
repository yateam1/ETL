import json
import logging
from contextlib import contextmanager
from datetime import datetime
from time import sleep
from typing import List
from urllib.parse import urljoin

import psycopg2
import requests
from environs import Env
from functools import wraps
from psycopg2.extras import RealDictCursor
from redis import Redis

from process import ETLMovie, ETLSerial
from storage import State, RedisStorage, REDIS_HOST


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


@contextmanager
def conn_postgres():
    """
    Подключаемся к базе данных и возвращаем курсор
    """
    env = Env()
    env.read_env()
    POSTGRES_HOST = env.str("POSTGRES_HOST", default="localhost")
    POSTGRES_PORT = env.int("POSTGRES_PORT", default=5432)
    POSTGRES_DB = env.str("POSTGRES_DB")
    POSTGRES_USER = env.str("POSTGRES_USER")
    POSTGRES_PASSWORD = env.str("POSTGRES_PASSWORD")

    conn = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB, user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD)
    logging.info(f'The connection to the database {POSTGRES_DB} is established')

    # Create a cursor object
    cur = conn.cursor(cursor_factory=RealDictCursor)

    yield cur

    cur.close()
    conn.close()
    logging.info(f'The connection to the database {POSTGRES_DB} is closed')


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        """
        Готовим bulk-запрос в Elasticsearch
        :return: запрос
        """
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row),
            ])
        return prepared_query

    def load_to_es(self, records: List[dict], index_name: str):
        """
        Отправка запроса в ES и разбор ошибок сохранения данных
        :records: список словарей данных о кинопроизведениях (фильмы и сериалы)
        :index_name: имя индекса
        """
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'
        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logging.error(error_message)
                return

        logging.info(f'Documents have been successfully uploaded to the index {index_name}')

    def remove_from_es(self, index_name: str):
        """
        Сервисный метод очистки индекса от документов
        :index_name: имя индекса
        """
        url = urljoin(self.url, index_name) + '/_delete_by_query'
        str_query = '{"query": {"match_all": {}}}'
        response = requests.post(url=url, data=str_query, headers={'Content-Type': 'application/json'})
        logging.info(f'All documents have been removed from the index {index_name}')


if __name__ == '__main__':

    """
    params:
    :portion: размер "пачки", перегружаемых за один в раз
    :key_name: имя ключа, в котором хранится состояние процесса
    :interval: период в секундах, через который запускаются процессы перегрузки
    """
    portion = 5
    key_name = 'producer'
    interval = 2

    logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    
    foo = 0
    
    while True:
        logging.info(f'Start loop at {datetime.now()}')
        with conn_postgres() as cursor:
            es_loader = ESLoader("http://127.0.0.1:9200/")
            movies = ETLMovie(es_loader=es_loader)
            serials = ETLSerial(es_loader=es_loader)
            # movies.delete_from_es('movies')
            
            storage = RedisStorage(Redis(REDIS_HOST))
            state = State(storage)
            last_created = state.get_state(key_name)
            if last_created:
                last_created = datetime.fromisoformat(last_created)
                logging.info(f'Looking for updates from {last_created}')
            now = datetime.now()
            
            # не забыть удалить
            last_created = None
            
            movies.load_to_es('movies', cursor, last_created, now, portion)
            serials.load_to_es('movies', cursor, last_created, now, portion)
            state.set_state(key_name, now.isoformat())


        logging.info(f'Pause {interval} seconds to the next ETL processes')
        sleep(interval)
        foo += 1
        if foo == 2:
            break
        
        

# @coroutine
# def print_sum():
#     buf = []
#     while value := (yield):
#         buf.append(value)
#         if len(buf) == 10:
#             print(sum(buf))
#             buf.clear()
