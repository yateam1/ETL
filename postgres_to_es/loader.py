import json
import logging
from contextlib import contextmanager
from typing import List
from urllib.parse import urljoin

import psycopg2
import requests
from environs import Env
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()


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
    logger.info(f'The connection to the database {POSTGRES_DB} is established')

    # Create a cursor object
    cur = conn.cursor(cursor_factory=RealDictCursor)

    yield cur

    cur.close()
    conn.close()
    logger.info(f'The connection to the database {POSTGRES_DB} is closed')


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
                json.dumps(row)
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
                logger.error(error_message)
                return

        logger.info(f'Documents have been successfully uploaded to the index {index_name}')

    def remove_from_es(self, index_name: str):
        """
        Сервисный метод очистки индекса от документов
        :index_name: имя индекса
        """
        url = urljoin(self.url, index_name) + '/_delete_by_query'
        str_query = '{"query": {"match_all": {}}}'
        response = requests.post(url=url, data=str_query, headers={'Content-Type': 'application/json'})
        logger.info(f'All documents have been removed from the index {index_name}')


class ETL:
    def __init__(self, es_loader: ESLoader, es_host='http://127.0.0.1/'):
        self.es_loader = es_loader
        self.es_host = es_host

    def extract_filmworks(self, cursor):
        records = list()

        with open('movies.sql') as sql_file:
            sql = sql_file.read()
        cursor.execute(f"""{sql}""")
        records.extend(cursor.fetchall())

        with open('serials.sql') as sql_file:
            sql = sql_file.read()
        cursor.execute(f"""{sql}""")
        records.extend(cursor.fetchall())

        return records

    def load_to_es(self, index_name: str, cursor):
        """
        Основной метод ETL загрузки документов в индекс
        :index_name: имя индекса
        :cursor: соединения с базой данных
        """

        records = self.extract_filmworks(cursor)
        self.es_loader.load_to_es(records, index_name)

    def delete_from_es(self, index_name: str):
        """
        Дополнительный метод для ETL. Удаляет из индекса все документы
        :index_name: имя индекса
        """
        self.es_loader.remove_from_es(index_name)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')

    es_loader = ESLoader("http://127.0.0.1:9200/")
    etl = ETL(es_loader=es_loader)

    with conn_postgres() as cursor:
        etl.load_to_es('movies', cursor)

    etl.delete_from_es('movies')
