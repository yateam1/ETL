import json
import logging
from pprint import pprint

import requests

from typing import List
from urllib.parse import urljoin
import psycopg2
from psycopg2.extras import RealDictCursor
from environs import Env

logger = logging.getLogger()

class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        """
        Подготавливает bulk-запрос в Elasticsearch
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

        logger.info('Документы успешно загружены в индекс')


class ETL:
    def __init__(self, es_loader: ESLoader, host='http://127.0.0.1/'):
        self.es_loader = es_loader
        self.host = host

    def get_movies(self, api_urls):
        filmworks = list()
        for api_url in api_urls:
            url = urljoin(self.host, api_url)
            print(url)
            response = requests.get(url)
            print(json.dumps(response.content))
            filmworks.extend(list(response.content))

    def load_to_es(self, index_name: str, records):
        self.es_loader.load_to_es(records, index_name)

    def clear(self, index_name: str):
        '''
        Дополнительный метод для ETL.
        Удаляет из индекса все документы
        '''

        logger.info('На всякий случай очищаем индекс от документов')

        url = urljoin(self.es_loader.url, index_name) + '/_delete_by_query'
        str_query = '{"query": {"match_all": {}}}'

        response = requests.post(
            url=url,
            data=str_query,
            headers={'Content-Type': 'application/json'}
        )


if __name__ == '__main__':
    env = Env()
    env.read_env()
    POSTGRES_HOST = env.str("POSTGRES_HOST", default="localhost")
    POSTGRES_PORT = env.int("POSTGRES_PORT", default=5432)
    POSTGRES_DB = env.str("POSTGRES_DB")
    POSTGRES_USER = env.str("POSTGRES_USER")
    POSTGRES_PASSWORD = env.str("POSTGRES_PASSWORD")

    conn = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB, user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD)
    logger.info('The connection to the database is established')
    # Create a cursor object
    cur = conn.cursor(cursor_factory=RealDictCursor)

    with open('movies.sql') as sql_file:
        sql = sql_file.read()
    # print(sql)


    cur.execute(f"{sql}")
    query_results = cur.fetchall()
    pprint(query_results)

    es_loader = ESLoader("http://127.0.0.1:9200/")
    etl = ETL(es_loader=es_loader)
    # etl.load_to_es('movies', query_results)
    etl.clear('movies')

    cur.close()
    conn.close()


