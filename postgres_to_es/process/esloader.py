import json
import logging
from typing import List
from urllib.parse import urljoin

import requests


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