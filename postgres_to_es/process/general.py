from elasticsearch.helpers import bulk

from postgres_to_es.config import ELASTICSEARCH_INDEX, es
from .coroutine import coroutine


class ETLGeneral:
    
    def __init__(self, date_from, date_to, batch_size):
        """
        Задаем параметры поиска изменений в модели данных и размер пачки данных для выборки
        :param date_from: начало временного интервала поиска изменений в БД
        :param date_to: окончание временного интервала поиска изменений в БД
        :param batch_size: размер пачки данных для ETL-процесса
        """
        self.date_from, self.date_to, self.batch_size = date_from, date_to, batch_size
    
    @coroutine
    def transform(self, batch):
        enrich = lambda row: {
            '_index': ELASTICSEARCH_INDEX,
            '_id': row['id'],
            'title': row['title'],
            'description': row['description'],
            'creation_date': row['creation_date'],
            'rating': row['rating'],
            'type': row['type'],
            'genres': row['genres'],
            'actors': row['actors'],
            'writers': row['writers'],
            'directors': row['directors'],
        }
        
        while True:
            movies = (yield)
            documents = map(enrich, movies)
            batch.send(documents)
    
    @coroutine
    def load(self):
        while True:
            batch = (yield)
            bulk(client=es, actions=batch, chunk_size=self.batch_size, request_timeout=200)

