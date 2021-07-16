from elasticsearch.helpers import bulk

from .coroutine import coroutine
from loader import es, batch_size, index


@coroutine
def transform(batch):
    """
    Преобразование выборки из базы данных batch в словарь для заполнения индекса ES
    :param batch:
    :return:
    """
    enrich = lambda row: {
        '_index': index,
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
    
    while movies := (yield):
        documents = map(enrich, movies)
        batch.send(documents)


@coroutine
def load():
    """
    Заполнение индекса Elastic Search
    :return:
    """
    while batch := (yield):
        bulk(client=es, actions=batch, chunk_size=batch_size, request_timeout=200)
