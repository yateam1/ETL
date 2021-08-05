import re

from .coroutine import coroutine
from loader import index_movies, index_genres, index_persons


@coroutine
def transform_to_filmworks_index(batch):
    """
    Преобразование выборки из базы данных batch в словарь для заполнения индекса ES
    :param batch:
    :return:
    """
    enrich = lambda row: {
        '_index': index_movies,
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
def transform_to_genres_index(batch):
    """
    Преобразование выборки из базы данных batch в словарь для заполнения индекса ES
    :param batch:
    :return:
    """
    enrich = lambda row: {
        '_index': index_genres,
        '_id': row['id'],
        'name': row['name'],
    }
    
    while genres := (yield):
        documents = map(enrich, genres)
        batch.send(documents)


@coroutine
def transform_to_persons_index(batch):
    """
    Преобразование выборки из базы данных batch в словарь для заполнения индекса ES
    :param batch:
    :return:
    """
    enrich = lambda row: {
        '_index': index_persons,
        '_id': row['id'],
        'first_name': row['first_name'],
        'last_name': row['last_name'],
        'birth_date': row['birth_date'],
        'actor': re.split(',\s*', re.sub(r'[{}]', '', row['actor'])) if isinstance(row['actor'], str) else [],
        'director': re.split(',\s*', re.sub(r'[{}]', '', row['director'])) if isinstance(row['director'], str) else [],
        'writer': re.split(',\s*', re.sub(r'[{}]', '', row['screenwriter'])) if isinstance(row['screenwriter'], str) else [],
        'producer': re.split(',\s*', re.sub(r'[{}]', '', row['producer'])) if isinstance(row['producer'], str) else [],
    }
    
    while persons := (yield):
        documents = map(enrich, persons)
        batch.send(documents)

