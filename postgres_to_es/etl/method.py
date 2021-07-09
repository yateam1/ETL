from datetime import datetime

import psycopg2
from elasticsearch.helpers import bulk
from psycopg2.extras import RealDictCursor

from .coroutine import coroutine
from postgres_to_es.config import es, ELASTICSEARCH_INDEX, dsn


def extract_filmwork_ids(batch, query: str, batch_size: int, date_from: datetime, date_to: datetime):
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
def extract_filmworks_by_ids(batch, query: str, batch_size: int):
    """
    Основной метод извлечения записей из базы данных
    :param batch: пачка извлеченных из БД данных
    :return: порция данных из БД, которые были изменены в заданный временной интервал
    """
    with psycopg2.connect(dsn=dsn) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            while True:
                movie_ids = (yield)
                movie_ids = [movie['movie_id'] for movie in movie_ids]
                cursor.execute(f"""{query}""", {'movie_ids': movie_ids})
                movies = cursor.fetchmany(batch_size)
                while movies:
                    batch.send(movies)
                    movies = cursor.fetchmany(batch_size)


@coroutine
def transform(batch):
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
def load(batch_size: int):
    while True:
        batch = (yield)
        bulk(client=es, actions=batch, chunk_size=batch_size, request_timeout=200)
