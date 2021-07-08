from elasticsearch.helpers import bulk

from .coroutine import coroutine
from postgres_to_es.config import es, ELASTICSEARCH_INDEX



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
        