from elasticsearch.helpers import bulk

from postgres_to_es.etl.coroutine import coroutine
from postgres_to_es.loader import es, batch_size, index


@coroutine
def transform(batch):
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
    while batch := (yield):
        bulk(client=es, actions=batch, chunk_size=batch_size, request_timeout=200)
