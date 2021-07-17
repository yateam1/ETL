from elasticsearch.helpers import bulk

from .coroutine import coroutine
from loader import es, batch_size


@coroutine
def load():
    """
    Заполнение индекса Elastic Search
    :return:
    """
    while batch := (yield):
        bulk(client=es, actions=batch, chunk_size=batch_size, request_timeout=200)