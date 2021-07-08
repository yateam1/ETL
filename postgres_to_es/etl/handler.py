import backoff
import elasticsearch
import psycopg2
import redis

from .method import load, transform, extract_filmworks_by_ids, extract_filmwork_ids
from .query import queries


@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def launch_etl(batch_size: int):
    load_filmworks = load(batch_size)
    transform_filmworks = transform(load_filmworks)

    for query in queries:
        extract_filmworks = extract_filmworks_by_ids(transform_filmworks)
        extract_filmwork_ids(extract_filmworks, query)
