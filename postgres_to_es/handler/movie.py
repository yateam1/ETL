import backoff
import elasticsearch
import psycopg2
import redis

from postgres_to_es.process import ETLMovie


@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def movie_etl(last_created, now, batch_size):
    etl_movie = ETLMovie(last_created, now, batch_size)
    load_movies = etl_movie.load()
    transform_movies = etl_movie.transform(load_movies)
    etl_movie.extract(transform_movies)
