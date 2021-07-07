import backoff
import elasticsearch
import psycopg2
import redis

from postgres_to_es.process import ETLGenre

@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def genre_etl(last_created, now, batch_size):
    etl_genre = ETLGenre(last_created, now, batch_size)
    load_movies = etl_genre.load()
    transform_movies = etl_genre.transform(load_movies)

    movies = etl_genre.extract_movie(transform_movies)
    etl_genre.extract_movie_id(movies)

    serilas = etl_genre.extract_serial(transform_movies)
    etl_genre.extract_serial_id(serilas)
