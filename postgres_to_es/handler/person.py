import backoff
import elasticsearch
import psycopg2
import redis

from postgres_to_es.process import ETLPerson

@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def person_etl(last_created, now, batch_size):
    etl_person = ETLPerson(last_created, now, batch_size)
    load_movies = etl_person.load()
    transform_movies = etl_person.transform(load_movies)

    movies = etl_person.extract_movie(transform_movies)
    etl_person.extract_movie_id(movies)

    serilas = etl_person.extract_serial(transform_movies)
    etl_person.extract_serial_id(serilas)
