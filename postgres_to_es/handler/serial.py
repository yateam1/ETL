import backoff
import elasticsearch
import psycopg2
import redis

from postgres_to_es.process import ETLSerial

@backoff.on_exception(backoff.expo,
                      (elasticsearch.exceptions.ConnectionError,
                       psycopg2.OperationalError,
                       redis.exceptions.ConnectionError),
                      max_time=10)
def serial_etl(last_created, now, batch_size):
    etl_serial = ETLSerial(last_created, now, batch_size)
    load_movies = etl_serial.load()
    transform_movies = etl_serial.transform(load_movies)
    etl_serial.extract(transform_movies)
