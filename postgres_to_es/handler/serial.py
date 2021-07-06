from postgres_to_es.process import ETLSerial


def serial_etl(last_created, now, batch_size):
    etl_serial = ETLSerial(last_created, now, batch_size)
    load_movies = etl_serial.load()
    transform_movies = etl_serial.transform(load_movies)
    etl_serial.extract(transform_movies)
