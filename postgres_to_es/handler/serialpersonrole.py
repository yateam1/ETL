from postgres_to_es.process import ETLSerialPersonRole


def serialpersonrole_etl(last_created, now, batch_size):
    etl_serialpersonrole = ETLSerialPersonRole(last_created, now, batch_size)
    load_movies = etl_serialpersonrole.load()
    transform_movies = etl_serialpersonrole.transform(load_movies)
    serials = etl_serialpersonrole.extract_serial(transform_movies)
    etl_serialpersonrole.extract_serial_id(serials)