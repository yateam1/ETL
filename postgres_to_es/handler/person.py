from postgres_to_es.process import ETLPerson


def person_etl(last_created, now, batch_size):
    etl_person = ETLPerson(last_created, now, batch_size)
    load_movies = etl_person.load()
    transform_movies = etl_person.transform(load_movies)

    movies = etl_person.extract_movie(transform_movies)
    etl_person.extract_movie_id(movies)

    serilas = etl_person.extract_serial(transform_movies)
    etl_person.extract_serial_id(serilas)
