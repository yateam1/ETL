from postgres_to_es.process import ETLMoviePersonRole


def moviepersonrole_etl(last_created, now, batch_size):
    etl_moviepersonrole = ETLMoviePersonRole(last_created, now, batch_size)
    load_movies = etl_moviepersonrole.load()
    transform_movies = etl_moviepersonrole.transform(load_movies)
    etl_moviepersonrole.extract(transform_movies)
