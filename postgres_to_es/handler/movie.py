from postgres_to_es.process import ETLMovie


def movie_etl(last_created, now, batch_size):
    etl_movie = ETLMovie(last_created, now, batch_size)
    load_movies = etl_movie.load()
    transform_movies = etl_movie.transform(load_movies)
    etl_movie.extract(transform_movies)
