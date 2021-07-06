from postgres_to_es.process import ETLGenre


def genre_etl(last_created, now, batch_size):
    etl_genre = ETLGenre(last_created, now, batch_size)
    load_movies = etl_genre.load()
    transform_movies = etl_genre.transform(load_movies)
    etl_genre.extract(transform_movies)
