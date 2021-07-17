SQL_GET_FROM_GENRE_SERIAL_IDS = """
    SELECT id, name
        FROM content.movie_genre
         WHERE movie_genre.modified BETWEEN %(date_from)s AND %(date_to)s
"""

query = SQL_GET_FROM_GENRE_SERIAL_IDS