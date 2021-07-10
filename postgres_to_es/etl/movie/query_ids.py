SQL_GET_FROM_PERSON_MOVIE_IDS = """
    SELECT distinct movie_moviepersonrole.movie_id
        FROM content.movie_person
        LEFT JOIN content.movie_moviepersonrole ON movie_person.id=movie_moviepersonrole.person_id
         WHERE movie_person.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_GENRE_MOVIE_IDS = """
    SELECT distinct movie_movie_genres.movie_id
        FROM content.movie_genre
        LEFT JOIN content.movie_movie_genres ON movie_genre.id=movie_movie_genres.genre_id
         WHERE movie_genre.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_MOVIEPERSONROLE_MOVIE_IDS = """
    SELECT distinct movie_moviepersonrole.movie_id
        FROM content.movie_moviepersonrole
         WHERE movie_moviepersonrole.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_MOVIE_MOVIE_IDS = """
    SELECT distinct movie_movie.id AS movie_id
        FROM content.movie_movie
         WHERE movie_movie.modified BETWEEN %(date_from)s AND %(date_to)s
"""


queries = [
    SQL_GET_FROM_PERSON_MOVIE_IDS,
    SQL_GET_FROM_GENRE_MOVIE_IDS,
    SQL_GET_FROM_MOVIEPERSONROLE_MOVIE_IDS,
    SQL_GET_FROM_MOVIE_MOVIE_IDS,
]
