"""
Запросы к БД
"""


SQL_GET_MOVIES_BY_IDS = """
    SELECT movie_movie.id, movie_movie.title, movie_movie.description,
           movie_movie.creation_date, movie_movie.rating, 'movie' AS type,
           ARRAY_AGG(DISTINCT movie_genre.name ) AS genres, ARRAY_AGG(DISTINCT CONCAT(movie_person.last_name,
           CONCAT(' ', movie_person.first_name)) ) FILTER (WHERE movie_moviepersonrole.role = 0) AS actors,
           ARRAY_AGG(DISTINCT CONCAT(movie_person.last_name, CONCAT(' ', movie_person.first_name)) )
           FILTER (WHERE movie_moviepersonrole.role = 1) AS directors,
           ARRAY_AGG(DISTINCT CONCAT(movie_person.last_name, CONCAT(' ', movie_person.first_name)) )
           FILTER (WHERE movie_moviepersonrole.role = 2) AS writers
FROM content.movie_movie
LEFT OUTER JOIN content.movie_movie_genres ON (content.movie_movie.id = content.movie_movie_genres.movie_id)
LEFT OUTER JOIN content.movie_genre ON (content.movie_movie_genres.genre_id = content.movie_genre.id)
LEFT OUTER JOIN content.movie_moviepersonrole ON (content.movie_movie.id = content.movie_moviepersonrole.movie_id)
LEFT OUTER JOIN content.movie_person ON (content.movie_moviepersonrole.person_id = content.movie_person.id)
WHERE movie_movie.id = ANY(%(movie_ids)s::uuid[])
GROUP BY content.movie_movie.id
"""


SQL_GET_SERIALS_BY_IDS = """
    SELECT movie_serial.id, movie_serial.title, movie_serial.description,
            movie_serial.creation_date, movie_serial.rating, 'serial' AS type,
            ARRAY_AGG(DISTINCT movie_genre.name ) AS genres, ARRAY_AGG(DISTINCT CONCAT(movie_person.last_name,
            CONCAT(' ', movie_person.first_name)) ) FILTER (WHERE movie_serialpersonrole.role = 0) AS actors,
            ARRAY_AGG(DISTINCT CONCAT(movie_person.last_name,
            CONCAT(' ', movie_person.first_name)) ) FILTER (WHERE movie_serialpersonrole.role = 1) AS directors,
            ARRAY_AGG(DISTINCT CONCAT(movie_person.last_name, CONCAT(' ', movie_person.first_name)) )
            FILTER (WHERE movie_serialpersonrole.role = 2) AS writers
    FROM content.movie_serial
    LEFT OUTER JOIN content.movie_serial_genres ON (content.movie_serial.id = content.movie_serial_genres.serial_id)
    LEFT OUTER JOIN content.movie_genre ON (content.movie_serial_genres.genre_id = content.movie_genre.id)
    LEFT OUTER JOIN content.movie_serialpersonrole ON (content.movie_serial.id = content.movie_serialpersonrole.serial_id)
    LEFT OUTER JOIN content.movie_person ON (content.movie_serialpersonrole.person_id = content.movie_person.id)
    WHERE movie_serial.id = ANY(%(serial_ids)s::uuid[])
    GROUP BY content.movie_serial.id
"""


SQL_GET_FROM_PERSON_MOVIE_IDS = """
    SELECT distinct movie_moviepersonrole.movie_id
        FROM content.movie_person
        LEFT JOIN content.movie_moviepersonrole ON movie_person.id=movie_moviepersonrole.person_id
         WHERE movie_person.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_PERSON_SERIAL_IDS = """
    SELECT distinct movie_serialpersonrole.serial_id
        FROM content.movie_person
        LEFT JOIN content.movie_serialpersonrole ON movie_person.id=movie_serialpersonrole.person_id
         WHERE movie_person.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_GENRE_MOVIE_IDS = """
    SELECT distinct movie_movie_genres.movie_id
        FROM content.movie_genre
        LEFT JOIN content.movie_movie_genres ON movie_genre.id=movie_movie_genres.genre_id
         WHERE movie_genre.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_GENRE_SERIAL_IDS = """
    SELECT distinct movie_serial_genres.serial_id
        FROM content.movie_genre
        LEFT JOIN content.movie_serial_genres ON movie_genre.id=movie_serial_genres.genre_id
         WHERE movie_genre.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_MOVIEPERSONROLE_MOVIE_IDS = """
    SELECT distinct movie_moviepersonrole.movie_id
        FROM content.movie_moviepersonrole
         WHERE movie_moviepersonrole.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_SERIALPERSONROLE_SERIAL_IDS = """
    SELECT distinct movie_serialpersonrole.serial_id
    FROM content.movie_serialpersonrole
         WHERE movie_serialpersonrole.modified BETWEEN %(date_from)s AND %(date_to)s
"""


# SQL_GET_FROM_MOVIE_MOVIE_IDS = """SELECT * FROM"""
# SQL_GET_FROM_SERIAL_SERIAL_IDS = """SELECT * FROM"""


queries = [
    SQL_GET_FROM_PERSON_MOVIE_IDS,
    SQL_GET_FROM_PERSON_SERIAL_IDS,
    SQL_GET_FROM_GENRE_MOVIE_IDS,
    SQL_GET_FROM_GENRE_SERIAL_IDS
    SQL_GET_FROM_MOVIEPERSONROLE_MOVIE_IDS,
    SQL_GET_FROM_SERIALPERSONROLE_SERIAL_IDS,
    # SQL_GET_FROM_MOVIE_MOVIE_IDS,
    # SQL_GET_FROM_SERIAL_SERIAL_IDS,
]