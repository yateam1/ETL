SQL_GET_FROM_PERSON_SERIAL_IDS = """
    SELECT distinct movie_serialpersonrole.serial_id
        FROM content.movie_person
        LEFT JOIN content.movie_serialpersonrole ON movie_person.id=movie_serialpersonrole.person_id
         WHERE movie_person.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_GENRE_SERIAL_IDS = """
    SELECT distinct movie_serial_genres.serial_id
        FROM content.movie_genre
        LEFT JOIN content.movie_serial_genres ON movie_genre.id=movie_serial_genres.genre_id
         WHERE movie_genre.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_SERIALPERSONROLE_SERIAL_IDS = """
    SELECT distinct movie_serialpersonrole.serial_id
    FROM content.movie_serialpersonrole
         WHERE movie_serialpersonrole.modified BETWEEN %(date_from)s AND %(date_to)s
"""


SQL_GET_FROM_SERIAL_SERIAL_IDS = """
    SELECT distinct movie_serial.id AS serial_id
    FROM content.movie_serial
         WHERE movie_serial.modified BETWEEN %(date_from)s AND %(date_to)s
"""


queries = [
    SQL_GET_FROM_PERSON_SERIAL_IDS,
    SQL_GET_FROM_GENRE_SERIAL_IDS,
    SQL_GET_FROM_SERIALPERSONROLE_SERIAL_IDS,
    SQL_GET_FROM_SERIAL_SERIAL_IDS,
]
