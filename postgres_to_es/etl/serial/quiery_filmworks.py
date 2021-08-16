SQL_GET_SERIALS_BY_IDS = """
    SELECT movie_serial.id, movie_serial.title, movie_serial.description,
            movie_serial.creation_date, movie_serial.rating, movie_serial.by_subscription, 
            movie_serial.age_classification, 'serial' AS type,
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