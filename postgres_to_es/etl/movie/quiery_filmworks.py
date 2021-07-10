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