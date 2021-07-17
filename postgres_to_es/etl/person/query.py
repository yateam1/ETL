SQL_GET_PERSONS = """
    SELECT movie_person.id, movie_person.first_name, movie_person.last_name, movie_person.birth_date,
        (count(distinct movie_moviepersonrole.*) FILTER (WHERE movie_moviepersonrole.role = 0) +
             count(distinct movie_serialpersonrole.*) FILTER (WHERE movie_serialpersonrole.role = 0)) as actor,
        (count(distinct movie_moviepersonrole.*) FILTER (WHERE movie_moviepersonrole.role = 1) +
             count(distinct movie_serialpersonrole.*) FILTER (WHERE movie_serialpersonrole.role = 1)) as director,
        (count(distinct movie_moviepersonrole.*) FILTER (WHERE movie_moviepersonrole.role = 2) +
             count(distinct movie_serialpersonrole.*) FILTER (WHERE movie_serialpersonrole.role = 2)) as screenwriter,
        (count(distinct movie_moviepersonrole.*) FILTER (WHERE movie_moviepersonrole.role = 3) +
             count(distinct movie_serialpersonrole.*) FILTER (WHERE movie_serialpersonrole.role = 3)) as producer
    FROM content.movie_person
    LEFT OUTER JOIN content.movie_moviepersonrole ON movie_moviepersonrole.person_id=movie_person.id
    LEFT OUTER JOIN content.movie_serialpersonrole ON movie_serialpersonrole.person_id=movie_person.id
    WHERE movie_person.modified BETWEEN %(date_from)s AND %(date_to)s
       OR movie_moviepersonrole.modified BETWEEN %(date_from)s AND %(date_to)s
       OR movie_serialpersonrole.modified BETWEEN %(date_from)s AND %(date_to)s
    GROUP BY movie_person.id
"""

query = SQL_GET_PERSONS