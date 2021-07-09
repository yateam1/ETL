from elasticsearch.helpers import bulk

from postgres_to_es.config import ELASTICSEARCH_INDEX
from ..loader import es
from .coroutine import coroutine


class ETLGeneral:
    """
    Родительский класс для ETL-процессов
    """
    
    SQL_MOVIE = """
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
    
    SQL_SERIAL = """
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
    
    def __init__(self, date_from, date_to, batch_size):
        """
        Задаем параметры поиска изменений в модели данных и размер пачки данных для выборки
        :param date_from: начало временного интервала поиска изменений в БД
        :param date_to: окончание временного интервала поиска изменений в БД
        :param batch_size: размер пачки данных для ETL-процесса
        """
        self.date_from, self.date_to, self.batch_size = date_from, date_to, batch_size
    
    @coroutine
    def transform(self, batch):
        enrich = lambda row: {
            '_index': ELASTICSEARCH_INDEX,
            '_id': row['id'],
            'title': row['title'],
            'description': row['description'],
            'creation_date': row['creation_date'],
            'rating': row['rating'],
            'type': row['type'],
            'genres': row['genres'],
            'actors': row['actors'],
            'writers': row['writers'],
            'directors': row['directors'],
        }
        
        while True:
            movies = (yield)
            documents = map(enrich, movies)
            batch.send(documents)
    
    @coroutine
    def load(self):
        while True:
            batch = (yield)
            bulk(client=es, actions=batch, chunk_size=self.batch_size, request_timeout=200)

