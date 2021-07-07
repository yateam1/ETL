from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from postgres_to_es.config import dsn
from .general import ETLGeneral


class ETLMovie(ETLGeneral):
    SQL_MOVIE = """SELECT movie_movie.id, movie_movie.title, movie_movie.description,
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
    WHERE movie_movie.modified BETWEEN %(date_from)s AND %(date_to)s
    GROUP BY content.movie_movie.id
    """
    
    def __init__(self, date_from, date_to, batch_size):
        """
        Задаем параметры поиска изменений в модели данных и размер пачки данных для выборки
        :param date_from: начало временного интервала поиска изменений в БД
        :param date_to: окончание временного интервала поиска изменений в БД
        :param batch_size: размер пачки данных для ETL-процесса
        """
        super().__init__(date_from, date_to, batch_size)
    
    def extract(self, batch):
        """
        Основной метод извлечения записей из базы данных
        :param batch: пачка извлеченных из БД данных
        :return: порция данных из БД, которые были изменены в заданный временной интервал
        """
        with psycopg2.connect(dsn=dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
    
                if not self.date_from:
                    self.date_from = datetime(1900, 1, 1, 0, 0, 0, 0)
                cursor.execute(f"""{self.SQL_MOVIE}""", {'date_from': self.date_from, 'date_to': self.date_to})
            
                movies = cursor.fetchmany(self.batch_size)
                while movies:
                    batch.send(movies)
                    movies = cursor.fetchmany(self.batch_size)
