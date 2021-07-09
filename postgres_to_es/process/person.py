from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from .coroutine import coroutine
from .general import ETLGeneral
from ..loader import dsn


class ETLPerson(ETLGeneral):
    SQL_MOVIE_ID = """
        SELECT distinct movie_moviepersonrole.movie_id
            FROM content.movie_person
            LEFT JOIN content.movie_moviepersonrole ON movie_person.id=movie_moviepersonrole.person_id
             WHERE movie_person.modified BETWEEN %(date_from)s AND %(date_to)s
    """
    
    SQL_SERIAL_ID = """
        SELECT distinct movie_serialpersonrole.serial_id
            FROM content.movie_person
            LEFT JOIN content.movie_serialpersonrole ON movie_person.id=movie_serialpersonrole.person_id
             WHERE movie_person.modified BETWEEN %(date_from)s AND %(date_to)s
    """
    
    def __init__(self, date_from, date_to, batch_size):
        """
        Задаем параметры поиска изменений в модели данных и размер пачки данных для выборки
        :param date_from: начало временного интервала поиска изменений в БД
        :param date_to: окончание временного интервала поиска изменений в БД
        :param batch_size: размер пачки данных для ETL-процесса
        """
        super().__init__(date_from, date_to, batch_size)
    
    def extract_movie_id(self, batch):
        """
        Основной метод извлечения записей из базы данных
        :param batch: пачка извлеченных из БД данных
        :return: порция данных из БД, которые были изменены в заданный временной интервал
        """
        with psycopg2.connect(dsn=dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                if not self.date_from:
                    self.date_from = datetime(1900, 1, 1, 0, 0, 0, 0)
                cursor.execute(f"""{self.SQL_MOVIE_ID}""", {'date_from': self.date_from, 'date_to': self.date_to})
                
                movie_ids = cursor.fetchmany(self.batch_size)
                while movie_ids:
                    batch.send(movie_ids)
                    movie_ids = cursor.fetchmany(self.batch_size)
    
    @coroutine
    def extract_movie(self, batch):
        """
        Основной метод извлечения записей из базы данных
        :param batch: пачка извлеченных из БД данных
        :return: порция данных из БД, которые были изменены в заданный временной интервал
        """
        with psycopg2.connect(dsn=dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                while True:
                    movie_ids = (yield)
                    movie_ids = [movie['movie_id'] for movie in movie_ids]
                    cursor.execute(f"""{self.SQL_MOVIE}""", {'movie_ids': movie_ids})
                    movies = cursor.fetchmany(self.batch_size)
                    while movies:
                        batch.send(movies)
                        movies = cursor.fetchmany(self.batch_size)
    
    def extract_serial_id(self, batch):
        """
        Основной метод извлечения записей из базы данных
        :param batch: пачка извлеченных из БД данных
        :return: порция данных из БД, которые были изменены в заданный временной интервал
        """
        with psycopg2.connect(dsn=dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                if not self.date_from:
                    self.date_from = datetime(1900, 1, 1, 0, 0, 0, 0)
                cursor.execute(f"""{self.SQL_SERIAL_ID}""", {'date_from': self.date_from, 'date_to': self.date_to})
                
                serial_ids = cursor.fetchmany(self.batch_size)
                while serial_ids:
                    batch.send(serial_ids)
                    serial_ids = cursor.fetchmany(self.batch_size)
    
    @coroutine
    def extract_serial(self, batch):
        """
        Основной метод извлечения записей из базы данных
        :param batch: пачка извлеченных из БД данных
        :return: порция данных из БД, которые были изменены в заданный временной интервал
        """
        with psycopg2.connect(dsn=dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                while True:
                    serial_ids = (yield)
                    serial_ids = [serial['serial_id'] for serial in serial_ids]
                    cursor.execute(f"""{self.SQL_SERIAL}""", {'serial_ids': serial_ids})
                    serials = cursor.fetchmany(self.batch_size)
                    while serials:
                        batch.send(serials)
                        serials = cursor.fetchmany(self.batch_size)
