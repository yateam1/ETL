from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from .coroutine import coroutine
from .general import ETLGeneral
from ..config import dsn


class ETLSerialPersonRole(ETLGeneral):
    SQL_SERIAL_ID = """
        SELECT distinct movie_serialpersonrole.serial_id
        FROM content.movie_serialpersonrole
             WHERE movie_serialpersonrole.modified BETWEEN %(date_from)s AND %(date_to)s
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
        super().__init__(date_from, date_to, batch_size)
    
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
