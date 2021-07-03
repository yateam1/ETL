import datetime


class ETLFilmwork:

    def __init__(self, es_loader, es_host):
        self.es_loader = es_loader
        self.es_host = es_host

    def load_to_es(self, index_name: str, cursor, date_from, date_to, portion):
        """
        Основной метод ETL загрузки документов в индекс
        :index_name: имя индекса
        :cursor: соединения с базой данных
        """

        records = self.extract_filmworks(cursor, date_from, date_to, portion)
        if records:
            self.es_loader.load_to_es(records, index_name)

    def delete_from_es(self, index_name: str):
        """
        Дополнительный метод для ETL. Удаляет из индекса все документы
        :index_name: имя индекса
        """
        self.es_loader.remove_from_es(index_name)


class ETLMovie(ETLFilmwork):
    SQL = """SELECT movie_movie.id, movie_movie.title, movie_movie.description,
                    to_char(movie_movie.creation_date, 'YYYY') AS creation_year, movie_movie.rating, 'movie' AS type,
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

    def __init__(self, es_loader, es_host='http://127.0.0.1/'):
        super().__init__(es_loader, es_host)
    
    def extract_filmworks(self, cursor, date_from, date_to, portion):
        records = list()

        if not date_from:
            date_from = datetime.datetime(1900, 1, 1, 0, 0, 0, 0)
        cursor.execute(f"""{self.SQL}""", {'date_from': date_from, 'date_to': date_to})
        records.extend(cursor.fetchmany(portion))
        return records


class ETLSerial(ETLFilmwork):
    SQL = """SELECT movie_serial.id, movie_serial.title, movie_serial.description,
                to_char(movie_serial.creation_date, 'YYYY') AS creation_year, movie_serial.rating, 'serial' AS type,
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
    WHERE movie_serial.modified BETWEEN %(date_from)s AND %(date_to)s
    GROUP BY content.movie_serial.id
    """

    def __init__(self, es_loader, es_host='http://127.0.0.1/'):
        super().__init__(es_loader, es_host)
    
    def extract_filmworks(self, cursor, date_from, date_to, portion):
        records = list()

        if not date_from:
            date_from = datetime.datetime(1900, 1, 1, 0, 0, 0, 0)
        cursor.execute(f"""{self.SQL}""", {'date_from': date_from, 'date_to': date_to})

        records.extend(cursor.fetchall())

        return records
