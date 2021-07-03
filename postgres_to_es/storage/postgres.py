import logging

import psycopg2
from environs import Env
from psycopg2.extras import RealDictCursor

from postgres_to_es.util import backoff


class DBConnect:
    
    
    
    @backoff()
    def conn_postgres(self):
        """
        Подключаемся к базе данных и возвращаем курсор
        """
        env = Env()
        env.read_env()
        POSTGRES_HOST = env.str("POSTGRES_HOST", default="localhost")
        POSTGRES_PORT = env.int("POSTGRES_PORT", default=5432)
        POSTGRES_DB = env.str("POSTGRES_DB")
        POSTGRES_USER = env.str("POSTGRES_USER")
        POSTGRES_PASSWORD = env.str("POSTGRES_PASSWORD")
    
        conn = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB, user=POSTGRES_USER,
                                password=POSTGRES_PASSWORD)
        logging.info(f'The connection to the database {POSTGRES_DB} is established')
    
        # Create a cursor object
        cur = conn.cursor(cursor_factory=RealDictCursor)
    
        yield cur
    
        cur.close()
        conn.close()
        logging.info(f'The connection to the database {POSTGRES_DB} is closed')
