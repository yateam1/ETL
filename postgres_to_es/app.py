import logging
from datetime import datetime
from time import sleep

import elasticsearch
import psycopg2
import redis

from postgres_to_es.etl import launch_etl
from postgres_to_es.loader import interval, debug


def start():
    """
    Запускаем бесконечный цикл поиска изменений и запуска ETL-процессов.
    :return:
    """
    if debug:
        logging.warning('DEBUG MODE. Only one iteration.')

    while True:
        logging.info(f'Start loop at {datetime.now()}')
        
        try:
            launch_etl()
        except (psycopg2.OperationalError,
                elasticsearch.exceptions.ConnectionError,
                redis.exceptions.ConnectionError):
            pass
        
        if debug:
            break

        logging.info(f'Pause {interval} seconds to the next ETL processes')
        sleep(interval)


if __name__ == '__main__':
    
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger('backoff').addHandler(logging.StreamHandler())

    start()
