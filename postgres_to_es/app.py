import logging
from datetime import datetime
from time import sleep

import elasticsearch
import psycopg2
import redis

from postgres_to_es.etl import launch_etl
from postgres_to_es.loader import interval, test_pass


def start():
    """
    Запускаем бесконечный цикл поиска изменений и запуска ETL-процессов.
    :batch_size: размер пачки данных, чтобы не перегружать сервисы
    :interval: интервал в секундах, через который запускается каждый новый цикл
    :test_pass: если данный параметр указан, то цикл остановится через указанное в нем кол-во повторений
    :return:
    """
    counter = 0
    while True:
        logging.info(f'Start loop at {datetime.now()}')
        
        try:
            launch_etl()
        except (psycopg2.OperationalError,
                elasticsearch.exceptions.ConnectionError,
                redis.exceptions.ConnectionError):
            pass
        
        logging.info(f'Pause {interval} seconds to the next ETL processes')
        sleep(interval)
        
        # Если мы в режиме отладки, считаем количество итераций
        if test_pass:
            counter += 1
        if test_pass and counter == test_pass:
            break


if __name__ == '__main__':
    
    logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger('backoff').addHandler(logging.StreamHandler())

    start()
