import argparse
import logging
from datetime import datetime
from time import sleep

import elasticsearch
import psycopg2
import redis

from handler import movie_etl, serial_etl, genre_etl, person_etl, moviepersonrole_etl, serialpersonrole_etl


def start(batch_size: int, interval: int, test_pass: int):
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
            movie_etl(batch_size)
            serial_etl(batch_size)
            genre_etl(batch_size)
            person_etl(batch_size)
            moviepersonrole_etl(batch_size)
            serialpersonrole_etl(batch_size)
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


def get_args():
    """
    Получаем аргументы командной строки.
    :return batch_size: размер пачки данных, чтобы не перегружать сервисы
    :return interval: интервал в секундах, через который запускается каждый новый цикл
    :return test_pass: если данный параметр указан, то цикл остановится через указанное в нем кол-во повторений
    """
    parser = argparse.ArgumentParser(description='ETL process parameters')
    parser.add_argument("--batch_size", type=int, default=5, help="Batch size to load in Elastic Search")
    parser.add_argument("--interval", type=int, default=2, help="Seconds between iteration")
    parser.add_argument("--test_pass", type=int, default=0, help="Number of test passes")
    
    args = parser.parse_args()
    if args.batch_size < 1:
        raise ValueError('the parameter batch_size should be greater than 0')
    if args.interval < 1:
        raise ValueError('the parameter interval should be greater than 0')
    if args.test_pass < 0:
        raise ValueError('the parameter test_pass should be greater than or equal 0')
    
    return args.batch_size, args.interval, args.test_pass


if __name__ == '__main__':
    
    logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger('backoff').addHandler(logging.StreamHandler())

    try:
        batch_size, interval, test_pass = get_args()  # Получаем аргументы командной строки
    except ValueError as e:
        logging.error(f'Exception {e}')
    else:
        start(batch_size, interval, test_pass)
