import argparse
import logging
from datetime import datetime
from time import sleep

import elasticsearch
import psycopg2
import redis

from handler import movie_etl, serial_etl, genre_etl, person_etl, moviepersonrole_etl, serialpersonrole_etl


def get_args():
    """
    Получаем аргументы командной строки.
    :return batch_size: размер "пачки", перегружаемых за один в раз.
    :return interval: период в секундах, через который запускаются процессы перегрузки.
    :return test_pass: число итераций скрипта. Использовать для процесса отдладки.
    """
    parser = argparse.ArgumentParser(description='ETL process parameters')
    parser.add_argument("--batch_size", type=int, default=5, help="Batch size to load in Elastic Search")
    parser.add_argument("--interval", type=int, default=2, help="Seconds between iteration")
    parser.add_argument("--test_pass", type=int, default=0, help="Number of test passes")
    
    args = parser.parse_args()
    return args.batch_size, args.interval, args.test_pass


def loop(batch_size, interval, test_pass):
    counter = 0
    while True:
        logging.info(f'Start loop at {datetime.now()}')

        try:
            # movie_etl(batch_size)
            # serial_etl(batch_size)
            # genre_etl(batch_size)
            # person_etl(batch_size)
            moviepersonrole_etl(batch_size)
            serialpersonrole_etl(batch_size)
        except psycopg2.OperationalError:
            pass
        except elasticsearch.exceptions.ConnectionError:
            pass
        except redis.exceptions.ConnectionError:
            pass
        else:
            pass
        
        logging.info(f'Pause {interval} seconds to the next ETL processes')
        sleep(interval)
        
        # Если мы в режиме отладки, считаем количество итераций
        if test_pass:
            counter += 1
        if test_pass and counter == test_pass:
            break


if __name__ == '__main__':
    
    batch_size, interval, test_pass = get_args()  # Получаем аргументы командной строки.
    
    logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger('backoff').addHandler(logging.StreamHandler())
    
    loop(batch_size, interval, test_pass)