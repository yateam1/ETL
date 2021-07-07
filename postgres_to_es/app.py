import argparse
import logging
from datetime import datetime
from time import sleep

import elasticsearch
import psycopg2
import redis
from pydantic import BaseModel, validator, ValidationError

from handler import movie_etl, serial_etl, genre_etl, person_etl, moviepersonrole_etl, serialpersonrole_etl


class Loop(BaseModel):
    """
    Класс бесконечного цикла поиска изменений и запуска ETL-процессов.
    :batch_size: размер пачки данных, чтобы не перегружать сервисы
    :interval: интервал в секундах, через который запускается каждый новый цикл
    :test_pass: если данный параметр указан, то цикл остановится через указанное в нем кол-во повторений
    """
    batch_size: int
    interval: int
    test_pass: int
    
    @validator('batch_size')
    def validate_age(cls, value):
        if int(value) < 1:
            raise ValueError('the parameter batch_size should be greater than 0')
        return str(value)
    
    @validator('interval')
    def validate_interval(cls, value):
        if int(value) < 1:
            raise ValueError('the parameter interval should be greater than 0')
        return str(value)
    
    @validator('test_pass')
    def validate_test_pass(cls, value):
        if int(value) < 0:
            raise ValueError('the parameter test_pass should be greater than or equal 0')
        return str(value)
    
    def start(self):
        """
        Запускаем бесконечный цикл поиска изменений и запуска ETL-процессов.
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


def get_args():
    """
    Получаем аргументы командной строки.
    :return batch_size: размер "пачки", перегружаемых за один раз.
    :return interval: период в секундах, через который запускаются процессы перегрузки.
    :return test_pass: число итераций скрипта. Использовать для процесса отдладки.
    """
    parser = argparse.ArgumentParser(description='ETL process parameters')
    parser.add_argument("--batch_size", type=int, default=5, help="Batch size to load in Elastic Search")
    parser.add_argument("--interval", type=int, default=2, help="Seconds between iteration")
    parser.add_argument("--test_pass", type=int, default=0, help="Number of test passes")
    
    args = parser.parse_args()
    return args.batch_size, args.interval, args.test_pass


if __name__ == '__main__':
   
    logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger('backoff').addHandler(logging.StreamHandler())

    batch_size, interval, test_pass = get_args()  # Получаем аргументы командной строки

    try:
        loop = Loop(batch_size=batch_size, interval=interval, test_pass=test_pass)
    except ValidationError as e:
        logging.info(f'Exception {e.json()}')
    else:
        loop.start()