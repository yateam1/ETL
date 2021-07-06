import argparse
import logging
from datetime import datetime
from time import sleep

from redis import Redis

from handler import movie_etl, serial_etl, genre_etl, person_etl, moviepersonrole_etl, serialpersonrole_etl
from state import State, RedisStorage
from config import REDIS_HOST, STATE_KEY


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
        
        # Получаем из REDIS дату и время последней успешной итерации. Фиксируем текущее время. Изменения в БД
        # будем искать в заданный временной промежуток.
        storage = RedisStorage(Redis(REDIS_HOST))
        state = State(storage)
        last_created = state.get_state(STATE_KEY)
        if last_created:
            last_created = datetime.fromisoformat(last_created)
        now = datetime.now()
        logging.info(f'Looking for updates from {last_created} to {now}')
        
        last_created = None  # FIXME удалить, используется для тестирования
        
        # Запускаем ETL-процессы
        # movie_etl(last_created, now, batch_size)
        # serial_etl(last_created, now, batch_size)
        # genre_etl(last_created, now, batch_size)
        # person_etl(last_created, now, batch_size)
        moviepersonrole_etl(last_created, now, batch_size)
        serialpersonrole_etl(last_created, now, batch_size)
        
        # TODO Если процессы завершились успешно, обновляем дату в REDIS
        state.set_state(STATE_KEY, now.isoformat())
        
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