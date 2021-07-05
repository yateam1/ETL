import argparse
import logging
from datetime import datetime
from time import sleep

from redis import Redis

from postgres_to_es.process import ETLGenre, ETLPerson, ETLMoviePersonRole, ETLSerialPersonRole
from process import ETLMovie, ETLSerial, ESLoader
from state import State, RedisStorage
from postgres_to_es.config import REDIS_HOST, REDIS_DICT_KEY


def get_args():
    """
    Получаем аргументы командной строки.
    :return portion: размер "пачки", перегружаемых за один в раз.
    :return interval: период в секундах, через который запускаются процессы перегрузки.
    :return debug: число итераций скрипта. Использовать для процесса отдладки.
    """
    parser = argparse.ArgumentParser(description='ETL process parameters')
    parser.add_argument("--portion", type=int, default=5, help="Batch size to load in Elastic Search")
    parser.add_argument("--interval", type=int, default=2, help="Seconds between iteration")
    parser.add_argument("--debug", type=int, default=0, help="Count of iteration for testing")
    
    args = parser.parse_args()
    return args.portion, args.interval, args.debug


if __name__ == '__main__':
    
    portion, interval, debug = get_args()  # Получаем аргументы командной строки.
    
    logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    counter = 0
    
    
    while True:
        logging.info(f'Start loop at {datetime.now()}')
        
        # Получаем из REDIS дату и время последней успешной итерации. Фиксируем текущее время. Изменения в БД
        # будем искать в заданный временной промежуток.
        storage = RedisStorage(Redis(REDIS_HOST))
        state = State(storage)
        last_created = state.get_state(REDIS_DICT_KEY)
        if last_created:
            last_created = datetime.fromisoformat(last_created)
        now = datetime.now()
        logging.info(f'Looking for updates from {last_created} to {now}')
        
        last_created = None  # FIXME удалить, используется для тестирования
        
        es_loader = ESLoader("http://127.0.0.1:9200/")
        
        # Определяем ETL-процессы
        movies = ETLMovie(es_loader=es_loader)
        serials = ETLSerial(es_loader=es_loader)
        genres = ETLGenre(es_loader=es_loader)
        persons = ETLPerson(es_loader=es_loader)
        movie_roles = ETLMoviePersonRole(es_loader=es_loader)
        serial_roles = ETLSerialPersonRole(es_loader=es_loader)
        
        # Запускаем ETL-процессы
        movies.load_to_es('movies', last_created, now, portion)
        serials.load_to_es('movies', last_created, now, portion)
        # TODO genres.load_to_es('movies', last_created, now, portion)
        # TODO persons.load_to_es('movies', last_created, now, portion)
        # TODO movie_roles.load_to_es('movies', last_created, now, portion)
        # TODO movie_roles.load_to_es('movies', last_created, now, portion)
        
        # TODO Если процессы завершились успешно, обновляем дату в REDIS
        # state.set_state(REDIS_DICT_KEY, now.isoformat())
        
        logging.info(f'Pause {interval} seconds to the next ETL processes')
        sleep(interval)
        
        # Если мы в режиме отладки, считаем количество итераций
        if debug:
            counter += 1
        if debug and counter == debug:
            break
