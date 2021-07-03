import logging
from datetime import datetime
from time import sleep

from redis import Redis

from process import ETLMovie, ETLSerial, ESLoader
from state import State, RedisStorage, REDIS_HOST

if __name__ == '__main__':

    """
    params:
    :portion: размер "пачки", перегружаемых за один в раз
    :key_name: имя ключа, в котором хранится состояние процесса
    :interval: период в секундах, через который запускаются процессы перегрузки
    """
    portion = 5
    key_name = 'producer'
    interval = 2

    logging.basicConfig(level=logging.INFO, filename='log.txt', filemode='w',
                        format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    
    foo = 0
    
    while True:
        logging.info(f'Start loop at {datetime.now()}')
        # with conn_postgres() as cursor:
        es_loader = ESLoader("http://127.0.0.1:9200/")
        movies = ETLMovie(es_loader=es_loader)
        serials = ETLSerial(es_loader=es_loader)
        # movies.delete_from_es('movies')
        
        storage = RedisStorage(Redis(REDIS_HOST))
        state = State(storage)
        last_created = state.get_state(key_name)
        if last_created:
            last_created = datetime.fromisoformat(last_created)
        now = datetime.now()
        
        # не забыть удалить
        last_created = None

        logging.info(f'Looking for updates from {last_created}')
        movies.load_to_es('movies', last_created, now, portion)
        serials.load_to_es('movies', last_created, now, portion)
        state.set_state(key_name, now.isoformat())

        logging.info(f'Pause {interval} seconds to the next ETL processes')
        sleep(interval)
        foo += 1
        if foo == 2:
            break
        
        

# @coroutine
# def print_sum():
#     buf = []
#     while value := (yield):
#         buf.append(value)
#         if len(buf) == 10:
#             print(sum(buf))
#             buf.clear()
