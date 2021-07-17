import argparse
import logging
from typing import Tuple


def get_args() -> Tuple[int, int, bool]:
    """
    Получаем аргументы командной строки.
    :return batch_size: размер пачки данных, чтобы не перегружать сервисы
    :return interval: интервал в секундах, через который запускается каждый новый цикл
    :return debug: если данный параметр указан, то произойдет одна итерация цикла
    """
    parser = argparse.ArgumentParser(description='Launch ETL process for filmworks database',
                                     epilog='The process is logged in a file log.txt')
    parser.add_argument('--batch_size', type=int, default=5, help='Batch size to load in Elastic Search')
    parser.add_argument('--interval', type=int, default=2, help='Seconds between iteration')
    parser.add_argument('--debug', action='store_true')
    
    args = parser.parse_args()
    if args.batch_size < 1:
        raise ValueError('the parameter batch_size should be greater than 0')
    if args.interval < 1:
        raise ValueError('the parameter interval should be greater than 0')
    
    return args.batch_size, args.interval, args.debug