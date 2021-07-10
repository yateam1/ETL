import argparse
import logging


def get_args():
    """
    Получаем аргументы командной строки.
    :return batch_size: размер пачки данных, чтобы не перегружать сервисы
    :return interval: интервал в секундах, через который запускается каждый новый цикл
    :return test_pass: если данный параметр указан, то цикл остановится через указанное в нем кол-во повторений
    """
    parser = argparse.ArgumentParser(description='Launch ETL process for filmworks database',
                                     epilog='The process is logged in a file log.txt')
    parser.add_argument('--index', type=str, default='movies', help='Name of index for Elastic Search')
    parser.add_argument('--batch_size', type=int, default=5, help='Batch size to load in Elastic Search')
    parser.add_argument('--interval', type=int, default=2, help='Seconds between iteration')
    parser.add_argument('--debug', action='store_true')# action=argparse.BooleanOptionalAction)
    
    args = parser.parse_args()
    if len(args.index) < 1:
        raise ValueError('the length of parameter index should be greater than 0')
    if args.batch_size < 1:
        raise ValueError('the parameter batch_size should be greater than 0')
    if args.interval < 1:
        raise ValueError('the parameter interval should be greater than 0')
    
    return args.index, args.batch_size, args.interval, args.debug