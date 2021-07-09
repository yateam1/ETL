import argparse


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