import time
from functools import wraps


def backoff(logger, start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    def func_wrapper(function):
        @wraps(function)
        def inner(*args, **kwargs):
            timeout = start_sleep_time
            while timeout < border_sleep_time:
                try:
                    value = function(*args, **kwargs)
                    # return value if value else None
                    next(value)
                except:
                    logger.error(f'Повторная попытка через {timeout} секунд')
                    time.sleep(timeout)
                    timeout *= 2 ** factor
        
        return inner
    return func_wrapper
