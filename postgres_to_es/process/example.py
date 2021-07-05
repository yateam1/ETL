import random
from time import sleep

from functools import wraps

def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner

import random
from time import sleep

def generate_numbers(target):
    for _ in range(3):
        value = random.randint(1, 11)
        target.send(value)
        sleep(0.1)


@coroutine
def double_odd(target):
    while True:
        value = (yield)
        if value % 2 != 0:
            value = value ** 2
        target.send(value)


@coroutine
def halve_even(target):
    while True:
        value = (yield)
        if value % 2 == 0:
            value = value // 2
        target.send(value)


@coroutine
def print_sum():
    buf = []
    while True:
        value = (yield)
        buf.append(value)
        if len(buf) == 10:
            print(sum(buf))
            buf.clear()
        


printer_sink = print_sum()
even_filter = halve_even(printer_sink)
odd_filter = double_odd(even_filter)
generate_numbers(odd_filter)