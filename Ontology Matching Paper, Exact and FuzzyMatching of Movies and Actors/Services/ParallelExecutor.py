import concurrent.futures
import itertools
from typing import Callable, Iterable


class ParallelExecutor:

    @staticmethod
    def chunk(n, iterable):
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, n))
            if not chunk:
                return
            yield chunk

    @staticmethod
    def execute(func: Callable, items: Iterable):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            return executor.map(func, items)
