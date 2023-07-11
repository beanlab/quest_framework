import inspect
from asyncio import Task
from functools import wraps
from typing import Callable, Coroutine

from .historian import find_historian


def step(func):
    @wraps(func)
    async def new_func(*args, **kwargs):
        return await find_historian().handle_step(func.__name__, func, *args, **kwargs)

    return new_func


def task(func: Callable[..., Coroutine]) -> Callable[..., Task]:
    @wraps(func)
    def new_func(*args, **kwargs):
        return find_historian().start_task(func.__name__, func(*args, **kwargs))

    return new_func
