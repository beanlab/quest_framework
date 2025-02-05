import inspect
from asyncio import Task
from functools import wraps
from typing import Callable, Coroutine, TypeVar

from .historian import find_historian


def step(func):
    if not inspect.iscoroutinefunction(func):
        raise ValueError(f'Step function must be async: {func.__name__}')

    if hasattr(func, '_is_quest_step'):
        raise ValueError(f'Step function is already wrapped in @step: {func.__name__}')

    @wraps(func)
    async def new_func(*args, **kwargs):
        return await find_historian().handle_step(func.__name__, func, *args, **kwargs)

    new_func._is_quest_step = True

    return new_func


def task(func: Callable[..., Coroutine]) -> Callable[..., Task]:
    if not inspect.iscoroutinefunction(func):
        raise ValueError(f'Task function must be async: {func.__name__}')

    @wraps(func)
    def new_func(*args, **kwargs):
        return find_historian().start_task(func, *args, **kwargs)

    return new_func


T = TypeVar('T')


def wrap_steps(obj: T, methods: list[str] = None) -> T:
    class Wrapped:
        pass

    wrapped = Wrapped()
    for field in dir(obj):
        if field.startswith('_'):
            continue

        if callable(method := getattr(obj, field)):
            if methods is not None and method in methods:
                method = step(method)
            setattr(wrapped, field, method)

    return wrapped
