import inspect
from asyncio import Task
from functools import wraps
from typing import Callable, Coroutine, TypeVar

from .historian import find_historian


def _get_func_name(func) -> str:
    if hasattr(func, '__name__'):
        return func.__name__
    # Probably callable class
    return func.__class__.__name__


def step(func):
    if not callable(func):
        raise ValueError(f'Step can only wrap functions.')

    func_name = _get_func_name(func)

    if not inspect.iscoroutinefunction(func) and not inspect.iscoroutinefunction(getattr(func, '__call__')):
        raise ValueError(f'Step function must be async: {func_name}')

    if hasattr(func, '_is_quest_step'):
        raise ValueError(f'Step function is already wrapped in @step: {func_name}')

    @wraps(func)
    async def new_func(*args, **kwargs):
        return await find_historian().handle_step(func_name, func, *args, **kwargs)

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

        method = getattr(obj, field)
        if callable(method) and (methods is None or field in methods):
            method = step(method)
        setattr(wrapped, field, method)

    return wrapped
