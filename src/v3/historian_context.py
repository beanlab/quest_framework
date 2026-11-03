import asyncio
import inspect
from contextvars import ContextVar
from functools import wraps

SUSPENDED = '__WORKFLOW_SUSPENDED__'

historian_context = ContextVar('historian', default=None)


class HistorianNotFoundException(Exception):
    pass


def suspendable(func):
    """
    Makes a __aexit__ or __exit__ method suspendable
    With this decorator, the exit method will not be called
      when the workflow is suspending.
    It will only be called when the with context exits for other reasons.
    """
    if inspect.iscoroutinefunction(func):
        @wraps(func)
        async def new_func(self, exc_type, exc_val, exc_tb):
            if exc_type is asyncio.CancelledError and exc_val.args[0] == SUSPENDED:
                return
            await func(self, exc_type, exc_val, exc_tb)
    else:
        @wraps(func)
        def new_func(self, exc_type, exc_val, exc_tb):
            if exc_type is asyncio.CancelledError and exc_val.args[0] == SUSPENDED:
                return
            func(self, exc_type, exc_val, exc_tb)

    return new_func


def find_historian():
    workflow = historian_context.get()
    if workflow is not None:
        return workflow

    from .historian import Historian

    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise HistorianNotFoundException("Historian object not found in event stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Historian)
    return outer_frame.f_locals.get('self')
