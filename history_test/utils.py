import asyncio
import sys
from functools import wraps

from history import Historian, History
from history.persistence import InMemoryBlobStorage, PersistentList
from history.serializer import NoopSerializer


def timeout(delay):
    if 'pydevd' in sys.modules:  # i.e. debug mode
        # Return a no-op decorator
        return lambda func: func

    def decorator(func):
        @wraps(func)
        async def new_func(*args, **kwargs):
            async with asyncio.timeout(delay):
                return await func(*args, **kwargs)

        return new_func

    return decorator


def create_in_memory_historian(workflows: dict, serializer=None):
    storage = InMemoryBlobStorage()
    books = {}

    def create_book(wid: str):
        if wid not in books:
            books[wid] = PersistentList(wid, InMemoryBlobStorage())
        return books[wid]

    def create_workflow(wtype: str):
        return workflows[wtype]

    if serializer is None:
        serializer = NoopSerializer()

    return Historian('test', storage, create_book, create_workflow, serializer=serializer)


def create_test_history(workflow_name, workflow):
    history = History(workflow_name, workflow, [], NoopSerializer())
    return history
