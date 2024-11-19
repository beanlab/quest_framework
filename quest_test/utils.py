import asyncio
import sys
from functools import wraps

from quest import WorkflowManager
from quest.persistence import InMemoryBlobStorage, PersistentHistory
from quest.serializer import NoopSerializer


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


def create_in_memory_workflow_manager(workflows: dict, serializer=None):
    storage = InMemoryBlobStorage()
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    def create_workflow(wtype: str):
        return workflows[wtype]

    if serializer is None:
        serializer = NoopSerializer()

    return WorkflowManager('test', storage, create_history, create_workflow, serializer=serializer)
