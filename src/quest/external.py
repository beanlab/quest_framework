import asyncio
import uuid
from typing import TypeVar, Generic

from .historian import find_historian, SUSPENDED, wrap_methods_as_historian_events


#
# Note: The following Queue and Event classes
# are intended to be used as type hints
#


class Queue:
    def __init__(self):
        self._queue = asyncio.Queue()

    async def put(self, item):
        return await self._queue.put(item)

    async def get(self):
        return await self._queue.get()

    async def empty(self):
        return self._queue.empty()


class Event:
    # Why this class?
    # When wrapped for historians, all methods become async
    # So this class gives async versions of the methods
    # so IDEs and typehints indicate the actual behavior
    def __init__(self):
        self._event = asyncio.Event()

    async def wait(self):
        await self._event.wait()

    async def set(self):
        self._event.set()

    async def clear(self):
        self._event.clear()

    async def is_set(self):
        return self._event.is_set()


class State:
    def __init__(self, value):
        self._value = value

    async def get(self):
        return self._value

    async def set(self, value):
        self._value = value

    def value(self):
        # TODO - why do we need this?
        return self._value


class IdentityQueue:
    """Put and Get return and identity + the value"""

    def __init__(self, *args, **kwargs):
        self._queue = asyncio.Queue(*args, **kwargs)

    async def put(self, value):
        identity = str(uuid.uuid4())
        return identity, await self._queue.put((identity, value))

    async def get(self):
        return await self._queue.get()


T = TypeVar('T')


class InternalResource(Generic[T]):
    """Internal resources are used inside the workflow context"""

    def __init__(self, name, identity, resource: T):
        self._name = name
        self._identity = identity
        self._resource: T = resource
        self._historian = find_historian()

    async def __aenter__(self) -> T:
        await self._historian.register_resource(self._name, self._identity, self._resource)
        return wrap_methods_as_historian_events(self._resource, self._name, self._identity, self._historian)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        suspending = (exc_type == asyncio.CancelledError and exc_val.args and exc_val.args[0] == SUSPENDED)
        await self._historian.delete_resource(self._name, self._identity, suspending=suspending)


class MultiQueue:
    def __init__(self, name: str, players: dict[str, str], single_response: bool = False):
        self.queues: dict[str, Queue] = {ident: queue(name, ident) for ident in players}
        self.single_response = single_response
        self.task_to_ident: dict[asyncio.Task, str] = {}
        self.ident_to_task: dict[str, asyncio.Task] = {}

    def _add_task(self, ident: str, q: Queue):
        historian = find_historian()
        task = historian.start_task(
            q.get,
            name=f"mq-get-{ident}"
        )

        self.task_to_ident[task] = ident
        self.ident_to_task[ident] = task

    async def __aenter__(self):
        # Listen on all queues -> create a task for each queue.get()
        for ident, q in self.queues.items():
            self._add_task(ident, q)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cancel all pending tasks - context exits
        for task in self.task_to_ident:
            task.cancel()

    def remove(self, ident: str):
        # Stop listening to this identity queue
        if ident not in self.ident_to_task:
            raise KeyError(f"Identity '{ident}' does not exist in MultiQueue.")

        task = self.ident_to_task.pop(ident)
        self.task_to_ident.pop(task)
        task.cancel()

    async def __aiter__(self):
        while self.task_to_ident:
            # Wait until any of the current task is done
            done, _ = await asyncio.wait(self.task_to_ident.keys(), return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                ident = self.task_to_ident.pop(task)
                # Stop listening to this identity
                del self.ident_to_task[ident]

                try:
                    result = await task
                    yield ident, result

                    # Start listening again
                    if not self.single_response:
                        self._add_task(ident, self.queues[ident])

                except asyncio.CancelledError:
                    continue


def queue(name, identity):
    return InternalResource(name, identity, Queue())


def event(name, identity):
    return InternalResource(name, identity, Event())


def state(name, identity, value):
    return InternalResource(name, identity, State(value))


def identity_queue(name):
    return InternalResource(name, None, IdentityQueue())
