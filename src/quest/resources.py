import asyncio
import uuid
from functools import wraps
from typing import TypeVar, Generic

from .historian import Historian, find_historian, SUSPENDED, _get_type_name

"""
Methods on these resource classes are always async

This is because they are all wrapped by the historian
so that all interactions with them are recorded and can be replayed.
"""


def external(func):
    func._external = True
    return func


class Queue:
    _rtype = 'queue'

    def __init__(self):
        self._queue = asyncio.Queue()

    @external
    async def put(self, item):
        # This is often an external event that needs to be recorded
        return await self._queue.put(item)

    async def get(self):
        # This is always an internal event
        return await self._queue.get()

    async def empty(self):
        return self._queue.empty()

    def _get_value(self):
        return None


class State:
    _rtype = 'state'

    def __init__(self, value):
        self._value = value

    @external
    async def get(self):
        return self._value

    async def set(self, value):
        self._value = value

    def _get_value(self):
        return self._value


class IdentityQueue:
    """Put and Get return and identity + the value"""

    _rtype = 'idqueue'

    def __init__(self, *args, **kwargs):
        self._queue = asyncio.Queue(*args, **kwargs)

    @external
    async def put(self, value):
        identity = str(uuid.uuid4())
        return identity, await self._queue.put((identity, value))

    async def get(self):
        return await self._queue.get()

    def _get_value(self):
        return None


T = TypeVar('T')


class _Wrapper:
    pass


def _wrap_methods_as_historian_events(resource: T, rtype: str, name: str, identity: str | None, historian: 'Historian',
                                      internal=True) -> T:
    wrapper = _Wrapper()

    historian_action = historian.handle_internal_event if internal else historian.record_external_event

    for field in dir(resource):
        if field.startswith('_'):
            continue

        if callable(method := getattr(resource, field)):
            # Use default-value kwargs to force value binding instead of late binding
            @wraps(method)
            async def record(*args, _rtype=rtype, _name=name, _identity=identity, _field=field, **kwargs):
                return await historian_action(_rtype, _name, _identity, _field, *args, **kwargs)

            setattr(wrapper, field, record)

    return wrapper


class InternalResource(Generic[T]):
    """Internal resources are used inside the workflow context"""

    # noinspection PyProtectedMember
    def __init__(self, name, identity, resource: T):
        self._name = name
        self._identity = identity
        self._rtype = resource._rtype
        self._resource: T = resource
        self._historian = find_historian()

    async def __aenter__(self) -> T:
        await self._historian.register_resource(self._name, self._identity, self._resource)
        return _wrap_methods_as_historian_events(
            self._resource, self._rtype, self._name, self._identity, self._historian)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        suspending = (exc_type == asyncio.CancelledError and exc_val.args and exc_val.args[0] == SUSPENDED)
        await self._historian.delete_resource(self._rtype, self._name, self._identity, suspending=suspending)


def queue(name, identity):
    return InternalResource(name, identity, Queue())


def state(name, identity, value):
    return InternalResource(name, identity, State(value))


def identity_queue(name):
    return InternalResource(name, None, IdentityQueue())


class MultiQueue:
    def __init__(self, name: str, players: dict[str, str], single_response: bool = False):
        self.queues: dict[str, InternalResource[Queue]] = {ident: queue(name, ident) for ident in players}
        self.single_response = single_response
        self.task_to_ident: dict[asyncio.Task, str] = {}
        self.ident_to_task: dict[str, asyncio.Task] = {}

        # Hold unwrapped Queue objects after __aenter__
        self.active_queues: dict[str, Queue] = {}

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
        for ident, wrapper in self.queues.items():
            # Unwrap queue object
            queue_obj = await wrapper.__aenter__()
            self.active_queues[ident] = queue_obj
            self._add_task(ident, queue_obj)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cancel all pending tasks - context exits
        for task in self.task_to_ident:
            task.cancel()
        # Exit all queues properly
        for ident, wrapper in self.queues.items():
            await wrapper.__aexit__(exc_type, exc_val, exc_tb)

    async def remove(self, ident: str):
        # Stop listening to this identity queue
        task = self.ident_to_task.pop(ident, None)

        if task is not None:
            self.task_to_ident.pop(task)
            task.cancel()

        # Call __aexit__ on the corresponding queue wrapper
        wrapper = self.queues.pop(ident, None)
        if wrapper:
            await wrapper.__aexit__(None, None, None)

        self.active_queues.pop(ident, None)

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
                        q = self.active_queues.get(ident)
                        if q:
                            self._add_task(ident, q)

                except asyncio.CancelledError:
                    continue

# class _ResourceWrapper:
#     def __init__(self, name: str, identity: str | None, historian: 'Historian', resource_class):
#         self._name = name
#         self._identity = identity
#         self._historian = historian
#         self._resource_class = resource_class
#
#     # TODO: Is it fine that we essentially don't do anything if `field` is an attribute or private?
#     def __getattr__(self, field):
#         if field.startswith('_'):
#             return
#         if not callable(getattr(self._resource_class, field)):
#             return
#
#         async def wrapper(*args, _name=self._name, _identity=self._identity, **kwargs):
#             return await self._historian.record_external_event(_name, _identity, field, *args, **kwargs)
#
#         return wrapper
#
#
# # noinspection PyTypeChecker
# def wrap_as_queue(name: str, identity: str | None, historian: Historian) -> Queue:
#     return _ResourceWrapper(name, identity, historian, Queue)
#
#
# # noinspection PyTypeChecker
# def wrap_as_state(name: str, identity: str | None, historian: Historian) -> State:
#     return _ResourceWrapper(name, identity, historian, State)
#
#
# # noinspection PyTypeChecker
# def wrap_as_identity_queue(name: str, identity: str | None, historian: Historian) -> IdentityQueue:
#     return _ResourceWrapper(name, identity, historian, IdentityQueue)
