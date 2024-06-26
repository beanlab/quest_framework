import asyncio
import uuid
from functools import wraps
from typing import TypeVar, Generic

from .historian import find_historian, SUSPENDED


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
    def __init__(self, *args, **kwargs):
        self._queue = asyncio.Queue(*args, **kwargs)

    async def put(self, value):
        identity = str(uuid.uuid4())
        return identity, await self._queue.put((identity, value))

    async def get(self):
        return await self._queue.get()


T = TypeVar('T')


class _Wrapper:
    pass


def _wrap(resource: T, name, identity, historian) -> T:
    wrapper = _Wrapper()

    for field in dir(resource):
        if field.startswith('_'):
            continue

        if callable(method := getattr(resource, field)):
            # Use default-value kwargs to force value binding instead of late binding

            @wraps(method)
            async def record(*args, _name=name, _identity=identity, _field=field, **kwargs):
                return await historian.handle_internal_event(_name, _identity, _field, *args, **kwargs)

            setattr(wrapper, field, record)

    return wrapper


class ExternalResource(Generic[T]):

    def __init__(self, name, identity, resource: T):
        self._name = name
        self._identity = identity
        self._resource: T = resource
        self._historian = find_historian()

    async def __aenter__(self) -> T:
        await self._historian.register_resource(self._name, self._identity, self._resource)
        return _wrap(self._resource, self._name, self._identity, self._historian)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        suspending = (exc_type == asyncio.CancelledError and exc_val.args and exc_val.args[0] == SUSPENDED)
        await self._historian.delete_resource(self._name, self._identity, suspending=suspending)


def queue(name, identity):
    return ExternalResource(name, identity, Queue())


def event(name, identity):
    return ExternalResource(name, identity, Event())


def state(name, identity, value):
    return ExternalResource(name, identity, State(value))


def identity_queue(name):
    return ExternalResource(name, None, IdentityQueue())
