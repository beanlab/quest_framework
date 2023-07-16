import asyncio
import inspect
import uuid
from functools import wraps
from typing import TypeVar, Generic

from src.quest.historian import find_historian


class State:
    def __init__(self, value):
        self._value = value

    def get(self):
        return self._value

    def set(self, value):
        self._value = value

    def value(self):
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
        await self._historian.delete_resource(self._name, self._identity)


def queue(name, identity):
    return ExternalResource(name, identity, asyncio.Queue())


def event(name, identity):
    return ExternalResource(name, identity, asyncio.Event())


def state(name, identity, value):
    return ExternalResource(name, identity, State(value))


def identity_queue(name):
    return ExternalResource(name, None, IdentityQueue())
