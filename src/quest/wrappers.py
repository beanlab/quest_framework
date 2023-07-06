import asyncio
import inspect
from asyncio import Task
from functools import wraps
from typing import Callable, Coroutine, Any

from .workflow import Workflow, WorkflowSuspended, workflow_context


class WorkflowNotFoundException(Exception):
    pass


def _find_workflow() -> Workflow:
    if (workflow := workflow_context.get()) is not None:
        return workflow

    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise WorkflowNotFoundException("Workflow object not found in event stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Workflow)
    return outer_frame.f_locals.get('self')


class State:
    def __init__(self, name: str, initial_value, identity):
        self.name = name
        self.initial_value = initial_value
        self.identity = identity

    async def set(self, value):
        # Make invoke state also act like an event
        return await _find_workflow().set_state(self.name, self.identity, value)

    async def get(self):
        return await _find_workflow().set_state(self.name, self.identity)

    async def __aenter__(self):
        await _find_workflow().create_state(self.name, self.initial_value, self.identity)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type != WorkflowSuspended:
            await _find_workflow().remove_state(self.name, self.identity)


def state(name, initial_value=None, identity=None) -> State:
    return State(name, initial_value, identity)


class Queue:
    def __init__(self, name, identity, identity_queue=False):
        self.name: str = name
        self.identity: str | None = identity
        self._identity_queue = identity_queue
        self._closed = False

    async def check(self):
        return await _find_workflow().check_queue(self.name, self.identity)

    async def pop(self):
        index, _, (identity, value) = await _find_workflow().pop_queues([(self.name, self.identity)])
        if self._identity_queue:
            return identity, value
        else:
            return value

    async def push(self, value):
        return await _find_workflow().push_queue(self.name, value, self.identity)

    async def close(self):
        if not self._closed:
            await _find_workflow().remove_queue(self.name, self.identity)
            self._closed = True

    async def __aenter__(self):
        await _find_workflow().create_queue(self.name, self.identity)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type != WorkflowSuspended:
            await self.close()


def queue(name, identity=None) -> Queue:
    return Queue(name, identity)


def identity_queue(name, identity=None) -> Queue:
    """Create an identity queue
    Each call to `pop()` will return the identity, value pair
    """
    return Queue(name, identity, identity_queue=True)


def step(func):
    @wraps(func)
    async def new_func(*args, **kwargs):
        return await _find_workflow().handle_step(func.__name__, func, *args, **kwargs)

    return new_func


async def pop_any(*queues: Queue):
    index, _, (ident, value) = await _find_workflow().pop_queues([(q.name, q.identity) for q in queues])
    return queues[index], ident, value


def task(func: Callable[..., Coroutine]) -> Callable[..., Task]:
    @wraps(func)
    def new_func(*args, **kwargs):
        return _find_workflow().create_task(func.__name__, func(*args, **kwargs))

    return new_func