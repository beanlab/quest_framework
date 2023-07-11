# Service that provides queues, state
# Comprehends identity
import asyncio
import inspect
import uuid
from functools import wraps

from src.quest.workflow import Workflow, workflow_context
from src.quest.wrappers import WorkflowNotFoundException


# What object holds these things?
# Does it care which workflow is using them?
# Do you have to use them from a workflow?

# asyncio has a queue - we should try to use that.
# We want a way of registering the queue (or any other resource)
#  so it is externally accessible.

def _find_workflow() -> Workflow:
    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise WorkflowNotFoundException("Workflow object not found in call stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Workflow)
    return outer_frame.f_locals.get('self')


def workflow_step(func):
    @wraps(func)
    def new_func(*args, **kwargs):
        if (workflow := workflow_context.get()) is None:
            # External caller
            return func(*args, **kwargs)
        else:
            # Workflow caller
            return workflow.handle_step(func.__name__, func, *args, **kwargs)

    return new_func


class State:
    def __init__(self, name, value):
        self._name = name
        self._value = value

    @workflow_step
    def get(self):
        return self._value

    @workflow_step
    def set(self, value):
        self._value = value


def _wrap(resource):
    for name in dir(resource):
        if callable(method := getattr(resource, name)):
            setattr(resource, workflow_step(method))
    return resource


# noinspection PyPep8Naming
def Queue(*args, **kwargs):
    return _wrap(asyncio.Queue(*args, **kwargs))


class IdentityQueue:
    def __init__(self, *args, **kwargs):
        self._queue = asyncio.Queue(*args, **kwargs)

    @workflow_step
    async def put(self, value):
        identity = str(uuid.uuid4())
        return identity, await self._queue.put((identity, value))

    @workflow_step
    async def get(self):
        return await self._queue.get()


class Scope:
    def __init__(self, provider, name, identity, resource):
        self.provider = provider
        self.name = name
        self.identity = identity
        self.resource = resource

    def __enter__(self):
        # noinspection PyProtectedMember
        self.provider._register(self.name, self.identity, self.resource)
        return self.resource

    def __exit__(self, exc_type, exc_val, exc_tb):
        # noinspection PyProtectedMember
        self.provider._remove(self.name, self.identity)


def _create_id(name: str, identity: str) -> str:
    return f'{name}|{identity}' if identity is not None else name


class ExternalProvider:
    def __init__(self):
        self.resources = {}

    def _register(self, name, identity, resource):
        key = _create_id(name, identity)
        self.resources[key] = resource

    def _remove(self, name, identity):
        del self.resources[_create_id(name, identity)]

    def discover(self, identity):
        # Return all resources belong to None or the identity
        # Prefer resources specific to the identity
        result = {key: resource for key, resource in self.resources.items() if len(key.split('|')) == 1}
        if identity is not None:
            result.update({
                key[:-len(identity) - 1]: resource
                for key, resource in self.resources.items()
                if key.split('|')[-1] == identity
            })
        return result

    def get(self, name, identity):
        key = _create_id(name, identity)
        if key not in self.resources:
            raise KeyError(f'No resource found for {name}/{identity}')
        return self.resources[key]

    def queue(self, name, identity, *args, **kwargs):
        return Scope(self, name, identity, asyncio.Queue(*args, **kwargs))

    def identity_queue(self, name, *args, **kwargs):
        return Scope(self, name, None, IdentityQueue(*args, **kwargs))

    def state(self, name, identity, *args, **kwargs):
        return Scope(self, name, identity, State(*args, **kwargs))
