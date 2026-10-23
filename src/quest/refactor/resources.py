import asyncio
import inspect
from functools import wraps

from .core.historian import find_historian


class State:
    def __init__(self, identity, name, value):
        self._identity = identity
        self._name = name
        self._initial_value = value
        self._historian = find_historian()

    async def __aenter__(self):
        await self.set(self._initial_value)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._historian.remove_state(self._identity, self._name)

    async def get(self):
        # Called to publish value in workflow surface
        return await self._historian.get_state(self._identity, self._name)

    async def set(self, value):
        # Indicates to historian that the value has changed and needs to be republished
        return await self._historian.set_state(self._identity, self._name, value)


class Action:
    def __init__(self, identity, name, func):
        self._identity = identity
        self._name = name
        self._func = func
        self._historian = find_historian()

    async def __aenter__(self):
        await self._historian.add_action(self._identity, self._name, self._func)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._historian.remove_action(self._identity, self._name)


_quest_action_name = '__quest_action_name__'
_quest_action_external = '__quest_action_external__'


def resource(_cls=None, *, _scope_name=None):
    # Add context methods to register all state and actions on class with historian

    # The class should define self._identity in __init__ if it wants to support
    # identity-specific resources

    # The class should define self._name in __init__ if it wants to support
    # custom runtime resource names

    def decorate(cls):

        def add(func):
            setattr(cls, func.__name__, func)
            return func

        @add
        def _get_name(self, name, member):
            # Scope name is: self._name, or decorated scope_name, or class name
            scope_name = getattr(self, '_name', _scope_name or cls.__name__)
            name = (getattr(member, _quest_action_name) or name)
            return f'{scope_name}.{name}'

        @add
        def _get_identity(self):
            return getattr(self, '_identity', None)

        @add
        def _iter_actions(self):
            for name, member in vars(self).items():
                if inspect.isfunction(member) and hasattr(member, _quest_action_name):
                    yield self._get_name(name, member), member

        @add
        def _iter_state(self):
            for name, member in vars(self).items():
                if isinstance(member, State):
                    yield name, member

        # TODO iter_state
        # Define use-case for state and actions on resource
        # Figure out how to synchronize internal actions
        # Any method that performs a side effect (mutates state)
        #  must be invoked via historian.invoke_action.
        # This includes internal actions as well as external actions
        # So all actions must be registered, AND the action body must route through historian

        # So, iterate all

        def decorate_action(func):
            @wraps(func)
            async def new_action_func(self, *args, **kwargs):
                historian = find_historian()
                return await historian.invoke_action(self._get_identity(), self._get_name(), *args, **kwargs)
            return new_action_func

        for name, member in vars(cls).items():
            if inspect.isfunction(member) and hasattr(member, _quest_action_name):
                setattr(cls, name, decorate_action(member))

        @add
        async def __aenter__(self):
            # Find all State instances and @action methods
            historian = find_historian()

            for name, member in self._iter_actions():
                await historian.add_action(self._get_identity(), name, member)

            return self

        @add
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            historian = find_historian()

            for name, member in self._iter_actions():
                await historian.remove_action(self._get_identity(), name)

        return cls

    if _cls is None:
        return decorate

    return decorate(_cls)


def action(_func=None, *, name=None, external=False):
    def decorator(func):
        setattr(func, _quest_action_name, name)
        setattr(func, _quest_action_external, external)
        return func

    if _func is None:
        return decorator

    return decorator(_func)


@resource
class Queue:
    def __init__(self):
        self._queue = asyncio.Queue()

    @action
    async def put(self, item):
        return await self._queue.put(item)

    async def get(self):
        return await self._queue.get()
