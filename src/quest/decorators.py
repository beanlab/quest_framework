import inspect
from functools import wraps
from types import MethodType
from typing import Self

from .core.historian import find_historian
from .core.resources import IDENTITY
from .core.utils import get_function_name


# Two things need to be tracked for actions:
# - Is the action recorded in the ledger, and thus replay sequence enforced between separate tasks?
#   - Anytime two separate tasks mutate the same state (e.g. touch a queue), the actions they take must be tracked
#   - This includes both internal and external tasks
# - Is the action visible in the workflow surface, allowing external actors to invoke the action?

# @action - the method is recorded and sequenced by the ledger; visible=True means the action can be called through the
# workflow API surface.

# @resource - adds the needed __aenter__, __aexit__ methods to register @action methods and State objects with historian


class State:
    # State is a special class.
    # We need a first-class way to monitor state as a resource, so we know when that state changes
    #  and can publish a resource update.

    # All members of this type in a @resource will be registered as state with the historian.
    # You can also use State directly in a workflow.

    def __init__(self, identity, name, value, external=True):
        self._identity = identity
        self._name = name
        self._initial_value = value
        self._external = external
        self._historian = find_historian()

    async def __aenter__(self):
        await self.set(self._initial_value)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._historian.remove_state(self._identity, self._name)

    async def get(self):
        # Called to publish value in workflow surface
        return await self._historian.get_state(self._identity, self._name)

    async def set(self, value):
        # Indicates to historian that the value has changed and needs to be republished
        return await self._historian.set_state(self._identity, self._name, value, self._external)


class Action:
    def __init__(self, identity, name, func, external=True):
        self._identity = identity
        self._name = name
        self._func = func
        self._external = external
        self._historian = find_historian()

    async def __aenter__(self):
        await self._historian.add_action(self._identity, self._name, self._func, self._external)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._historian.remove_action(self._identity, self._name)


_quest_action_name = '__quest_action_name__'
_quest_action_external = '__quest_action_external__'


def _decorate_action(method, historian, identity, name):
    @wraps(method)
    async def routed(*args, **kwargs):
        return await historian.invoke_action(identity, name, *args, **kwargs)

    return routed


class Resource:
    """
    Convenience base class for custom resources.
    
    This class provides the __aenter__, __aexit__ methods needed
    to register @action and State members with the Historian.
    
    Subclasses must supply `identity` and `name` to super().__init__(...).
    """

    def __init__(self, identity: IDENTITY, name: str):
        self._identity = identity
        self._scope_name = name

        self._original_actions: dict[str, tuple[str, MethodType]] = {}

    def _get_name(self, member_name, member):
        # _quest_action_name might be None, in which case we use the member name
        name = getattr(member, _quest_action_name) or member_name
        return f'{self._scope_name}.{name}'

    def _get_identity(self):
        return self._identity

    def _iter_actions(self):
        for name, method in inspect.getmembers(self, inspect.ismethod):
            func = method.__func__
            if hasattr(func, _quest_action_name):
                is_external = getattr(func, _quest_action_external)
                yield name, self._get_name(name, func), method, is_external

    def _iter_state(self):
        for name, member in vars(self).items():
            if isinstance(member, State):
                yield name, member

    async def __aenter__(self) -> Self:
        # Find all State instances and @action methods
        historian = find_historian()

        for attr_name, action_name, method, is_external in self._iter_actions():
            # Register original method with Historian
            await historian.add_action(self._get_identity(), action_name, method, is_external)
            # Replace method with route-through-Historian version
            setattr(self, attr_name, _decorate_action(method, historian, self._get_identity(), action_name))
            self._original_actions[attr_name] = (action_name, method)

        for name, member in self._iter_state():
            # Register state with Historian
            await member.__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        historian = find_historian()

        for attr_name, (action_name, method) in self._original_actions.items():
            await historian.remove_action(self._get_identity(), action_name)
            setattr(self, attr_name, method)

        for name, member in self._iter_state():
            await member.__aexit__(exc_type, exc_val, exc_tb)


def action(_func=None, *, name=None, external=True):
    def decorator(func):
        setattr(func, _quest_action_name, name)
        setattr(func, _quest_action_external, external)
        return func

    if _func is None:
        return decorator

    return decorator(_func)


def step(func):
    @wraps(func)
    async def new_func(*args, **kwargs):
        return await find_historian().run_step(
            get_function_name(func),
            func,
            *args,
            **kwargs
        )

    return new_func


def task(func):
    # TODO - typehint modification to return a Task of result?
    @wraps(func)
    def new_func(*args, **kwargs):
        return find_historian().start_task(
            func,
            args,
            kwargs
        )

    return new_func


def when():
    return find_historian().when()
