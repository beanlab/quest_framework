import inspect
from functools import wraps

from .core.historian import find_historian
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

    def __init__(self, identity, name, value, external=False):
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
    def __init__(self, identity, name, func, external=False):
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


def resource(_cls=None, *, _scope_name=None):
    # Add context methods to register all state and actions on class with historian

    # The class should define self._identity in __init__ if it wants to support
    # identity-specific resources

    # The class should define self._name in __init__ if it wants to support
    # custom runtime resource names

    # Anything that can be mutated and observed by multiple tasks needs to be tracked
    #  by Historian. This includes State set_state, get_state and any @action methods.

    # Actions
    # At __enter__, any @action needs to be registered with Historian
    # Later, when someone calls the @action method on the class,
    #  that method should route the call to Historian.
    #  So the body of @action method is completely replaced during __enter__.

    # State
    # At __enter__, any State needs to be registered with Historian
    # Later, when someone calls set_state or get_state,
    #  that method should route the call to Historian.
    #  So the body of these methods need to be replaced.

    # IMPORTANT: What happens if we try to re-enter a resource? Is that allowed?
    # To support this, in __exit__ we'd have to restore the original method bodies.

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
            # Identity is: self._identity, or public
            return getattr(self, '_identity', None)

        @add
        def _iter_actions(self):
            for name, method in inspect.getmembers(self, inspect.ismethod):
                func = method.__func__
                if hasattr(func, _quest_action_name):
                    yield name, self._get_name(name, func), method

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

        def decorate_action(method, historian, identity, name):
            @wraps(method)
            async def routed(*args, **kwargs):
                return await historian.invoke_action(identity, name, *args, **kwargs)

            return routed

        @add
        async def __aenter__(self):
            # Find all State instances and @action methods
            historian = find_historian()

            for attr_name, action_name, method in self._iter_actions():
                # Register original method with Historian
                await historian.add_action(self._get_identity(), action_name, method)
                # Replace method with route-through-Historian version
                setattr(self, attr_name, decorate_action(method, historian, self._get_identity(), action_name))

            for name, member in self._iter_state():
                # Register state with Historian
                await member.__aenter__()

            return self

        @add
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            historian = find_historian()

            for name, member in self._iter_actions():
                await historian.remove_action(self._get_identity(), name)
                # TODO - revert member body to original form?

            for name, member in self._iter_state():
                await member.__aexit__(exc_type, exc_val, exc_tb)

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
    async def new_func(*args, **kwargs):
        return await find_historian().start_task(
            func,
            args,
            kwargs
        )

    return new_func
