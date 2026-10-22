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


def resource(cls):
    # Add context funcs to register all state and actions on class with historian
    async def __aenter__(self):
        # Find all State instances and @action methods
        historian = find_historian()
        
        for name, member in vars(self):
            if inspect.isfunction(member) and hasattr(member, '__quest_action__'):
                historian.add_action(self._identity, self._name)        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    cls.__aenter__ = __aenter__
    cls.__aexit__ = __aexit__
    
    return cls


def action(_func=None, *, visible=False):
    def decorator(func):
        func.__quest_action__ = True
        func.__quest_action_visible__ = visible
        return func
    
    if _func is None:
        return decorator
    
    return decorator(_func)
        

@resource
class Queue:
    def __init__(self):
        self._queue = asyncio.Queue()

    @action(visible=True)
    async def put(self, item):
        return await self._queue.put(item)

    @action
    async def get(self):
        return await self._queue.get()
