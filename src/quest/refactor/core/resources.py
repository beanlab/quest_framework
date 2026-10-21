import asyncio
from typing import Any

# Two things need to be tracked for actions:
# - is the action recorded in the ledger, and thus replay sequence enforced between separate tasks
# - is the action visible in the workflow surface, allowing external actors to invoke the action

# @action - the method is recorded and sequenced by the ledger; visible=True means the action can be called through the
# workflow API surface.

# @resource - adds the needed __aenter__, __aexit__ methods to register @action methods and State objects with historian


class State:
    def __init__(self, identity, name, value):
        self._identity = identity
        self._name = name
        self._initial_value = value
        self._historian = find_historian()

    async def __aenter__(self):
        await self.set(self._initial_value)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._historian.delete_state(self._identity, self._name)

    async def get(self):
        # Called to publish value in workflow surface
        return await self._historian.get_state(self._identity, self._name)

    async def set(self, value):
        # Indicates to historian that the value has changed and needs to be republished
        return await self._historian.set_state(self._identity, self._name, value)


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

#
# These ^^^ belong a level up, referencing the historian
#


class ResourceManager:
    def __init__(self):
        self._state: dict[str | None, Any] = {None: {}}
        self._actions: dict[str | None, Any] = {None: {}}

    def get_resources(self):
        """Return a snapshot of state and actions"""

    def set_state(self, identity: str, name: str, value):
        if identity not in self._state:
            self._state[identity] = {}
        self._state[identity][name] = value

    def get_state(self, identity, name):
        # Return the specified value
        return self._state[identity][name]

    def delete_state(self, identity: str, name: str):
        del self._state[identity][name]

    def add_action(self, identity: str, name: str, func):
        if identity not in self._actions:
            self._actions[identity] = {}
        self._actions[identity][name] = func

    def remove_action(self, identity: str, name: str):
        del self._actions[identity][name]
        if not self._actions[identity]:
            del self._actions[identity]

    def get_action(self, identity: str, name: str):
        # Should this reference the task manager?
        # Or should this just reference the Ledger and let the historian handle the task manager?
        # I think it should handle the tasks also
        return self._actions[identity][name]
