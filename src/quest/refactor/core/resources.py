import asyncio
from typing import Any, Callable

from .ledger import Ledger
from .replay_stream import ReplayStream
from .tasks import TaskManager
from .utils import get_function_name


# Two things need to be tracked for actions:
# - is the action recorded in the ledger, and thus replay sequence enforced between separate tasks
# - is the action visible in the workflow surface, allowing external actors to invoke the action

# @action - the method is recorded and sequenced by the ledger; visible=True means the action can be called through the
# workflow API surface.

# @resource - adds the needed __aenter__, __aexit__ methods to register @action methods and State objects with historian



#
# These ^^^ belong a level up, referencing the historian
#


def _create_resource_id(name: str, identity: str | None) -> str:
    return f'{name}|{identity}' if identity is not None else name


class ResourceManager:
    def __init__(self, ledger: Ledger, replay_stream: ReplayStream):
        self._state: dict[str | None, Any] = {}
        self._actions: dict[str | None, Any] = {}
        self._ledger = ledger
        self._replay_stream = replay_stream

    def get_resources(self, identity: str | None):
        """Return a snapshot of state and actions"""
        return {
            **self._actions.get(None, {}),
            **self._state.get(None, {}),
            **self._actions.get(identity, {}),
            **self._state.get(identity, {})
        }

    async def _with_single_record(self, record_type, task_id, identity, name, value):
        if (next_record := await self._replay_stream.get_next_record(task_id)) is None:
            await self._ledger.add_record(
                task_id,
                record_type,
                _create_resource_id(name, identity),
                value
            )
        else:
            with next_record as record:
                assert record['entry_type'] == record_type, str(record)

    async def set_state(self, task_id, identity: str, name: str, value):
        await self._with_single_record('set_state', task_id, identity, name, value)

        if identity not in self._state:
            self._state[identity] = {}
        self._state[identity][name] = value

    async def get_state(self, task_id, identity, name):
        await self._with_single_record('get_state', task_id, identity, name, None)

        return self._state[identity][name]

    async def delete_state(self, task_id, identity: str, name: str):
        await self._with_single_record('del_state', task_id, identity, name, None)
        del self._state[identity][name]

    async def add_action(self, task_id, identity: str, name: str, func: Callable, visible: bool):
        await self._with_single_record('add_action', task_id, identity, name, get_function_name(func))

        if identity not in self._actions:
            self._actions[identity] = {}
        self._actions[identity][name] = func

    async def remove_action(self, task_id, identity: str, name: str):
        await self._with_single_record('remove_action', task_id, identity, name, None)

        del self._actions[identity][name]
        if not self._actions[identity]:
            del self._actions[identity]

    async def invoke_action(self, caller_task_id, identity: str, name: str, *args, **kwargs):
        await self._with_single_record('invoke_action', caller_task_id, identity, name, (args, kwargs))
        await self._actions[identity][name](*args, **kwargs)
        