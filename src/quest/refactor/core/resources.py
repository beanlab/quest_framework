from dataclasses import dataclass
from itertools import chain
from typing import Any, Callable

from .ledger import Ledger
from .replay_stream import ReplayStream
from .utils import get_function_name

# Two kinds of actions:
# 1) Ledger-monitored action on state. For when multiple tasks touch the same state.
# 2) Externally-visible actions. A subset of (1). Displayed in get_resources().

# Why not have state be handled by synchronous getters?
# Because we won't know when the state has changed in order to publish another get_resources().
# We need to store the values ourselves so we can monitor their change.


IDENTITY = str | None
NAME = str


def _create_resource_id(name: NAME, identity: IDENTITY) -> str:
    return f'{name}|{identity}' if identity is not None else name


@dataclass
class Action:
    func: Callable
    external: bool


@dataclass
class State:
    value: Any
    external: bool


@dataclass
class Resources:
    state: list[tuple[IDENTITY, NAME, Any]]
    actions: list[tuple[IDENTITY, NAME]]


class ResourceManager:
    def __init__(self, ledger: Ledger, replay_stream: ReplayStream):
        self._state: dict[IDENTITY, dict[NAME, State]] = {}
        self._actions: dict[IDENTITY, dict[NAME, Action]] = {}
        self._ledger = ledger
        self._replay_stream = replay_stream

    def get_resources(self, identity: IDENTITY) -> Resources:
        """Return a snapshot of external state and actions"""
        state = [
            (ident, name, state)
            for ident, name, state, in chain(
                map(lambda t: (None, *t), self._state.get(None, {}).items()),
                map(lambda t: (identity, *t), self._state.get(identity, {}).items()),
            )
            if state.external
        ]

        actions = [
            (ident, name)
            for ident, name, action, in chain(
                map(lambda t: (None, *t), self._actions.get(None, {}).items()),
                map(lambda t: (identity, *t), self._actions.get(identity, {}).items()),
            )
            if action.external
        ]

        return Resources(state, actions)

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

    async def set_state(self, task_id, identity: str, name: str, value, external: bool):
        await self._with_single_record('set_state', task_id, identity, name, value)

        if identity not in self._state:
            self._state[identity] = {}
        self._state[identity][name] = State(value, external)

    async def get_state(self, task_id, identity, name):
        await self._with_single_record('get_state', task_id, identity, name, None)

        return self._state[identity][name].value

    async def delete_state(self, task_id, identity: str, name: str):
        await self._with_single_record('del_state', task_id, identity, name, None)
        del self._state[identity][name]

    async def add_action(self, task_id, identity: str, name: str, func: Callable, external: bool):
        await self._with_single_record('add_action', task_id, identity, name, get_function_name(func))

        if identity not in self._actions:
            self._actions[identity] = {}
        self._actions[identity][name] = Action(func, external)

    async def remove_action(self, task_id, identity: str, name: str):
        await self._with_single_record('remove_action', task_id, identity, name, None)

        del self._actions[identity][name]
        if not self._actions[identity]:
            del self._actions[identity]

    async def invoke_action(self, caller_task_id, identity: str, name: str, *args, **kwargs):
        await self._with_single_record('invoke_action', caller_task_id, identity, name, (args, kwargs))
        await self._actions[identity][name].func(*args, **kwargs)
